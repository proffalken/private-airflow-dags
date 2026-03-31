"""Unit tests for the TRUST STOMP consumer.

Tests are written first (TDD) and cover:
  - STOMP frame parsing
  - Per-message row extraction for all major TRUST message types
  - Parquet serialisation
  - S3 key generation
  - TrustConsumer buffering, flushing, and batch counter behaviour
"""
from __future__ import annotations

import io
import json
import sys
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "images", "trust-consumer"))
import consumer as c  # noqa: E402  (import after sys.path modification)


# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------

MOVEMENT_MSG = {
    "header": {
        "msg_type": "0003",
        "source_dev_id": "",
        "user_id": "",
        "original_data_source": "SMART",
        "msg_queue_timestamp": "1705320000000",
        "source_system_id": "TRUST",
    },
    "body": {
        "event_type": "DEPARTURE",
        "planned_timestamp": "1705320000000",
        "actual_timestamp": "1705320060000",
        "timetable_variation": "1",
        "variation_status": "LATE",
        "train_id": "225Y05MX06",
        "train_uid": "P72253",
        "toc_id": "88",
        "loc_stanox": "73300",
        "platform": " 2",
        "line_ind": "",
        "offroute_ind": "false",
        "train_terminated": "false",
        "next_report_stanox": "73530",
        "next_report_run_time": "3",
    },
}

CANCELLATION_MSG = {
    "header": {
        "msg_type": "0002",
        "msg_queue_timestamp": "1705320000000",
        "source_system_id": "TRUST",
    },
    "body": {
        "train_id": "225Y05MX07",
        "train_uid": "P72254",
        "toc_id": "88",
        "loc_stanox": "73300",
        "canx_timestamp": "1705320000000",
        "canx_reason_code": "YI",
        "canx_type": "EN ROUTE",
        "dep_timestamp": "1705319000000",
        "variation_status": "LATE",
    },
}

ACTIVATION_MSG = {
    "header": {
        "msg_type": "0001",
        "msg_queue_timestamp": "1705310000000",
        "source_system_id": "TRUST",
    },
    "body": {
        "train_id": "225Y05MX06",
        "train_uid": "P72253",
        "toc_id": "88",
        "origin_dep_timestamp": "1705310000000",
        "schedule_start_date": "2024-01-15",
        "schedule_end_date": "2024-01-15",
    },
}


# ---------------------------------------------------------------------------
# parse_messages
# ---------------------------------------------------------------------------

class TestParseMessages:
    def test_single_message(self):
        raw = json.dumps([MOVEMENT_MSG])
        result = c.parse_messages(raw)
        assert len(result) == 1
        assert result[0]["header"]["msg_type"] == "0003"

    def test_multiple_messages_in_one_frame(self):
        raw = json.dumps([MOVEMENT_MSG, CANCELLATION_MSG])
        result = c.parse_messages(raw)
        assert len(result) == 2

    def test_invalid_json_returns_empty(self):
        assert c.parse_messages("not json") == []

    def test_empty_array_returns_empty(self):
        assert c.parse_messages("[]") == []

    def test_non_array_json_returns_empty(self):
        assert c.parse_messages('{"key": "value"}') == []


# ---------------------------------------------------------------------------
# message_to_row
# ---------------------------------------------------------------------------

class TestMessageToRow:
    def test_movement_core_fields(self):
        row = c.message_to_row(MOVEMENT_MSG)
        assert row["msg_type"] == "0003"
        assert row["train_id"] == "225Y05MX06"
        assert row["train_uid"] == "P72253"
        assert row["toc_id"] == "88"
        assert row["event_type"] == "DEPARTURE"
        assert row["loc_stanox"] == "73300"
        assert row["variation_status"] == "LATE"

    def test_movement_timetable_variation_is_int(self):
        row = c.message_to_row(MOVEMENT_MSG)
        assert row["timetable_variation"] == 1

    def test_movement_bool_fields_converted(self):
        row = c.message_to_row(MOVEMENT_MSG)
        assert row["offroute_ind"] is False
        assert row["train_terminated"] is False

    def test_movement_timestamps_epoch_ms(self):
        row = c.message_to_row(MOVEMENT_MSG)
        assert row["actual_ts"] == 1705320060000
        assert row["planned_ts"] == 1705320000000
        assert row["msg_queue_ts"] == 1705320000000

    def test_cancellation_fields(self):
        row = c.message_to_row(CANCELLATION_MSG)
        assert row["msg_type"] == "0002"
        assert row["canx_reason_code"] == "YI"
        assert row["canx_type"] == "EN ROUTE"
        assert row["dep_ts"] == 1705319000000
        assert row["event_type"] is None

    def test_activation_fields(self):
        row = c.message_to_row(ACTIVATION_MSG)
        assert row["msg_type"] == "0001"
        assert row["origin_dep_ts"] == 1705310000000
        assert row["schedule_start_date"] == "2024-01-15"
        assert row["schedule_end_date"] == "2024-01-15"

    def test_movement_cancellation_fields_are_none(self):
        row = c.message_to_row(MOVEMENT_MSG)
        assert row["canx_reason_code"] is None
        assert row["canx_type"] is None
        assert row["dep_ts"] is None

    def test_raw_json_is_round_trippable(self):
        row = c.message_to_row(MOVEMENT_MSG)
        parsed = json.loads(row["raw_json"])
        assert parsed["header"]["msg_type"] == "0003"

    def test_empty_string_timestamps_become_none(self):
        msg = {
            "header": {"msg_type": "0003", "msg_queue_timestamp": "1705320000000"},
            "body": {
                "train_id": "X",
                "actual_timestamp": "1705320060000",
                "planned_timestamp": "",   # empty → None
                "timetable_variation": "0",
                "variation_status": "ON TIME",
                "offroute_ind": "false",
                "train_terminated": "false",
            },
        }
        row = c.message_to_row(msg)
        assert row["planned_ts"] is None


# ---------------------------------------------------------------------------
# rows_to_parquet
# ---------------------------------------------------------------------------

class TestRowsToParquet:
    def test_valid_parquet_output(self):
        rows = [c.message_to_row(MOVEMENT_MSG), c.message_to_row(CANCELLATION_MSG)]
        data = c.rows_to_parquet(rows)
        table = pq.read_table(io.BytesIO(data))
        assert table.num_rows == 2

    def test_expected_columns_present(self):
        rows = [c.message_to_row(MOVEMENT_MSG)]
        data = c.rows_to_parquet(rows)
        table = pq.read_table(io.BytesIO(data))
        for col in ("msg_type", "train_id", "actual_ts", "raw_json", "msg_queue_ts"):
            assert col in table.column_names, f"missing column: {col}"

    def test_empty_rows_returns_empty_bytes(self):
        assert c.rows_to_parquet([]) == b""


# ---------------------------------------------------------------------------
# s3_key
# ---------------------------------------------------------------------------

class TestS3Key:
    def test_key_format(self):
        dt = datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
        key = c.s3_key("trust-movements", dt, 3)
        assert key == "trust-movements/date=2024-01-15/hour=14/batch-003.parquet"

    def test_zero_padded_hour(self):
        dt = datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
        key = c.s3_key("trust-movements", dt, 1)
        assert key == "trust-movements/date=2024-01-15/hour=09/batch-001.parquet"

    def test_zero_padded_batch(self):
        dt = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        key = c.s3_key("trust-movements", dt, 42)
        assert key == "trust-movements/date=2024-06-01/hour=00/batch-042.parquet"


# ---------------------------------------------------------------------------
# TrustConsumer
# ---------------------------------------------------------------------------

class TestTrustConsumer:
    def _make_consumer(self):
        s3 = MagicMock()
        consumer = c.TrustConsumer(
            s3_client=s3,
            bucket="test-bucket",
            prefix="trust-movements",
        )
        return consumer, s3

    def _make_frame(self, *msgs):
        frame = MagicMock()
        frame.body = json.dumps(list(msgs))
        return frame

    def test_on_message_buffers_single_row(self):
        consumer, _ = self._make_consumer()
        consumer.on_message(self._make_frame(MOVEMENT_MSG))
        assert len(consumer._buffer) == 1

    def test_on_message_buffers_multiple_rows_from_one_frame(self):
        consumer, _ = self._make_consumer()
        consumer.on_message(self._make_frame(MOVEMENT_MSG, CANCELLATION_MSG))
        assert len(consumer._buffer) == 2

    def test_on_message_ignores_invalid_frames(self):
        consumer, _ = self._make_consumer()
        frame = MagicMock()
        frame.body = "not json"
        consumer.on_message(frame)
        assert len(consumer._buffer) == 0

    def test_force_flush_uploads_to_s3(self):
        consumer, s3 = self._make_consumer()
        consumer.on_message(self._make_frame(MOVEMENT_MSG))
        consumer.flush(force=True)
        s3.put_object.assert_called_once()

    def test_force_flush_uses_correct_bucket_and_prefix(self):
        consumer, s3 = self._make_consumer()
        consumer.on_message(self._make_frame(MOVEMENT_MSG))
        consumer.flush(force=True)
        kwargs = s3.put_object.call_args.kwargs
        assert kwargs["Bucket"] == "test-bucket"
        assert kwargs["Key"].startswith("trust-movements/")

    def test_force_flush_clears_buffer(self):
        consumer, _ = self._make_consumer()
        consumer.on_message(self._make_frame(MOVEMENT_MSG))
        consumer.flush(force=True)
        assert len(consumer._buffer) == 0

    def test_flush_noop_when_buffer_empty(self):
        consumer, s3 = self._make_consumer()
        consumer.flush(force=True)
        s3.put_object.assert_not_called()

    def test_flush_noop_within_same_hour(self):
        """flush() without force=True is a no-op when the hour hasn't changed."""
        consumer, s3 = self._make_consumer()
        consumer.on_message(self._make_frame(MOVEMENT_MSG))
        # datetime.now returns the same hour as _buffer_hour → no flush
        with patch("consumer.datetime") as mock_dt:
            mock_dt.now.return_value = consumer._buffer_hour.replace(minute=30)
            consumer.flush(force=False)
        s3.put_object.assert_not_called()
        assert len(consumer._buffer) == 1

    def test_batch_counter_increments_across_flushes(self):
        consumer, s3 = self._make_consumer()
        consumer.on_message(self._make_frame(MOVEMENT_MSG))
        consumer.flush(force=True)
        consumer.on_message(self._make_frame(CANCELLATION_MSG))
        consumer.flush(force=True)
        keys = [call.kwargs["Key"] for call in s3.put_object.call_args_list]
        assert "batch-001" in keys[0]
        assert "batch-002" in keys[1]

    def test_flush_uploads_valid_parquet(self):
        consumer, s3 = self._make_consumer()
        consumer.on_message(self._make_frame(MOVEMENT_MSG))
        consumer.flush(force=True)
        body = s3.put_object.call_args.kwargs["Body"]
        table = pq.read_table(io.BytesIO(body))
        assert table.num_rows == 1

    def test_on_message_records_last_message_time(self):
        consumer, _ = self._make_consumer()
        assert consumer._last_message_time is None
        consumer.on_message(self._make_frame(MOVEMENT_MSG))
        assert consumer._last_message_time is not None

    def test_emit_metrics_sets_buffer_size(self):
        consumer, _ = self._make_consumer()
        gauge = MagicMock()
        consumer._buffer_size_gauge = gauge
        consumer.on_message(self._make_frame(MOVEMENT_MSG, CANCELLATION_MSG))
        consumer.emit_metrics(connected=True)
        gauge.set.assert_called_with(2)

    def test_emit_metrics_sets_connected_1_when_up(self):
        consumer, _ = self._make_consumer()
        gauge = MagicMock()
        consumer._connected_gauge = gauge
        consumer.emit_metrics(connected=True)
        gauge.set.assert_called_with(1)

    def test_emit_metrics_sets_connected_0_when_down(self):
        consumer, _ = self._make_consumer()
        gauge = MagicMock()
        consumer._connected_gauge = gauge
        consumer.emit_metrics(connected=False)
        gauge.set.assert_called_with(0)

    def test_emit_metrics_last_message_age_negative_before_first_message(self):
        consumer, _ = self._make_consumer()
        gauge = MagicMock()
        consumer._last_message_age_gauge = gauge
        consumer.emit_metrics(connected=True)
        gauge.set.assert_called_with(-1)

    def test_emit_metrics_last_message_age_positive_after_message(self):
        consumer, _ = self._make_consumer()
        gauge = MagicMock()
        consumer._last_message_age_gauge = gauge
        consumer.on_message(self._make_frame(MOVEMENT_MSG))
        consumer.emit_metrics(connected=True)
        age = gauge.set.call_args[0][0]
        assert age >= 0
