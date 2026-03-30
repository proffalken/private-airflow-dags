"""Unit tests for TRUST movements loader DAG logic.

Tests cover the pure helper functions extracted from the Airflow @task
wrappers so they can be exercised without a running Airflow instance.
"""
from __future__ import annotations

import io
import json
import sys
import os
from datetime import datetime, date, timezone
from unittest.mock import MagicMock, call, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ---------------------------------------------------------------------------
# Stub out Airflow and airflow_otel before importing DAG modules so tests
# run without a full Airflow installation.
# ---------------------------------------------------------------------------
for _mod in [
    "pendulum",
    "airflow", "airflow.sdk", "airflow.providers",
    "airflow.providers.amazon", "airflow.providers.amazon.aws",
    "airflow.providers.amazon.aws.hooks", "airflow.providers.amazon.aws.hooks.s3",
    "airflow.providers.postgres", "airflow.providers.postgres.hooks",
    "airflow.providers.postgres.hooks.postgres",
    "airflow_otel",
]:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

# pendulum needs datetime-like objects; keep the real one
import pendulum as _pendulum_stub  # noqa: F401 — already a MagicMock above

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))
import trust_movements_loader as loader  # noqa: E402
import trust_movements_aggregate as agg  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers: build a minimal Parquet file in memory
# ---------------------------------------------------------------------------

def _make_parquet(rows: list[dict]) -> bytes:
    """Build a Parquet file from a list of flat row dicts."""
    if not rows:
        return b""
    schema = pa.schema([
        pa.field("msg_type", pa.string()),
        pa.field("train_id", pa.string()),
        pa.field("train_uid", pa.string()),
        pa.field("toc_id", pa.string()),
        pa.field("event_type", pa.string()),
        pa.field("loc_stanox", pa.string()),
        pa.field("actual_ts", pa.int64()),
        pa.field("planned_ts", pa.int64()),
        pa.field("timetable_variation", pa.int32()),
        pa.field("variation_status", pa.string()),
        pa.field("platform", pa.string()),
        pa.field("line_ind", pa.string()),
        pa.field("offroute_ind", pa.bool_()),
        pa.field("train_terminated", pa.bool_()),
        pa.field("canx_reason_code", pa.string()),
        pa.field("canx_type", pa.string()),
        pa.field("dep_ts", pa.int64()),
        pa.field("origin_dep_ts", pa.int64()),
        pa.field("schedule_start_date", pa.string()),
        pa.field("schedule_end_date", pa.string()),
        pa.field("msg_queue_ts", pa.int64()),
        pa.field("raw_json", pa.string()),
    ])
    arrays = {f.name: [] for f in schema}
    for row in rows:
        for f in schema:
            arrays[f.name].append(row.get(f.name))
    table = pa.table({k: pa.array(v, type=schema.field(k).type) for k, v in arrays.items()})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


SAMPLE_ROW = {
    "msg_type": "0003",
    "train_id": "225Y05MX06",
    "train_uid": "P72253",
    "toc_id": "88",
    "event_type": "DEPARTURE",
    "loc_stanox": "73300",
    "actual_ts": 1705320060000,
    "planned_ts": 1705320000000,
    "timetable_variation": 1,
    "variation_status": "LATE",
    "platform": "2",
    "line_ind": None,
    "offroute_ind": False,
    "train_terminated": False,
    "canx_reason_code": None,
    "canx_type": None,
    "dep_ts": None,
    "origin_dep_ts": None,
    "schedule_start_date": None,
    "schedule_end_date": None,
    "msg_queue_ts": 1705320000000,
    "raw_json": '{"header":{"msg_type":"0003"},"body":{}}',
}

CANCELLED_ROW = {
    **SAMPLE_ROW,
    "msg_type": "0002",
    "event_type": None,
    "variation_status": "LATE",
    "canx_reason_code": "YI",
    "canx_type": "EN ROUTE",
    "actual_ts": None,
    "planned_ts": None,
    "dep_ts": 1705319000000,
}


# ---------------------------------------------------------------------------
# _list_pending_keys
# ---------------------------------------------------------------------------

class TestListPendingKeys:
    def test_returns_keys_for_given_hour(self):
        s3 = MagicMock()
        # First call: original prefix; second call: processed prefix (empty)
        s3.list_objects_v2.side_effect = [
            {"Contents": [
                {"Key": "trust-movements/date=2024-01-15/hour=14/batch-001.parquet"},
                {"Key": "trust-movements/date=2024-01-15/hour=14/batch-002.parquet"},
            ]},
            {},  # no files under processed/ yet
        ]
        dt = datetime(2024, 1, 15, 14, 0, tzinfo=timezone.utc)
        keys = loader._list_pending_keys(s3, "my-bucket", "trust-movements", dt)
        assert len(keys) == 2
        assert all("hour=14" in k for k in keys)

    def test_returns_empty_when_no_objects(self):
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {}
        dt = datetime(2024, 1, 15, 14, 0, tzinfo=timezone.utc)
        keys = loader._list_pending_keys(s3, "my-bucket", "trust-movements", dt)
        assert keys == []

    def test_excludes_already_processed_keys(self):
        s3 = MagicMock()
        s3.list_objects_v2.side_effect = [
            # original prefix
            {"Contents": [
                {"Key": "trust-movements/date=2024-01-15/hour=14/batch-001.parquet"},
                {"Key": "trust-movements/date=2024-01-15/hour=14/batch-002.parquet"},
            ]},
            # processed prefix
            {"Contents": [
                {"Key": "processed/trust-movements/date=2024-01-15/hour=14/batch-001.parquet"},
            ]},
        ]
        dt = datetime(2024, 1, 15, 14, 0, tzinfo=timezone.utc)
        keys = loader._list_pending_keys(s3, "my-bucket", "trust-movements", dt)
        # batch-001 already processed, only batch-002 returned
        assert len(keys) == 1
        assert "batch-002" in keys[0]


# ---------------------------------------------------------------------------
# _ensure_partition
# ---------------------------------------------------------------------------

class TestEnsurePartition:
    def test_creates_partition_if_not_exists(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value.__enter__ = lambda s: cur
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        loader._ensure_partition(conn, 2024, 1)
        sql = cur.execute.call_args[0][0]
        assert "train_movements_2024_01" in sql
        assert "PARTITION OF" in sql

    def test_uses_correct_date_range(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value.__enter__ = lambda s: cur
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        loader._ensure_partition(conn, 2024, 12)
        # Dates are passed as %s parameters, not embedded in the SQL string
        _, args = cur.execute.call_args[0]
        assert "2024-12-01" in args
        assert "2025-01-01" in args

    def test_december_partition_wraps_year(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value.__enter__ = lambda s: cur
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        loader._ensure_partition(conn, 2024, 12)
        _, args = cur.execute.call_args[0]
        assert "2025-01-01" in args


# ---------------------------------------------------------------------------
# _load_parquet_to_db
# ---------------------------------------------------------------------------

class TestLoadParquetToDb:
    def test_returns_row_count(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value.__enter__ = lambda s: cur
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        parquet_bytes = _make_parquet([SAMPLE_ROW, CANCELLED_ROW])
        count = loader._load_parquet_to_db(conn, parquet_bytes)
        assert count == 2

    def test_calls_execute_with_rows(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value.__enter__ = lambda s: cur
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        parquet_bytes = _make_parquet([SAMPLE_ROW])
        loader._load_parquet_to_db(conn, parquet_bytes)
        assert cur.execute.called or cur.executemany.called

    def test_timestamps_converted_from_epoch_ms(self):
        """Rows inserted into DB should have datetime objects, not raw ints."""
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value.__enter__ = lambda s: cur
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        parquet_bytes = _make_parquet([SAMPLE_ROW])
        loader._load_parquet_to_db(conn, parquet_bytes)
        # Find the args passed to executemany / execute
        insert_args = None
        if cur.executemany.called:
            insert_args = cur.executemany.call_args[0][1]
        elif cur.execute.called:
            insert_args = cur.execute.call_args[0][1]
        assert insert_args is not None
        # First row, actual_ts should be a datetime
        row = list(insert_args)[0]
        # actual_ts is index 6 in our INSERT column order
        actual_ts_val = row[6]
        assert isinstance(actual_ts_val, datetime), f"expected datetime, got {type(actual_ts_val)}"


# ---------------------------------------------------------------------------
# _is_file_loaded / _mark_file_loaded
# ---------------------------------------------------------------------------

class TestFileTracking:
    def _conn(self, fetchone_result=None):
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.return_value = fetchone_result
        conn.cursor.return_value.__enter__ = lambda s: cur
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return conn, cur

    def test_is_file_loaded_returns_true_when_found(self):
        conn, cur = self._conn(fetchone_result=("trust-movements/...",))
        assert loader._is_file_loaded(conn, "trust-movements/some-key") is True

    def test_is_file_loaded_returns_false_when_not_found(self):
        conn, cur = self._conn(fetchone_result=None)
        assert loader._is_file_loaded(conn, "trust-movements/some-key") is False

    def test_mark_file_loaded_inserts_record(self):
        conn, cur = self._conn()
        loader._mark_file_loaded(conn, "trust-movements/key.parquet", 500)
        assert cur.execute.called
        sql = cur.execute.call_args[0][0]
        assert "trust_loaded_files" in sql


# ---------------------------------------------------------------------------
# _drop_old_partitions (aggregate DAG)
# ---------------------------------------------------------------------------

class TestDropOldPartitions:
    def test_drops_partitions_older_than_retention(self):
        conn = MagicMock()
        cur = MagicMock()
        # today=2024-03-15, retention=90d → cutoff=2023-12-15
        # 2023-09 ends 2023-10-01 < cutoff → DROP
        # 2023-10 ends 2023-11-01 < cutoff → DROP
        # 2024-01 ends 2024-02-01 > cutoff → KEEP
        cur.fetchall.return_value = [
            ("train_movements_2023_09",),
            ("train_movements_2023_10",),
            ("train_movements_2024_01",),
        ]
        conn.cursor.return_value.__enter__ = lambda s: cur
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        today = date(2024, 3, 15)
        dropped = agg._drop_old_partitions(conn, retention_days=90, today=today)
        assert "train_movements_2023_09" in dropped
        assert "train_movements_2023_10" in dropped
        assert "train_movements_2024_01" not in dropped

    def test_returns_empty_list_when_nothing_to_drop(self):
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchall.return_value = [("train_movements_2024_01",)]
        conn.cursor.return_value.__enter__ = lambda s: cur
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        today = date(2024, 1, 15)
        dropped = agg._drop_old_partitions(conn, retention_days=90, today=today)
        assert dropped == []


# ---------------------------------------------------------------------------
# _compute_daily_summary
# ---------------------------------------------------------------------------

class TestComputeDailySummary:
    def test_calls_insert_on_conn(self):
        conn = MagicMock()
        cur = MagicMock()
        # fetchall is called three times: movements, cancellations, avg_variation
        cur.fetchall.side_effect = [
            [("88", "ON TIME", 120), ("88", "LATE", 30), ("88", "EARLY", 5)],  # movements
            [("88", 2)],   # cancellations (2-tuple)
            [("88", 1.2)], # avg variation
        ]
        conn.cursor.return_value.__enter__ = lambda s: cur
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        agg._compute_daily_summary(conn, date(2024, 1, 14))
        assert cur.execute.called
