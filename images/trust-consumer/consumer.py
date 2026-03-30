"""TRUST STOMP Consumer
======================
Subscribes to the Network Rail Open Data (NROD) TRAIN_MVT_ALL_TOC STOMP topic,
buffers incoming messages in memory, and flushes to Garage S3 as Parquet files
on a configurable interval (default: hourly).

S3 key layout:
  {S3_PREFIX}/date=YYYY-MM-DD/hour=HH/batch-NNN.parquet

The flush interval and all connection parameters are controlled via environment
variables so the same image can be used in any environment without rebuilding.

Environment variables:
  NROD_HOST            STOMP broker host (default: datafeeds.networkrail.co.uk)
  NROD_PORT            STOMP broker port (default: 61618)
  NROD_USERNAME        NROD portal username  [required]
  NROD_PASSWORD        NROD portal password  [required]
  S3_ENDPOINT_URL      Garage/S3 endpoint URL  [required]
  AWS_ACCESS_KEY_ID    S3 access key  [required]
  AWS_SECRET_ACCESS_KEY S3 secret key  [required]
  S3_BUCKET            Target bucket (default: airflow-staging)
  S3_PREFIX            Key prefix    (default: trust-movements)
  FLUSH_INTERVAL_SECS  Seconds between S3 flushes (default: 3600)
  OTEL_EXPORTER_OTLP_ENDPOINT  OTel collector endpoint (optional)
  OTEL_SERVICE_NAME    OTel service name (default: trust-consumer)
"""
from __future__ import annotations

import io
import json
import logging
import os
import signal
import sys
import time  # still used in main loop sleep and HEALTHCHECK
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import stomp

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("trust_consumer")

# ---------------------------------------------------------------------------
# OTel setup — configured entirely via env vars; no hard-coded endpoints
# ---------------------------------------------------------------------------

def _setup_otel() -> None:
    """Initialise OTel tracing and metrics if an exporter endpoint is set."""
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        logger.info("OTEL_EXPORTER_OTLP_ENDPOINT not set — OTel disabled")
        return
    try:
        from opentelemetry import trace, metrics
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource, SERVICE_NAME
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

        resource = Resource({SERVICE_NAME: os.getenv("OTEL_SERVICE_NAME", "trust-consumer")})

        tracer_provider = TracerProvider(resource=resource)
        tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(tracer_provider)

        reader = PeriodicExportingMetricReader(OTLPMetricExporter(), export_interval_millis=60_000)
        meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
        metrics.set_meter_provider(meter_provider)

        logger.info("OTel initialised, exporting to %s", endpoint)
    except Exception as exc:  # noqa: BLE001
        logger.warning("OTel setup failed (non-fatal): %s", exc)


def _get_tracer():
    try:
        from opentelemetry import trace
        return trace.get_tracer("trust_consumer")
    except Exception:  # noqa: BLE001
        return None


def _get_meter():
    try:
        from opentelemetry import metrics
        return metrics.get_meter("trust_consumer")
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# Parquet schema
# ---------------------------------------------------------------------------

SCHEMA = pa.schema([
    pa.field("msg_type", pa.string()),
    pa.field("train_id", pa.string()),
    pa.field("train_uid", pa.string()),
    pa.field("toc_id", pa.string()),
    pa.field("event_type", pa.string()),           # 0003: DEPARTURE/ARRIVAL/PASS
    pa.field("loc_stanox", pa.string()),            # 0003
    pa.field("actual_ts", pa.int64()),              # 0003 epoch ms
    pa.field("planned_ts", pa.int64()),             # 0003 epoch ms
    pa.field("timetable_variation", pa.int32()),    # 0003
    pa.field("variation_status", pa.string()),      # 0003, 0002
    pa.field("platform", pa.string()),              # 0003
    pa.field("line_ind", pa.string()),              # 0003
    pa.field("offroute_ind", pa.bool_()),           # 0003
    pa.field("train_terminated", pa.bool_()),       # 0003
    pa.field("canx_reason_code", pa.string()),      # 0002
    pa.field("canx_type", pa.string()),             # 0002
    pa.field("dep_ts", pa.int64()),                 # 0002 epoch ms
    pa.field("origin_dep_ts", pa.int64()),          # 0001 epoch ms
    pa.field("schedule_start_date", pa.string()),   # 0001
    pa.field("schedule_end_date", pa.string()),     # 0001
    pa.field("msg_queue_ts", pa.int64()),           # all, from header
    pa.field("raw_json", pa.string()),              # full message for recovery
])


# ---------------------------------------------------------------------------
# Pure functions (testable without STOMP or S3)
# ---------------------------------------------------------------------------

def _ts(val: Any) -> int | None:
    """Convert an epoch-millisecond string/int to int, or None if blank."""
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _bool(val: Any) -> bool | None:
    """Convert 'true'/'false' string (or bool) to Python bool, or None."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() == "true"


def _int(val: Any) -> int | None:
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def parse_messages(raw: str) -> list[dict]:
    """Parse a STOMP frame body — a JSON array of TRUST message objects.

    Returns an empty list on any parse error or if the payload is not a
    non-empty JSON array, so the caller never needs to handle exceptions.
    """
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return []
    if not isinstance(parsed, list) or not parsed:
        return []
    return parsed


def message_to_row(msg: dict) -> dict:
    """Flatten a single TRUST message dict into a Parquet-ready row dict.

    Fields that do not apply to a given message type are set to None so
    that all rows share the same schema regardless of msg_type.
    """
    header = msg.get("header", {})
    body = msg.get("body", {})

    return {
        "msg_type": header.get("msg_type"),
        "train_id": body.get("train_id"),
        "train_uid": body.get("train_uid"),
        "toc_id": body.get("toc_id"),
        # Movement fields (0003)
        "event_type": body.get("event_type") or None,
        "loc_stanox": body.get("loc_stanox") or None,
        "actual_ts": _ts(body.get("actual_timestamp")),
        "planned_ts": _ts(body.get("planned_timestamp")),
        "timetable_variation": _int(body.get("timetable_variation")),
        "variation_status": body.get("variation_status") or None,
        "platform": (body.get("platform") or "").strip() or None,
        "line_ind": body.get("line_ind") or None,
        "offroute_ind": _bool(body.get("offroute_ind")),
        "train_terminated": _bool(body.get("train_terminated")),
        # Cancellation fields (0002)
        "canx_reason_code": body.get("canx_reason_code") or None,
        "canx_type": body.get("canx_type") or None,
        "dep_ts": _ts(body.get("dep_timestamp")),
        # Activation fields (0001)
        "origin_dep_ts": _ts(body.get("origin_dep_timestamp")),
        "schedule_start_date": body.get("schedule_start_date") or None,
        "schedule_end_date": body.get("schedule_end_date") or None,
        # Header fields (all types)
        "msg_queue_ts": _ts(header.get("msg_queue_timestamp")),
        "raw_json": json.dumps(msg),
    }


def rows_to_parquet(rows: list[dict]) -> bytes:
    """Serialise a list of row dicts to Parquet bytes using the fixed schema.

    Returns b"" if the row list is empty.
    """
    if not rows:
        return b""

    columns: dict[str, list] = {f.name: [] for f in SCHEMA}
    for row in rows:
        for field in SCHEMA:
            columns[field.name].append(row.get(field.name))

    arrays = {
        name: pa.array(vals, type=SCHEMA.field(name).type)
        for name, vals in columns.items()
    }
    table = pa.table(arrays, schema=SCHEMA)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def s3_key(prefix: str, dt: datetime, batch: int) -> str:
    """Return the S3 object key for a given flush datetime and batch number.

    Format: {prefix}/date=YYYY-MM-DD/hour=HH/batch-NNN.parquet
    """
    return (
        f"{prefix}/"
        f"date={dt.strftime('%Y-%m-%d')}/"
        f"hour={dt.strftime('%H')}/"
        f"batch-{batch:03d}.parquet"
    )


# ---------------------------------------------------------------------------
# TrustConsumer
# ---------------------------------------------------------------------------

class TrustConsumer(stomp.ConnectionListener):
    """STOMP listener that buffers TRUST messages and flushes to S3 as Parquet."""

    def __init__(
        self,
        s3_client,
        bucket: str,
        prefix: str,
    ) -> None:
        self._s3 = s3_client
        self._bucket = bucket
        self._prefix = prefix
        self._buffer: list[dict] = []
        # Track the hour we started buffering so flushes always align to the
        # hour boundary and the S3 key reflects the hour the data belongs to.
        self._buffer_hour = datetime.now(tz=timezone.utc).replace(
            minute=0, second=0, microsecond=0
        )
        self._batch = 0

        # OTel instruments (None if OTel is not configured)
        meter = _get_meter()
        self._messages_received = (
            meter.create_counter("trust.messages_received", unit="1",
                                 description="TRUST messages received from STOMP")
            if meter else None
        )
        self._flushes_total = (
            meter.create_counter("trust.flushes_total", unit="1",
                                 description="S3 flushes performed")
            if meter else None
        )
        self._rows_flushed = (
            meter.create_counter("trust.rows_flushed", unit="1",
                                 description="Rows written to S3 Parquet")
            if meter else None
        )

    # -- stomp.ConnectionListener callbacks ----------------------------------

    def on_message(self, frame) -> None:
        messages = parse_messages(frame.body)
        if not messages:
            return
        rows = [message_to_row(m) for m in messages]
        self._buffer.extend(rows)
        if self._messages_received:
            self._messages_received.add(len(rows))
        self.flush()

    def on_error(self, frame) -> None:
        logger.error("STOMP error: %s", frame.body)

    def on_disconnected(self) -> None:
        logger.warning("STOMP disconnected — will attempt reconnect")

    # -- Flush ---------------------------------------------------------------

    def flush(self, force: bool = False) -> None:
        """Upload the buffer to S3 at the top of each hour (or immediately if forced).

        The S3 key uses _buffer_hour so data collected during e.g. the 19:xx
        hour is always written to hour=19, regardless of when the flush runs.
        After a successful flush, _buffer_hour advances to the current hour so
        the next flush picks up the new hour's data.
        """
        if not self._buffer:
            return
        now_hour = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
        if not force and now_hour <= self._buffer_hour:
            return

        self._batch += 1
        key = s3_key(self._prefix, self._buffer_hour, self._batch)
        data = rows_to_parquet(self._buffer)
        row_count = len(self._buffer)

        tracer = _get_tracer()
        span_ctx = tracer.start_as_current_span("trust.s3_flush") if tracer else nullcontext()
        try:
            with span_ctx:
                self._s3.put_object(Bucket=self._bucket, Key=key, Body=data)
                logger.info("Flushed %d rows to s3://%s/%s", row_count, self._bucket, key)
                if self._flushes_total:
                    self._flushes_total.add(1)
                if self._rows_flushed:
                    self._rows_flushed.add(row_count)
        except Exception:
            logger.exception("Failed to flush to S3 — retaining buffer for next attempt")
            self._batch -= 1
            return

        self._buffer = []
        self._buffer_hour = now_hour


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _make_s3_client():
    endpoint = os.environ["S3_ENDPOINT_URL"]
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-1",  # Garage ignores region but boto3 requires one
    )


def main() -> None:
    _setup_otel()

    nrod_host = os.getenv("NROD_HOST", "datafeeds.networkrail.co.uk")
    nrod_port = int(os.getenv("NROD_PORT", "61618"))
    username = os.environ["NROD_USERNAME"]
    password = os.environ["NROD_PASSWORD"]
    bucket = os.getenv("S3_BUCKET", "airflow-staging")
    prefix = os.getenv("S3_PREFIX", "trust-movements")
    flush_interval = int(os.getenv("FLUSH_INTERVAL_SECS", "3600"))

    s3 = _make_s3_client()
    listener = TrustConsumer(s3, bucket, prefix)

    conn = stomp.Connection([(nrod_host, nrod_port)], heartbeats=(10000, 10000))
    conn.set_listener("", listener)

    # Graceful shutdown on SIGTERM / SIGINT
    def _shutdown(signum, frame):
        logger.info("Shutting down — flushing remaining buffer")
        listener.flush(force=True)
        conn.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    logger.info("Connecting to NROD STOMP at %s:%d", nrod_host, nrod_port)
    conn.connect(username, password, wait=True)
    conn.subscribe(destination="/topic/TRAIN_MVT_ALL_TOC", id=1, ack="auto")
    logger.info("Subscribed to TRAIN_MVT_ALL_TOC — listening")

    try:
        while True:
            time.sleep(30)
            listener.flush()
    except Exception:
        logger.exception("Unexpected error in main loop")
        listener.flush(force=True)
        conn.disconnect()
        sys.exit(1)


if __name__ == "__main__":
    main()
