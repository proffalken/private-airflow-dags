"""TRUST Movements Loader DAG
============================
Hourly pipeline that picks up Parquet files written by the trust-consumer
k8s Deployment from Garage S3 and loads them into the rail_network PostgreSQL
database.

Data flow:
  S3 (trust-movements/date=.../hour=.../) ──► ensure_schema
                                          ──► load_hour ──► mark_processed

Airflow Connection required:
  garage_s3       — S3-compatible connection for Garage
  rail_network_db — PostgreSQL connection (rail_network DB)

S3 bucket: airflow-staging
Partitioning: one Postgres partition per calendar month, created on demand.
Idempotency: processed files are recorded in trust_loaded_files; reloading a
             file that is already in that table is a no-op.
"""
from __future__ import annotations

import io
import logging
from datetime import datetime, timezone
from typing import Any

import pyarrow.parquet as pq
import pendulum

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import chain, dag, get_current_context, task
from airflow_otel import instrument_task_context, get_meter

logger = logging.getLogger("airflow.trust_movements_loader")

STAGING_BUCKET = "airflow-staging"
S3_PREFIX = "trust-movements"
PROCESSED_PREFIX = "processed/trust-movements"

# ---------------------------------------------------------------------------
# Schema SQL
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS train_movements (
    id                   BIGSERIAL,
    msg_type             CHAR(4)      NOT NULL,
    train_id             VARCHAR(16),
    train_uid            VARCHAR(6),
    toc_id               CHAR(2),
    event_type           VARCHAR(16),
    loc_stanox           VARCHAR(5),
    actual_ts            TIMESTAMPTZ,
    planned_ts           TIMESTAMPTZ,
    timetable_variation  SMALLINT,
    variation_status     VARCHAR(16),
    platform             VARCHAR(3),
    line_ind             VARCHAR(1),
    offroute_ind         BOOLEAN,
    train_terminated     BOOLEAN,
    canx_reason_code     VARCHAR(4),
    canx_type            VARCHAR(32),
    dep_ts               TIMESTAMPTZ,
    origin_dep_ts        TIMESTAMPTZ,
    schedule_start_date  DATE,
    schedule_end_date    DATE,
    msg_queue_ts         TIMESTAMPTZ  NOT NULL,
    raw_json             TEXT,
    loaded_at            TIMESTAMPTZ  DEFAULT NOW()
) PARTITION BY RANGE (msg_queue_ts);

CREATE TABLE IF NOT EXISTS trust_loaded_files (
    s3_key     TEXT        PRIMARY KEY,
    loaded_at  TIMESTAMPTZ DEFAULT NOW(),
    row_count  INTEGER
);
"""

_INSERT_SQL = """
INSERT INTO train_movements (
    msg_type, train_id, train_uid, toc_id, event_type, loc_stanox,
    actual_ts, planned_ts, timetable_variation, variation_status,
    platform, line_ind, offroute_ind, train_terminated,
    canx_reason_code, canx_type, dep_ts, origin_dep_ts,
    schedule_start_date, schedule_end_date, msg_queue_ts, raw_json
) VALUES (
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s
)
"""


# ---------------------------------------------------------------------------
# Pure helper functions (tested directly in tests/test_trust_loader.py)
# ---------------------------------------------------------------------------

def _epoch_ms_to_dt(val: int | None) -> datetime | None:
    """Convert an epoch-millisecond integer to a timezone-aware datetime."""
    if val is None:
        return None
    return datetime.fromtimestamp(val / 1000, tz=timezone.utc)


def _list_pending_keys(s3_client, bucket: str, prefix: str, dt: datetime) -> list[str]:
    """Return S3 keys in the given hour's partition that have not yet been processed.

    Excludes keys that already exist under the processed/ prefix.
    """
    date_str = dt.strftime("%Y-%m-%d")
    hour_str = dt.strftime("%H")
    original_prefix = f"{prefix}/date={date_str}/hour={hour_str}/"
    processed_prefix = f"processed/{prefix}/date={date_str}/hour={hour_str}/"

    def _keys(pfx: str) -> set[str]:
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=pfx)
        return {obj["Key"] for obj in resp.get("Contents", [])}

    original = _keys(original_prefix)
    already_processed = {
        k.replace("processed/", "", 1) for k in _keys(processed_prefix)
    }
    return sorted(original - already_processed)


def _ensure_partition(conn, year: int, month: int) -> None:
    """Create a monthly partition of train_movements if it does not exist.

    Partition name: train_movements_YYYY_MM
    Range: [YYYY-MM-01, next month)
    """
    start = f"{year}-{month:02d}-01"
    if month == 12:
        end = f"{year + 1}-01-01"
    else:
        end = f"{year}-{month + 1:02d}-01"
    table_name = f"train_movements_{year}_{month:02d}"

    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            PARTITION OF train_movements
            FOR VALUES FROM (%s) TO (%s)
            """,
            (start, end),
        )
    conn.commit()


def _is_file_loaded(conn, s3_key: str) -> bool:
    """Return True if this S3 key has already been recorded in trust_loaded_files."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT s3_key FROM trust_loaded_files WHERE s3_key = %s",
            (s3_key,),
        )
        return cur.fetchone() is not None


def _mark_file_loaded(conn, s3_key: str, row_count: int) -> None:
    """Record a successfully loaded S3 file in trust_loaded_files."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO trust_loaded_files (s3_key, row_count) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (s3_key, row_count),
        )
    conn.commit()


def _load_parquet_to_db(conn, parquet_bytes: bytes) -> int:
    """Read Parquet bytes and insert all rows into train_movements.

    Returns the number of rows inserted.
    """
    table = pq.read_table(io.BytesIO(parquet_bytes))
    rows = table.to_pydict()
    n = table.num_rows

    tuples = []
    for i in range(n):
        tuples.append((
            rows["msg_type"][i],
            rows["train_id"][i],
            rows["train_uid"][i],
            rows["toc_id"][i],
            rows["event_type"][i],
            rows["loc_stanox"][i],
            _epoch_ms_to_dt(rows["actual_ts"][i]),        # index 6
            _epoch_ms_to_dt(rows["planned_ts"][i]),
            rows["timetable_variation"][i],
            rows["variation_status"][i],
            rows["platform"][i],
            rows["line_ind"][i],
            rows["offroute_ind"][i],
            rows["train_terminated"][i],
            rows["canx_reason_code"][i],
            rows["canx_type"][i],
            _epoch_ms_to_dt(rows["dep_ts"][i]),
            _epoch_ms_to_dt(rows["origin_dep_ts"][i]),
            rows["schedule_start_date"][i],
            rows["schedule_end_date"][i],
            _epoch_ms_to_dt(rows["msg_queue_ts"][i]),
            rows["raw_json"][i],
        ))

    with conn.cursor() as cur:
        cur.executemany(_INSERT_SQL, tuples)
    conn.commit()
    return n


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="trust_movements_loader",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["rail", "trust", "nrod"],
    doc_md=__doc__,
)
def trust_movements_loader():

    @task
    def ensure_schema() -> None:
        """Create the train_movements partitioned table and tracking table if absent."""
        with instrument_task_context({"nrod.feed": "TRUST", "task": "ensure_schema"}):
            pg = PostgresHook(postgres_conn_id="rail_network_db")
            conn = pg.get_conn()
            with conn.cursor() as cur:
                for stmt in SCHEMA_SQL.split(";"):
                    s = stmt.strip()
                    if s:
                        cur.execute(s)
            conn.commit()
            conn.close()
            logger.info("Schema verified")

    @task
    def load_hour() -> dict:
        """Load the previous hour's Parquet files from S3 into Postgres."""
        ctx = get_current_context()
        # data_interval_start is the beginning of the window we should process
        interval_start: datetime = ctx["data_interval_start"]

        with instrument_task_context({
            "nrod.feed": "TRUST",
            "task": "load_hour",
            "interval_start": interval_start.isoformat(),
        }):
            meter = get_meter("trust.loader")
            rows_counter = meter.create_counter(
                "trust.loader.rows_loaded", unit="1",
                description="Rows loaded into train_movements",
            )
            files_counter = meter.create_counter(
                "trust.loader.files_loaded", unit="1",
                description="Parquet files loaded from S3",
            )

            s3_hook = S3Hook(aws_conn_id="garage_s3")
            s3 = s3_hook.get_conn()

            pg = PostgresHook(postgres_conn_id="rail_network_db")
            conn = pg.get_conn()

            # Ensure the partition for this month exists before inserting
            _ensure_partition(conn, interval_start.year, interval_start.month)

            keys = _list_pending_keys(s3, STAGING_BUCKET, S3_PREFIX, interval_start)
            logger.info("Found %d pending files for %s", len(keys), interval_start.isoformat())

            total_rows = 0
            total_files = 0

            for key in keys:
                if _is_file_loaded(conn, key):
                    logger.info("Skipping already-loaded file: %s", key)
                    continue

                obj = s3.get_object(Bucket=STAGING_BUCKET, Key=key)
                parquet_bytes = obj["Body"].read()

                row_count = _load_parquet_to_db(conn, parquet_bytes)
                _mark_file_loaded(conn, key, row_count)

                # Move to processed/ prefix (copy + delete)
                processed_key = f"processed/{key}"
                s3.copy_object(
                    Bucket=STAGING_BUCKET,
                    CopySource={"Bucket": STAGING_BUCKET, "Key": key},
                    Key=processed_key,
                )
                s3.delete_object(Bucket=STAGING_BUCKET, Key=key)

                total_rows += row_count
                total_files += 1
                logger.info("Loaded %d rows from %s", row_count, key)

            rows_counter.add(total_rows)
            files_counter.add(total_files)
            conn.close()

            return {"files_loaded": total_files, "rows_loaded": total_rows}

    chain(ensure_schema(), load_hour())


trust_movements_loader()
