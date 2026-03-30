"""TRUST Movements Aggregate DAG
================================
Daily pipeline that:
  1. Computes on-time / delay / cancellation summary statistics for the
     previous day and upserts them into trust_daily_summary.
  2. Drops monthly train_movements partitions that are older than the
     configured retention window (default: 90 days).

Airflow Connection required:
  rail_network_db — PostgreSQL connection (rail_network DB)
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import pendulum

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import chain, dag, get_current_context, task
from airflow.traces import otel_tracer
from airflow.traces.tracer import Trace
from airflow_otel import instrument_task_context, get_meter

logger = logging.getLogger("airflow.trust_movements_aggregate")

RETENTION_DAYS = 90

SUMMARY_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS trust_daily_summary (
    summary_date     DATE        NOT NULL,
    toc_id           CHAR(2)     NOT NULL,
    on_time_count    INTEGER     NOT NULL DEFAULT 0,
    late_count       INTEGER     NOT NULL DEFAULT 0,
    early_count      INTEGER     NOT NULL DEFAULT 0,
    cancelled_count  INTEGER     NOT NULL DEFAULT 0,
    total_movements  INTEGER     NOT NULL DEFAULT 0,
    avg_variation    NUMERIC(6,2),
    PRIMARY KEY (summary_date, toc_id)
);
"""


# ---------------------------------------------------------------------------
# Pure helper functions (tested directly in tests/test_trust_loader.py)
# ---------------------------------------------------------------------------

def _drop_old_partitions(conn, retention_days: int, today: date) -> list[str]:
    """Identify and drop monthly train_movements partitions older than retention_days.

    A partition named train_movements_YYYY_MM is considered old when its last
    day (the last day of that month) is more than retention_days before today.
    Returns the list of table names that were dropped.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename ~ '^train_movements_\\d{4}_\\d{2}$'
            ORDER BY tablename
            """
        )
        partitions = [row[0] for row in cur.fetchall()]

    cutoff = today - timedelta(days=retention_days)
    dropped = []

    for table in partitions:
        # Parse year and month from the table name
        parts = table.split("_")  # ['train', 'movements', 'YYYY', 'MM']
        year, month = int(parts[-2]), int(parts[-1])
        # The partition covers up to the first day of the next month
        if month == 12:
            partition_end = date(year + 1, 1, 1)
        else:
            partition_end = date(year, month + 1, 1)
        # If the entire partition is before the cutoff, drop it
        if partition_end <= cutoff:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table}")  # noqa: S608
            conn.commit()
            dropped.append(table)
            logger.info("Dropped old partition: %s", table)

    return dropped


def _compute_daily_summary(conn, summary_date: date) -> None:
    """Compute per-TOC performance statistics for summary_date and upsert results.

    Counts movement events (msg_type=0003) by variation_status and cancellations
    (msg_type=0002) separately, then upserts into trust_daily_summary.
    """
    # Step 1: movement counts by toc_id + variation_status
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT toc_id, variation_status, COUNT(*) AS cnt
            FROM train_movements
            WHERE msg_type = '0003'
              AND actual_ts >= %s
              AND actual_ts < %s
              AND toc_id IS NOT NULL
            GROUP BY toc_id, variation_status
            """,
            (summary_date, summary_date + timedelta(days=1)),
        )
        movement_rows = cur.fetchall()

    # Step 2: cancellation counts by toc_id
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT toc_id, COUNT(*) AS cnt
            FROM train_movements
            WHERE msg_type = '0002'
              AND msg_queue_ts >= %s
              AND msg_queue_ts < %s
              AND toc_id IS NOT NULL
            GROUP BY toc_id
            """,
            (summary_date, summary_date + timedelta(days=1)),
        )
        canx_rows = cur.fetchall()

    # Step 3: average timetable variation per toc_id (movements only)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT toc_id, AVG(timetable_variation)
            FROM train_movements
            WHERE msg_type = '0003'
              AND actual_ts >= %s
              AND actual_ts < %s
              AND toc_id IS NOT NULL
              AND timetable_variation IS NOT NULL
            GROUP BY toc_id
            """,
            (summary_date, summary_date + timedelta(days=1)),
        )
        avg_rows = {row[0]: row[1] for row in cur.fetchall()}

    # Aggregate into per-TOC buckets
    toc_stats: dict[str, dict] = {}

    for toc_id, status, cnt in movement_rows:
        s = toc_stats.setdefault(toc_id, {
            "on_time": 0, "late": 0, "early": 0, "cancelled": 0, "total": 0
        })
        status_upper = (status or "").upper()
        if status_upper == "ON TIME":
            s["on_time"] += cnt
        elif status_upper == "LATE":
            s["late"] += cnt
        elif status_upper == "EARLY":
            s["early"] += cnt
        s["total"] += cnt

    for toc_id, cnt in canx_rows:
        s = toc_stats.setdefault(toc_id, {
            "on_time": 0, "late": 0, "early": 0, "cancelled": 0, "total": 0
        })
        s["cancelled"] += cnt

    # Upsert
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO trust_daily_summary
                (summary_date, toc_id, on_time_count, late_count, early_count,
                 cancelled_count, total_movements, avg_variation)
            VALUES %s
            ON CONFLICT (summary_date, toc_id) DO UPDATE SET
                on_time_count   = EXCLUDED.on_time_count,
                late_count      = EXCLUDED.late_count,
                early_count     = EXCLUDED.early_count,
                cancelled_count = EXCLUDED.cancelled_count,
                total_movements = EXCLUDED.total_movements,
                avg_variation   = EXCLUDED.avg_variation
            """.replace(
                "VALUES %s",
                "VALUES " + ",".join(["(%s,%s,%s,%s,%s,%s,%s,%s)"] * len(toc_stats))
                if toc_stats else "VALUES (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL) WHERE false",
            ),
            [
                val
                for toc_id, s in toc_stats.items()
                for val in (
                    summary_date, toc_id,
                    s["on_time"], s["late"], s["early"], s["cancelled"],
                    s["total"], avg_rows.get(toc_id),
                )
            ] if toc_stats else [],
        )
    conn.commit()
    logger.info("Upserted summary for %s: %d TOCs", summary_date, len(toc_stats))


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="trust_movements_aggregate",
    schedule="0 2 * * *",   # 02:00 UTC daily — previous day's data is complete
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["rail", "trust", "nrod"],
    doc_md=__doc__,
)
def trust_movements_aggregate():

    @task
    def ensure_summary_schema() -> None:
        otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
        with instrument_task_context({"nrod.feed": "TRUST", "task": "ensure_summary_schema"}):
            pg = PostgresHook(postgres_conn_id="rail_network_db")
            conn = pg.get_conn()
            with otel_task_tracer.start_child_span(span_name="trust.ensure_summary_schema") as span:
                span.set_attribute("nrod.feed", "TRUST")
                with conn.cursor() as cur:
                    for stmt in SUMMARY_SCHEMA_SQL.split(";"):
                        s = stmt.strip()
                        if s:
                            cur.execute(s)
                conn.commit()
                conn.close()

    @task
    def compute_daily_aggregates() -> dict:
        """Aggregate yesterday's movements into trust_daily_summary."""
        ctx = get_current_context()
        yesterday: date = ctx["data_interval_start"].date()
        otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)

        with instrument_task_context({
            "nrod.feed": "TRUST",
            "task": "compute_daily_aggregates",
            "summary_date": str(yesterday),
        }):
            meter = get_meter("trust.aggregate")
            meter.create_gauge(
                "trust.aggregate.tocs_summarised", unit="1",
                description="Number of TOCs with data in latest summary run",
            )

            pg = PostgresHook(postgres_conn_id="rail_network_db")
            conn = pg.get_conn()
            with otel_task_tracer.start_child_span(span_name="trust.compute_daily_summary") as span:
                span.set_attribute("trust.summary_date", str(yesterday))
                _compute_daily_summary(conn, yesterday)
            conn.close()

            logger.info("Daily aggregate complete for %s", yesterday)
            return {"summary_date": str(yesterday)}

    @task
    def enforce_retention() -> dict:
        """Drop monthly partitions older than RETENTION_DAYS."""
        otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
        with instrument_task_context({
            "nrod.feed": "TRUST",
            "task": "enforce_retention",
            "retention_days": RETENTION_DAYS,
        }):
            pg = PostgresHook(postgres_conn_id="rail_network_db")
            conn = pg.get_conn()
            with otel_task_tracer.start_child_span(span_name="trust.enforce_retention") as span:
                span.set_attribute("trust.retention_days", RETENTION_DAYS)
                dropped = _drop_old_partitions(conn, RETENTION_DAYS, today=date.today())
                span.set_attribute("trust.partitions_dropped", len(dropped))
            conn.close()
            logger.info("Dropped %d old partitions: %s", len(dropped), dropped)
            return {"dropped_partitions": dropped}

    chain(ensure_summary_schema(), compute_daily_aggregates(), enforce_retention())


trust_movements_aggregate()
