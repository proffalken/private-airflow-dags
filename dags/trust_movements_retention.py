"""
trust_movements_retention
=========================
Enforces a 48-hour rolling retention window on train movement data.

Runs every 6 hours and performs three cleanup steps:
  1. Drop entire monthly partitions of train_movements whose msg_queue_ts
     range is entirely older than the cutoff (fast DDL, no row scan).
  2. Delete individual rows from the current month's partition that are
     older than the cutoff.
  3. Delete processed/ S3 files whose date prefix is older than the cutoff.
  4. Remove trust_loaded_files entries for files that have been purged.
"""

import logging
import re
from datetime import datetime, timedelta, timezone

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

STAGING_BUCKET = "airflow-staging"
S3_PREFIX = "trust-movements"
RETENTION_HOURS = 48


def _cutoff() -> datetime:
    return datetime.now(tz=timezone.utc) - timedelta(hours=RETENTION_HOURS)


@dag(
    dag_id="trust_movements_retention",
    schedule="0 */6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["rail", "retention"],
)
def trust_movements_retention():

    @task
    def prune_db() -> dict:
        """Drop old monthly partitions; delete old rows from the current partition."""
        cutoff = _cutoff()
        pg = PostgresHook(postgres_conn_id="rail_network_db")
        conn = pg.get_conn()

        dropped = []
        deleted_rows = 0

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT tablename FROM pg_tables
                    WHERE tablename ~ '^train_movements_\\d{4}_\\d{2}$'
                    ORDER BY tablename
                """)
                partitions = [row[0] for row in cur.fetchall()]

            for partition in partitions:
                m = re.match(r"train_movements_(\d{4})_(\d{2})$", partition)
                if not m:
                    continue
                year, month = int(m.group(1)), int(m.group(2))
                # First day of the NEXT month = upper bound of partition range
                if month == 12:
                    month_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
                else:
                    month_end = datetime(year, month + 1, 1, tzinfo=timezone.utc)

                with conn.cursor() as cur:
                    if month_end <= cutoff:
                        # Entire partition predates the cutoff — drop it wholesale
                        cur.execute(f"DROP TABLE IF EXISTS {partition}")
                        conn.commit()
                        dropped.append(partition)
                        logger.info("Dropped partition %s", partition)
                    else:
                        # Partial month — delete old rows
                        cur.execute(
                            f"DELETE FROM {partition} WHERE msg_queue_ts < %s",
                            (cutoff,),
                        )
                        deleted_rows += cur.rowcount
                        conn.commit()
                        logger.info(
                            "Deleted %d rows from %s older than %s",
                            cur.rowcount, partition, cutoff.isoformat(),
                        )
        finally:
            conn.close()

        logger.info(
            "DB prune complete: dropped=%s, deleted_rows=%d", dropped, deleted_rows
        )
        return {"dropped_partitions": dropped, "deleted_rows": deleted_rows}

    @task
    def prune_loaded_files() -> int:
        """Remove trust_loaded_files entries for data that no longer exists."""
        cutoff = _cutoff()
        pg = PostgresHook(postgres_conn_id="rail_network_db")
        conn = pg.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM trust_loaded_files WHERE loaded_at < %s", (cutoff,)
                )
                deleted = cur.rowcount
            conn.commit()
        finally:
            conn.close()
        logger.info("Removed %d trust_loaded_files entries", deleted)
        return deleted

    @task
    def prune_s3_processed() -> int:
        """Delete processed/ S3 files whose date prefix is older than 48 hours."""
        cutoff = _cutoff()
        s3 = S3Hook(aws_conn_id="garage_s3").get_conn()

        to_delete = []
        paginator = s3.get_paginator("list_objects_v2")
        date_re = re.compile(r"date=(\d{4}-\d{2}-\d{2})/hour=(\d{2})/")

        for page in paginator.paginate(
            Bucket=STAGING_BUCKET, Prefix=f"processed/{S3_PREFIX}/"
        ):
            for obj in page.get("Contents", []):
                m = date_re.search(obj["Key"])
                if not m:
                    continue
                file_hour = datetime.strptime(
                    f"{m.group(1)} {m.group(2)}", "%Y-%m-%d %H"
                ).replace(tzinfo=timezone.utc)
                if file_hour < cutoff:
                    to_delete.append(obj["Key"])

        # Batch-delete in chunks of 1000 (S3 API limit)
        deleted = 0
        for i in range(0, len(to_delete), 1000):
            batch = to_delete[i : i + 1000]
            s3.delete_objects(
                Bucket=STAGING_BUCKET,
                Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
            )
            deleted += len(batch)

        logger.info("Deleted %d processed S3 files older than %s", deleted, cutoff.isoformat())
        return deleted

    db_result = prune_db()
    prune_loaded_files()
    prune_s3_processed()


trust_movements_retention()
