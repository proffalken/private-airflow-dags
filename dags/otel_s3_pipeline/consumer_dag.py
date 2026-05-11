"""S3 OTEL Consumer DAG.

Reads a payload from Garage S3, extracting the W3C traceparent from the
object's metadata and attaching it as a span link.

Each task is wrapped with instrument_task_context so it appears as its own
named service in the Dash0 service map:
  service.name      = task_id   (e.g. "download_from_s3")
  service.namespace = dag_id    (e.g. "s3_otel_consumer")

The service map will show:
  upload_to_s3 → aws_s3 → download_from_s3

Triggered by s3_otel_producer via TriggerDagRunOperator.  Can also be
triggered manually with conf:
  {"s3_bucket": "airflow-staging", "s3_key": "otel-pipeline/2025-01-01/..."}
"""
from __future__ import annotations

import json
import logging

import pendulum
from airflow.sdk import Variable, dag, task

log = logging.getLogger(__name__)


@dag(
    dag_id="s3_otel_consumer",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["otel", "s3", "example"],
    doc_md=__doc__,
)
def s3_otel_consumer():

    @task
    def download_from_s3(**context) -> dict:
        from airflow_otel import instrument_task_context
        from otel_s3_pipeline.s3_otel import s3_get_with_context

        conf = context["dag_run"].conf or {}
        bucket = conf.get("s3_bucket") or Variable.get("S3_BUCKET")
        key = conf["s3_key"]

        with instrument_task_context({}):
            raw = s3_get_with_context(bucket, key)

        log.info("Downloaded %d bytes from s3://%s/%s", len(raw), bucket, key)
        return json.loads(raw)

    @task
    def process_payload(payload: dict) -> None:
        from airflow_otel import instrument_task_context
        with instrument_task_context({}):
            log.info(
                "Processing payload: run_id=%s logical_date=%s value=%s",
                payload.get("run_id"),
                payload.get("logical_date"),
                payload.get("value"),
            )
            # Replace with real processing logic.

    process_payload(download_from_s3())


s3_otel_consumer()
