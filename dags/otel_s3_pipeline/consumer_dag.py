"""S3 OTEL Consumer DAG.

Reads a payload from Garage S3, extracting the W3C traceparent from the
object's metadata and attaching it as a span link.  In the trace detail view
you will see a navigable "linked trace" button pointing at the producer run.
In the service map, both DAGs appear connected through the S3 (aws_s3) node.

This DAG is designed to be triggered by s3_otel_producer via the trigger task,
which passes the bucket and key in dag_run.conf.  It can also be triggered
manually with the same conf structure for testing:

    {"s3_bucket": "my-bucket", "s3_key": "otel-pipeline/2025-01-01/..."}

Optional Airflow Variables:
  OTEL_SERVICE_NAME   — override service name (default: "s3-otel-consumer")
"""
from __future__ import annotations

import json
import logging

import pendulum
from airflow.sdk import Variable, dag, task

log = logging.getLogger(__name__)


@dag(
    dag_id="s3_otel_consumer",
    schedule=None,  # triggered by s3_otel_producer
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["otel", "s3", "example"],
    doc_md=__doc__,
)
def s3_otel_consumer():

    @task
    def download_from_s3(**context) -> dict:
        from otel_s3_pipeline.s3_otel import init_otel, s3_get_with_link

        conf = context["dag_run"].conf or {}
        bucket = conf.get("s3_bucket") or Variable.get("S3_BUCKET")
        key = conf["s3_key"]

        service_name = Variable.get("OTEL_SERVICE_NAME", default="s3-otel-consumer")
        init_otel(service_name)

        raw = s3_get_with_link(bucket, key)
        log.info("Downloaded %d bytes from s3://%s/%s", len(raw), bucket, key)
        return json.loads(raw)

    @task
    def process_payload(payload: dict) -> None:
        log.info(
            "Processing payload: run_id=%s logical_date=%s value=%s",
            payload.get("run_id"),
            payload.get("logical_date"),
            payload.get("value"),
        )
        # Replace this task with real processing logic.

    process_payload(download_from_s3())


s3_otel_consumer()
