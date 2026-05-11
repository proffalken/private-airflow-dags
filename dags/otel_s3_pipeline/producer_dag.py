"""S3 OTEL Producer DAG.

Generates a payload, uploads it to Garage S3 with a W3C traceparent injected
into the object metadata, then triggers the consumer DAG with the S3 key.

Each task is wrapped with instrument_task_context so it appears as its own
named service in the Dash0 service map:
  service.name      = task_id   (e.g. "upload_to_s3")
  service.namespace = dag_id    (e.g. "s3_otel_producer")

Required Airflow Variables (Admin → Variables):
  S3_BUCKET  — target bucket name in Garage (use "airflow-staging")
"""
from __future__ import annotations

import json
import logging

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable, dag, task

log = logging.getLogger(__name__)

S3_KEY_TEMPLATE = "otel-pipeline/{ds}/{run_id}/payload.json"


@dag(
    dag_id="s3_otel_producer",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["otel", "s3", "example"],
    doc_md=__doc__,
)
def s3_otel_producer():

    @task
    def generate_payload(**context) -> dict:
        from airflow_otel import instrument_task_context
        with instrument_task_context({}):
            return {
                "run_id": context["run_id"],
                "logical_date": context["logical_date"].isoformat(),
                "value": 42,
            }

    @task
    def upload_to_s3(payload: dict, **context) -> str:
        from airflow_otel import instrument_task_context
        from otel_s3_pipeline.s3_otel import s3_put_with_context

        bucket = Variable.get("S3_BUCKET")
        key = S3_KEY_TEMPLATE.format(ds=context["ds"], run_id=context["run_id"])

        with instrument_task_context({}):
            s3_put_with_context(
                bucket,
                key,
                json.dumps(payload).encode(),
                ContentType="application/json",
            )
        log.info("Uploaded payload to s3://%s/%s", bucket, key)
        return key

    payload = generate_payload()
    key = upload_to_s3(payload)

    trigger = TriggerDagRunOperator(
        task_id="trigger_consumer",
        trigger_dag_id="s3_otel_consumer",
        wait_for_completion=False,
        conf={
            "s3_bucket": "{{ var.value.S3_BUCKET }}",
            "s3_key": "{{ ti.xcom_pull(task_ids='upload_to_s3') }}",
        },
    )
    key >> trigger


s3_otel_producer()
