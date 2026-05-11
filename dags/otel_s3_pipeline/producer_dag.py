"""S3 OTEL Producer DAG.

Generates a payload, uploads it to Garage S3 with a W3C traceparent injected
into the object metadata, then triggers the consumer DAG with the S3 key so
it can pick up exactly the right object.

Required Airflow Variables (Admin → Variables):
  S3_BUCKET           — target bucket name in Garage

Optional Airflow Variables:
  OTEL_SERVICE_NAME   — override service name (default: "s3-otel-producer")
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
        return {
            "run_id": context["run_id"],
            "logical_date": context["logical_date"].isoformat(),
            "value": 42,
        }

    @task
    def upload_to_s3(payload: dict, **context) -> str:
        from otel_s3_pipeline.s3_otel import init_otel, s3_put_with_context

        service_name = Variable.get("OTEL_SERVICE_NAME", default="s3-otel-producer")
        bucket = Variable.get("S3_BUCKET")
        key = S3_KEY_TEMPLATE.format(
            ds=context["ds"],
            run_id=context["run_id"],
        )

        init_otel(service_name)
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
        # conf is a template_field; ti.xcom_pull pulls the key emitted above
        conf={
            "s3_bucket": "{{ var.value.S3_BUCKET }}",
            "s3_key": "{{ ti.xcom_pull(task_ids='upload_to_s3') }}",
        },
    )
    key >> trigger


s3_otel_producer()
