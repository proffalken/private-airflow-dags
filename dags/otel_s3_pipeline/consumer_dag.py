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
        from airflow_otel._context import extract_airflow_context
        from airflow_otel._instrumentation import get_tracer, setup_otel, shutdown_otel
        from airflow_otel._propagation import push_current_context
        from opentelemetry import propagate
        from opentelemetry.trace import SpanKind, StatusCode
        from otel_s3_pipeline.s3_otel import _s3_client

        ctx_attrs = extract_airflow_context(context)
        dag_id = ctx_attrs.get("dag_id", "s3_otel_consumer")
        task_id = ctx_attrs.get("task_id", "download_from_s3")
        run_id = ctx_attrs.get("run_id", "unknown")
        try_number = int(ctx_attrs.get("try_number", 1))

        conf = context["dag_run"].conf or {}
        bucket = conf.get("s3_bucket") or Variable.get("S3_BUCKET")
        key = conf["s3_key"]

        # Fetch S3 object and extract the producer's traceparent BEFORE calling
        # setup_otel, so we can use it as the parent for our root task span.
        # instrument_task_context can't do this because it reads parent context
        # from within-DAG XCom only, not from S3 metadata.
        response = _s3_client().get_object(Bucket=bucket, Key=key)
        metadata = response.get("Metadata", {})
        body_bytes = response["Body"].read()
        upstream_ctx = propagate.extract(metadata)

        setup_otel(dag_id, task_id, run_id, try_number)
        tracer = get_tracer(__name__)

        try:
            with tracer.start_as_current_span(
                f"{dag_id}.{task_id}",
                context=upstream_ctx,     # parent = producer's s3.put_object span
                kind=SpanKind.CONSUMER,
                attributes={
                    "airflow.dag_id": dag_id,
                    "airflow.task_id": task_id,
                    "airflow.run_id": run_id,
                    "airflow.try_number": try_number,
                    "messaging.system": "aws_s3",
                    "messaging.source.name": bucket,
                    "messaging.s3.key": key,
                },
            ) as span:
                # Push traceparent to XCom so process_payload connects via
                # instrument_task_context's extract_upstream_context.
                push_current_context(ctx_attrs)
                span.set_status(StatusCode.OK)
                log.info(
                    "Downloaded %d bytes from s3://%s/%s", len(body_bytes), bucket, key
                )
                return json.loads(body_bytes)
        except Exception as exc:
            span.set_status(StatusCode.ERROR, str(exc))
            raise
        finally:
            shutdown_otel()

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
