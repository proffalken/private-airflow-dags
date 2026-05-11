"""S3-based OTEL context propagation utilities.

Injects W3C traceparent into S3 object metadata on write (PRODUCER span),
and extracts it as a span link on read (CONSUMER span).

DAG tasks must wrap their body with instrument_task_context (or the
@instrument_task decorator) from airflow_otel BEFORE calling these helpers.
That sets service.name = task_id and service.namespace = dag_id on the
resource, so each task appears as a distinct node in the Dash0 service map,
and calls shutdown_otel() on exit to force-flush spans before the process ends.

Required env vars (set in k8s/airflow-values-otel.yaml):
  OTEL_EXPORTER_OTLP_ENDPOINT — HTTP OTLP endpoint

Optional env vars:
  GARAGE_S3_ENDPOINT  — defaults to https://s3.wallace.network
  GARAGE_S3_CONN_ID   — Airflow connection ID (default: garage_s3)
"""
from __future__ import annotations

import logging
import os

import boto3
from opentelemetry import propagate, trace

log = logging.getLogger(__name__)

GARAGE_ENDPOINT = os.getenv("GARAGE_S3_ENDPOINT", "https://s3.wallace.network")
GARAGE_S3_CONN_ID = os.getenv("GARAGE_S3_CONN_ID", "garage_s3")


def _get_tracer() -> trace.Tracer:
    """Return the active tracer for this module.

    Prefers airflow_otel.get_tracer() which reads from the module-level
    _tracer_provider installed by instrument_task_context / setup_otel.
    This bypasses Airflow's set-once global TracerProvider and ensures
    our PRODUCER/CONSUMER spans carry the correct service.name.
    Falls back to the OTel global for use outside Airflow (tests, CLI).
    """
    try:
        from airflow_otel import get_tracer
        return get_tracer(__name__)
    except ImportError:
        return trace.get_tracer(__name__)


def _s3_client():
    # Inside Airflow workers the credentials live in the 'garage_s3' connection,
    # which also carries the endpoint_url in its extra JSON field.  AwsBaseHook
    # wires all of that into the boto3 client automatically.
    # Outside Airflow (local dev / tests) fall back to the standard boto3
    # credential chain with the endpoint set via env var.
    try:
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
        hook = AwsBaseHook(aws_conn_id=GARAGE_S3_CONN_ID, client_type="s3")
        return hook.get_client_type()
    except ImportError:
        return boto3.client("s3", endpoint_url=GARAGE_ENDPOINT)


def s3_put_with_context(bucket: str, key: str, body: bytes, **put_kwargs) -> None:
    """Upload *body* to S3, embedding the active trace context in object metadata.

    Creates a PRODUCER span so the service map shows an edge from this task's
    service to the S3 bucket node.  Call this inside an instrument_task_context
    block so the span inherits the correct service.name resource attribute.
    """
    tracer = _get_tracer()
    with tracer.start_as_current_span(
        "s3.put_object",
        kind=trace.SpanKind.PRODUCER,
        attributes={
            "messaging.system": "aws_s3",
            "messaging.destination.name": bucket,
            "messaging.s3.key": key,
        },
    ):
        # Inject inside the span so the carrier contains *this* PRODUCER span's
        # traceparent — the consumer will link directly to it.
        carrier: dict[str, str] = {}
        propagate.inject(carrier)
        _s3_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            Metadata=carrier,
            **put_kwargs,
        )
        span_ctx = trace.get_current_span().get_span_context()
        log.info(
            "s3_put_with_context: s3://%s/%s  trace_id=%032x  carrier_keys=%s",
            bucket, key, span_ctx.trace_id, list(carrier),
        )


def s3_get_with_context(bucket: str, key: str) -> bytes:
    """Download from S3, continuing the producer's trace as a CONSUMER child span.

    Extracts the W3C traceparent from the object's metadata and uses it as the
    parent context for the CONSUMER span.  Both DAG runs share the same trace ID,
    which gives Dash0 the connected path it needs to render a straight service map
    edge:  upload_to_s3 → aws_s3 → download_from_s3

    If no traceparent is present in the metadata (e.g. the object was uploaded
    without OTEL instrumentation) the CONSUMER span starts a new root trace.
    """
    response = _s3_client().get_object(Bucket=bucket, Key=key)
    metadata = response.get("Metadata", {})

    upstream_ctx = propagate.extract(metadata)
    upstream_span_ctx = trace.get_current_span(upstream_ctx).get_span_context()

    if upstream_span_ctx.is_valid:
        log.info(
            "s3_get_with_context: continuing trace %032x from s3://%s/%s",
            upstream_span_ctx.trace_id, bucket, key,
        )
    else:
        log.warning(
            "s3_get_with_context: no valid traceparent in metadata for s3://%s/%s — "
            "starting a new root span",
            bucket, key,
        )

    tracer = _get_tracer()
    with tracer.start_as_current_span(
        "s3.get_object",
        context=upstream_ctx,        # parent-child: same trace ID as producer
        kind=trace.SpanKind.CONSUMER,
        attributes={
            "messaging.system": "aws_s3",
            "messaging.source.name": bucket,
            "messaging.s3.key": key,
        },
    ):
        body = response["Body"].read()
        span_ctx = trace.get_current_span().get_span_context()
        log.info(
            "s3_get_with_context: s3://%s/%s  trace_id=%032x",
            bucket, key, span_ctx.trace_id,
        )
        return body
