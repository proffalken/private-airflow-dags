"""S3-based OTEL context propagation utilities.

Injects W3C traceparent into S3 object metadata on write (PRODUCER span),
and extracts it as a span link on read (CONSUMER span).  This allows two
DAGs that communicate via S3 to appear connected in service maps and trace
detail views without a direct HTTP call between them.

Required env vars (set in airflow-values-otel.yaml):
  OTEL_EXPORTER_OTLP_ENDPOINT — HTTP OTLP endpoint, e.g.
      http://dash0-operator-opentelemetry-collector-service.dash0-system.svc.cluster.local:4318

Optional env vars:
  GARAGE_S3_ENDPOINT — defaults to https://s3.wallace.network
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY — S3 credentials (or use IAM)
"""
from __future__ import annotations

import logging
import os

import boto3
from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.trace import ProxyTracerProvider
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

log = logging.getLogger(__name__)

GARAGE_ENDPOINT = os.getenv("GARAGE_S3_ENDPOINT", "https://s3.wallace.network")
GARAGE_S3_CONN_ID = os.getenv("GARAGE_S3_CONN_ID", "garage_s3")

# Module-level flag: each Airflow task worker runs in its own process, so this
# resets to False for every new task — exactly the behaviour we want.
_initialized = False


def init_otel(service_name: str) -> None:
    """Set up the OTEL SDK for this process.  Safe to call more than once."""
    global _initialized
    if _initialized:
        return

    current = trace.get_tracer_provider()
    if isinstance(current, ProxyTracerProvider):
        # No real provider yet — install our own.  SimpleSpanProcessor exports
        # synchronously, which is essential in short-lived Airflow task processes
        # where BatchSpanProcessor may not flush its queue before the process exits.
        provider = TracerProvider(
            resource=Resource.create({"service.name": service_name})
        )
        provider.add_span_processor(SimpleSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(provider)
        log.info("OTEL TracerProvider installed for service '%s'", service_name)
    else:
        # Airflow (or another framework) already installed a TracerProvider —
        # the set-once guard prevents replacing it.  Spans will be emitted via
        # the existing provider; service.name will reflect its resource, not ours.
        log.warning(
            "OTEL TracerProvider already active (%s); cannot set service.name='%s'. "
            "Spans will appear under the existing provider's service name in Dash0.",
            type(current).__name__,
            service_name,
        )

    propagate.set_global_textmap(TraceContextTextMapPropagator())
    _initialized = True


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

    Creates a PRODUCER span so the service map shows an edge from this service
    to the S3 bucket node.  The injected 'traceparent' metadata value is what
    s3_get_with_link() reads on the consumer side.
    """
    tracer = trace.get_tracer(__name__)
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
        # traceparent — the consumer will link directly to it rather than its parent.
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


def s3_get_with_link(bucket: str, key: str) -> bytes:
    """Download from S3, linking this trace back to the producer's trace.

    Creates a CONSUMER span with a span link pointing at the producer's span
    context (read from the object's 'traceparent' metadata).  This gives you:

    • Service map: edge from S3 bucket node to this service
    • Trace detail: a navigable "linked trace" button to jump to the producer
    """
    response = _s3_client().get_object(Bucket=bucket, Key=key)
    metadata = response.get("Metadata", {})

    upstream_ctx = propagate.extract(metadata)
    upstream_span_ctx = trace.get_current_span(upstream_ctx).get_span_context()

    links: list[trace.Link] = []
    if upstream_span_ctx.is_valid:
        links = [trace.Link(upstream_span_ctx, attributes={"messaging.s3.key": key})]
        log.info(
            "s3_get_with_link: linked to upstream trace %016x", upstream_span_ctx.trace_id
        )
    else:
        log.warning(
            "s3_get_with_link: no valid traceparent in metadata for s3://%s/%s — "
            "consumer span will have no upstream link",
            bucket,
            key,
        )

    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span(
        "s3.get_object",
        kind=trace.SpanKind.CONSUMER,
        links=links,
        attributes={
            "messaging.system": "aws_s3",
            "messaging.source.name": bucket,
            "messaging.s3.key": key,
        },
    ):
        body = response["Body"].read()
        span_ctx = trace.get_current_span().get_span_context()
        log.info(
            "s3_get_with_link: s3://%s/%s  trace_id=%032x  linked=%s",
            bucket, key, span_ctx.trace_id, bool(links),
        )
        return body
