"""Shared OpenTelemetry utilities for all Airflow DAGs.

Provides:
- create_task_provider / create_meter_provider with correct resource attributes
- resolve_parent_context for W3C trace propagation across tasks
- task_root_span context manager (CONSUMER root + PRODUCER handoff)
- instrument_requests (idempotent, fixes the _REQUESTS_INSTRUMENTED=True bug)
- get_trace_context for structured log correlation
- _parse_llm_json for robust LLM response parsing
"""
from __future__ import annotations

import json
import logging
import os
import re
from contextlib import contextmanager
from typing import Iterator, Union

from opentelemetry import trace
from opentelemetry.propagate import inject, extract as otel_extract
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.trace import SpanKind

_log = logging.getLogger("airflow.otel_utils")

# Logical grouping for all DAGs in this project
_SERVICE_NAMESPACE = "social-archive"

# Guards for idempotent instrumentation
_requests_instrumented = False
_llm_instrumented = False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _otlp_base_url() -> str:
    # Use the endpoint injected by the Dash0 operator (or any OTel-compliant
    # collector that sets OTEL_EXPORTER_OTLP_ENDPOINT).  Strip trailing slash
    # so callers can always safely append /v1/traces or /v1/metrics.
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").rstrip("/")
    if not endpoint:
        _log.warning(
            "OTEL_EXPORTER_OTLP_ENDPOINT is not set — telemetry export will be skipped"
        )
    return endpoint


def _make_resource(service_name: str) -> Resource:
    """Build a Resource with all recommended attributes.

    service_name — the DAG name, e.g. "reddit-import" (NOT the task_id)

    service.instance.id is set to the pod hostname (HOSTNAME env var), which
    is stable for the lifetime of a pod and unique across pods.  It must NOT
    be set to dag_run_id — that creates a new metric series per run, causing
    unbounded cardinality in the backend.  Run-level correlation is handled
    via the airflow.run_id span attribute, not via the resource.
    """
    return Resource.create({
        SERVICE_NAME: service_name,
        "service.namespace": _SERVICE_NAMESPACE,
        # Injected at image build time; falls back to "unknown" in local dev.
        "service.version": os.environ.get("DAG_VERSION", "unknown"),
        # Injected via Helm values (e.g. "production", "staging").
        "deployment.environment.name": os.environ.get("DEPLOYMENT_ENVIRONMENT", "production"),
        # Pod hostname — stable per pod, unique across pods.
        "service.instance.id": os.environ.get("HOSTNAME", "unknown"),
    })


# ---------------------------------------------------------------------------
# Provider factories
# ---------------------------------------------------------------------------

def create_task_provider(service_name: str, dag_run_id: str, task_id: str = "") -> TracerProvider:
    """Create a TracerProvider scoped to a single DAG task.

    Call shutdown_providers() at the end of every task (in a try/finally block
    if possible) to flush and stop the background exporter thread.

    Args:
        service_name: The DAG name, e.g. "reddit-import".
        dag_run_id:   ti.run_id — logged for debugging, not used in the resource.
        task_id:      ti.task_id — appended to service_name to give each task a
                      distinct service.name (e.g. "reddit-import.get_saved_posts").
                      This produces a separate node per task in Dash0's service map
                      and trace graph with bounded cardinality (stable task names).
    """
    effective_service = f"{service_name}.{task_id}" if task_id else service_name
    provider = TracerProvider(resource=_make_resource(effective_service))
    base = _otlp_base_url()
    if base:
        endpoint = f"{base}/v1/traces"
        _log.info("[OTLPSpanExporter] Connecting to OpenTelemetry Collector at %s", endpoint)
        provider.add_span_processor(SimpleSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    return provider


def create_meter_provider(service_name: str, dag_run_id: str, task_id: str = "") -> MeterProvider:
    """Create a MeterProvider scoped to a single DAG task.

    Call shutdown_providers() at the end of every task to flush buffered metric
    data and stop the PeriodicExportingMetricReader background thread.

    Args:
        service_name: The DAG name (same value passed to create_task_provider).
        dag_run_id:   ti.run_id — logged for debugging, not used in the resource.
        task_id:      ti.task_id — see create_task_provider for rationale.
    """
    effective_service = f"{service_name}.{task_id}" if task_id else service_name
    base = _otlp_base_url()
    if base:
        endpoint = f"{base}/v1/metrics"
        _log.info("[OTLPMetricExporter] Connecting to OpenTelemetry Collector at %s", endpoint)
        exporter = OTLPMetricExporter(endpoint=endpoint)
        reader = PeriodicExportingMetricReader(exporter)
    else:
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader  # noqa: PLC0415
        reader = InMemoryMetricReader()
    provider = MeterProvider(
        resource=_make_resource(effective_service),
        metric_readers=[reader],
    )
    return provider


def shutdown_providers(*providers: Union[TracerProvider, MeterProvider]) -> None:
    """Flush and shut down one or more OTEL providers.

    Call this at the end of every task function (ideally in a try/finally block)
    to ensure all buffered spans/metrics are exported and the background threads
    are stopped.  This replaces the previous atexit-based approach, which
    accumulated live provider references and background threads across multiple
    task executions in the same long-running Celery worker process.
    """
    for provider in providers:
        try:
            provider.force_flush()
        except Exception:
            _log.exception("Error flushing provider %r", provider)
        try:
            provider.shutdown()
        except Exception:
            _log.exception("Error shutting down provider %r", provider)


# ---------------------------------------------------------------------------
# Instrumentation helpers
# ---------------------------------------------------------------------------

def instrument_llm(task_provider: TracerProvider) -> None:
    """Activate the official OTel GenAI instrumentation for the OpenAI SDK (idempotent).

    Uses opentelemetry-instrumentation-openai-v2 from opentelemetry-python-contrib
    (CNCF/OTel GenAI SIG). Works with Ollama because it exposes an OpenAI-compatible
    API — the instrumentation wraps the SDK client, not the endpoint.

    Produces spans with gen_ai.* semantic convention attributes:
      gen_ai.system, gen_ai.request.model, gen_ai.usage.input_tokens,
      gen_ai.usage.output_tokens, gen_ai.response.finish_reasons

    Install: opentelemetry-instrumentation-openai-v2
    """
    global _llm_instrumented
    if _llm_instrumented:
        return
    from opentelemetry.instrumentation.openai_v2 import OpenAIInstrumentor  # noqa: PLC0415
    OpenAIInstrumentor().instrument(tracer_provider=task_provider)
    _llm_instrumented = True


def instrument_requests(task_provider: TracerProvider) -> None:
    """Activate opentelemetry-instrumentation-requests (idempotent).

    Must be called after create_task_provider; the flag is module-level so
    repeated calls within the same worker process are safe no-ops.
    """
    global _requests_instrumented
    if _requests_instrumented:
        return
    # Import here so DAGs that don't use requests don't require this package.
    from opentelemetry.instrumentation.requests import RequestsInstrumentor  # noqa: PLC0415
    RequestsInstrumentor().instrument(tracer_provider=task_provider)
    _requests_instrumented = True


# ---------------------------------------------------------------------------
# Context propagation
# ---------------------------------------------------------------------------

def resolve_parent_context(ti, otel_task_tracer, previous_task_id: str | None = None):
    """Resolve the parent trace context for a DAG task.

    Resolution order:
    1. W3C carrier pushed to XCom by the previous task (preferred — maintains
       the single trace across the DAG run).
    2. Airflow's native ti.context_carrier (fallback for first task in a run).
    3. None — starts a new root trace (last resort; logged as error).
    """
    if previous_task_id:
        carrier = ti.xcom_pull(task_ids=previous_task_id, key="otel_context")
        if carrier:
            _log.info(
                "Using XCom trace handoff",
                extra={"previous_task_id": previous_task_id, **get_trace_context()},
            )
            return otel_extract(carrier)
        _log.warning(
            "No XCom handoff found, falling back to Airflow carrier",
            extra={"previous_task_id": previous_task_id},
        )

    if ti.context_carrier is not None:
        _log.info("Using Airflow context carrier")
        return otel_task_tracer.extract(ti.context_carrier)

    _log.error("No parent trace context available — starting new root trace")
    return None


# ---------------------------------------------------------------------------
# Trace context for structured logging
# ---------------------------------------------------------------------------

def get_trace_context() -> dict:
    """Return trace_id and span_id from the active span.

    Use as extra= kwargs when logging inside a span to correlate log records
    with the trace that produced them:

        logger.info("item processed", extra={**get_trace_context(), "item.id": item_id})
    """
    ctx = trace.get_current_span().get_span_context()
    if ctx.trace_id == 0:
        return {}
    return {
        "trace_id": format(ctx.trace_id, "032x"),
        "span_id": format(ctx.span_id, "016x"),
    }


# ---------------------------------------------------------------------------
# Root span context manager
# ---------------------------------------------------------------------------

@contextmanager
def task_root_span(
    ti,
    task_provider: TracerProvider,
    parent_context,
    next_task_id: str | None = None,
) -> Iterator:
    """Create the root CONSUMER span for a DAG task.

    On entry: starts a CONSUMER span named dag.<dag_id>.task.<task_id> with
              standard airflow.* and messaging.* attributes.
    On exit:  creates a PRODUCER child span and pushes the W3C trace context to
              XCom so the next task can continue the trace.

    Args:
        ti:             Airflow TaskInstance.
        task_provider:  TracerProvider created by create_task_provider().
        parent_context: Context returned by resolve_parent_context().
        next_task_id:   task_id of the immediately downstream task, if any.
                        Used to set peer.service on the PRODUCER span so Dash0
                        can draw the service map edge to the next task's service.
                        Omit for terminal tasks or fan-out steps.
    """
    tracer = trace.get_tracer(ti.task_id, tracer_provider=task_provider)

    # Derive the next task's service name from this provider's resource.
    # Resource service.name follows the pattern "{dag-name}.{task_id}".
    _next_service: str | None = None
    if next_task_id:
        this_service = task_provider.resource.attributes.get(SERVICE_NAME, "")
        if "." in this_service:
            base = this_service.rsplit(".", 1)[0]
            _next_service = f"{base}.{next_task_id}"

    with tracer.start_as_current_span(
        f"dag.{ti.dag_id}.task.{ti.task_id}",
        context=parent_context,
        kind=SpanKind.CONSUMER,
    ) as span:
        span.set_attribute("airflow.dag_id", ti.dag_id)
        span.set_attribute("airflow.task_id", ti.task_id)
        span.set_attribute("airflow.run_id", ti.run_id)
        # messaging.* attributes tell Dash0 this is a messaging span and enable
        # service map edge detection between tasks.
        span.set_attribute("messaging.system", "airflow")
        span.set_attribute("messaging.destination.name", f"{ti.dag_id}.{ti.task_id}")
        span.set_attribute("messaging.operation.type", "process")

        ctx = span.get_span_context()
        if ctx.trace_id == 0:
            _log.error(
                "Span has trace_id=0 — OpenTelemetry not initialised",
                extra={"airflow.task_id": ti.task_id},
            )
        else:
            _log.info(
                "Task span started",
                extra={**get_trace_context(), "airflow.dag_id": ti.dag_id, "airflow.task_id": ti.task_id},
            )

        yield span

        with tracer.start_as_current_span(
            f"publish {ti.dag_id}",
            kind=SpanKind.PRODUCER,
        ) as producer_span:
            producer_span.set_attribute("messaging.system", "airflow")
            producer_span.set_attribute("messaging.destination.name", ti.dag_id)
            producer_span.set_attribute("messaging.operation.type", "publish")
            if _next_service:
                producer_span.set_attribute("peer.service", _next_service)
            carrier: dict = {}
            inject(carrier)
            ti.xcom_push(key="otel_context", value=carrier)
            _log.info("Trace handoff pushed to XCom", extra=get_trace_context())


# ---------------------------------------------------------------------------
# LLM response parsing (shared across all import DAGs)
# ---------------------------------------------------------------------------

def parse_llm_json(raw: str, item_id: str) -> dict:
    """Robustly parse an LLM response into {"tags": [...], "summary": "..."}.

    Handles:
    - Markdown code fences (```json ... ```)
    - Trailing commas before } or ] (common LLM mistake)
    - JSON array wrapping a single dict ([{...}])
    - tags returned as a comma-separated string instead of a list
    - Any other non-dict result → returns empty defaults
    """
    text = raw.strip()

    # Strip markdown code fences
    if text.startswith("```"):
        lines = text.splitlines()
        end = -1 if lines[-1].strip() == "```" else len(lines)
        text = "\n".join(lines[1:end]).strip()

    # Fix trailing commas before } or ]
    text = re.sub(r",\s*([}\]])", r"\1", text)

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        _log.warning(
            "LLM response is not valid JSON",
            extra={"item_id": item_id, "error": str(exc), "raw_preview": raw[:200]},
        )
        return {"tags": [], "summary": ""}

    # Unwrap a list — some models return [{...}] instead of {...}
    if isinstance(data, list):
        dicts = [x for x in data if isinstance(x, dict)]
        if not dicts:
            _log.warning(
                "LLM response is a list with no dicts",
                extra={"item_id": item_id, "raw_preview": raw[:200]},
            )
            return {"tags": [], "summary": ""}
        data = dicts[0]

    if not isinstance(data, dict):
        _log.warning(
            "LLM response is not a dict",
            extra={"item_id": item_id, "type": type(data).__name__, "raw_preview": raw[:200]},
        )
        return {"tags": [], "summary": ""}

    tags = data.get("tags", [])
    if isinstance(tags, str):
        tags = [t.strip().lower() for t in tags.replace(",", " ").split() if t.strip()]
    elif not isinstance(tags, list):
        tags = []
    else:
        tags = [str(t).strip().lower() for t in tags if t]

    return {"tags": tags, "summary": str(data.get("summary") or "")}
