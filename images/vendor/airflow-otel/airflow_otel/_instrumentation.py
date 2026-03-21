"""
Core OTel SDK setup and instrumentation helpers for Airflow tasks.

Resource attribute mapping:
  service.namespace  = dag_id        (bounded — fixed set of DAGs)
  service.name       = task_id       (bounded — fixed set of tasks per DAG)
  service.instance.id = UUID v4      (opaque, stable per process, cardinality-safe)

High-cardinality values (run_id, try_number, logical_date) are placed on the
root span and log records only — never on resource attributes — to avoid
unbounded metric time series.
"""

import functools
import logging
import os
import traceback
import uuid
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator, Optional

from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor, SpanExporter
from opentelemetry.trace import SpanKind, StatusCode

from airflow_otel._context import extract_airflow_context
from airflow_otel._propagation import extract_upstream_context, push_current_context

_tracer_provider: Optional[TracerProvider] = None
_meter_provider: Optional[MeterProvider] = None
_logger_provider: Optional[LoggerProvider] = None

# Test seam: set this to an InMemorySpanExporter before calling instrument_task.
# When set, SimpleSpanProcessor is used so spans are captured synchronously.
# Reset to None after each test.
_TEST_EXPORTER: Optional[SpanExporter] = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _otlp_headers() -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for kv in os.environ.get("OTEL_EXPORTER_OTLP_HEADERS", "").split(","):
        if "=" in kv:
            k, v = kv.strip().split("=", 1)
            if k:
                headers[k] = v
    return headers


def _otlp_endpoint() -> str:
    return os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")


def _make_exporter() -> OTLPSpanExporter:
    """Build the OTLP HTTP span exporter. Extracted for test patching.

    Do NOT pass endpoint= explicitly: when the SDK reads OTEL_EXPORTER_OTLP_ENDPOINT
    from the environment it automatically appends /v1/traces.  Passing the base
    URL as an explicit argument bypasses that logic and causes 404s.
    """
    return OTLPSpanExporter()


def _build_resource(dag_id: str, task_id: str) -> Resource:
    base = {
        "service.name": task_id,
        "service.namespace": dag_id,
        # UUID v4: opaque, stable for this process's lifetime, cardinality-safe
        "service.instance.id": str(uuid.uuid4()),
        "deployment.environment.name": os.environ.get(
            "AIRFLOW_ENV", os.environ.get("ENV", "production")
        ),
    }
    # Honour any extra resource attributes from the environment
    for kv in os.environ.get("OTEL_RESOURCE_ATTRIBUTES", "").split(","):
        if "=" in kv:
            k, v = kv.strip().split("=", 1)
            if k:
                base[k] = v
    return Resource.create(base)


# ---------------------------------------------------------------------------
# SDK lifecycle
# ---------------------------------------------------------------------------

def setup_otel(
    dag_id: str,
    task_id: str,
    run_id: str = "unknown",
    try_number: int = 1,
    *,
    _exporter_override: Optional[SpanExporter] = None,
) -> None:
    """
    Initialise TracerProvider, MeterProvider, and LoggerProvider.

    Call once per task execution.  shutdown_otel() must be called when done
    (the instrument_task decorator and instrument_task_context manager do this
    automatically via a finally block).

    _exporter_override is a test seam — pass an InMemorySpanExporter to capture
    spans without needing a running collector.
    """
    global _tracer_provider, _meter_provider, _logger_provider

    resource = _build_resource(dag_id, task_id)

    is_test = _exporter_override is not None or _TEST_EXPORTER is not None
    span_exporter = _exporter_override if _exporter_override is not None else (_TEST_EXPORTER or _make_exporter())
    processor = SimpleSpanProcessor(span_exporter) if is_test else BatchSpanProcessor(span_exporter)

    _tracer_provider = TracerProvider(resource=resource)
    _tracer_provider.add_span_processor(processor)

    if is_test:
        # In test mode: do NOT register globally (set_tracer_provider can only be
        # called once per process; subsequent calls are silently ignored).
        # get_tracer() reads from _tracer_provider directly, so tests are isolated.
        # Skip metric/log exporters — they would make real HTTP connections.
        return

    trace.set_tracer_provider(_tracer_provider)

    # Airflow 3.x initialises its own global MeterProvider at startup.
    # set_meter_provider() is a one-time operation; attempting to override it
    # silently fails and logs a warning.  If a real provider is already set,
    # reuse it so our get_meter() calls land in the same pipeline.
    existing_mp = metrics.get_meter_provider()
    if isinstance(existing_mp, MeterProvider):
        _meter_provider = existing_mp
    else:
        metric_exporter = OTLPMetricExporter()
        reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=30_000)
        _meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
        metrics.set_meter_provider(_meter_provider)

    log_exporter = OTLPLogExporter()
    _logger_provider = LoggerProvider(resource=resource)
    _logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    set_logger_provider(_logger_provider)

    handler = LoggingHandler(level=logging.NOTSET, logger_provider=_logger_provider)
    logging.getLogger().addHandler(handler)


def shutdown_otel() -> None:
    """Flush and shut down all providers. Called automatically by the decorator."""
    global _tracer_provider, _meter_provider, _logger_provider
    if _tracer_provider:
        _tracer_provider.force_flush(timeout_millis=30_000)
        _tracer_provider.shutdown()
        _tracer_provider = None
    if _meter_provider:
        _meter_provider.force_flush(timeout_millis=30_000)
        _meter_provider.shutdown()
        _meter_provider = None
    if _logger_provider:
        _logger_provider.force_flush(timeout_millis=30_000)
        _logger_provider.shutdown()
        _logger_provider = None


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def get_tracer(name: str = "airflow-otel") -> trace.Tracer:
    """Return the active tracer. Call after setup_otel() or inside an instrumented task."""
    if _tracer_provider is not None:
        return _tracer_provider.get_tracer(name)
    return trace.get_tracer_provider().get_tracer(name)


def get_meter(name: str = "airflow-otel") -> metrics.Meter:
    """Return the active meter. Call after setup_otel() or inside an instrumented task."""
    return metrics.get_meter_provider().get_meter(name)


# ---------------------------------------------------------------------------
# Root span helpers
# ---------------------------------------------------------------------------

def _root_span_attributes(ctx_attrs: Dict[str, Any]) -> Dict[str, Any]:
    """Build span attributes from extracted Airflow context.

    High-cardinality values belong here (span attributes), not on the resource.
    """
    attrs: Dict[str, Any] = {
        "airflow.dag_id": ctx_attrs.get("dag_id", "unknown-dag"),
        "airflow.task_id": ctx_attrs.get("task_id", "unknown-task"),
        "airflow.run_id": ctx_attrs.get("run_id", "unknown-run"),
        "airflow.try_number": int(ctx_attrs.get("try_number", 1)),
    }
    if ctx_attrs.get("logical_date"):
        attrs["airflow.logical_date"] = ctx_attrs["logical_date"]
    return attrs


def _emit_exception_log(
    logger: logging.Logger,
    span_name: str,
    span_ctx: Any,
    exc: Exception,
    ctx_attrs: Dict[str, Any],
) -> None:
    """Emit a structured log record for an exception, correlated to the active span."""
    logger.error(
        "%s.failed",
        span_name,
        extra={
            "trace_id": format(span_ctx.trace_id, "032x"),
            "span_id": format(span_ctx.span_id, "016x"),
            "exception.type": type(exc).__name__,
            "exception.message": str(exc),
            "exception.stacktrace": traceback.format_exc(),
            "airflow.dag_id": ctx_attrs.get("dag_id"),
            "airflow.task_id": ctx_attrs.get("task_id"),
            "airflow.run_id": ctx_attrs.get("run_id"),
        },
    )


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------

def instrument_task(func: Callable) -> Callable:
    """
    Decorator that wraps an Airflow task with OTel instrumentation.

    Usage — TaskFlow API::

        from airflow.decorators import task
        from airflow_otel import instrument_task, get_tracer

        @task
        @instrument_task
        def my_task():
            tracer = get_tracer()
            with tracer.start_as_current_span("fetch data"):
                ...

    The decorator:
    - Extracts the Airflow execution context (DAG ID, task ID, run ID, …)
    - Sets service.namespace = dag_id and service.name = task_id on the resource
    - Creates a CONSUMER root span named ``{dag_id}.{task_id}``
    - Sets span status ERROR and emits a correlated log record on exception
    - Flushes and shuts down all OTel providers on exit
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        ctx_attrs = extract_airflow_context(kwargs)
        dag_id = ctx_attrs.get("dag_id", "unknown-dag")
        task_id = ctx_attrs.get("task_id", func.__name__)
        run_id = ctx_attrs.get("run_id", "unknown-run")
        try_number = int(ctx_attrs.get("try_number", 1))

        setup_otel(dag_id, task_id, run_id, try_number)
        tracer = get_tracer()
        span_name = f"{dag_id}.{task_id}"
        logger = logging.getLogger(func.__module__)
        parent_ctx = extract_upstream_context(ctx_attrs)

        try:
            with tracer.start_as_current_span(
                span_name,
                context=parent_ctx,
                kind=SpanKind.CONSUMER,
                attributes=_root_span_attributes(ctx_attrs),
            ) as span:
                push_current_context(ctx_attrs)
                try:
                    result = func(*args, **kwargs)
                    span.set_status(StatusCode.OK)
                    return result
                except Exception as exc:
                    _emit_exception_log(logger, span_name, span.get_span_context(), exc, ctx_attrs)
                    span.set_status(StatusCode.ERROR, f"{type(exc).__name__}: {exc}")
                    raise
        finally:
            # shutdown_otel() must run AFTER the with-block so span.end() fires
            # before the exporter is shut down.
            shutdown_otel()

    return wrapper


# ---------------------------------------------------------------------------
# Context manager (traditional PythonOperator)
# ---------------------------------------------------------------------------

@contextmanager
def instrument_task_context(context: Dict[str, Any]) -> Generator[trace.Span, None, None]:
    """
    Context manager for traditional PythonOperator callables.

    Usage::

        def my_callable(**context):
            with instrument_task_context(context) as span:
                span.set_attribute("records.processed", 42)
                ...

    Behaves identically to the ``@instrument_task`` decorator.
    """
    ctx_attrs = extract_airflow_context(context)
    dag_id = ctx_attrs.get("dag_id", "unknown-dag")
    task_id = ctx_attrs.get("task_id", "unknown-task")
    run_id = ctx_attrs.get("run_id", "unknown-run")
    try_number = int(ctx_attrs.get("try_number", 1))

    setup_otel(dag_id, task_id, run_id, try_number)
    tracer = get_tracer()
    span_name = f"{dag_id}.{task_id}"
    logger = logging.getLogger(__name__)
    parent_ctx = extract_upstream_context(ctx_attrs)

    try:
        with tracer.start_as_current_span(
            span_name,
            context=parent_ctx,
            kind=SpanKind.CONSUMER,
            attributes=_root_span_attributes(ctx_attrs),
        ) as span:
            push_current_context(ctx_attrs)
            try:
                yield span
                span.set_status(StatusCode.OK)
            except Exception as exc:
                _emit_exception_log(logger, span_name, span.get_span_context(), exc, ctx_attrs)
                span.set_status(StatusCode.ERROR, f"{type(exc).__name__}: {exc}")
                raise
    finally:
        shutdown_otel()
