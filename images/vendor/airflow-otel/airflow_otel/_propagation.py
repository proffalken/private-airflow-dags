"""W3C Trace Context propagation via Airflow XCom for cross-task trace linking."""

from typing import Any, Dict

from opentelemetry import context as otel_context
from opentelemetry.propagate import extract, inject

OTEL_XCOM_KEY = "__otel_trace_context__"


def extract_upstream_context(ctx_attrs: Dict[str, Any]) -> otel_context.Context:
    """
    Pull W3C trace context from the nearest upstream task's XCom.

    Tries each upstream task in order and returns the context from the first
    one that has a stored carrier.  Returns the current (empty) context if no
    upstream context is found, causing the caller to create a new root span
    (i.e. this is the first task in the DAG or no upstream has pushed context).
    """
    ti = ctx_attrs.get("task_instance")
    if ti is None:
        return otel_context.get_current()

    try:
        upstream_task_ids = list(ti.task.upstream_task_ids)
    except Exception:
        return otel_context.get_current()

    for task_id in upstream_task_ids:
        try:
            carrier = ti.xcom_pull(task_ids=task_id, key=OTEL_XCOM_KEY)
            if carrier:
                return extract(carrier)
        except Exception:
            continue

    return otel_context.get_current()


def push_current_context(ctx_attrs: Dict[str, Any]) -> None:
    """
    Inject the current span context into XCom so downstream tasks can link to it.

    Call this immediately after starting the root span so the span ID is set.
    """
    ti = ctx_attrs.get("task_instance")
    if ti is None:
        return

    carrier: Dict[str, str] = {}
    inject(carrier)
    if not carrier:
        return

    try:
        ti.xcom_push(key=OTEL_XCOM_KEY, value=carrier)
    except Exception:
        pass
