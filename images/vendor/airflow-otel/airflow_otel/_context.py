from typing import Any, Callable, Dict, Optional

# Imported at module level so tests can patch airflow_otel._context.get_current_context.
# Airflow 3.x moved get_current_context to airflow.sdk; fall back to the deprecated
# airflow.operators.python location for older Airflow versions.
try:
    from airflow.sdk import get_current_context
except ImportError:
    try:
        from airflow.operators.python import get_current_context  # type: ignore[no-redef]
    except Exception:  # pragma: no cover
        get_current_context = None  # type: ignore[assignment]


def extract_airflow_context(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a flat dict of Airflow execution attributes.

    Tries get_current_context() first (TaskFlow API), then falls back to the
    kwargs dict passed by a traditional PythonOperator.

    Returned keys: dag_id, task_id, run_id, try_number, logical_date (optional).
    """
    try:
        if get_current_context is None:
            raise RuntimeError("airflow not available")
        ctx = get_current_context()
    except Exception:
        ctx = kwargs

    attrs: Dict[str, Any] = {}

    dag = ctx.get("dag")
    if dag is not None:
        attrs["dag_id"] = dag.dag_id
    elif "dag_id" in ctx:
        attrs["dag_id"] = ctx["dag_id"]

    task = ctx.get("task")
    if task is not None:
        attrs["task_id"] = task.task_id
    elif "task_id" in ctx:
        attrs["task_id"] = ctx["task_id"]

    dag_run = ctx.get("dag_run")
    if dag_run is not None:
        attrs["run_id"] = dag_run.run_id
        # Airflow 2.4+ uses logical_date; earlier versions use execution_date
        if hasattr(dag_run, "logical_date") and dag_run.logical_date is not None:
            attrs["logical_date"] = dag_run.logical_date.isoformat()
        elif hasattr(dag_run, "execution_date") and dag_run.execution_date is not None:
            attrs["logical_date"] = dag_run.execution_date.isoformat()
    elif "run_id" in ctx:
        attrs["run_id"] = ctx["run_id"]

    ti = ctx.get("task_instance") or ctx.get("ti")
    if ti is not None:
        attrs["task_instance"] = ti
        attrs["try_number"] = ti.try_number
        if "task_id" not in attrs:
            attrs["task_id"] = ti.task_id
        if "dag_id" not in attrs:
            attrs["dag_id"] = ti.dag_id

    return attrs
