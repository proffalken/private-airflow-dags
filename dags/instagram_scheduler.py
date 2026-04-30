from __future__ import annotations

import json
import logging
import random

import pendulum

from airflow.sdk import dag, task, Variable

logger = logging.getLogger("airflow.instagram_scheduler")

_SCHEDULE_VARIABLE = "INSTAGRAM_SCHEDULE"
_MIN_GAP_HOURS = 24
_EARLIEST_HOUR = 8   # slots are only generated between 08:00 …
_LATEST_HOUR = 21    # … and 21:59 UTC to approximate waking hours


def _week_key(dt: pendulum.DateTime) -> str:
    iso = dt.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def get_or_create_schedule(now: pendulum.DateTime, current: dict) -> dict:
    """Return the weekly schedule dict, generating two fresh random slots when the week rolls over."""
    key = _week_key(now)
    if current.get("week") == key:
        return current

    monday = now.start_of("week")
    slots: list[pendulum.DateTime] = []
    attempts = 0
    while len(slots) < 2 and attempts < 500:
        attempts += 1
        candidate = monday.add(days=random.randint(0, 6)).set(
            hour=random.randint(_EARLIEST_HOUR, _LATEST_HOUR),
            minute=random.randint(0, 59),
            second=0,
            microsecond=0,
        )
        if all(abs((candidate - s).total_seconds()) >= _MIN_GAP_HOURS * 3600 for s in slots):
            slots.append(candidate)

    new_schedule = {
        "week": key,
        "slots": [s.isoformat() for s in slots],
        "triggered": [],
    }
    logger.info(f"New week {key}: Instagram runs scheduled at {new_schedule['slots']}")
    return new_schedule


def check_trigger_window(now: pendulum.DateTime, schedule: dict) -> tuple[bool, dict]:
    """Return (should_trigger, updated_schedule).

    If an untriggered slot falls within the current clock-hour, mark it triggered
    and return True. Otherwise return False and the original schedule unchanged.
    """
    triggered_set = set(schedule.get("triggered", []))
    hour_start = now.start_of("hour")
    hour_end = hour_start.add(hours=1)

    for slot_str in schedule.get("slots", []):
        if slot_str in triggered_set:
            continue
        if hour_start <= pendulum.parse(slot_str) < hour_end:
            triggered_set.add(slot_str)
            return True, {**schedule, "triggered": list(triggered_set)}

    return False, schedule


@task
def check_and_maybe_trigger(ti) -> bool:
    """Read the weekly Instagram schedule and fire the import DAG if a slot is due."""
    now = pendulum.now("UTC")

    try:
        current = json.loads(Variable.get(_SCHEDULE_VARIABLE, default="{}"))
    except (json.JSONDecodeError, ValueError):
        current = {}

    schedule = get_or_create_schedule(now, current)
    should_trigger, schedule = check_trigger_window(now, schedule)

    Variable.set(_SCHEDULE_VARIABLE, json.dumps(schedule))

    if should_trigger:
        from airflow.api.common.trigger_dag import trigger_dag
        run_id = f"instagram_scheduler__{now.strftime('%Y%m%dT%H%M%S')}"
        trigger_dag(dag_id="instagram_import", run_id=run_id)
        logger.info(f"Triggered instagram_import run_id={run_id}")
    else:
        logger.info("No Instagram run scheduled this hour")

    return should_trigger


@dag(
    schedule="0 * * * *",
    start_date=pendulum.datetime(2025, 8, 30, tz="UTC"),
    catchup=False,
)
def instagram_scheduler():
    check_and_maybe_trigger()


instagram_scheduler()
