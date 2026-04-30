"""Tests for instagram_scheduler DAG helper functions.

Tests cover the two pure functions that handle schedule generation and
trigger-window detection. No Airflow context or live Variables required.
"""
from __future__ import annotations

import sys
import os
from unittest.mock import MagicMock

import pendulum
import pytest

# ---------------------------------------------------------------------------
# Stubs — must be set before the module loads
# ---------------------------------------------------------------------------

for mod in ("airflow_otel", "dag_utils"):
    sys.modules.setdefault(mod, MagicMock())

airflow_stub = MagicMock()
airflow_sdk_stub = MagicMock()
airflow_sdk_stub.dag = lambda **kw: (lambda f: MagicMock())
airflow_sdk_stub.task = lambda f=None, **kw: (f if f else lambda g: g)
airflow_sdk_stub.Variable = MagicMock()
sys.modules.setdefault("airflow", airflow_stub)
sys.modules["airflow.sdk"] = airflow_sdk_stub

import importlib.util

spec = importlib.util.spec_from_file_location(
    "instagram_scheduler",
    os.path.join(os.path.dirname(__file__), "..", "dags", "instagram_scheduler.py"),
)
_module = importlib.util.module_from_spec(spec)
sys.modules["instagram_scheduler"] = _module
spec.loader.exec_module(_module)

get_or_create_schedule = _module.get_or_create_schedule
check_trigger_window = _module.check_trigger_window
_MIN_GAP_HOURS = _module._MIN_GAP_HOURS
_EARLIEST_HOUR = _module._EARLIEST_HOUR
_LATEST_HOUR = _module._LATEST_HOUR

# A fixed Monday for deterministic tests (2026-W18)
MONDAY = pendulum.datetime(2026, 4, 27, 10, 0, 0, tz="UTC")


# ---------------------------------------------------------------------------
# get_or_create_schedule
# ---------------------------------------------------------------------------

class TestGetOrCreateSchedule:
    def test_creates_two_slots_for_new_week(self):
        result = get_or_create_schedule(MONDAY, {})
        assert result["week"] == "2026-W18"
        assert len(result["slots"]) == 2
        assert result["triggered"] == []

    def test_reuses_existing_schedule_same_week(self):
        existing = {
            "week": "2026-W18",
            "slots": ["2026-04-28T10:30:00+00:00", "2026-04-30T14:00:00+00:00"],
            "triggered": [],
        }
        result = get_or_create_schedule(MONDAY, existing)
        assert result is existing

    def test_regenerates_schedule_for_new_week(self):
        old = {
            "week": "2026-W17",
            "slots": ["2026-04-20T10:00:00+00:00"],
            "triggered": ["2026-04-20T10:00:00+00:00"],
        }
        result = get_or_create_schedule(MONDAY, old)
        assert result["week"] == "2026-W18"
        assert result["triggered"] == []

    def test_slots_are_at_least_24h_apart(self):
        for _ in range(30):
            result = get_or_create_schedule(MONDAY, {})
            s0 = pendulum.parse(result["slots"][0])
            s1 = pendulum.parse(result["slots"][1])
            gap = abs((s1 - s0).total_seconds())
            assert gap >= _MIN_GAP_HOURS * 3600, f"Gap too small: {result['slots']}"

    def test_slot_hours_within_allowed_range(self):
        for _ in range(30):
            result = get_or_create_schedule(MONDAY, {})
            for slot_str in result["slots"]:
                hour = pendulum.parse(slot_str).hour
                assert _EARLIEST_HOUR <= hour <= _LATEST_HOUR, f"Hour {hour} out of range"

    def test_handles_empty_existing_schedule(self):
        result = get_or_create_schedule(MONDAY, {})
        assert "week" in result and "slots" in result


# ---------------------------------------------------------------------------
# check_trigger_window
# ---------------------------------------------------------------------------

class TestCheckTriggerWindow:
    def _sched(self, slots, triggered=None):
        return {"week": "2026-W18", "slots": slots, "triggered": triggered or []}

    def test_triggers_when_slot_falls_in_current_hour(self):
        now = pendulum.datetime(2026, 4, 28, 14, 0, 0, tz="UTC")
        slot = "2026-04-28T14:45:00+00:00"
        should, updated = check_trigger_window(now, self._sched([slot]))
        assert should is True
        assert slot in updated["triggered"]

    def test_no_trigger_when_slot_in_different_hour(self):
        now = pendulum.datetime(2026, 4, 28, 14, 0, 0, tz="UTC")
        slot = "2026-04-28T15:30:00+00:00"
        should, _ = check_trigger_window(now, self._sched([slot]))
        assert should is False

    def test_no_trigger_for_already_triggered_slot(self):
        now = pendulum.datetime(2026, 4, 28, 14, 0, 0, tz="UTC")
        slot = "2026-04-28T14:30:00+00:00"
        should, _ = check_trigger_window(now, self._sched([slot], triggered=[slot]))
        assert should is False

    def test_second_slot_triggers_in_its_own_hour(self):
        now = pendulum.datetime(2026, 4, 30, 16, 0, 0, tz="UTC")
        slot1 = "2026-04-28T10:00:00+00:00"
        slot2 = "2026-04-30T16:45:00+00:00"
        should, updated = check_trigger_window(
            now, self._sched([slot1, slot2], triggered=[slot1])
        )
        assert should is True
        assert slot2 in updated["triggered"]

    def test_slot_at_exact_hour_boundary_triggers(self):
        now = pendulum.datetime(2026, 4, 28, 10, 0, 0, tz="UTC")
        slot = "2026-04-28T10:00:00+00:00"
        should, _ = check_trigger_window(now, self._sched([slot]))
        assert should is True

    def test_slot_at_next_hour_does_not_trigger(self):
        now = pendulum.datetime(2026, 4, 28, 10, 0, 0, tz="UTC")
        slot = "2026-04-28T11:00:00+00:00"
        should, _ = check_trigger_window(now, self._sched([slot]))
        assert should is False

    def test_schedule_unchanged_when_no_trigger(self):
        now = pendulum.datetime(2026, 4, 28, 14, 0, 0, tz="UTC")
        slot = "2026-04-28T16:00:00+00:00"
        original = self._sched([slot])
        should, returned = check_trigger_window(now, original)
        assert should is False
        assert returned is original
