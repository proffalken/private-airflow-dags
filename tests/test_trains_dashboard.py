"""Tests for the trains dashboard backend query functions.

Tests are written first (TDD) and mock the database connection so no
live PostgreSQL instance is required in CI.
"""
from __future__ import annotations

import sys
import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "trains-dashboard", "backend")
)
from app.queries import (  # noqa: E402
    get_hourly_counts,
    get_performance,
    get_recent_movements,
    get_summary,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_conn(fetchone=None, fetchall=None):
    """Build a mock connection whose cursor() returns an async context manager.

    psycopg3 AsyncConnection.cursor() is a regular (non-async) call, so conn
    must be a plain MagicMock.  The returned cursor IS used with `async with`,
    so its __aenter__/__aexit__ must be AsyncMocks.
    """
    cur = AsyncMock()
    cur.fetchone.return_value = fetchone
    cur.fetchall.return_value = fetchall or []

    cursor_ctx = MagicMock()
    cursor_ctx.__aenter__ = AsyncMock(return_value=cur)
    cursor_ctx.__aexit__ = AsyncMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cursor_ctx
    return conn, cur


# ---------------------------------------------------------------------------
# get_summary
# ---------------------------------------------------------------------------

class TestGetSummary:
    @pytest.mark.asyncio
    async def test_returns_correct_keys(self):
        conn, _ = _mock_conn(fetchone=(1234, 45, 76.5, 2.3))
        result = await get_summary(conn)
        assert set(result.keys()) == {"total_movements", "cancellations", "on_time_pct", "avg_delay_mins"}

    @pytest.mark.asyncio
    async def test_values_are_correct_types(self):
        conn, _ = _mock_conn(fetchone=(1234, 45, 76.5, 2.3))
        result = await get_summary(conn)
        assert isinstance(result["total_movements"], int)
        assert isinstance(result["cancellations"], int)
        assert isinstance(result["on_time_pct"], float)
        assert isinstance(result["avg_delay_mins"], float)

    @pytest.mark.asyncio
    async def test_maps_values_correctly(self):
        conn, _ = _mock_conn(fetchone=(500, 10, 88.0, 1.5))
        result = await get_summary(conn)
        assert result["total_movements"] == 500
        assert result["cancellations"] == 10
        assert result["on_time_pct"] == 88.0
        assert result["avg_delay_mins"] == 1.5

    @pytest.mark.asyncio
    async def test_none_values_become_zero(self):
        conn, _ = _mock_conn(fetchone=(0, 0, None, None))
        result = await get_summary(conn)
        assert result["on_time_pct"] == 0.0
        assert result["avg_delay_mins"] == 0.0


# ---------------------------------------------------------------------------
# get_performance
# ---------------------------------------------------------------------------

class TestGetPerformance:
    @pytest.mark.asyncio
    async def test_returns_list(self):
        conn, _ = _mock_conn(fetchall=[("88", 456, 345, 89, 22, 75.7)])
        result = await get_performance(conn)
        assert isinstance(result, list)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_row_has_required_keys(self):
        conn, _ = _mock_conn(fetchall=[("88", 456, 345, 89, 22, 75.7)])
        result = await get_performance(conn)
        row = result[0]
        assert set(row.keys()) == {"toc_id", "total", "on_time", "late", "early", "on_time_pct"}

    @pytest.mark.asyncio
    async def test_maps_values_correctly(self):
        conn, _ = _mock_conn(fetchall=[("88", 456, 345, 89, 22, 75.7)])
        result = await get_performance(conn)
        row = result[0]
        assert row["toc_id"] == "88"
        assert row["total"] == 456
        assert row["on_time"] == 345
        assert row["late"] == 89
        assert row["early"] == 22
        assert row["on_time_pct"] == 75.7

    @pytest.mark.asyncio
    async def test_empty_returns_empty_list(self):
        conn, _ = _mock_conn(fetchall=[])
        result = await get_performance(conn)
        assert result == []

    @pytest.mark.asyncio
    async def test_none_on_time_pct_becomes_zero(self):
        conn, _ = _mock_conn(fetchall=[("88", 1, 0, 1, 0, None)])
        result = await get_performance(conn)
        assert result[0]["on_time_pct"] == 0.0


# ---------------------------------------------------------------------------
# get_recent_movements
# ---------------------------------------------------------------------------

class TestGetRecentMovements:
    _ts = datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
    _row = ("225Y05MX06", "P72253", "88", "DEPARTURE", "73300", "LATE", 3, _ts, _ts, _ts)

    @pytest.mark.asyncio
    async def test_returns_list(self):
        conn, _ = _mock_conn(fetchall=[self._row])
        result = await get_recent_movements(conn)
        assert isinstance(result, list)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_row_has_required_keys(self):
        conn, _ = _mock_conn(fetchall=[self._row])
        result = await get_recent_movements(conn)
        row = result[0]
        for key in ("train_id", "train_uid", "toc_id", "event_type", "loc_stanox",
                    "variation_status", "timetable_variation",
                    "actual_ts", "planned_ts", "msg_queue_ts"):
            assert key in row, f"missing key: {key}"

    @pytest.mark.asyncio
    async def test_timestamps_are_iso_strings(self):
        conn, _ = _mock_conn(fetchall=[self._row])
        result = await get_recent_movements(conn)
        assert result[0]["actual_ts"] == self._ts.isoformat()

    @pytest.mark.asyncio
    async def test_none_timestamp_stays_none(self):
        row = list(self._row)
        row[7] = None  # actual_ts = None
        conn, _ = _mock_conn(fetchall=[tuple(row)])
        result = await get_recent_movements(conn)
        assert result[0]["actual_ts"] is None

    @pytest.mark.asyncio
    async def test_passes_limit_to_query(self):
        conn, cur = _mock_conn(fetchall=[])
        await get_recent_movements(conn, limit=50)
        call_args = cur.execute.call_args[0]
        assert 50 in call_args[1]


# ---------------------------------------------------------------------------
# get_hourly_counts
# ---------------------------------------------------------------------------

class TestGetHourlyCounts:
    _ts = datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc)

    @pytest.mark.asyncio
    async def test_returns_list(self):
        conn, _ = _mock_conn(fetchall=[(self._ts, 234)])
        result = await get_hourly_counts(conn)
        assert isinstance(result, list)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_row_has_hour_and_count(self):
        conn, _ = _mock_conn(fetchall=[(self._ts, 234)])
        result = await get_hourly_counts(conn)
        assert "hour" in result[0]
        assert "count" in result[0]

    @pytest.mark.asyncio
    async def test_hour_is_iso_string(self):
        conn, _ = _mock_conn(fetchall=[(self._ts, 234)])
        result = await get_hourly_counts(conn)
        assert result[0]["hour"] == self._ts.isoformat()

    @pytest.mark.asyncio
    async def test_count_is_int(self):
        conn, _ = _mock_conn(fetchall=[(self._ts, 234)])
        result = await get_hourly_counts(conn)
        assert result[0]["count"] == 234

    @pytest.mark.asyncio
    async def test_empty_returns_empty_list(self):
        conn, _ = _mock_conn(fetchall=[])
        result = await get_hourly_counts(conn)
        assert result == []
