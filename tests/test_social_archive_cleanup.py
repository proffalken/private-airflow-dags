"""Tests for social_archive_cleanup DAG task functions.

Focuses on the cleanup_bookmarks task (and the remove_cleaned_items plumbing)
since these are the areas most likely to regress. No live DB or external APIs
are required — everything is mocked.
"""
from __future__ import annotations

import sys
import os
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Path / import setup
# ---------------------------------------------------------------------------

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

# Stub heavy third-party imports so the module loads without real credentials.
_STUBS = [
    "praw", "prawcore", "requests",
    "googleapiclient", "googleapiclient.discovery",
    "google", "google.oauth2", "google.oauth2.credentials",
    "google.auth", "google.auth.transport", "google.auth.transport.requests",
    "instagrapi", "instagrapi.exceptions",
    "airflow_otel", "dag_utils",
]
for mod in _STUBS:
    sys.modules.setdefault(mod, MagicMock())

# Airflow SDK stubs
airflow_mock = MagicMock()
# @dag(...) must replace the decorated function with a no-op so the module-level
# `social_archive_cleanup()` call does not execute the DAG body during import.
airflow_mock.sdk.dag = lambda **kw: (lambda f: MagicMock())
# @task must pass the function through so we can call it directly in tests.
airflow_mock.sdk.task = lambda f=None, **kw: (f if f else lambda g: g)
airflow_mock.sdk.Variable = MagicMock()
sys.modules.setdefault("airflow", airflow_mock)
sys.modules.setdefault("airflow.sdk", airflow_mock.sdk)
sys.modules.setdefault("airflow.exceptions", MagicMock())
sys.modules.setdefault("airflow.providers", MagicMock())
sys.modules.setdefault("airflow.providers.postgres", MagicMock())
sys.modules.setdefault("airflow.providers.postgres.hooks", MagicMock())
sys.modules.setdefault("airflow.providers.postgres.hooks.postgres", MagicMock())
sys.modules.setdefault("airflow.traces", MagicMock())
sys.modules.setdefault("airflow.traces.tracer", MagicMock())

# Import the bare functions after stubs are in place.
# We import them directly to avoid executing the @dag decorator wiring.
import importlib
spec = importlib.util.spec_from_file_location(
    "social_archive_cleanup",
    os.path.join(os.path.dirname(__file__), "..", "dags", "social_archive_cleanup.py"),
)
_module = importlib.util.module_from_spec(spec)
sys.modules["social_archive_cleanup"] = _module  # needed for patch() to resolve targets
spec.loader.exec_module(_module)

cleanup_bookmarks = _module.cleanup_bookmarks.__wrapped__ if hasattr(_module.cleanup_bookmarks, "__wrapped__") else _module.cleanup_bookmarks
remove_cleaned_items = _module.remove_cleaned_items.__wrapped__ if hasattr(_module.remove_cleaned_items, "__wrapped__") else _module.remove_cleaned_items


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _item(id_, source):
    return {"id": id_, "source": source, "external_id": f"ext-{id_}", "type": "post"}


def _ti():
    return MagicMock()


# ---------------------------------------------------------------------------
# cleanup_bookmarks
# ---------------------------------------------------------------------------

def _mock_hook():
    """Return a mock PostgresHook with a chain-accessible cursor."""
    hook = MagicMock()
    return hook


def _run_cleanup_bookmarks(items):
    """Call cleanup_bookmarks with a mocked DB and return (result, captured_ids)."""
    hook = _mock_hook()
    conn = hook.get_conn.return_value.__enter__.return_value
    cur = conn.cursor.return_value.__enter__.return_value

    with patch("social_archive_cleanup.PostgresHook", return_value=hook):
        result = cleanup_bookmarks(items, _ti())

    call = cur.execute.call_args
    captured = sorted(call[0][1][0]) if call else []
    return result, captured


class TestCleanupBookmarks:
    def test_soft_deletes_brave_items(self):
        items = [_item(1, "brave"), _item(2, "reddit"), _item(3, "brave")]
        result, updated_ids = _run_cleanup_bookmarks(items)
        assert result == []           # never hard-deleted
        assert updated_ids == [1, 3]  # soft-deleted via UPDATE

    def test_soft_deletes_chrome_items(self):
        items = [_item(10, "chrome"), _item(11, "instagram")]
        result, updated_ids = _run_cleanup_bookmarks(items)
        assert result == []
        assert updated_ids == [10]

    def test_soft_deletes_mixed_brave_and_chrome(self):
        items = [_item(1, "brave"), _item(2, "chrome"), _item(3, "github")]
        result, updated_ids = _run_cleanup_bookmarks(items)
        assert result == []
        assert updated_ids == [1, 2]

    def test_returns_empty_and_no_db_call_when_no_bookmarks(self):
        hook = _mock_hook()
        with patch("social_archive_cleanup.PostgresHook", return_value=hook):
            result = cleanup_bookmarks([_item(1, "reddit")], _ti())
        assert result == []
        hook.get_conn.assert_not_called()

    def test_returns_empty_for_empty_input(self):
        hook = _mock_hook()
        with patch("social_archive_cleanup.PostgresHook", return_value=hook):
            result = cleanup_bookmarks([], _ti())
        assert result == []
        hook.get_conn.assert_not_called()

    def test_does_not_soft_delete_unknown_sources(self):
        items = [_item(1, "twitter"), _item(2, "brave")]
        result, updated_ids = _run_cleanup_bookmarks(items)
        assert updated_ids == [2]


# ---------------------------------------------------------------------------
# remove_cleaned_items — flattening logic (unit-tested without DB)
# ---------------------------------------------------------------------------

class TestRemoveCleanedItemsFlattening:
    """Verify the id-list flattening logic, which is where data gets dropped."""

    def _captured_ids(self, id_lists):
        """Call remove_cleaned_items with mocked infra and capture the ids passed to DELETE."""
        mock_hook = MagicMock()
        # With `with hook.get_conn() as conn:`, conn is:
        #   hook.get_conn().__enter__()  →  get_conn.return_value.__enter__.return_value
        conn = mock_hook.get_conn.return_value.__enter__.return_value
        # With `with conn.cursor() as cur:`, cur is:
        #   conn.cursor().__enter__()  →  cursor.return_value.__enter__.return_value
        cur = conn.cursor.return_value.__enter__.return_value

        ctx = MagicMock()
        ctx.__enter__.return_value = MagicMock()  # span

        with patch("social_archive_cleanup.PostgresHook", return_value=mock_hook), \
             patch("social_archive_cleanup.instrument_task_context", return_value=ctx), \
             patch("social_archive_cleanup.otel_tracer"):
            remove_cleaned_items(id_lists, _ti())

        call = cur.execute.call_args
        if call is None:
            return []
        return sorted(call[0][1][0])

    def test_flattens_all_source_lists(self):
        assert self._captured_ids([[1, 2], [3], [4, 5], [6]]) == [1, 2, 3, 4, 5, 6]

    def test_bookmarks_return_empty_so_not_hard_deleted(self):
        # cleanup_bookmarks returns [] — hard-delete list has only the other sources
        assert self._captured_ids([[1], [2], [3], [4], []]) == [1, 2, 3, 4]

    def test_skips_delete_when_all_lists_empty(self):
        mock_hook = MagicMock()
        ctx = MagicMock()
        ctx.__enter__.return_value = MagicMock()
        with patch("social_archive_cleanup.PostgresHook", return_value=mock_hook), \
             patch("social_archive_cleanup.instrument_task_context", return_value=ctx), \
             patch("social_archive_cleanup.otel_tracer"):
            # All five sources return empty — nothing should be deleted
            remove_cleaned_items([[], [], [], [], []], _ti())
        mock_hook.get_conn.assert_not_called()
