"""
bookmark_processor — processes bookmarks synced by the browser extension.

The browser extension (social-archive/extension/) writes bookmarks directly to
saved_items via the backend /api/bookmarks/sync endpoint. Each bookmark lands
with source='brave'/'chrome', type='bookmark', uri, title, source_context
(folder), and tags (ancestor folders) — but no body, summary, or content_type.

This DAG runs every 6 hours and enriches those items:
  1. find_pending      — find bookmarks with no body yet
  2. scrape_bodies     — fetch each URL and extract article text
  3. warm_up_model     — pull Ollama model if not cached
  4. classify_content  — LLM content_type classification
  5. estimate_time     — LLM time estimation
  6. extract_structure — LLM recipe/project structure extraction
"""
from __future__ import annotations

import logging
from datetime import timedelta

import pendulum

from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.traces import otel_tracer
from airflow.traces.tracer import Trace

from airflow_otel import instrument_task_context, get_meter
from dag_utils import (
    warm_up_ollama, run_estimate_items, run_extract_structure, run_classify_items,
)

logger = logging.getLogger("airflow.bookmark_processor")

_SCRAPE_TIMEOUT = 15   # seconds per URL fetch
_SCRAPE_BODY_MAX = 8000  # chars stored in body column


@task
def find_pending() -> list[int]:
    """Return IDs of browser-synced bookmarks that still need body scraping."""
    hook = PostgresHook(postgres_conn_id="social_archive_db")
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id FROM saved_items
                WHERE type = 'bookmark'
                  AND (body IS NULL OR body = '')
                  AND flagged_for_deletion = FALSE
                  AND deleted_at IS NULL
                ORDER BY saved_at DESC
            """)
            ids = [r[0] for r in cursor.fetchall()]

    logger.info("%d bookmarks need scraping", len(ids))
    return ids


@task
def scrape_bodies(pending_ids: list[int], ti) -> list[int]:
    """
    Fetch the URL for each pending bookmark and extract article text.
    Updates saved_items.body in-place.
    Returns IDs of items where text was successfully extracted.
    """
    if not pending_ids:
        raise AirflowSkipException("No bookmarks need scraping")

    import requests as _req

    try:
        import trafilatura
        _use_trafilatura = True
    except ImportError:
        logger.warning("trafilatura not available — falling back to stdlib HTML parser")
        _use_trafilatura = False

    if not _use_trafilatura:
        from html.parser import HTMLParser

        class _TextExtractor(HTMLParser):
            def __init__(self):
                super().__init__()
                self._parts: list[str] = []
                self._skip = False

            def handle_starttag(self, tag, attrs):
                if tag in ("script", "style", "nav", "header", "footer"):
                    self._skip = True

            def handle_endtag(self, tag):
                if tag in ("script", "style", "nav", "header", "footer"):
                    self._skip = False

            def handle_data(self, data):
                if not self._skip:
                    stripped = data.strip()
                    if stripped:
                        self._parts.append(stripped)

            def get_text(self) -> str:
                return " ".join(self._parts)

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    hook = PostgresHook(postgres_conn_id="social_archive_db")

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id, uri FROM saved_items WHERE id = ANY(%s)",
                (pending_ids,),
            )
            items = [{"id": r[0], "uri": r[1]} for r in cursor.fetchall()]

    scraped_ids: list[int] = []
    failed = 0

    with instrument_task_context({"items.total": len(items)}) as span:
        meter = get_meter("bookmark.scraper")
        scraped_counter = meter.create_counter(
            "bookmark.items_scraped",
            description="Bookmark URLs successfully scraped",
        )
        failed_counter = meter.create_counter(
            "bookmark.items_scrape_failed",
            description="Bookmark URLs that failed scraping",
        )

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for item in items:
                    url = item["uri"]
                    if not url:
                        failed += 1
                        failed_counter.add(1)
                        continue

                    with otel_task_tracer.start_child_span(span_name="scrape_url") as sc_span:
                        sc_span.set_attribute("url", url[:200])
                        try:
                            resp = _req.get(
                                url,
                                headers={
                                    "User-Agent": (
                                        "Mozilla/5.0 (compatible; SocialArchiveBot/1.0)"
                                    )
                                },
                                timeout=_SCRAPE_TIMEOUT,
                                allow_redirects=True,
                            )
                            resp.raise_for_status()
                            html = resp.text

                            if _use_trafilatura:
                                body = trafilatura.extract(
                                    html,
                                    include_comments=False,
                                    include_tables=False,
                                    no_fallback=False,
                                ) or ""
                            else:
                                extractor = _TextExtractor()
                                extractor.feed(html)
                                body = extractor.get_text()

                            body = body[:_SCRAPE_BODY_MAX].strip()
                        except Exception as exc:
                            logger.warning("Scrape failed for %s: %s", url, exc)
                            failed += 1
                            failed_counter.add(1)
                            continue

                    if not body:
                        logger.warning("No text extracted from %s — skipping", url)
                        failed += 1
                        failed_counter.add(1)
                        continue

                    cursor.execute(
                        "UPDATE saved_items SET body = %s WHERE id = %s",
                        (body, item["id"]),
                    )
                    conn.commit()
                    scraped_ids.append(item["id"])
                    scraped_counter.add(1)
                    logger.debug(
                        "Scraped %d chars from %s (id=%s)", len(body), url, item["id"]
                    )

                    if len(scraped_ids) % 20 == 0 and scraped_ids:
                        logger.info(
                            "Progress: %d/%d scraped", len(scraped_ids), len(items)
                        )

        span.set_attribute("items.scraped", len(scraped_ids))
        logger.info(
            "Scraping complete: %d scraped, %d failed", len(scraped_ids), failed
        )

    return scraped_ids


@task
def warm_up_model(scraped_ids: list[int]) -> list[int]:
    """Warm up Ollama before LLM processing tasks. Pass-through for scraped item IDs."""
    if not scraped_ids:
        logger.info("No scraped items — skipping model warm-up.")
        return []
    warm_up_ollama()
    return scraped_ids


@task
def classify_content(scraped_ids: list[int]) -> list[int]:
    """LLM-classify content_type for scraped bookmarks that don't have it yet."""
    run_classify_items(scraped_ids, "bookmark")
    return scraped_ids


@task
def estimate_time(scraped_ids: list[int]) -> list[dict]:
    """Estimate build effort for newly-scraped bookmark items."""
    return run_estimate_items(scraped_ids, "bookmark")


@task
def extract_structure(scraped_ids: list[int]) -> list[dict]:
    """Extract structured data for recipe/project bookmark items."""
    return run_extract_structure(scraped_ids, "bookmark")


@dag(
    dag_id="bookmark_processor",
    schedule=timedelta(hours=6),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["social-archive", "llm", "bookmarks"],
)
def bookmark_processor():
    pending = find_pending()
    scraped = scrape_bodies(pending)
    warmed = warm_up_model(scraped)
    classify_content(warmed)
    estimate_time(warmed)
    extract_structure(warmed)


bookmark_processor()
