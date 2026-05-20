"""
bookmark_processor — daily DAG that imports Brave/Chrome bookmarks and enriches them.

Workflow:
  1. import_bookmarks   — reads bookmarks/export.json from Garage S3, stores new items
  2. find_pending       — finds all bookmark items lacking a scraped body
  3. scrape_bodies      — fetches the URL for each and extracts article text
  4. warm_up_model      — pull Ollama model if not cached
  5. classify_content   — LLM content_type classification (post-scrape, when body exists)
  6. estimate_time      — LLM time estimation
  7. extract_structure  — LLM recipe/project structure extraction

To import bookmarks: export from Brave (Settings → Bookmarks → Export) and upload
the JSON file to Garage S3 at s3://<GARAGE_S3_BUCKET>/bookmarks/export.json.
The DAG renames it to bookmarks/export.<timestamp>.json after processing to avoid
re-importing on the next run.

Required Airflow Variables (set via setup.sh / airflow-variables secret):
  GARAGE_S3_ENDPOINT  — e.g. https://s3.wallace.network
  GARAGE_S3_BUCKET    — bucket name, e.g. social-archive

Required Airflow Connection:
  garage_s3           — AWS-type connection with login=access_key, password=secret_key
"""
from __future__ import annotations

import logging
from datetime import timedelta

import pendulum

from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task, Variable
from airflow.traces import otel_tracer
from airflow.traces.tracer import Trace

from airflow_otel import instrument_task_context, get_meter
from dag_utils import (
    warm_up_ollama, run_estimate_items, run_extract_structure, run_classify_items,
)

logger = logging.getLogger("airflow.bookmark_processor")

_S3_EXPORT_KEY = "bookmarks/export.json"
_SCRAPE_TIMEOUT = 15  # seconds per URL fetch
_SCRAPE_BODY_MAX = 8000  # chars stored in body column


# ---------------------------------------------------------------------------
# Bookmark export parsing
# ---------------------------------------------------------------------------

def _parse_chromium_bookmarks(data: dict) -> list[dict]:
    """Flatten a Chromium/Brave bookmark export JSON into a list of {name, url} dicts."""
    results = []

    def _walk(node):
        if node.get("type") == "url":
            results.append({
                "name": node.get("name", ""),
                "url": node.get("url", ""),
                "folder": "",
            })
        elif node.get("type") == "folder":
            folder_name = node.get("name", "")
            for child in node.get("children", []):
                child_copy = dict(child)
                if child_copy.get("type") == "url":
                    results.append({
                        "name": child_copy.get("name", ""),
                        "url": child_copy.get("url", ""),
                        "folder": folder_name,
                    })
                else:
                    _walk(child_copy)

    roots = data.get("roots", {})
    for root_key in ("bookmark_bar", "other", "synced"):
        root = roots.get(root_key)
        if root:
            _walk(root)

    return results


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task
def import_bookmarks() -> list[int]:
    """
    Read bookmarks/export.json from Garage S3 and insert new bookmarks into saved_items.
    Returns the list of newly-inserted item IDs.
    If no export file is found, returns [] and the DAG continues to process
    any previously-imported bookmarks that still need scraping.
    """
    import json
    import boto3
    from botocore.exceptions import ClientError
    from airflow.hooks.base import BaseHook

    endpoint = Variable.get("GARAGE_S3_ENDPOINT", default="https://s3.wallace.network")
    bucket = Variable.get("GARAGE_S3_BUCKET", default="social-archive")

    try:
        conn = BaseHook.get_connection("garage_s3")
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
        )
    except Exception as exc:
        logger.warning("Could not connect to Garage S3 (%s) — skipping import step", exc)
        return []

    try:
        response = s3.get_object(Bucket=bucket, Key=_S3_EXPORT_KEY)
        raw = response["Body"].read().decode("utf-8")
        data = json.loads(raw)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            logger.info(
                "No bookmark export found at s3://%s/%s — skipping import",
                bucket, _S3_EXPORT_KEY,
            )
            return []
        raise

    bookmarks = _parse_chromium_bookmarks(data)
    logger.info("Parsed %d bookmarks from export", len(bookmarks))

    if not bookmarks:
        return []

    hook = PostgresHook(postgres_conn_id="social_archive_db")
    inserted_ids: list[int] = []

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS saved_items (
                    id                   SERIAL PRIMARY KEY,
                    source               VARCHAR(50)  NOT NULL,
                    external_id          VARCHAR(50)  NOT NULL,
                    type                 VARCHAR(20)  NOT NULL,
                    title                TEXT,
                    body                 TEXT,
                    uri                  TEXT,
                    source_context       VARCHAR(255),
                    tags                 TEXT[],
                    summary              TEXT,
                    flagged_for_deletion BOOLEAN      NOT NULL DEFAULT false,
                    saved_at             TIMESTAMPTZ  DEFAULT NOW(),
                    UNIQUE (source, external_id)
                )
            """)
            conn.commit()

            for bm in bookmarks:
                url = bm["url"]
                if not url or url.startswith(("javascript:", "data:", "chrome:")):
                    continue

                # Use URL as the stable external_id
                external_id = url[:50] if len(url) > 50 else url

                cursor.execute("""
                    INSERT INTO saved_items
                        (source, external_id, type, title, uri, source_context)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, external_id) DO NOTHING
                    RETURNING id
                """, (
                    "bookmark",
                    external_id,
                    "bookmark",
                    bm["name"] or url,
                    url,
                    bm["folder"] or "Bookmarks",
                ))
                conn.commit()
                row = cursor.fetchone()
                if row:
                    inserted_ids.append(row[0])

    logger.info("Imported %d new bookmarks from S3 export", len(inserted_ids))

    # Rename the processed file so it won't be re-imported next run
    if inserted_ids:
        timestamp = pendulum.now("UTC").strftime("%Y%m%dT%H%M%S")
        archive_key = f"bookmarks/export.{timestamp}.json"
        try:
            s3.copy_object(
                Bucket=bucket,
                Key=archive_key,
                CopySource={"Bucket": bucket, "Key": _S3_EXPORT_KEY},
            )
            s3.delete_object(Bucket=bucket, Key=_S3_EXPORT_KEY)
            logger.info("Archived export to %s", archive_key)
        except Exception as exc:
            logger.warning("Could not archive S3 export: %s", exc)

    return inserted_ids


@task
def find_pending(imported_ids: list[int]) -> list[int]:
    """
    Return IDs of all bookmark items that still need processing:
    newly imported + any existing items with empty body (from previous failed scrapes).
    """
    hook = PostgresHook(postgres_conn_id="social_archive_db")
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id FROM saved_items
                WHERE source = 'bookmark'
                  AND (body IS NULL OR body = '')
                  AND flagged_for_deletion = FALSE
                  AND deleted_at IS NULL
            """)
            existing_pending = [r[0] for r in cursor.fetchall()]

    # Union: newly imported + existing unscraped (deduplicated)
    all_pending = list(dict.fromkeys(imported_ids + existing_pending))
    logger.info(
        "%d pending bookmark items (%d newly imported, %d existing unscraped)",
        len(all_pending), len(imported_ids), len(existing_pending),
    )
    return all_pending


@task
def scrape_bodies(pending_ids: list[int], ti) -> list[int]:
    """
    Fetch the URL for each pending bookmark and extract article text.
    Updates saved_items.body in-place.
    Returns IDs of items that were successfully scraped (body populated).
    """
    if not pending_ids:
        logger.info("No pending bookmarks to scrape.")
        raise AirflowSkipException("No bookmarks need scraping")

    import requests as _req
    try:
        import trafilatura
        _use_trafilatura = True
    except ImportError:
        logger.warning("trafilatura not available — falling back to basic extraction")
        _use_trafilatura = False

    if not _use_trafilatura:
        from html.parser import HTMLParser

        class _TextExtractor(HTMLParser):
            def __init__(self):
                super().__init__()
                self._text = []
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
                        self._text.append(stripped)

            def get_text(self):
                return " ".join(self._text)

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    hook = PostgresHook(postgres_conn_id="social_archive_db")

    # Fetch URIs for the pending items
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
                            headers = {
                                "User-Agent": (
                                    "Mozilla/5.0 (compatible; SocialArchive/1.0; "
                                    "+https://github.com/proffalken/private-airflow-dags)"
                                )
                            }
                            resp = _req.get(url, headers=headers, timeout=_SCRAPE_TIMEOUT)
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
                    logger.debug("Scraped %d chars from %s (id=%s)", len(body), url, item["id"])

                    if len(scraped_ids) % 20 == 0 and scraped_ids:
                        logger.info(
                            "Progress: %d/%d scraped", len(scraped_ids), len(items)
                        )

        span.set_attribute("items.scraped", len(scraped_ids))
        logger.info("Scraping complete: %d scraped, %d failed", len(scraped_ids), failed)

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


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="bookmark_processor",
    schedule=timedelta(hours=6),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["social-archive", "llm", "bookmarks"],
)
def bookmark_processor():
    imported = import_bookmarks()
    pending = find_pending(imported)
    scraped = scrape_bodies(pending)
    warmed = warm_up_model(scraped)
    classify_content(warmed)
    estimate_time(warmed)
    extract_structure(warmed)


bookmark_processor()
