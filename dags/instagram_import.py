"""
instagram_import — hourly DAG that imports, analyses, and enriches Instagram saves.

Runs every hour but only performs the full import when a weekly scheduled slot is due
(two random slots per week, between 08:00–21:59 UTC, at least 24 h apart). This avoids
hammering the Instagram API while still running regularly enough to be timely.

Full pipeline per run:
  1. check_schedule_and_fetch — verify a slot is due, then fetch saved collections
  2. analyse_and_store        — LLM tag/summary/classify, upsert to saved_items
  3. warm_up_model            — pull Ollama model if not cached
  4. estimate_time            — LLM time estimation for newly stored items
  5. extract_structure        — LLM recipe/project extraction for newly stored items
"""
from __future__ import annotations

import json
import logging
import random
import re

import pendulum

from instagrapi import Client
from instagrapi import extractors as _ig_extractors
from instagrapi.exceptions import LoginRequired, ChallengeRequired, ClientError

# Monkey-patch: extract_resource_v1 crashes with IndexError when
# image_versions2.candidates is an empty list (happens with Reels/newer formats
# inside carousels). Patch it to handle that gracefully.
_orig_extract_resource_v1 = _ig_extractors.extract_resource_v1


def _safe_extract_resource_v1(data):
    if "video_versions" in data:
        video_versions = data.get("video_versions") or []
        if video_versions:
            data["video_url"] = sorted(
                video_versions, key=lambda o: o["height"] * o["width"]
            )[-1]["url"]
    candidates = (data.get("image_versions2") or {}).get("candidates") or []
    if candidates:
        data["thumbnail_url"] = sorted(
            candidates, key=lambda o: o["height"] * o["width"]
        )[-1]["url"]
        return _ig_extractors.Resource(**data)
    else:
        data["thumbnail_url"] = None
        return _ig_extractors.Resource.model_construct(**data)


_ig_extractors.extract_resource_v1 = _safe_extract_resource_v1

from instagrapi.mixins import collection as _ig_collection
_orig_extract_media_v1 = _ig_collection.extract_media_v1


def _safe_extract_media_v1(data):
    data.setdefault("code", "")
    return _orig_extract_media_v1(data)


_ig_collection.extract_media_v1 = _safe_extract_media_v1

from openai import OpenAI

from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task, Variable
from airflow.traces import otel_tracer
from airflow.traces.tracer import Trace

from airflow_otel import instrument_task_context, get_meter
from dag_utils import (
    instrument_llm, parse_llm_json,
    OLLAMA_BASE_URL, OLLAMA_MODEL, CONTENT_TYPE_PROMPT_FRAGMENT, CONTENT_TYPES,
    warm_up_ollama, run_estimate_items, run_extract_structure,
)

logger = logging.getLogger("airflow.instagram_dag")

_MEDIA_TYPE_NAMES = {1: "photo", 2: "video", 8: "album"}
_SCHEDULE_VARIABLE = "INSTAGRAM_SCHEDULE"
_MIN_GAP_HOURS = 24
_EARLIEST_HOUR = 8
_LATEST_HOUR = 21


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _week_key(dt: pendulum.DateTime) -> str:
    iso = dt.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _get_or_create_schedule(now: pendulum.DateTime, current: dict) -> dict:
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
    logger.info("New week %s: Instagram runs scheduled at %s", key, new_schedule["slots"])
    return new_schedule


def _check_trigger_window(now: pendulum.DateTime, schedule: dict) -> tuple[bool, dict]:
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


def get_instagram_client() -> Client:
    """Return an authenticated instagrapi Client, reusing a saved session where possible."""
    cl = Client()
    cl.delay_range = [1, 3]

    session_str = Variable.get("INSTAGRAM_SESSION", default="")
    if session_str:
        try:
            cl.set_settings(json.loads(session_str))
            logger.info("Loaded existing Instagram session")
            return cl
        except Exception as exc:
            logger.warning(f"Could not load session ({exc}), performing fresh login")

    try:
        cl.login(
            Variable.get("INSTAGRAM_USERNAME"),
            Variable.get("INSTAGRAM_PASSWORD"),
        )
    except (LoginRequired, ChallengeRequired) as exc:
        raise RuntimeError(
            "Instagram login failed — check INSTAGRAM_USERNAME / INSTAGRAM_PASSWORD "
            "Variables and clear INSTAGRAM_SESSION if it is stale"
        ) from exc

    Variable.set("INSTAGRAM_SESSION", json.dumps(cl.get_settings()))
    return cl


def extract_hashtags(caption: str | None) -> list[str]:
    if not caption:
        return []
    return [tag.lower() for tag in re.findall(r"#(\w+)", caption)]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task
def check_schedule_and_fetch(ti) -> dict[str, list[dict]]:
    """Check weekly schedule; skip if not due. If due, fetch saved collections."""
    now = pendulum.now("UTC")

    try:
        current = json.loads(Variable.get(_SCHEDULE_VARIABLE, default="{}"))
    except (json.JSONDecodeError, ValueError):
        current = {}

    schedule = _get_or_create_schedule(now, current)
    should_trigger, schedule = _check_trigger_window(now, schedule)
    Variable.set(_SCHEDULE_VARIABLE, json.dumps(schedule))

    if not should_trigger:
        logger.info("No Instagram run scheduled this hour — skipping")
        raise AirflowSkipException("No Instagram run scheduled this hour")

    logger.info("Instagram slot is due — fetching saved collections")

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)

    try:
        hook = PostgresHook(postgres_conn_id="social_archive_db")
        rows = hook.get_records(
            "SELECT external_id FROM saved_items WHERE source = 'instagram'"
        )
        known_ids = {row[0] for row in rows}
        logger.info(f"=== {len(known_ids)} Instagram items already in database, skipping")
    except Exception:
        logger.info("=== saved_items table not found yet, treating all items as new")
        known_ids = set()

    sorted_posts: dict[str, list[dict]] = {}
    seen_pks: set[str] = set()
    new_count = 0

    cl = get_instagram_client()

    with instrument_task_context({}) as span:
        meter = get_meter("instagram.saved")
        media_gauge = meter.create_gauge(
            "instagram.saved.media_count",
            unit="1",
            description="Number of new saved Instagram media items fetched in this run",
        )

        with otel_task_tracer.start_child_span(span_name="fetch_collections"):
            collections = cl.collections()
            logger.info(f"Found {len(collections)} Instagram collections")

            all_posts_collection = None
            named_collections = []
            for c in collections:
                if c.type == "ALL_MEDIA_AUTO_COLLECTION":
                    all_posts_collection = c
                else:
                    named_collections.append(c)

            for collection in named_collections:
                collection_name = collection.name
                logger.info(f"Fetching collection: {collection_name!r} (id={collection.id})")

                with otel_task_tracer.start_child_span(
                    span_name="fetch collection"
                ) as fetch_span:
                    fetch_span.set_attribute("instagram.collection.id", str(collection.id))
                    fetch_span.set_attribute("instagram.collection.name", collection_name)
                    try:
                        medias = cl.collection_medias(collection.id, amount=200)
                    except ClientError as exc:
                        logger.warning(
                            f"Instagram rate-limited while fetching collection "
                            f"{collection_name!r}: {exc}. Skipping collection."
                        )
                        continue

                for media in medias:
                    pk_str = str(media.pk)
                    seen_pks.add(pk_str)
                    if pk_str in known_ids:
                        continue

                    caption = media.caption_text or ""
                    media_type = _MEDIA_TYPE_NAMES.get(media.media_type, "post")
                    uri = f"https://www.instagram.com/p/{media.code}/" if media.code else None

                    sorted_posts.setdefault(collection_name, []).append({
                        "external_id": pk_str,
                        "type": media_type,
                        "title": None,
                        "body": caption,
                        "uri": uri,
                        "caption_hashtags": extract_hashtags(caption),
                    })
                    new_count += 1

        uncollected_count = 0
        if all_posts_collection:
            with otel_task_tracer.start_child_span(span_name="fetch_uncollected_saves"):
                try:
                    medias = cl.collection_medias(all_posts_collection.id, amount=200)
                except ClientError as exc:
                    logger.warning(
                        f"Instagram rate-limited while fetching all-posts collection: {exc}. "
                        f"Skipping uncollected saves this run."
                    )
                    medias = []

                for media in medias:
                    pk_str = str(media.pk)
                    if pk_str in seen_pks:
                        continue
                    seen_pks.add(pk_str)
                    if pk_str in known_ids:
                        continue

                    caption = media.caption_text or ""
                    media_type = _MEDIA_TYPE_NAMES.get(media.media_type, "post")
                    uri = f"https://www.instagram.com/p/{media.code}/" if media.code else None

                    sorted_posts.setdefault("Saved", []).append({
                        "external_id": pk_str,
                        "type": media_type,
                        "title": None,
                        "body": caption,
                        "uri": uri,
                        "caption_hashtags": extract_hashtags(caption),
                    })
                    new_count += 1
                    uncollected_count += 1

        logger.info(f"Found {uncollected_count} uncollected saves")
        span.set_attribute("instagram.new_items", new_count)
        logger.info(f"=== {new_count} new Instagram items to process")
        media_gauge.set(new_count)

    return sorted_posts


@task
def analyse_and_store(sorted_posts: dict[str, list[dict]], ti) -> list[int]:
    """LLM-analyse each Instagram item and store in archive DB. Returns inserted item IDs."""
    logger.info("=" * 80)
    logger.info(f"Analysing and storing — DAG: {ti.dag_id}, Run: {ti.run_id}")

    total = sum(len(items) for items in sorted_posts.values())
    if total == 0:
        logger.info("No new items to analyse, skipping.")
        raise AirflowSkipException("No new Instagram items to analyse")

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)

    client = OpenAI(base_url=OLLAMA_BASE_URL, api_key="ollama")
    hook = PostgresHook(postgres_conn_id="social_archive_db")
    inserted_ids: list[int] = []

    with instrument_task_context({}) as span:
        instrument_llm()
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                with otel_task_tracer.start_child_span(span_name="create_schema"):
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

                with otel_task_tracer.start_child_span(span_name="analyse_and_insert_items") as insert_span:
                    for collection_name, items in sorted_posts.items():
                        for item in items:
                            external_id = item["external_id"]
                            caption_hashtags: list[str] = item.get("caption_hashtags", [])
                            caption = item["body"] or ""
                            label = caption[:60] or external_id

                            logger.info(
                                f"Analysing: {label!r} ({item['type']}) "
                                f"from {collection_name!r} "
                                f"[{len(caption_hashtags)} caption hashtags]"
                            )

                            with otel_task_tracer.start_child_span(
                                span_name="llm.analyse_item"
                            ) as llm_span:
                                llm_span.set_attribute("item.external_id", external_id)
                                llm_span.set_attribute("item.type", item["type"])
                                llm_span.set_attribute("item.collection", collection_name)

                                hashtag_hint = (
                                    f"Caption hashtags already found: "
                                    f"{', '.join(caption_hashtags)}\n"
                                    if caption_hashtags else ""
                                )

                                response = client.chat.completions.create(
                                    model=OLLAMA_MODEL,
                                    max_tokens=600,
                                    messages=[{
                                        "role": "user",
                                        "content": (
                                            f"Analyse this Instagram {item['type']} "
                                            f"from the collection {collection_name!r} "
                                            f"and return a JSON object with:\n"
                                            f'- "tags": a list of 3-8 single-concept '
                                            f"keyword tags (lowercase, no spaces).\n"
                                            f"  TAGGING RULES — follow these strictly:\n"
                                            f"  1. Each tag must represent ONE concept. "
                                            f"Never combine concepts: use 'christmas' "
                                            f"and 'food' as separate tags, never "
                                            f"'christmasfood' or 'foodchristmas'.\n"
                                            f"  2. Always include a broad category tag "
                                            f"when the content clearly belongs to one: "
                                            f"'food', 'music', 'travel', 'fitness', "
                                            f"'art', 'fashion', 'technology', etc.\n"
                                            f"  3. Include relevant caption hashtags "
                                            f"(split any compound hashtags into "
                                            f"separate tags).\n"
                                            f"  4. Add specific descriptive tags beyond "
                                            f"the broad category (e.g. 'chocolate', "
                                            f"'recipe', 'guitar', 'jazz').\n"
                                            f'- "summary": a one-sentence summary\n'
                                            f"{CONTENT_TYPE_PROMPT_FRAGMENT}\n\n"
                                            f"{hashtag_hint}"
                                            f"Caption:\n{caption[:2000]}\n\n"
                                            f"Respond with only valid JSON, no trailing commas."
                                        ),
                                    }],
                                )

                                raw = response.choices[0].message.content or ""
                                analysis = parse_llm_json(raw, external_id)

                            llm_tags = analysis.get("tags", [])
                            merged_tags = list(
                                dict.fromkeys(caption_hashtags + llm_tags)
                            )
                            llm_span.set_attribute("item.tags", str(merged_tags))

                            llm_summary = analysis.get("summary") or ""
                            if llm_summary:
                                display_title = llm_summary
                            elif caption:
                                display_title = caption[:120].rstrip()
                                if len(caption) > 120:
                                    display_title += "…"
                            else:
                                display_title = external_id

                            raw_ct = analysis.get("content_type", "other")
                            content_type = raw_ct if raw_ct in CONTENT_TYPES else "other"

                            cursor.execute("""
                                INSERT INTO saved_items
                                    (source, external_id, type, title, body,
                                     uri, source_context, tags, summary, content_type)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (source, external_id) DO NOTHING
                                RETURNING id
                            """, (
                                "instagram",
                                external_id,
                                item["type"],
                                display_title,
                                caption,
                                item["uri"],
                                collection_name,
                                merged_tags,
                                analysis.get("summary"),
                                content_type,
                            ))
                            conn.commit()
                            row = cursor.fetchone()
                            if row:
                                inserted_ids.append(row[0])
                                logger.info(
                                    f"=== Stored {external_id} tags={merged_tags} (id={row[0]})"
                                )

                    insert_span.set_attribute("items.inserted", len(inserted_ids))
                    logger.info(f"=== Finished — {len(inserted_ids)} items stored")

    logger.info("=" * 80)
    return inserted_ids


@task
def warm_up_model(new_ids: list[int]) -> list[int]:
    """Warm up Ollama before LLM processing tasks. Pass-through for new item IDs."""
    if not new_ids:
        logger.info("No new items — skipping model warm-up.")
        return []
    warm_up_ollama()
    return new_ids


@task
def estimate_time(new_ids: list[int]) -> list[dict]:
    """Estimate build effort for newly-imported Instagram items."""
    return run_estimate_items(new_ids, "instagram")


@task
def extract_structure(new_ids: list[int]) -> list[dict]:
    """Extract structured data for recipe/project Instagram items."""
    return run_extract_structure(new_ids, "instagram")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    schedule="0 * * * *",   # hourly; check_schedule_and_fetch skips if no slot due
    start_date=pendulum.datetime(2025, 8, 30, tz="UTC"),
    catchup=False,
)
def instagram_import():
    posts = check_schedule_and_fetch()
    new_ids = analyse_and_store(posts)
    warmed_ids = warm_up_model(new_ids)
    estimate_time(warmed_ids)
    extract_structure(warmed_ids)


instagram_import()
