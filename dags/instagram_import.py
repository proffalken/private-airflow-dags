from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
import json
import logging
import os
import re
from typing import Iterator

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
        # No thumbnail available (Reels/newer format in carousel) — use
        # model_construct to bypass Pydantic URL validation. We don't use
        # carousel resource thumbnails anywhere, so this is safe.
        data["thumbnail_url"] = None
        return _ig_extractors.Resource.model_construct(**data)


_ig_extractors.extract_resource_v1 = _safe_extract_resource_v1

# Patch 2: extract_media_v1 crashes when the API omits the `code` field
# (shortcode). collection.py imports extract_media_v1 directly via
# `from ... import`, so we must patch it in that module's namespace.
from instagrapi.mixins import collection as _ig_collection
_orig_extract_media_v1 = _ig_collection.extract_media_v1


def _safe_extract_media_v1(data):
    data.setdefault("code", "")
    return _orig_extract_media_v1(data)


_ig_collection.extract_media_v1 = _safe_extract_media_v1

from openai import OpenAI

from opentelemetry import trace
from opentelemetry.propagate import inject, extract as otel_extract
from opentelemetry.trace import SpanKind
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task, Variable
from airflow.traces import otel_tracer
from airflow.traces.tracer import Trace

logger = logging.getLogger("airflow.instagram_dag")

_MEDIA_TYPE_NAMES = {1: "photo", 2: "video", 8: "album"}


def get_instagram_client() -> Client:
    """Return an authenticated instagrapi Client, reusing a saved session where possible.

    Avoids a full re-login (and the resulting "new device" email) by verifying
    the stored session with a lightweight API call before falling back to login().
    """
    cl = Client()
    cl.delay_range = [1, 3]  # random delay between API calls to avoid rate-limiting

    session_str = Variable.get("INSTAGRAM_SESSION", default="")
    if session_str:
        try:
            cl.load_settings(json.loads(session_str))
            cl.get_timeline_feed()  # verify session is still valid
            logger.info("Reused existing Instagram session — no login required")
            return cl
        except LoginRequired:
            logger.warning("Stored Instagram session expired, performing fresh login")
        except Exception as exc:
            logger.warning(f"Could not verify stored session ({exc}), performing fresh login")

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

    # Persist the updated session (device fingerprint + cookies) for next run
    Variable.set("INSTAGRAM_SESSION", json.dumps(cl.get_settings()))
    return cl


def extract_hashtags(caption: str | None) -> list[str]:
    """Return lowercase hashtags found in the caption, without the # prefix."""
    if not caption:
        return []
    return [tag.lower() for tag in re.findall(r"#(\w+)", caption)]


# ---------------------------------------------------------------------------
# OTEL helpers (same pattern as reddit.py)
# ---------------------------------------------------------------------------

def create_task_provider(task_id: str) -> TracerProvider:
    host = os.environ["AIRFLOW_OTEL_COLLECTOR_SERVICE_HOST"]
    port = os.environ["AIRFLOW_OTEL_COLLECTOR_SERVICE_PORT_OTLP_HTTP"]
    endpoint = f"http://{host}:{port}/v1/traces"
    logger.info(f"Creating task provider for '{task_id}' exporting to {endpoint}")
    resource = Resource.create({SERVICE_NAME: task_id})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    return provider


def create_meter_provider(task_id: str) -> MeterProvider:
    host = os.environ["AIRFLOW_OTEL_COLLECTOR_SERVICE_HOST"]
    port = os.environ["AIRFLOW_OTEL_COLLECTOR_SERVICE_PORT_OTLP_HTTP"]
    endpoint = f"http://{host}:{port}/v1/metrics"
    resource = Resource.create({SERVICE_NAME: task_id})
    exporter = OTLPMetricExporter(endpoint=endpoint)
    reader = PeriodicExportingMetricReader(exporter)
    return MeterProvider(resource=resource, metric_readers=[reader])


def resolve_parent_context(ti, otel_task_tracer, previous_task_id=None):
    if previous_task_id:
        carrier = ti.xcom_pull(task_ids=previous_task_id, key="otel_context")
        if carrier:
            logger.info(f"✓ Using handoff context from {previous_task_id}: {carrier}")
            return otel_extract(carrier)
        logger.warning(f"⚠ No XCom handoff from {previous_task_id}, falling back to Airflow carrier")

    if ti.context_carrier is not None:
        logger.info(f"✓ Using Airflow context carrier: {ti.context_carrier}")
        return otel_task_tracer.extract(ti.context_carrier)

    logger.error("❌ No parent context available")
    return None


@contextmanager
def task_root_span(ti, task_provider, parent_context) -> Iterator:
    tracer = trace.get_tracer(ti.task_id, tracer_provider=task_provider)
    with tracer.start_as_current_span(
        f"dag.{ti.dag_id}.task.{ti.task_id}",
        context=parent_context,
        kind=SpanKind.CONSUMER,
    ) as span:
        span.set_attribute("airflow.dag_id", ti.dag_id)
        span.set_attribute("airflow.task_id", ti.task_id)
        span.set_attribute("airflow.run_id", ti.run_id)
        yield span
        with tracer.start_as_current_span(
            f"task.{ti.task_id}.trigger_next",
            kind=SpanKind.PRODUCER,
        ):
            carrier = {}
            inject(carrier)
            ti.xcom_push(key="otel_context", value=carrier)
            logger.info(f"✓ Handoff context pushed to XCom: {carrier}")


def _parse_llm_json(raw: str, item_id: str) -> dict:
    """Robustly parse LLM response into {tags, summary}."""
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        end = -1 if lines[-1].strip() == "```" else len(lines)
        text = "\n".join(lines[1:end]).strip()

    # Fix common LLM mistakes: trailing commas before } or ]
    text = re.sub(r",\s*([}\]])", r"\1", text)

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        logger.warning(f"LLM response for {item_id} is not valid JSON ({exc}); raw={raw!r:.200}")
        return {"tags": [], "summary": ""}

    if isinstance(data, list):
        dicts = [x for x in data if isinstance(x, dict)]
        if not dicts:
            logger.warning(f"LLM response for {item_id} is a list with no dicts; raw={raw!r:.200}")
            return {"tags": [], "summary": ""}
        data = dicts[0]

    if not isinstance(data, dict):
        logger.warning(f"LLM response for {item_id} is {type(data).__name__}, not dict; raw={raw!r:.200}")
        return {"tags": [], "summary": ""}

    tags = data.get("tags", [])
    if isinstance(tags, str):
        tags = [t.strip().lower() for t in tags.replace(",", " ").split() if t.strip()]
    elif not isinstance(tags, list):
        tags = []
    else:
        tags = [str(t).strip().lower() for t in tags if t]

    return {"tags": tags, "summary": str(data.get("summary") or "")}


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task
def get_saved_posts(ti) -> dict[str, list[dict]]:
    """Fetch saved Instagram media from all collections plus any uncollected saves.

    Returns a dict keyed by collection name (or "Saved" for uncollected items),
    each value being a list of media dicts ready for analysis and storage.
    """
    logger.info("=" * 80)
    logger.info(f"Getting Instagram saved posts — DAG: {ti.dag_id}, Run: {ti.run_id}")

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    task_provider = create_task_provider(ti.task_id)
    meter_provider = create_meter_provider(ti.task_id)
    parent_context = resolve_parent_context(ti, otel_task_tracer)

    meter = meter_provider.get_meter("instagram.saved")
    media_gauge = meter.create_gauge(
        "instagram.saved.media_count",
        description="Number of new saved Instagram media items",
    )

    # Fetch already-known IDs to skip re-processing
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

    with task_root_span(ti, task_provider, parent_context) as span:
        with otel_task_tracer.start_child_span(span_name="fetch_collections"):
            collections = cl.collections()
            logger.info(f"Found {len(collections)} Instagram collections")

            # Separate auto-collections (e.g. "All posts") from user-created ones.
            # ALL_MEDIA_AUTO_COLLECTION contains every saved item; we use it at the
            # end to catch anything not in a named collection.
            all_posts_collection = None
            named_collections = []
            for c in collections:
                if c.type == "ALL_MEDIA_AUTO_COLLECTION":
                    all_posts_collection = c
                else:
                    named_collections.append(c)

            # Process user-created named collections first
            for collection in named_collections:
                collection_name = collection.name
                logger.info(f"Fetching collection: {collection_name!r} (id={collection.id})")

                with otel_task_tracer.start_child_span(
                    span_name=f"fetch_collection.{collection.id}"
                ):
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

        # Use the "All posts" auto-collection to pick up saves not in any
        # named collection (source_context = "Saved")
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
                        continue  # already captured under a named collection
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

    attrs = {"dag_id": ti.dag_id, "run_id": ti.run_id}
    media_gauge.set(new_count, attrs)

    task_provider.force_flush()
    meter_provider.force_flush()
    meter_provider.shutdown()
    return sorted_posts


@task
def analyse_and_store(sorted_posts: dict[str, list[dict]], ti) -> None:
    """Analyse each Instagram item with the LLM to enhance hashtag-derived tags,
    then store results in the archive database.

    Tags are seeded from caption hashtags; the LLM is asked to augment and
    add any missing relevant tags, then generate a one-sentence summary.
    The final tag list is the deduplicated union of both sources.
    """
    logger.info("=" * 80)
    logger.info(f"Analysing and storing — DAG: {ti.dag_id}, Run: {ti.run_id}")

    total = sum(len(items) for items in sorted_posts.values())
    if total == 0:
        logger.info("No new items to analyse, skipping.")
        raise AirflowSkipException("No new Instagram items to analyse")

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    task_provider = create_task_provider(ti.task_id)
    parent_context = resolve_parent_context(
        ti, otel_task_tracer, previous_task_id="get_saved_posts"
    )

    client = OpenAI(base_url="http://ollama.geeohh.svc.cluster.local:11434/v1", api_key="ollama")
    hook = PostgresHook(postgres_conn_id="social_archive_db")

    with task_root_span(ti, task_provider, parent_context):
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

                inserted = 0
                with otel_task_tracer.start_child_span(span_name="analyse_and_insert_items") as span:
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
                                    model="dolphin-mistral:latest",
                                    max_tokens=512,
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
                                            f'- "summary": a one-sentence summary\n\n'
                                            f"{hashtag_hint}"
                                            f"Caption:\n{caption[:2000]}\n\n"
                                            f"Respond with only valid JSON, no trailing commas."
                                        ),
                                    }],
                                )

                                raw = response.choices[0].message.content or ""
                                analysis = _parse_llm_json(raw, external_id)

                            # Merge caption hashtags with LLM tags, deduplicated
                            llm_tags = analysis.get("tags", [])
                            merged_tags = list(
                                dict.fromkeys(caption_hashtags + llm_tags)
                            )

                            llm_span.set_attribute("item.tags", str(merged_tags))

                            # Derive a display title so the item is always
                            # clickable in the frontend: prefer LLM summary,
                            # fall back to truncated caption.
                            llm_summary = analysis.get("summary") or ""
                            if llm_summary:
                                display_title = llm_summary
                            elif caption:
                                display_title = caption[:120].rstrip()
                                if len(caption) > 120:
                                    display_title += "…"
                            else:
                                display_title = external_id

                            cursor.execute("""
                                INSERT INTO saved_items
                                    (source, external_id, type, title, body,
                                     uri, source_context, tags, summary)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (source, external_id) DO NOTHING
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
                            ))
                            conn.commit()
                            inserted += cursor.rowcount
                            logger.info(
                                f"=== Stored {external_id} "
                                f"tags={merged_tags} ({inserted} total)"
                            )

                    span.set_attribute("items.inserted", inserted)
                    logger.info(f"=== Finished — {inserted} items stored")

    task_provider.force_flush()
    logger.info("=" * 80)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    schedule=timedelta(hours=3),
    start_date=pendulum.datetime(2025, 8, 30, tz="UTC"),
    catchup=False,
)
def instagram_import():
    posts = get_saved_posts()
    analyse_and_store(posts)


instagram_import()
