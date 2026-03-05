from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
import json
import logging
import os
from typing import Iterator

import pendulum
import praw
import prawcore

from instagrapi import Client as InstagramClient
from instagrapi.exceptions import LoginRequired, ChallengeRequired

from opentelemetry import trace
from opentelemetry.propagate import inject, extract as otel_extract
from opentelemetry.trace import SpanKind
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task, Variable
from airflow.traces import otel_tracer
from airflow.traces.tracer import Trace

logger = logging.getLogger("airflow.social_archive_cleanup")

reddit = praw.Reddit(
    client_id=Variable.get("REDDIT_CLIENT_ID"),
    client_secret=Variable.get("REDDIT_CLIENT_SECRET"),
    user_agent="proffalken-airflow",
    username=Variable.get("REDDIT_USER"),
    password=Variable.get("REDDIT_PASSWORD"),
)


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


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task
def get_flagged_items(ti) -> list[dict]:
    """Fetch all items flagged for deletion from the archive database."""
    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    task_provider = create_task_provider(ti.task_id)
    parent_context = resolve_parent_context(ti, otel_task_tracer)

    with task_root_span(ti, task_provider, parent_context) as span:
        hook = PostgresHook(postgres_conn_id="social_archive_db")
        rows = hook.get_records(
            "SELECT id, source, external_id, type "
            "FROM saved_items WHERE flagged_for_deletion = true"
        )
        items = [
            {"id": r[0], "source": r[1], "external_id": r[2], "type": r[3]}
            for r in rows
        ]
        by_source = {}
        for item in items:
            by_source.setdefault(item["source"], 0)
            by_source[item["source"]] += 1
        span.set_attribute("flagged.total", len(items))
        logger.info(f"Found {len(items)} items flagged for deletion: {by_source}")

    task_provider.force_flush()
    return items


@task
def cleanup_reddit(flagged_items: list[dict], ti) -> list[int]:
    """Unsave flagged Reddit items from the authenticated user's Reddit profile.

    Only performs an "unsave" — it does NOT attempt to delete the post itself.
    Items that no longer exist on Reddit (already deleted by OP etc.) are treated
    as successfully cleaned and will be removed from the local archive.

    Returns the DB row IDs of all items that are safe to delete from the archive.
    To add a new source, create an analogous task and add it to the DAG.
    """
    reddit_items = [i for i in flagged_items if i["source"] == "reddit"]

    if not reddit_items:
        logger.info("No Reddit items flagged for cleanup")
        return []

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    task_provider = create_task_provider(ti.task_id)
    parent_context = resolve_parent_context(
        ti, otel_task_tracer, previous_task_id="get_flagged_items"
    )

    cleaned_ids: list[int] = []
    failed_ids: list[str] = []

    with task_root_span(ti, task_provider, parent_context) as span:
        span.set_attribute("reddit.items_to_clean", len(reddit_items))

        for item in reddit_items:
            external_id = item["external_id"]
            item_type = item["type"]

            with otel_task_tracer.start_child_span(
                span_name=f"reddit.unsave.{external_id}"
            ) as item_span:
                item_span.set_attribute("item.external_id", external_id)
                item_span.set_attribute("item.type", item_type)

                try:
                    if item_type == "post":
                        reddit.submission(id=external_id).unsave()
                    else:
                        reddit.comment(id=external_id).unsave()

                    cleaned_ids.append(item["id"])
                    logger.info(f"Unsaved Reddit {item_type} {external_id}")

                except prawcore.exceptions.NotFound:
                    # Post/comment already deleted from Reddit; nothing to unsave,
                    # but it's safe to remove from our archive.
                    logger.warning(
                        f"Reddit {item_type} {external_id} not found on Reddit "
                        f"(deleted by OP?); removing from archive regardless"
                    )
                    cleaned_ids.append(item["id"])

                except Exception as exc:
                    # Transient error (network, rate-limit, auth); leave in DB
                    # so the next run can retry.
                    logger.error(
                        f"Failed to unsave Reddit {item_type} {external_id}: {exc}"
                    )
                    item_span.set_attribute("error", str(exc))
                    failed_ids.append(external_id)

        span.set_attribute("reddit.items_cleaned", len(cleaned_ids))
        span.set_attribute("reddit.items_failed", len(failed_ids))
        logger.info(
            f"Reddit cleanup: {len(cleaned_ids)} cleaned, {len(failed_ids)} failed"
        )

    task_provider.force_flush()
    return cleaned_ids


def _get_instagram_client() -> InstagramClient:
    """Return an authenticated instagrapi Client, reusing a saved session."""
    cl = InstagramClient()
    cl.delay_range = [1, 3]
    session_str = Variable.get("INSTAGRAM_SESSION", default="")
    if session_str:
        try:
            cl.load_settings(json.loads(session_str))
        except Exception as exc:
            logger.warning(f"Could not load Instagram session: {exc}")
    try:
        cl.login(
            Variable.get("INSTAGRAM_USERNAME"),
            Variable.get("INSTAGRAM_PASSWORD"),
        )
    except (LoginRequired, ChallengeRequired) as exc:
        raise RuntimeError("Instagram login failed — check Variables and clear INSTAGRAM_SESSION") from exc
    Variable.set("INSTAGRAM_SESSION", json.dumps(cl.get_settings()))
    return cl


@task
def cleanup_instagram(flagged_items: list[dict], ti) -> list[int]:
    """Unsave flagged Instagram items from the authenticated user's saved posts.

    Calls media_unsave() for each item. Items that no longer exist on Instagram
    are treated as successfully cleaned. Transient errors are logged and retried
    on the next run.
    """
    instagram_items = [i for i in flagged_items if i["source"] == "instagram"]

    if not instagram_items:
        logger.info("No Instagram items flagged for cleanup")
        return []

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    task_provider = create_task_provider(ti.task_id)
    parent_context = resolve_parent_context(
        ti, otel_task_tracer, previous_task_id="get_flagged_items"
    )

    cl = _get_instagram_client()
    cleaned_ids: list[int] = []
    failed_ids: list[str] = []

    with task_root_span(ti, task_provider, parent_context) as span:
        span.set_attribute("instagram.items_to_clean", len(instagram_items))

        for item in instagram_items:
            external_id = item["external_id"]
            item_type = item["type"]

            with otel_task_tracer.start_child_span(
                span_name=f"instagram.unsave.{external_id}"
            ) as item_span:
                item_span.set_attribute("item.external_id", external_id)
                item_span.set_attribute("item.type", item_type)

                try:
                    cl.media_unsave(external_id)
                    cleaned_ids.append(item["id"])
                    logger.info(f"Unsaved Instagram {item_type} {external_id}")

                except Exception as exc:
                    exc_str = str(exc).lower()
                    if any(word in exc_str for word in ("not found", "404", "deleted", "media_not_found")):
                        logger.warning(
                            f"Instagram {item_type} {external_id} not found "
                            f"(deleted?); removing from archive regardless"
                        )
                        cleaned_ids.append(item["id"])
                    else:
                        logger.error(f"Failed to unsave Instagram {item_type} {external_id}: {exc}")
                        item_span.set_attribute("error", str(exc))
                        failed_ids.append(external_id)

        span.set_attribute("instagram.items_cleaned", len(cleaned_ids))
        span.set_attribute("instagram.items_failed", len(failed_ids))
        logger.info(
            f"Instagram cleanup: {len(cleaned_ids)} cleaned, {len(failed_ids)} failed"
        )

    task_provider.force_flush()
    return cleaned_ids


@task
def remove_cleaned_items(cleaned_id_lists: list[list[int]], ti) -> None:
    """Delete successfully cleaned items from the archive database.

    Accepts a list-of-lists (one list per source cleanup task) so new
    source tasks can be wired in without changing this function's signature.
    """
    all_ids = [id_ for sublist in cleaned_id_lists for id_ in sublist]

    if not all_ids:
        logger.info("No items to remove from archive database")
        return

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    task_provider = create_task_provider(ti.task_id)
    # Multiple upstream tasks — fall back to Airflow carrier for context
    parent_context = resolve_parent_context(ti, otel_task_tracer)

    with task_root_span(ti, task_provider, parent_context) as span:
        hook = PostgresHook(postgres_conn_id="social_archive_db")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM saved_items WHERE id = ANY(%s)",
                    (all_ids,),
                )
            conn.commit()

        span.set_attribute("items.deleted", len(all_ids))
        logger.info(f"Removed {len(all_ids)} items from archive database")

    task_provider.force_flush()


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    schedule=timedelta(hours=6),
    start_date=pendulum.datetime(2025, 8, 30, tz="UTC"),
    catchup=False,
)
def social_archive_cleanup():
    flagged = get_flagged_items()

    reddit_cleaned = cleanup_reddit(flagged)
    instagram_cleaned = cleanup_instagram(flagged)
    # Add new sources here, e.g.:
    #   mastodon_cleaned = cleanup_mastodon(flagged)

    remove_cleaned_items([reddit_cleaned, instagram_cleaned])


social_archive_cleanup()
