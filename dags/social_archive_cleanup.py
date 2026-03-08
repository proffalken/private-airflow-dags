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
import requests

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

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


YOUTUBE_WRITE_SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]
YOUTUBE_TOKEN_URI = "https://oauth2.googleapis.com/token"


def _get_youtube_write_client():
    """Return an authenticated YouTube API client with write scope."""
    creds = Credentials(
        token=None,
        refresh_token=Variable.get("YOUTUBE_REFRESH_TOKEN"),
        token_uri=YOUTUBE_TOKEN_URI,
        client_id=Variable.get("YOUTUBE_CLIENT_ID"),
        client_secret=Variable.get("YOUTUBE_CLIENT_SECRET"),
        scopes=YOUTUBE_WRITE_SCOPES,
    )
    creds.refresh(Request())
    return build("youtube", "v3", credentials=creds, cache_discovery=False)


@task
def cleanup_youtube(flagged_items: list[dict], ti) -> list[int]:
    """Remove flagged YouTube videos from their playlists.

    Uses source_context (playlist name) to locate the correct playlist, then
    removes the playlistItem entry. Items already removed from the playlist are
    treated as successfully cleaned. Requires the OAuth token to have been
    issued with youtube.force-ssl scope (write access).
    """
    youtube_items = [i for i in flagged_items if i["source"] == "youtube"]

    if not youtube_items:
        logger.info("No YouTube items flagged for cleanup")
        return []

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    task_provider = create_task_provider(ti.task_id)
    parent_context = resolve_parent_context(
        ti, otel_task_tracer, previous_task_id="get_flagged_items"
    )

    youtube = _get_youtube_write_client()
    cleaned_ids: list[int] = []
    failed_ids: list[str] = []

    # Build a name→id map for all user playlists up front (avoids repeated API calls)
    playlist_name_to_id: dict[str, str] = {}
    request = youtube.playlists().list(part="snippet", mine=True, maxResults=50)
    while request:
        response = request.execute()
        for pl in response.get("items", []):
            playlist_name_to_id[pl["snippet"]["title"]] = pl["id"]
        next_token = response.get("nextPageToken")
        request = youtube.playlists().list(
            part="snippet", mine=True, maxResults=50, pageToken=next_token
        ) if next_token else None

    with task_root_span(ti, task_provider, parent_context) as span:
        span.set_attribute("youtube.items_to_clean", len(youtube_items))

        for item in youtube_items:
            video_id = item["external_id"]
            # source_context is not in the flagged_items query; fetch it now
            hook = PostgresHook(postgres_conn_id="social_archive_db")
            row = hook.get_first(
                "SELECT source_context FROM saved_items WHERE id = %s",
                parameters=(item["id"],),
            )
            playlist_name = row[0] if row else None

            with otel_task_tracer.start_child_span(
                span_name=f"youtube.remove.{video_id}"
            ) as item_span:
                item_span.set_attribute("item.external_id", video_id)
                item_span.set_attribute("item.playlist", playlist_name or "")

                if not playlist_name or playlist_name not in playlist_name_to_id:
                    logger.warning(
                        f"Playlist {playlist_name!r} not found for video {video_id}; "
                        f"removing from archive regardless"
                    )
                    cleaned_ids.append(item["id"])
                    continue

                playlist_id = playlist_name_to_id[playlist_name]

                # Find the playlistItem ID for this video in this playlist
                try:
                    pi_response = youtube.playlistItems().list(
                        part="id",
                        playlistId=playlist_id,
                        videoId=video_id,
                        maxResults=1,
                    ).execute()
                    pi_items = pi_response.get("items", [])
                except Exception as exc:
                    logger.error(
                        f"Failed to look up playlistItem for video {video_id} "
                        f"in playlist {playlist_name!r}: {exc}"
                    )
                    item_span.set_attribute("error", str(exc))
                    failed_ids.append(video_id)
                    continue

                if not pi_items:
                    # Already removed from the playlist
                    logger.warning(
                        f"Video {video_id} not found in playlist {playlist_name!r} "
                        f"(already removed?); removing from archive regardless"
                    )
                    cleaned_ids.append(item["id"])
                    continue

                playlist_item_id = pi_items[0]["id"]
                try:
                    youtube.playlistItems().delete(id=playlist_item_id).execute()
                    cleaned_ids.append(item["id"])
                    logger.info(
                        f"Removed video {video_id} from playlist {playlist_name!r}"
                    )
                except Exception as exc:
                    logger.error(
                        f"Failed to remove video {video_id} from playlist "
                        f"{playlist_name!r}: {exc}"
                    )
                    item_span.set_attribute("error", str(exc))
                    failed_ids.append(video_id)

        span.set_attribute("youtube.items_cleaned", len(cleaned_ids))
        span.set_attribute("youtube.items_failed", len(failed_ids))
        logger.info(
            f"YouTube cleanup: {len(cleaned_ids)} cleaned, {len(failed_ids)} failed"
        )

    task_provider.force_flush()
    return cleaned_ids


@task
def cleanup_github(flagged_items: list[dict], ti) -> list[int]:
    """Unstar flagged GitHub repos from the authenticated user's starred list.

    Calls DELETE /user/starred/{owner}/{repo} for each item. Repos that no
    longer exist (404) are treated as successfully cleaned. Transient errors
    are logged and retried on the next run.
    """
    github_items = [i for i in flagged_items if i["source"] == "github"]

    if not github_items:
        logger.info("No GitHub items flagged for cleanup")
        return []

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    task_provider = create_task_provider(ti.task_id)
    parent_context = resolve_parent_context(
        ti, otel_task_tracer, previous_task_id="get_flagged_items"
    )

    token = Variable.get("GITHUB_TOKEN")
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    hook = PostgresHook(postgres_conn_id="social_archive_db")
    cleaned_ids: list[int] = []
    failed_ids: list[str] = []

    with task_root_span(ti, task_provider, parent_context) as span:
        span.set_attribute("github.items_to_clean", len(github_items))

        for item in github_items:
            external_id = item["external_id"]

            # title is stored as "owner/repo" (full_name)
            row = hook.get_first(
                "SELECT title FROM saved_items WHERE id = %s",
                parameters=(item["id"],),
            )
            full_name = row[0] if row else None

            with otel_task_tracer.start_child_span(
                span_name=f"github.unstar.{external_id}"
            ) as item_span:
                item_span.set_attribute("item.external_id", external_id)
                item_span.set_attribute("item.full_name", full_name or "")

                if not full_name or "/" not in full_name:
                    logger.warning(
                        f"Cannot unstar GitHub repo {external_id}: "
                        f"no valid full_name {full_name!r}; removing from archive regardless"
                    )
                    cleaned_ids.append(item["id"])
                    continue

                try:
                    response = requests.delete(
                        f"https://api.github.com/user/starred/{full_name}",
                        headers=headers,
                        timeout=30,
                    )
                    if response.status_code in (204, 404):
                        # 204 = successfully unstarred; 404 = repo gone, nothing to unstar
                        if response.status_code == 404:
                            logger.warning(
                                f"GitHub repo {full_name} not found (deleted?); "
                                f"removing from archive regardless"
                            )
                        else:
                            logger.info(f"Unstarred GitHub repo {full_name}")
                        cleaned_ids.append(item["id"])
                    else:
                        response.raise_for_status()

                except Exception as exc:
                    logger.error(f"Failed to unstar GitHub repo {full_name}: {exc}")
                    item_span.set_attribute("error", str(exc))
                    failed_ids.append(external_id)

        span.set_attribute("github.items_cleaned", len(cleaned_ids))
        span.set_attribute("github.items_failed", len(failed_ids))
        logger.info(
            f"GitHub cleanup: {len(cleaned_ids)} cleaned, {len(failed_ids)} failed"
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
    youtube_cleaned = cleanup_youtube(flagged)
    github_cleaned = cleanup_github(flagged)

    remove_cleaned_items([reddit_cleaned, instagram_cleaned, youtube_cleaned, github_cleaned])


social_archive_cleanup()
