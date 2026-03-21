from __future__ import annotations

from datetime import timedelta
import json
import logging

import pendulum
import praw
import prawcore
import requests

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from instagrapi import Client as InstagramClient
from instagrapi.exceptions import LoginRequired, ChallengeRequired

import requests

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task, Variable
from airflow.traces import otel_tracer
from airflow.traces.tracer import Trace

from airflow_otel import instrument_task_context, get_meter

logger = logging.getLogger("airflow.social_archive_cleanup")


def _get_reddit_client():
    return praw.Reddit(
        client_id=Variable.get("REDDIT_CLIENT_ID"),
        client_secret=Variable.get("REDDIT_CLIENT_SECRET"),
        user_agent="proffalken-airflow",
        username=Variable.get("REDDIT_USER"),
        password=Variable.get("REDDIT_PASSWORD"),
    )


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task
def get_flagged_items(ti) -> list[dict]:
    """Fetch all items flagged for deletion from the archive database."""
    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)

    with instrument_task_context({}) as span:
        meter = get_meter("social.archive.cleanup")
        flagged_gauge = meter.create_gauge(
            "social.archive.cleanup.flagged",
            unit="1",
            description="Number of items flagged for deletion at the start of this cleanup run",
        )
        hook = PostgresHook(postgres_conn_id="social_archive_db")
        rows = hook.get_records(
            "SELECT id, source, external_id, type "
            "FROM saved_items WHERE flagged_for_deletion = true"
        )
        items = [
            {"id": r[0], "source": r[1], "external_id": r[2], "type": r[3]}
            for r in rows
        ]
        by_source: dict[str, int] = {}
        for item in items:
            by_source.setdefault(item["source"], 0)
            by_source[item["source"]] += 1
        span.set_attribute("flagged.total", len(items))
        logger.info(f"Found {len(items)} items flagged for deletion: {by_source}")

        for source, count in by_source.items():
            flagged_gauge.set(count, {"source": source})

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
    cleaned_ids: list[int] = []
    failed_ids: list[str] = []
    reddit = _get_reddit_client()

    with instrument_task_context({}) as span:
        meter = get_meter("social.archive.cleanup")
        cleaned_gauge = meter.create_gauge(
            "social.archive.cleanup.cleaned",
            unit="1",
            description="Number of items successfully cleaned from the source platform in this run",
        )
        failed_gauge = meter.create_gauge(
            "social.archive.cleanup.failed",
            unit="1",
            description="Number of items that failed cleanup from the source platform in this run",
        )
        span.set_attribute("reddit.items_to_clean", len(reddit_items))

        with otel_task_tracer.start_child_span(span_name="reddit.unsave batch") as batch_span:
            batch_span.set_attribute("batch.size", len(reddit_items))

            for item in reddit_items:
                external_id = item["external_id"]
                item_type = item["type"]
                try:
                    if item_type == "post":
                        reddit.submission(id=external_id).unsave()
                    else:
                        reddit.comment(id=external_id).unsave()
                    cleaned_ids.append(item["id"])
                    logger.info(f"Unsaved Reddit {item_type} {external_id}")

                except prawcore.exceptions.NotFound:
                    # Post/comment already deleted from Reddit — safe to remove from archive.
                    logger.warning(
                        f"Reddit {item_type} {external_id} not found on Reddit "
                        f"(deleted by OP?); removing from archive regardless"
                    )
                    cleaned_ids.append(item["id"])

                except Exception as exc:
                    # Transient error (network, rate-limit, auth); leave in DB for retry.
                    logger.error(
                        f"Failed to unsave Reddit {item_type} {external_id}: {exc}"
                    )
                    batch_span.add_event(
                        "item.failed",
                        {"item.external_id": external_id, "item.type": item_type, "error": str(exc)},
                    )
                    failed_ids.append(external_id)

            batch_span.set_attribute("batch.cleaned", len(cleaned_ids))
            batch_span.set_attribute("batch.failed", len(failed_ids))

        span.set_attribute("reddit.items_cleaned", len(cleaned_ids))
        span.set_attribute("reddit.items_failed", len(failed_ids))
        logger.info(f"Reddit cleanup: {len(cleaned_ids)} cleaned, {len(failed_ids)} failed")
        cleaned_gauge.set(len(cleaned_ids), {"source": "reddit"})
        failed_gauge.set(len(failed_ids), {"source": "reddit"})

    return cleaned_ids


def _get_instagram_client() -> InstagramClient:
    """Return an authenticated instagrapi Client, reusing a saved session."""
    cl = InstagramClient()
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
    cl = _get_instagram_client()
    cleaned_ids: list[int] = []
    failed_ids: list[str] = []

    with instrument_task_context({}) as span:
        meter = get_meter("social.archive.cleanup")
        cleaned_gauge = meter.create_gauge(
            "social.archive.cleanup.cleaned",
            unit="1",
            description="Number of items successfully cleaned from the source platform in this run",
        )
        failed_gauge = meter.create_gauge(
            "social.archive.cleanup.failed",
            unit="1",
            description="Number of items that failed cleanup from the source platform in this run",
        )
        span.set_attribute("instagram.items_to_clean", len(instagram_items))

        with otel_task_tracer.start_child_span(span_name="instagram.unsave batch") as batch_span:
            batch_span.set_attribute("batch.size", len(instagram_items))

            for item in instagram_items:
                external_id = item["external_id"]
                item_type = item["type"]
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
                        batch_span.add_event(
                            "item.failed",
                            {"item.external_id": external_id, "item.type": item_type, "error": str(exc)},
                        )
                        failed_ids.append(external_id)

            batch_span.set_attribute("batch.cleaned", len(cleaned_ids))
            batch_span.set_attribute("batch.failed", len(failed_ids))

        span.set_attribute("instagram.items_cleaned", len(cleaned_ids))
        span.set_attribute("instagram.items_failed", len(failed_ids))
        logger.info(f"Instagram cleanup: {len(cleaned_ids)} cleaned, {len(failed_ids)} failed")
        cleaned_gauge.set(len(cleaned_ids), {"source": "instagram"})
        failed_gauge.set(len(failed_ids), {"source": "instagram"})

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

    hook = PostgresHook(postgres_conn_id="social_archive_db")

    with instrument_task_context({}) as span:
        meter = get_meter("social.archive.cleanup")
        cleaned_gauge = meter.create_gauge(
            "social.archive.cleanup.cleaned",
            unit="1",
            description="Number of items successfully cleaned from the source platform in this run",
        )
        failed_gauge = meter.create_gauge(
            "social.archive.cleanup.failed",
            unit="1",
            description="Number of items that failed cleanup from the source platform in this run",
        )
        span.set_attribute("youtube.items_to_clean", len(youtube_items))

        with otel_task_tracer.start_child_span(span_name="youtube.remove batch") as batch_span:
            batch_span.set_attribute("batch.size", len(youtube_items))

            for item in youtube_items:
                video_id = item["external_id"]
                row = hook.get_first(
                    "SELECT source_context FROM saved_items WHERE id = %s",
                    parameters=(item["id"],),
                )
                playlist_name = row[0] if row else None

                if not playlist_name or playlist_name not in playlist_name_to_id:
                    logger.warning(
                        f"Playlist {playlist_name!r} not found for video {video_id}; "
                        f"removing from archive regardless"
                    )
                    cleaned_ids.append(item["id"])
                    continue

                playlist_id = playlist_name_to_id[playlist_name]
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
                    batch_span.add_event(
                        "item.failed",
                        {"item.external_id": video_id, "item.playlist": playlist_name or "", "error": str(exc)},
                    )
                    failed_ids.append(video_id)
                    continue

                if not pi_items:
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
                    logger.info(f"Removed video {video_id} from playlist {playlist_name!r}")
                except Exception as exc:
                    logger.error(
                        f"Failed to remove video {video_id} from playlist {playlist_name!r}: {exc}"
                    )
                    batch_span.add_event(
                        "item.failed",
                        {"item.external_id": video_id, "item.playlist": playlist_name or "", "error": str(exc)},
                    )
                    failed_ids.append(video_id)

            batch_span.set_attribute("batch.cleaned", len(cleaned_ids))
            batch_span.set_attribute("batch.failed", len(failed_ids))

        span.set_attribute("youtube.items_cleaned", len(cleaned_ids))
        span.set_attribute("youtube.items_failed", len(failed_ids))
        logger.info(f"YouTube cleanup: {len(cleaned_ids)} cleaned, {len(failed_ids)} failed")
        cleaned_gauge.set(len(cleaned_ids), {"source": "youtube"})
        failed_gauge.set(len(failed_ids), {"source": "youtube"})

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
    token = Variable.get("GITHUB_TOKEN")
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    hook = PostgresHook(postgres_conn_id="social_archive_db")
    cleaned_ids: list[int] = []
    failed_ids: list[str] = []

    with instrument_task_context({}) as span:
        meter = get_meter("social.archive.cleanup")
        cleaned_gauge = meter.create_gauge(
            "social.archive.cleanup.cleaned",
            unit="1",
            description="Number of items successfully cleaned from the source platform in this run",
        )
        failed_gauge = meter.create_gauge(
            "social.archive.cleanup.failed",
            unit="1",
            description="Number of items that failed cleanup from the source platform in this run",
        )
        span.set_attribute("github.items_to_clean", len(github_items))

        with otel_task_tracer.start_child_span(span_name="github.unstar batch") as batch_span:
            batch_span.set_attribute("batch.size", len(github_items))

            for item in github_items:
                external_id = item["external_id"]
                # title is stored as "owner/repo" (full_name)
                row = hook.get_first(
                    "SELECT title FROM saved_items WHERE id = %s",
                    parameters=(item["id"],),
                )
                full_name = row[0] if row else None

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
                    batch_span.add_event(
                        "item.failed",
                        {"item.external_id": external_id, "item.full_name": full_name or "", "error": str(exc)},
                    )
                    failed_ids.append(external_id)

            batch_span.set_attribute("batch.cleaned", len(cleaned_ids))
            batch_span.set_attribute("batch.failed", len(failed_ids))

        span.set_attribute("github.items_cleaned", len(cleaned_ids))
        span.set_attribute("github.items_failed", len(failed_ids))
        logger.info(f"GitHub cleanup: {len(cleaned_ids)} cleaned, {len(failed_ids)} failed")
        cleaned_gauge.set(len(cleaned_ids), {"source": "github"})
        failed_gauge.set(len(failed_ids), {"source": "github"})

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

    with instrument_task_context({}) as span:
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
