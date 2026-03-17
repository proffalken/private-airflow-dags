from __future__ import annotations

from datetime import timedelta
import logging
import re

import pendulum

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from openai import OpenAI

from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task, Variable
from airflow.traces import otel_tracer
from airflow.traces.tracer import Trace

from otel_utils import (
    create_task_provider,
    create_meter_provider,
    instrument_llm,
    resolve_parent_context,
    task_root_span,
    parse_llm_json,
)

logger = logging.getLogger("airflow.youtube_dag")

YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]
YOUTUBE_TOKEN_URI = "https://oauth2.googleapis.com/token"


def get_youtube_client():
    """Return an authenticated YouTube API client using stored OAuth2 credentials."""
    creds = Credentials(
        token=None,
        refresh_token=Variable.get("YOUTUBE_REFRESH_TOKEN"),
        token_uri=YOUTUBE_TOKEN_URI,
        client_id=Variable.get("YOUTUBE_CLIENT_ID"),
        client_secret=Variable.get("YOUTUBE_CLIENT_SECRET"),
        scopes=YOUTUBE_SCOPES,
    )
    creds.refresh(Request())
    return build("youtube", "v3", credentials=creds, cache_discovery=False)


def playlist_name_to_tag(name: str) -> str:
    """Normalise a playlist name into a lowercase hyphenated tag."""
    tag = name.lower().strip()
    tag = re.sub(r"[^\w\s-]", "", tag)
    tag = re.sub(r"[\s_]+", "-", tag)
    return re.sub(r"-+", "-", tag).strip("-")


# ---------------------------------------------------------------------------
# YouTube API helpers
# ---------------------------------------------------------------------------

def _fetch_all_playlist_items(youtube, playlist_id: str) -> list[str]:
    """Return all video IDs in a playlist, handling pagination."""
    video_ids = []
    request = youtube.playlistItems().list(
        part="contentDetails",
        playlistId=playlist_id,
        maxResults=50,
    )
    while request:
        response = request.execute()
        for item in response.get("items", []):
            vid = item["contentDetails"].get("videoId")
            if vid:
                video_ids.append(vid)
        next_token = response.get("nextPageToken")
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_token,
        ) if next_token else None
    return video_ids


def _fetch_video_details(youtube, video_ids: list[str]) -> dict[str, dict]:
    """Batch-fetch video snippet for up to 50 IDs at a time."""
    details = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        response = youtube.videos().list(
            part="snippet",
            id=",".join(batch),
        ).execute()
        for item in response.get("items", []):
            details[item["id"]] = item["snippet"]
    return details


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task
def get_playlist_videos(ti) -> dict[str, list[dict]]:
    """Fetch all videos from every user playlist, skipping already-archived items."""
    logger.info("=" * 80)
    logger.info(f"Getting YouTube playlist videos — DAG: {ti.dag_id}, Run: {ti.run_id}")

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    task_provider = create_task_provider("youtube-import", ti.run_id)
    meter_provider = create_meter_provider("youtube-import", ti.run_id)
    parent_context = resolve_parent_context(ti, otel_task_tracer)

    meter = meter_provider.get_meter("youtube.playlists")
    video_gauge = meter.create_gauge(
        "youtube.playlist.video_count",
        unit="1",
        description="Number of new YouTube playlist videos fetched in this run",
    )

    try:
        hook = PostgresHook(postgres_conn_id="social_archive_db")
        rows = hook.get_records(
            "SELECT external_id FROM saved_items WHERE source = 'youtube'"
        )
        known_ids = {row[0] for row in rows}
        logger.info(f"=== {len(known_ids)} YouTube items already in database, skipping")
    except Exception:
        logger.info("=== saved_items table not found yet, treating all items as new")
        known_ids = set()

    youtube = get_youtube_client()
    sorted_videos: dict[str, list[dict]] = {}
    new_count = 0

    with task_root_span(ti, task_provider, parent_context) as span:
        with otel_task_tracer.start_child_span(span_name="fetch_playlists"):
            playlists = []
            request = youtube.playlists().list(
                part="snippet",
                mine=True,
                maxResults=50,
            )
            while request:
                response = request.execute()
                playlists.extend(response.get("items", []))
                next_token = response.get("nextPageToken")
                request = youtube.playlists().list(
                    part="snippet",
                    mine=True,
                    maxResults=50,
                    pageToken=next_token,
                ) if next_token else None

            logger.info(f"Found {len(playlists)} playlists")

        for playlist in playlists:
            playlist_id = playlist["id"]
            playlist_name = playlist["snippet"]["title"]
            logger.info(f"Fetching playlist: {playlist_name!r} (id={playlist_id})")

            with otel_task_tracer.start_child_span(
                span_name="fetch playlist"
            ) as fetch_span:
                fetch_span.set_attribute("youtube.playlist.id", playlist_id)
                fetch_span.set_attribute("youtube.playlist.name", playlist_name)
                video_ids = _fetch_all_playlist_items(youtube, playlist_id)
                new_ids = [vid for vid in video_ids if vid not in known_ids]
                logger.info(f"  {len(video_ids)} total, {len(new_ids)} new")

                if not new_ids:
                    continue

                details = _fetch_video_details(youtube, new_ids)

            for video_id in new_ids:
                snippet = details.get(video_id)
                if not snippet:
                    logger.warning(f"No snippet for video {video_id}, skipping")
                    continue

                # YouTube's own tags — cap at 20 to keep merged tag list sane
                yt_tags = [t.lower() for t in (snippet.get("tags") or [])][:20]

                sorted_videos.setdefault(playlist_name, []).append({
                    "external_id": video_id,
                    "type": "video",
                    "title": snippet.get("title") or "",
                    "body": snippet.get("description") or "",
                    "uri": f"https://www.youtube.com/watch?v={video_id}",
                    "channel": snippet.get("channelTitle") or "",
                    "youtube_tags": yt_tags,
                })
                new_count += 1

        span.set_attribute("youtube.new_videos", new_count)
        logger.info(f"=== {new_count} new YouTube videos to process")

    video_gauge.set(new_count)
    task_provider.force_flush()
    meter_provider.force_flush()
    return sorted_videos


@task
def analyse_and_store(sorted_videos: dict[str, list[dict]], ti) -> None:
    """LLM-analyse each video and store in the archive DB.

    Tag priority (deduplicated, in order):
      1. Normalised playlist name  — always first, guaranteed present
      2. YouTube's own video tags  — creator-supplied metadata
      3. LLM-generated tags        — fills gaps, adds broader categories
    """
    logger.info("=" * 80)
    logger.info(f"Analysing and storing YouTube videos — DAG: {ti.dag_id}, Run: {ti.run_id}")

    total = sum(len(items) for items in sorted_videos.values())
    if total == 0:
        logger.info("No new items to analyse, skipping.")
        raise AirflowSkipException("No new YouTube videos to analyse")

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    task_provider = create_task_provider("youtube-import", ti.run_id)
    instrument_llm(task_provider)
    parent_context = resolve_parent_context(
        ti, otel_task_tracer, previous_task_id="get_playlist_videos"
    )

    client = OpenAI(base_url="http://ollama.ollama.svc.cluster.local:11434/v1", api_key="ollama")
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
                    for playlist_name, items in sorted_videos.items():
                        playlist_tag = playlist_name_to_tag(playlist_name)

                        for item in items:
                            video_id = item["external_id"]
                            title = item["title"]
                            description = item["body"]
                            channel = item["channel"]
                            youtube_tags = item.get("youtube_tags", [])

                            logger.info(
                                f"Analysing: {title!r} "
                                f"(channel={channel!r}, playlist={playlist_name!r})"
                            )

                            with otel_task_tracer.start_child_span(
                                span_name="llm.analyse_item"
                            ) as llm_span:
                                llm_span.set_attribute("item.external_id", video_id)
                                llm_span.set_attribute("item.playlist", playlist_name)

                                tag_hint = (
                                    f"YouTube tags already on this video: "
                                    f"{', '.join(youtube_tags)}\n"
                                    if youtube_tags else ""
                                )

                                content = (
                                    f"Title: {title}\n"
                                    f"Channel: {channel}\n"
                                    f"Description: {description[:1500]}"
                                )

                                response = client.chat.completions.create(
                                    model="dolphin-mistral:latest",
                                    max_tokens=512,
                                    messages=[{
                                        "role": "user",
                                        "content": (
                                            f"Analyse this YouTube video from the playlist "
                                            f"{playlist_name!r} and return a JSON object with:\n"
                                            f'- "tags": a list of 3-8 single-concept keyword tags '
                                            f"(lowercase, no spaces).\n"
                                            f"  TAGGING RULES — follow these strictly:\n"
                                            f"  1. Each tag must represent ONE concept. "
                                            f"Never combine concepts.\n"
                                            f"  2. Always include a broad category tag: "
                                            f"'music', 'technology', 'cooking', 'gaming', etc.\n"
                                            f"  3. Add specific descriptive tags beyond the "
                                            f"broad category (e.g. 'python', 'jazz', 'recipe').\n"
                                            f"  4. Do NOT include the playlist name as a tag — "
                                            f"it will be added automatically.\n"
                                            f'- "summary": a one-sentence summary\n\n'
                                            f"{tag_hint}"
                                            f"Video:\n{content}\n\n"
                                            f"Respond with only valid JSON, no trailing commas."
                                        ),
                                    }],
                                )

                                raw = response.choices[0].message.content or ""
                                analysis = parse_llm_json(raw, video_id)

                            llm_tags = analysis.get("tags", [])
                            # playlist tag → YouTube's own tags → LLM tags (deduplicated)
                            merged_tags = list(dict.fromkeys(
                                [playlist_tag] + youtube_tags + llm_tags
                            ))
                            llm_span.set_attribute("item.tags", str(merged_tags))

                            cursor.execute("""
                                INSERT INTO saved_items
                                    (source, external_id, type, title, body,
                                     uri, source_context, tags, summary)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (source, external_id) DO NOTHING
                            """, (
                                "youtube",
                                video_id,
                                "video",
                                title,
                                description,
                                item["uri"],
                                playlist_name,
                                merged_tags,
                                analysis.get("summary"),
                            ))
                            conn.commit()
                            inserted += cursor.rowcount
                            logger.info(
                                f"=== Stored {video_id} "
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
def youtube_import():
    videos = get_playlist_videos()
    analyse_and_store(videos)


youtube_import()
