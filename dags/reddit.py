from __future__ import annotations

from datetime import timedelta
import logging
import json

import pendulum

from openai import OpenAI

from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import chain, dag, task, Variable
from airflow.traces import otel_tracer
from airflow.traces.tracer import Trace

import praw

from airflow_otel import instrument_task_context, get_meter
from dag_utils import instrument_llm, instrument_requests, parse_llm_json

logger = logging.getLogger("airflow.reddit_dag")

@task
def get_saved_posts(ti):
    logger.info("=" * 80)
    logger.info(f"Getting saved posts - DAG: {ti.dag_id}, Run: {ti.run_id}")

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)

    reddit = praw.Reddit(
        client_id=Variable.get("REDDIT_CLIENT_ID"),
        client_secret=Variable.get("REDDIT_CLIENT_SECRET"),
        user_agent="proffalken-airflow",
        username=Variable.get("REDDIT_USER"),
        password=Variable.get("REDDIT_PASSWORD"),
    )

    # Fetch already-known IDs so we only process new items
    try:
        hook = PostgresHook(postgres_conn_id="social_archive_db")
        rows = hook.get_records("SELECT external_id FROM saved_items WHERE source = 'reddit'")
        known_ids = {row[0] for row in rows}
        logger.info(f"=== {len(known_ids)} items already in database, will skip these")
    except Exception:
        logger.info("=== saved_items table not found yet, treating all items as new")
        known_ids = set()

    sorted_posts = {}
    post_count = 0
    comment_count = 0

    with instrument_task_context({}) as span:
        instrument_requests()
        meter = get_meter("reddit.saved")
        post_gauge = meter.create_gauge(
            "reddit.saved.post_count",
            unit="1",
            description="Number of new saved Reddit posts fetched in this run",
        )
        comment_gauge = meter.create_gauge(
            "reddit.saved.comment_count",
            unit="1",
            description="Number of new saved Reddit comments fetched in this run",
        )

        with otel_task_tracer.start_child_span(span_name="fetch_saved_items"):
            for item in reddit.user.me().saved(limit=None):
                if item.id in known_ids:
                    continue

                sr_name = item.subreddit.display_name

                if isinstance(item, praw.models.Submission):
                    item_object = {
                        "external_id": item.id,
                        "type": "post",
                        "title": item.title,
                        "body": item.selftext,
                        "uri": item.url,
                    }
                    post_count += 1
                    if post_count % 10 == 0:
                        logger.info(f"=== Fetched {post_count} new posts")
                else:
                    item_object = {
                        "external_id": item.id,
                        "type": "comment",
                        "title": None,
                        "body": item.body,
                        "uri": item.permalink,
                    }
                    comment_count += 1
                    if comment_count % 10 == 0:
                        logger.info(f"=== Fetched {comment_count} new comments")

                sorted_posts.setdefault(sr_name, []).append(item_object)

            logger.info(f"=== {post_count} new posts and {comment_count} new comments to process")

        post_gauge.set(post_count)
        comment_gauge.set(comment_count)

    logger.info("Reddit saved post download finished.")
    return sorted_posts


@task
def analyse_and_store(sorted_posts, ti):
    logger.info("=" * 80)
    logger.info(f"Analysing and storing posts - DAG: {ti.dag_id}, Run: {ti.run_id}")

    total = sum(len(items) for items in sorted_posts.values())
    if total == 0:
        logger.info("No new items to analyse, skipping.")
        raise AirflowSkipException("No new items to analyse")

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)

    client = OpenAI(base_url="http://ollama.ollama.svc.cluster.local:11434/v1", api_key="ollama")
    hook = PostgresHook(postgres_conn_id="social_archive_db")

    with instrument_task_context({}) as span:
        instrument_requests()
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
                        );
                        ALTER TABLE saved_items ADD COLUMN IF NOT EXISTS flagged_for_deletion BOOLEAN NOT NULL DEFAULT false
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS saved_items_fts
                        ON saved_items
                        USING gin(to_tsvector('english', coalesce(title,'') || ' ' || coalesce(body,'')))
                    """)
                    conn.commit()

                inserted = 0
                with otel_task_tracer.start_child_span(span_name="analyse_and_insert_items") as insert_span:
                    for subreddit, items in sorted_posts.items():
                        for item in items:
                            label = item['title'] or item['body'][:50]
                            logger.info(f"Analysing: {label} ({item['type']}) from r/{subreddit}")

                            content = f"Title: {item.get('title') or ''}\n\nBody: {item['body'][:2000]}"
                            with otel_task_tracer.start_child_span(span_name="llm.analyse_item") as llm_span:
                                llm_span.set_attribute("item.external_id", item["external_id"])
                                llm_span.set_attribute("item.type", item["type"])
                                llm_span.set_attribute("item.subreddit", subreddit)

                                response = client.chat.completions.create(
                                    model="dolphin-mistral:latest",
                                    max_tokens=512,
                                    messages=[{
                                        "role": "user",
                                        "content": (
                                            f"Analyse this Reddit {item['type']} from r/{subreddit} "
                                            f"and return a JSON object with:\n"
                                            f'- "tags": a list of 3-8 relevant keyword tags (lowercase, no spaces)\n'
                                            f'- "summary": a one-sentence summary\n\n'
                                            f"Content:\n{content}\n\n"
                                            f"Respond with only valid JSON."
                                        ),
                                    }],
                                )

                                raw = response.choices[0].message.content or ""
                                analysis = parse_llm_json(raw, item["external_id"])

                                llm_span.set_attribute("item.tags", str(analysis.get("tags", [])))

                            cursor.execute("""
                                INSERT INTO saved_items
                                    (source, external_id, type, title, body, uri, source_context, tags, summary)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (source, external_id) DO NOTHING
                            """, (
                                "reddit",
                                item["external_id"],
                                item["type"],
                                item.get("title"),
                                item["body"],
                                item["uri"],
                                subreddit,
                                analysis.get("tags", []),
                                analysis.get("summary"),
                            ))
                            conn.commit()
                            inserted += cursor.rowcount
                            logger.info(f"=== Stored item {item['external_id']} ({inserted} total)")

                    insert_span.set_attribute("items.inserted", inserted)
                    logger.info(f"=== Finished — {inserted} items stored")

    logger.info("=" * 80)


@dag(
    schedule=timedelta(hours=3),
    start_date=pendulum.datetime(2025, 8, 30, tz="UTC"),
    catchup=False,
)
def reddit_import():
    posts = get_saved_posts()
    analyse_and_store(posts)


reddit_import()
