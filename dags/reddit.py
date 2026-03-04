from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
import logging
import os
from typing import Iterator

import pendulum

import requests
import random
import json

from openai import OpenAI

from opentelemetry.instrumentation.requests import RequestsInstrumentor
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
from airflow.sdk import chain, dag, task, Variable
from airflow.traces import otel_tracer
from airflow.traces.tracer import Trace

import praw

logger = logging.getLogger("airflow.reddit_dag")
_REQUESTS_INSTRUMENTED = True

reddit = praw.Reddit(
    client_id=Variable.get("REDDIT_CLIENT_ID"),
    client_secret=Variable.get("REDDIT_CLIENT_SECRET"),
    user_agent="proffalken-airflow",
    username=Variable.get("REDDIT_USER"),
    password=Variable.get("REDDIT_PASSWORD")
)


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


def instrument_requests(task_provider):
    global _REQUESTS_INSTRUMENTED
    if _REQUESTS_INSTRUMENTED:
        return
    RequestsInstrumentor().instrument(tracer_provider=task_provider)
    _REQUESTS_INSTRUMENTED = True


def resolve_parent_context(ti, otel_task_tracer, previous_task_id=None):
    """Resolve parent context from previous task's XCom handoff, or Airflow's carrier."""
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

        span_context = span.get_span_context()
        if span_context.trace_id == 0:
            logger.error("❌ CRITICAL: Span has trace_id = 0! OpenTelemetry not initialized!")
        else:
            logger.info(f"✓ Trace ID: {format(span_context.trace_id, '032x')}")
            logger.info(f"✓ Span ID: {format(span_context.span_id, '016x')}")

        yield span

        with tracer.start_as_current_span(
            f"task.{ti.task_id}.trigger_next",
            kind=SpanKind.PRODUCER,
        ):
            carrier = {}
            inject(carrier)
            ti.xcom_push(key="otel_context", value=carrier)
            logger.info(f"✓ Handoff context pushed to XCom: {carrier}")


@task
def get_saved_posts(ti):
    logger.info("=" * 80)
    logger.info(f"Getting saved posts - DAG: {ti.dag_id}, Run: {ti.run_id}")

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    task_provider = create_task_provider(ti.task_id)
    meter_provider = create_meter_provider(ti.task_id)
    parent_context = resolve_parent_context(ti, otel_task_tracer)

    meter = meter_provider.get_meter("reddit.saved")
    post_gauge = meter.create_gauge("reddit.saved.post_count", description="Number of new saved Reddit posts")
    comment_gauge = meter.create_gauge("reddit.saved.comment_count", description="Number of new saved Reddit comments")

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

    with task_root_span(ti, task_provider, parent_context) as span:
        current_span = trace.get_current_span()
        ctx = current_span.get_span_context()
        if ctx.trace_id != 0:
            logger.info(f"✓ Active trace: {format(ctx.trace_id, '032x')}")

        instrument_requests(task_provider)

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

    attrs = {"dag_id": ti.dag_id, "run_id": ti.run_id}
    post_gauge.set(post_count, attrs)
    comment_gauge.set(comment_count, attrs)

    task_provider.force_flush()
    meter_provider.force_flush()
    meter_provider.shutdown()
    logger.info("Reddit saved post download finished.")
    return sorted_posts


def _parse_llm_json(raw: str, item_id: str) -> dict:
    """
    Robustly parse the LLM response into {tags, summary}.

    Handles:
    - Markdown code fences (```json ... ```)
    - JSON array wrapping a single dict ([{...}])
    - tags returned as a comma-separated string instead of a list
    - Any other non-dict result
    """
    text = raw.strip()

    # Strip markdown code fences
    if text.startswith("```"):
        lines = text.splitlines()
        # Drop first line (```json or ```) and last ``` if present
        end = -1 if lines[-1].strip() == "```" else len(lines)
        text = "\n".join(lines[1:end]).strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        logger.warning(f"LLM response for {item_id} is not valid JSON ({e}); raw={raw!r:.200}")
        return {"tags": [], "summary": ""}

    # Unwrap a list — some models return [{...}] instead of {...}
    if isinstance(data, list):
        dicts = [x for x in data if isinstance(x, dict)]
        if not dicts:
            logger.warning(f"LLM response for {item_id} is a list with no dicts; raw={raw!r:.200}")
            return {"tags": [], "summary": ""}
        data = dicts[0]

    if not isinstance(data, dict):
        logger.warning(f"LLM response for {item_id} is {type(data).__name__}, not dict; raw={raw!r:.200}")
        return {"tags": [], "summary": ""}

    # Normalise tags — may arrive as a string ("python, airflow") or missing
    tags = data.get("tags", [])
    if isinstance(tags, str):
        tags = [t.strip().lower() for t in tags.replace(",", " ").split() if t.strip()]
    elif not isinstance(tags, list):
        tags = []
    else:
        tags = [str(t).strip().lower() for t in tags if t]

    return {"tags": tags, "summary": str(data.get("summary") or "")}


@task
def analyse_and_store(sorted_posts, ti):
    logger.info("=" * 80)
    logger.info(f"Analysing and storing posts - DAG: {ti.dag_id}, Run: {ti.run_id}")

    total = sum(len(items) for items in sorted_posts.values())
    if total == 0:
        logger.info("No new items to analyse, skipping.")
        raise AirflowSkipException("No new items to analyse")

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    task_provider = create_task_provider(ti.task_id)
    instrument_requests(task_provider)
    parent_context = resolve_parent_context(ti, otel_task_tracer, previous_task_id="get_saved_posts")

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
                with otel_task_tracer.start_child_span(span_name="analyse_and_insert_items") as span:
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
                                analysis = _parse_llm_json(raw, item["external_id"])

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

                    span.set_attribute("items.inserted", inserted)
                    logger.info(f"=== Finished — {inserted} items stored")

    task_provider.force_flush()
    logger.info("=" * 80)


@dag(
    start_date=pendulum.datetime(2025, 8, 30, tz="UTC"),
    catchup=False,
)
def reddit_import():
    posts = get_saved_posts()
    analyse_and_store(posts)


reddit_import()
