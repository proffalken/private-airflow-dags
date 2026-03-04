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

from airflow.sdk import chain, dag, task, Variable
from airflow.traces import otel_tracer
from airflow.traces.tracer import Trace

from pprint import pformat

import praw

# EDIT: Update this logger name to match your DAG name
logger = logging.getLogger("airflow.reddit_dag")
_REQUESTS_INSTRUMENTED = True


# EDIT: Replace these placeholder credentials with real values.
# Prefer reading from Airflow Variables (Variable.get("REDDIT_CLIENT_ID")) or
# environment variables rather than hardcoding secrets here.
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

        # After task work completes, emit a PRODUCER handoff span as a child of
        # this CONSUMER span. The next task uses it as its parent, which is what
        # creates the directed edge in the service map.
        with tracer.start_as_current_span(
            f"task.{ti.task_id}.trigger_next",
            kind=SpanKind.PRODUCER,
        ):
            carrier = {}
            inject(carrier)
            ti.xcom_push(key="otel_context", value=carrier)
            logger.info(f"✓ Handoff context pushed to XCom: {carrier}")


# EDIT: Rename task1/analyse_saved_posts/task3 to meaningful names that describe what each step does,
# e.g. fetch_subreddit, process_posts, store_results.
# Update the chain() call at the bottom to match any renames.
@task
def get_saved_posts(ti):
    logger.info("=" * 80)
    logger.info(f"Getting saved posts - DAG: {ti.dag_id}, Run: {ti.run_id}")

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    task_provider = create_task_provider(ti.task_id)
    meter_provider = create_meter_provider(ti.task_id)
    parent_context = resolve_parent_context(ti, otel_task_tracer)

    meter = meter_provider.get_meter("reddit.saved")
    post_gauge = meter.create_gauge("reddit.saved.post_count", description="Number of saved Reddit posts")
    comment_gauge = meter.create_gauge("reddit.saved.comment_count", description="Number of saved Reddit comments")

    sorted_posts = {}
    post_count = 0
    comment_count = 0

    with task_root_span(ti, task_provider, parent_context) as span:
        current_span = trace.get_current_span()
        ctx = current_span.get_span_context()
        if ctx.trace_id != 0:
            logger.info(f"✓ Active trace: {format(ctx.trace_id, '032x')}")

        if ti.context_carrier is not None:
            logger.info("Found ti.context_carrier: %s.", ti.context_carrier)
            logger.info("Extracting the span context from the context_carrier.")
            with otel_task_tracer.start_child_span(
                span_name="part1_with_parent_ctx",
                parent_context=parent_context,
                component="dag",
            ) as p1_with_ctx_s:
                p1_with_ctx_s.set_attribute("using_parent_ctx", "true")
                logger.info("From part1_with_parent_ctx.")

                with otel_task_tracer.start_child_span("sub_span_without_setting_parent") as sub1_s:
                    sub1_s.set_attribute("get_parent_ctx_from_curr", "true")
                    logger.info("From sub_span_without_setting_parent.")

                    instrument_requests(task_provider)

                    # EDIT: Replace this placeholder HTTP call with your Reddit API logic,
                    # e.g. use the `reddit` client above to fetch posts from a subreddit.
                    with otel_task_tracer.start_child_span(span_name="get_saved_posts") as auto_instr_s:
                        # Fetch all saved posts
                        for item in reddit.user.me().saved(limit=None):
                            if isinstance(item, praw.models.Submission):
                               # print(f"Title: {item.title}")
                               # print(f"URL: {item.url}")
                               # print(f"ID: {item.id}")
                               # print(f"Sub: {item.subreddit}")
                               # print("-" * 50)

                                item_object = {
                                       "type": "post",
                                       "title": item.title,
                                       "body": item.selftext,
                                       "uri": item.url
                                       }
                                sr = item.subreddit
                                sr_name = sr.display_name
                                print(f"== Post Sub: {sr_name}")
                                if sr_name in sorted_posts:
                                    sorted_posts[sr_name].append(item_object)
                                else:
                                    sorted_posts[sr_name] = []
                                    sorted_posts[sr_name].append(item_object)
                                post_count = post_count + 1
                            else:
                                #print(f"Comment: {item.body}")
                                #print(f"ID: {item.id}")
                                #print("-" * 50)
                                item_object = {
                                       "type": "comment",
                                       "title": None,
                                       "body": item.body,
                                       "uri": item.permalink
                                       }
                                sr = item.subreddit
                                sr_name = sr.display_name
                                print(f"== Comment Sub: {sr_name}")
                                if sr_name in sorted_posts:
                                    sorted_posts[sr_name].append(item_object)
                                else:
                                    sorted_posts[sr_name] = []
                                    sorted_posts[sr_name].append(item_object)
                                comment_count = comment_count + 1

                        print(json.dumps(sorted_posts))

#                    tracer = trace.get_tracer("trace_test.tracer", tracer_provider=task_provider)
#                    with tracer.start_as_current_span(name="sub_span_start_as_current") as sub_curr_s:
#                        sub_curr_s.set_attribute("start_as_current", "true")
#                        logger.info("From sub_span_start_as_current.")

#            with otel_task_tracer.start_child_span(
#                span_name="part2_with_parent_ctx",
#                parent_context=parent_context,
#                component="dag",
#            ) as p2_with_ctx_s:
#                p2_with_ctx_s.set_attribute("using_parent_ctx", "true")
#                logger.info("From part2_with_parent_ctx.")

    attrs = {"dag_id": ti.dag_id, "run_id": ti.run_id}
    post_gauge.set(post_count, attrs)
    comment_gauge.set(comment_count, attrs)

    task_provider.force_flush()
    meter_provider.force_flush()
    meter_provider.shutdown()
    logger.info("Reddit saved post download finished.")
    return sorted_posts


@task
def analyse_saved_posts(sorted_posts, ti):
    logger.info("=" * 80)
    logger.info(f"Analyse and process the posts - DAG: {ti.dag_id}, Run: {ti.run_id}")

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    task_provider = create_task_provider(ti.task_id)
    instrument_requests(task_provider)
    # EDIT: If you rename task1, update the previous_task_id here to match.
    parent_context = resolve_parent_context(ti, otel_task_tracer, previous_task_id="get_saved_posts")

    with task_root_span(ti, task_provider, parent_context):
        # EDIT: Replace this URL with your Reddit processing logic.
        for item in sorted_posts:
            logger.info(f"Processing {item.title} ({ item.type})")

    task_provider.force_flush()
    logger.info("Analysis finished")
    logger.info("=" * 80)

@dag(
    # EDIT: Uncomment and set the schedule interval you want, e.g. timedelta(hours=1).
    # schedule=timedelta(seconds=30),
    # EDIT: Update start_date to an appropriate date for this DAG.
    start_date=pendulum.datetime(2025, 8, 30, tz="UTC"),
    catchup=False,
)
def reddit_import():
    # EDIT: Update this chain if you add, remove, or rename tasks above.
    posts = get_saved_posts()
    analysed_content = analyse_saved_posts(posts)


# EDIT: This must match the function name of the @dag above.
reddit_import()
