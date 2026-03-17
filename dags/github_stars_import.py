from __future__ import annotations

from datetime import timedelta
import logging
import re

import pendulum
import requests

import requests

from openai import OpenAI

from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task, Variable
from airflow.traces import otel_tracer
from airflow.traces.tracer import Trace

from otel_utils import (
    create_task_provider,
    create_meter_provider,
    resolve_parent_context,
    task_root_span,
    parse_llm_json,
)

logger = logging.getLogger("airflow.github_stars_dag")


# ---------------------------------------------------------------------------
# GitHub API helpers
# ---------------------------------------------------------------------------

def _fetch_all_starred_repos(token: str) -> list[dict]:
    """Page through /user/starred and return all starred repo objects."""
    repos = []
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    url = "https://api.github.com/user/starred"
    params = {"per_page": 100, "page": 1}
    page = 1

    while url:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break
        repos.extend(batch)
        logger.info(f"Fetched page {page}: {len(batch)} repos (total so far: {len(repos)})")
        page += 1

        # Follow Link header for next page
        link_header = response.headers.get("Link", "")
        next_url = None
        for part in link_header.split(","):
            part = part.strip()
            if 'rel="next"' in part:
                match = re.search(r"<([^>]+)>", part)
                if match:
                    next_url = match.group(1)
                    break

        if next_url:
            url = next_url
            params = {}  # next URL already contains query params
        else:
            break

    return repos


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task
def get_starred_repos(ti) -> dict[str, list[dict]]:
    """Fetch all GitHub starred repos, grouped by primary language."""
    logger.info("=" * 80)
    logger.info(f"Getting GitHub starred repos — DAG: {ti.dag_id}, Run: {ti.run_id}")

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    task_provider = create_task_provider("github-stars-import", ti.run_id)
    meter_provider = create_meter_provider("github-stars-import", ti.run_id)
    parent_context = resolve_parent_context(ti, otel_task_tracer)

    meter = meter_provider.get_meter("github.stars")
    repo_gauge = meter.create_gauge(
        "github.stars.repo_count",
        unit="1",
        description="Number of new GitHub starred repos fetched in this run",
    )

    try:
        hook = PostgresHook(postgres_conn_id="social_archive_db")
        rows = hook.get_records(
            "SELECT external_id FROM saved_items WHERE source = 'github'"
        )
        known_ids = {row[0] for row in rows}
        logger.info(f"=== {len(known_ids)} GitHub items already in database, skipping")
    except Exception:
        logger.info("=== saved_items table not found yet, treating all items as new")
        known_ids = set()

    token = Variable.get("GITHUB_TOKEN")
    sorted_repos: dict[str, list[dict]] = {}
    new_count = 0

    with task_root_span(ti, task_provider, parent_context) as span:
        with otel_task_tracer.start_child_span(span_name="fetch_starred_repos"):
            all_repos = _fetch_all_starred_repos(token)
            logger.info(f"Total starred repos: {len(all_repos)}")

        for repo in all_repos:
            repo_id = str(repo["id"])
            if repo_id in known_ids:
                continue

            language = repo.get("language") or "other"
            topics = [t.lower() for t in (repo.get("topics") or [])]
            is_private = repo.get("private", False)
            is_archived = repo.get("archived", False)

            sorted_repos.setdefault(language, []).append({
                "external_id": repo_id,
                "type": "repository",
                "title": repo.get("full_name") or repo.get("name") or "",
                "body": repo.get("description") or "",
                "uri": repo.get("html_url") or "",
                "language": language,
                "topics": topics,
                "stargazers_count": repo.get("stargazers_count", 0),
                "visibility": "private" if is_private else "public",
                "archived": is_archived,
            })
            new_count += 1

        span.set_attribute("github.new_repos", new_count)
        logger.info(f"=== {new_count} new GitHub starred repos to process")

    repo_gauge.set(new_count)
    task_provider.force_flush()
    meter_provider.force_flush()
    return sorted_repos


@task
def analyse_and_store(sorted_repos: dict[str, list[dict]], ti) -> None:
    """LLM-analyse each repo and store in the archive DB.

    Tag priority (deduplicated, in order):
      1. Primary language  — always first, guaranteed present
      2. GitHub topics     — owner-supplied metadata
      3. LLM-generated tags — fills gaps, adds broader categories
    """
    logger.info("=" * 80)
    logger.info(f"Analysing and storing GitHub starred repos — DAG: {ti.dag_id}, Run: {ti.run_id}")

    total = sum(len(items) for items in sorted_repos.values())
    if total == 0:
        logger.info("No new items to analyse, skipping.")
        raise AirflowSkipException("No new GitHub starred repos to analyse")

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    task_provider = create_task_provider("github-stars-import", ti.run_id)
    parent_context = resolve_parent_context(
        ti, otel_task_tracer, previous_task_id="get_starred_repos"
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
                    for language, items in sorted_repos.items():
                        language_tag = language.lower().replace(" ", "-")

                        for item in items:
                            repo_id = item["external_id"]
                            full_name = item["title"]
                            description = item["body"]
                            topics = item.get("topics", [])
                            visibility = item.get("visibility", "public")
                            is_archived = item.get("archived", False)

                            logger.info(
                                f"Analysing: {full_name!r} "
                                f"(language={language!r}, stars={item['stargazers_count']}, "
                                f"visibility={visibility}, archived={is_archived})"
                            )

                            with otel_task_tracer.start_child_span(
                                span_name="llm.analyse_item"
                            ) as llm_span:
                                llm_span.set_attribute("item.external_id", repo_id)
                                llm_span.set_attribute("item.language", language)

                                topic_hint = (
                                    f"GitHub topics on this repo: {', '.join(topics)}\n"
                                    if topics else ""
                                )

                                content = (
                                    f"Repository: {full_name}\n"
                                    f"Language: {language}\n"
                                    f"Description: {description[:1500]}"
                                )

                                response = client.chat.completions.create(
                                    model="dolphin-mistral:latest",
                                    max_tokens=512,
                                    messages=[{
                                        "role": "user",
                                        "content": (
                                            f"Analyse this GitHub repository and return a JSON object with:\n"
                                            f'- "tags": a list of 3-8 single-concept keyword tags '
                                            f"(lowercase, no spaces).\n"
                                            f"  TAGGING RULES — follow these strictly:\n"
                                            f"  1. Each tag must represent ONE concept. "
                                            f"Never combine concepts.\n"
                                            f"  2. Always include a broad category tag: "
                                            f"'tool', 'library', 'framework', 'cli', 'devops', etc.\n"
                                            f"  3. Add specific descriptive tags beyond the "
                                            f"broad category (e.g. 'python', 'kubernetes', 'llm').\n"
                                            f"  4. Do NOT include the language as a tag — "
                                            f"it will be added automatically.\n"
                                            f'- "summary": a one-sentence summary\n\n'
                                            f"{topic_hint}"
                                            f"Repository:\n{content}\n\n"
                                            f"Respond with only valid JSON, no trailing commas."
                                        ),
                                    }],
                                )

                                raw = response.choices[0].message.content or ""
                                analysis = parse_llm_json(raw, repo_id)

                            llm_tags = analysis.get("tags", [])
                            # language tag → visibility → archived (if set) → GitHub topics → LLM tags (deduplicated)
                            status_tags = [visibility] + (["archived"] if is_archived else [])
                            merged_tags = list(dict.fromkeys(
                                [language_tag] + status_tags + topics + llm_tags
                            ))
                            llm_span.set_attribute("item.tags", str(merged_tags))

                            cursor.execute("""
                                INSERT INTO saved_items
                                    (source, external_id, type, title, body,
                                     uri, source_context, tags, summary)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (source, external_id) DO NOTHING
                            """, (
                                "github",
                                repo_id,
                                "repository",
                                full_name,
                                description,
                                item["uri"],
                                language,
                                merged_tags,
                                analysis.get("summary"),
                            ))
                            conn.commit()
                            inserted += cursor.rowcount
                            logger.info(
                                f"=== Stored {full_name} "
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
def github_stars_import():
    repos = get_starred_repos()
    analyse_and_store(repos)


github_stars_import()
