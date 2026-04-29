"""
project_sizer — weekly DAG that estimates build effort for saved items.

For every item in saved_items that hasn't been sized (or was sized >7 days ago),
asks the local Ollama LLM to classify it into one of:

  quick        — under an hour (a script, a config tweak, a quick experiment)
  afternoon    — 2–4 hours
  full_day     — a solid day's work
  multi_day    — several days or more
  not_a_project — reference material, article, video with nothing to build

Results are stored back so the social-archive frontend can answer
"what can I build in the time I have?"
"""
from __future__ import annotations

import json
import logging

import pendulum

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from airflow_otel import instrument_task_context, get_meter
from dag_utils import get_llm_client, OLLAMA_MODEL, parse_llm_json

logger = logging.getLogger("airflow.project_sizer")

VALID_ESTIMATES = {"quick", "afternoon", "full_day", "multi_day", "not_a_project"}

SIZING_PROMPT = """\
/no_think
You are a software project estimator. Given information about a saved item \
(a GitHub repo, Reddit post, YouTube video, article, or similar), estimate \
how long it would take an experienced developer to build or implement \
the core idea.

Respond with a JSON object only — no markdown, no explanation outside the JSON:
{{
  "time_estimate": "<one of: quick, afternoon, full_day, multi_day, not_a_project>",
  "reasoning": "<one sentence explaining why>"
}}

Definitions:
- quick: under an hour — a script, small config change, or quick experiment
- afternoon: 2–4 hours — a meaningful feature or small standalone tool
- full_day: a solid 6–8 hour day of focused work
- multi_day: several days or more — a proper project
- not_a_project: reference material, an article to read, a video to watch — nothing to build

Item details:
Title: {title}
Source: {source} / {source_context}
Type: {type}
Summary: {summary}
Tags: {tags}
URL: {uri}
"""


@dag(
    dag_id="project_sizer",
    schedule="@weekly",
    start_date=pendulum.datetime(2026, 3, 21, tz="UTC"),
    catchup=False,
    tags=["social-archive", "llm"],
)
def project_sizer():

    @task
    def warm_up_model():
        """Pull the model if needed, then wait until Ollama is responding."""
        import time
        import requests

        ollama_base = "http://ollama.ollama.svc.cluster.local:11434"

        # Pull the model (no-op if already present; streams status lines until done)
        logger.info("Pulling model %s (no-op if already present)...", OLLAMA_MODEL)
        with requests.post(
            f"{ollama_base}/api/pull",
            json={"model": OLLAMA_MODEL},
            stream=True,
            timeout=600,
        ) as resp:
            resp.raise_for_status()
            for line in resp.iter_lines():
                if line:
                    logger.info("pull: %s", line.decode())

        # Load model into VRAM via native API (no prompt = just load, don't infer)
        logger.info("Loading model into memory...")
        resp = requests.post(
            f"{ollama_base}/api/generate",
            json={"model": OLLAMA_MODEL, "keep_alive": "30m"},
            timeout=300,
        )
        resp.raise_for_status()
        logger.info("Model loaded. Verifying inference...")

        client = get_llm_client()
        for attempt in range(12):  # up to 1 minute
            try:
                response = client.chat.completions.create(
                    model=OLLAMA_MODEL,
                    max_tokens=10,
                    messages=[{"role": "user", "content": "Reply with the word OK."}],
                )
                content = response.choices[0].message.content if response.choices else None
                if content is not None:
                    logger.info("Model ready (response: %r)", content[:50])
                    return
            except Exception as exc:
                logger.info("Warm-up attempt %d: %s", attempt + 1, exc)
            time.sleep(5)
        raise RuntimeError(f"Model {OLLAMA_MODEL} did not respond after pull")

    @task
    def fetch_unestimated(ti):
        hook = PostgresHook(postgres_conn_id="social_archive_db")
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # Ensure columns exist
                cursor.execute("""
                    ALTER TABLE saved_items
                        ADD COLUMN IF NOT EXISTS time_estimate     VARCHAR(20),
                        ADD COLUMN IF NOT EXISTS estimate_reasoning TEXT,
                        ADD COLUMN IF NOT EXISTS estimated_at      TIMESTAMPTZ
                """)
                conn.commit()

                cursor.execute("""
                    SELECT id, source, source_context, type, title, summary, tags, uri
                    FROM saved_items
                    WHERE estimated_at IS NULL
                       OR estimated_at < NOW() - INTERVAL '7 days'
                    ORDER BY saved_at DESC
                """)
                rows = cursor.fetchall()

        items = [
            {
                "id": r[0],
                "source": r[1],
                "source_context": r[2],
                "type": r[3],
                "title": r[4] or "",
                "summary": r[5] or "",
                "tags": r[6] or [],
                "uri": r[7] or "",
            }
            for r in rows
        ]
        logger.info("Items to size: %d", len(items))
        return items

    @task
    def estimate_items(items: list[dict], ti):
        if not items:
            logger.info("Nothing to estimate.")
            return []

        client = get_llm_client()
        hook = PostgresHook(postgres_conn_id="social_archive_db")

        results = []
        with instrument_task_context({"items.total": len(items)}) as span:
            meter = get_meter()
            estimated_counter = meter.create_counter(
                "project_sizer.items_estimated",
                description="Number of items sized by the LLM",
            )
            skipped_counter = meter.create_counter(
                "project_sizer.items_skipped",
                description="Number of items skipped due to LLM parse failure",
            )

            with hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    for item in items:
                        prompt = SIZING_PROMPT.format(
                            title=item["title"][:300],
                            source=item["source"],
                            source_context=item["source_context"] or "",
                            type=item["type"],
                            summary=item["summary"][:500],
                            tags=", ".join(item["tags"]) if item["tags"] else "none",
                            uri=item["uri"][:200],
                        )

                        try:
                            response = client.chat.completions.create(
                                model=OLLAMA_MODEL,
                                max_tokens=256,
                                temperature=0.2,
                                timeout=60,
                                messages=[{"role": "user", "content": prompt}],
                            )
                            raw = response.choices[0].message.content or ""
                            parsed = parse_llm_json(raw, str(item["id"]))
                        except Exception as exc:
                            logger.warning("LLM call failed for item %s: %s", item["id"], exc)
                            skipped_counter.add(1)
                            continue

                        estimate = parsed.get("time_estimate", "").strip().lower()
                        reasoning = parsed.get("reasoning", "").strip()

                        if estimate not in VALID_ESTIMATES:
                            logger.warning(
                                "Invalid estimate %r for item %s — skipping", estimate, item["id"]
                            )
                            skipped_counter.add(1)
                            continue

                        cursor.execute(
                            """
                            UPDATE saved_items
                               SET time_estimate      = %s,
                                   estimate_reasoning = %s,
                                   estimated_at       = NOW()
                             WHERE id = %s
                            """,
                            (estimate, reasoning, item["id"]),
                        )
                        conn.commit()
                        results.append({"id": item["id"], "estimate": estimate})
                        estimated_counter.add(1)
                        logger.debug(
                            "Item %s (%s): %s — %s",
                            item["id"], item["source"], estimate, reasoning,
                        )
                        if len(results) % 50 == 0:
                            logger.info("Progress: %d/%d items estimated", len(results), len(items))

        span.set_attribute("items.estimated", len(results))
        logger.info("Sizing complete: %d estimated, %d skipped", len(results), len(items) - len(results))
        return results

    ready = warm_up_model()
    items = fetch_unestimated()
    ready >> items
    estimate_items(items)


project_sizer()
