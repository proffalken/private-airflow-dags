"""
content_type_backfill — one-shot DAG to classify existing archive items.

Finds every item where content_type IS NULL and wasn't captured by the updated
import DAGs. Works in two passes:

  1. Heuristic pass (no LLM) — fast, no GPU needed:
       - source = 'github'   → 'tool'  (repos are almost always tools)
       - source = 'youtube'  → 'other' (overridden to 'recipe' by LLM if cooking)
     Written directly via SQL; these items are then excluded from the LLM pass.

  2. LLM pass — classifies everything else using existing title, summary, tags,
     and source context as evidence. No re-tagging; content_type only.

Designed to be triggered manually (schedule=None). Safe to re-run — items with
an existing content_type are never touched. Once backfill is complete the DAG
can be left in place as a safety net for any items that slip through the import
DAGs with content_type still NULL.
"""
from __future__ import annotations

import logging

import pendulum

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from airflow_otel import instrument_task_context, get_meter
from dag_utils import ollama_chat, OLLAMA_MODEL, CONTENT_TYPE_PROMPT_FRAGMENT, CONTENT_TYPES, parse_llm_json

logger = logging.getLogger("airflow.content_type_backfill")

# Sources where we can infer content_type reliably without the LLM
HEURISTIC_DEFAULTS: dict[str, str] = {
    "github": "tool",
    # bookmarks and reddit/instagram/youtube are left for the LLM pass
}

CLASSIFICATION_PROMPT = """\
You are classifying a saved item from a personal social archive.
Given the metadata below, determine the content_type and return a JSON object only.

{content_type_rules}

Return exactly:
{{"content_type": "<one of: recipe, project, article, reference, tool, other>"}}

Item metadata:
Source: {source}
Collection / subreddit / playlist: {source_context}
Media type: {type}
Title: {title}
Summary: {summary}
Tags: {tags}
URL: {uri}
"""


@dag(
    dag_id="content_type_backfill",
    schedule=None,  # trigger manually; leave in place as a safety-net catch-up
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["social-archive", "llm", "backfill"],
)
def content_type_backfill():

    @task
    def apply_heuristics() -> int:
        """Fast SQL-only pass for sources where content_type is unambiguous."""
        hook = PostgresHook(postgres_conn_id="social_archive_db")
        total_updated = 0

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for source, content_type in HEURISTIC_DEFAULTS.items():
                    cursor.execute(
                        """
                        UPDATE saved_items
                           SET content_type = %s
                         WHERE source = %s
                           AND content_type IS NULL
                           AND deleted_at IS NULL
                        """,
                        (content_type, source),
                    )
                    updated = cursor.rowcount
                    conn.commit()
                    logger.info(
                        "Heuristic: source=%r → content_type=%r — %d rows updated",
                        source, content_type, updated,
                    )
                    total_updated += updated

        logger.info("Heuristic pass complete: %d total rows updated", total_updated)
        return total_updated

    @task
    def warm_up_model(heuristic_count: int):
        """Pull the model and verify it responds before the LLM pass."""
        import time
        import requests

        ollama_base = "http://ollama.ollama.svc.cluster.local:11434"

        logger.info("Pulling model %s...", OLLAMA_MODEL)
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

        resp = requests.post(
            f"{ollama_base}/api/generate",
            json={"model": OLLAMA_MODEL, "keep_alive": "30m"},
            timeout=300,
        )
        resp.raise_for_status()

        for attempt in range(12):
            try:
                content = ollama_chat(
                    [{"role": "user", "content": "Reply with the word OK."}],
                    max_tokens=10,
                    timeout=15,
                )
                if content:
                    logger.info("Model ready (response: %r)", content[:50])
                    return
            except Exception as exc:
                logger.info("Warm-up attempt %d: %s", attempt + 1, exc)
            time.sleep(5)
        raise RuntimeError(f"Model {OLLAMA_MODEL} did not respond after pull")

    @task
    def fetch_unclassified(_warmed) -> list[dict]:
        """Return all items still lacking a content_type after the heuristic pass."""
        hook = PostgresHook(postgres_conn_id="social_archive_db")
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, source, source_context, type,
                           title, summary, tags, uri
                    FROM saved_items
                    WHERE content_type IS NULL
                      AND flagged_for_deletion = FALSE
                      AND deleted_at IS NULL
                    ORDER BY saved_at DESC
                """)
                rows = cursor.fetchall()

        items = [
            {
                "id": r[0],
                "source": r[1],
                "source_context": r[2] or "",
                "type": r[3] or "",
                "title": r[4] or "",
                "summary": r[5] or "",
                "tags": r[6] or [],
                "uri": r[7] or "",
            }
            for r in rows
        ]
        logger.info("%d items remain unclassified after heuristic pass", len(items))
        return items

    @task
    def classify_items(items: list[dict]) -> dict:
        """LLM classification pass — writes content_type for each remaining item."""
        if not items:
            logger.info("Nothing to classify.")
            return {"classified": 0, "skipped": 0}

        hook = PostgresHook(postgres_conn_id="social_archive_db")
        classified = 0
        skipped = 0

        with instrument_task_context({"items.total": len(items)}) as span:
            meter = get_meter()
            classified_counter = meter.create_counter(
                "content_type_backfill.items_classified",
                description="Items successfully classified",
            )
            skipped_counter = meter.create_counter(
                "content_type_backfill.items_skipped",
                description="Items skipped due to LLM parse failure",
            )

            with hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    for item in items:
                        tags_str = ", ".join(item["tags"]) if item["tags"] else "none"

                        prompt = CLASSIFICATION_PROMPT.format(
                            content_type_rules=CONTENT_TYPE_PROMPT_FRAGMENT,
                            source=item["source"],
                            source_context=item["source_context"],
                            type=item["type"],
                            title=item["title"][:300],
                            summary=item["summary"][:500],
                            tags=tags_str,
                            uri=item["uri"][:200],
                        )

                        try:
                            raw = ollama_chat(
                                [{"role": "user", "content": prompt}],
                                max_tokens=64,   # we only need one short field
                                temperature=0.1,
                                timeout=30,
                            )
                            parsed = parse_llm_json(raw, str(item["id"]))
                        except Exception as exc:
                            logger.warning(
                                "LLM call failed for item %s: %s", item["id"], exc
                            )
                            skipped += 1
                            skipped_counter.add(1)
                            continue

                        raw_ct = parsed.get("content_type", "").strip().lower()
                        content_type = raw_ct if raw_ct in CONTENT_TYPES else "other"

                        cursor.execute(
                            "UPDATE saved_items SET content_type = %s WHERE id = %s",
                            (content_type, item["id"]),
                        )
                        conn.commit()
                        classified += 1
                        classified_counter.add(1)

                        logger.debug(
                            "Item %s (%s / %s): → %s",
                            item["id"], item["source"], item["type"], content_type,
                        )
                        if classified % 50 == 0:
                            logger.info(
                                "Progress: %d/%d classified", classified, len(items)
                            )

        span.set_attribute("items.classified", classified)
        logger.info(
            "LLM pass complete: %d classified, %d skipped", classified, skipped
        )
        return {"classified": classified, "skipped": skipped}

    heuristic_count = apply_heuristics()
    warmed = warm_up_model(heuristic_count)
    unclassified = fetch_unclassified(warmed)
    classify_items(unclassified)


content_type_backfill()
