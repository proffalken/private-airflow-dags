"""
structure_extractor — weekly DAG that populates structured_data for recipes and projects.

For every item in saved_items where:
  - content_type IN ('recipe', 'project')
  - structured_data IS NULL
  - not flagged / deleted

…asks the local Ollama LLM to extract type-specific structured fields and stores
the result as JSONB. This makes the archive usable for downstream LLM workflows:
  - recipe  → push ingredients + steps to Mealie meal planner
  - project → compare materials list against Gridstock inventory

Run after the import DAGs and project_sizer have had a chance to populate
content_type. Structured data is never overwritten once set (re-extraction
would require manually nulling the column).
"""
from __future__ import annotations

import json
import logging

import pendulum

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from airflow_otel import instrument_task_context, get_meter
from dag_utils import ollama_chat, OLLAMA_MODEL, parse_llm_json

logger = logging.getLogger("airflow.structure_extractor")

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

RECIPE_PROMPT = """\
You are a recipe data extractor. Given information about a saved recipe item, \
extract structured data and return a JSON object only — no markdown, no explanation.

Return exactly this structure (omit keys you cannot determine from the content):
{{
  "ingredients": ["<quantity> <ingredient>", ...],
  "steps": ["<step 1>", "<step 2>", ...],
  "servings": <number or null>,
  "prep_time": "<e.g. 10 min or null>",
  "cook_time": "<e.g. 30 min or null>",
  "dietary": ["<e.g. vegetarian, vegan, gluten-free, dairy-free>"],
  "cuisine": "<e.g. Italian, Mexican, Japanese or null>"
}}

Item details:
Title: {title}
Source: {source} / {source_context}
Summary: {summary}
Body/Caption:
{body}
"""

PROJECT_PROMPT = """\
You are a project materials extractor. Given information about a saved maker/build \
project item, extract structured data and return a JSON object only — no markdown, \
no explanation.

Return exactly this structure (omit keys you cannot determine from the content):
{{
  "materials": ["<quantity> <material/component>", ...],
  "tools": ["<tool>", ...],
  "skills": ["<skill>", ...],
  "difficulty": "<one of: beginner, intermediate, advanced or null>",
  "estimated_cost": "<e.g. $10-30 or null>"
}}

Item details:
Title: {title}
Source: {source} / {source_context}
Summary: {summary}
Body/Caption:
{body}
"""

PROMPTS = {
    "recipe": RECIPE_PROMPT,
    "project": PROJECT_PROMPT,
}


@dag(
    dag_id="structure_extractor",
    schedule="@weekly",
    start_date=pendulum.datetime(2026, 5, 21, tz="UTC"),
    catchup=False,
    tags=["social-archive", "llm"],
)
def structure_extractor():

    @task
    def warm_up_model():
        """Pull the model if needed and verify it's responding before the main run."""
        import time
        import requests

        ollama_base = "http://ollama.ollama.svc.cluster.local:11434"

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

        logger.info("Loading model into memory...")
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
    def fetch_unextracted(ti):
        """Return items that need structure extraction."""
        hook = PostgresHook(postgres_conn_id="social_archive_db")
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # Ensure the column exists (safe to run repeatedly)
                cursor.execute("""
                    ALTER TABLE saved_items
                        ADD COLUMN IF NOT EXISTS structured_data JSONB
                """)
                conn.commit()

                cursor.execute("""
                    SELECT id, source, source_context, type, content_type,
                           title, summary, body, uri
                    FROM saved_items
                    WHERE content_type IN ('recipe', 'project')
                      AND structured_data IS NULL
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
                "content_type": r[4],
                "title": r[5] or "",
                "summary": r[6] or "",
                "body": r[7] or "",
                "uri": r[8] or "",
            }
            for r in rows
        ]
        logger.info(
            "Items to extract: %d (%d recipes, %d projects)",
            len(items),
            sum(1 for i in items if i["content_type"] == "recipe"),
            sum(1 for i in items if i["content_type"] == "project"),
        )
        return items

    @task
    def extract_structure(items: list[dict], ti):
        """Run the LLM extraction and write structured_data back to the DB."""
        if not items:
            logger.info("Nothing to extract.")
            return []

        hook = PostgresHook(postgres_conn_id="social_archive_db")

        results = []
        skipped = 0

        with instrument_task_context({"items.total": len(items)}) as span:
            meter = get_meter()
            extracted_counter = meter.create_counter(
                "structure_extractor.items_extracted",
                description="Items with structured_data successfully populated",
            )
            skipped_counter = meter.create_counter(
                "structure_extractor.items_skipped",
                description="Items skipped due to LLM parse failure",
            )

            with hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    for item in items:
                        content_type = item["content_type"]
                        prompt_template = PROMPTS.get(content_type)
                        if not prompt_template:
                            logger.warning(
                                "No prompt template for content_type %r on item %s — skipping",
                                content_type, item["id"],
                            )
                            skipped += 1
                            skipped_counter.add(1)
                            continue

                        # Truncate body to keep prompt within model context window.
                        # Instagram captions are short; Reddit/YouTube can be long.
                        body_excerpt = item["body"][:3000]

                        prompt = prompt_template.format(
                            title=item["title"][:300],
                            source=item["source"],
                            source_context=item["source_context"],
                            summary=item["summary"][:500],
                            body=body_excerpt,
                        )

                        try:
                            raw = ollama_chat(
                                [{"role": "user", "content": prompt}],
                                max_tokens=1024,
                                temperature=0.1,  # very low — we want consistent extraction
                                timeout=90,
                            )
                            parsed = parse_llm_json(raw, str(item["id"]))
                        except Exception as exc:
                            logger.warning(
                                "LLM call failed for item %s (%s): %s",
                                item["id"], content_type, exc,
                            )
                            skipped += 1
                            skipped_counter.add(1)
                            continue

                        if not parsed:
                            logger.warning(
                                "Empty or unparseable LLM response for item %s — skipping",
                                item["id"],
                            )
                            skipped += 1
                            skipped_counter.add(1)
                            continue

                        cursor.execute(
                            """
                            UPDATE saved_items
                               SET structured_data = %s
                             WHERE id = %s
                            """,
                            (json.dumps(parsed), item["id"]),
                        )
                        conn.commit()
                        results.append({"id": item["id"], "content_type": content_type})
                        extracted_counter.add(1)
                        logger.debug(
                            "Item %s (%s / %s): extracted keys=%s",
                            item["id"], item["source"], content_type, list(parsed.keys()),
                        )

                        if len(results) % 20 == 0:
                            logger.info(
                                "Progress: %d/%d extracted", len(results), len(items)
                            )

        span.set_attribute("items.extracted", len(results))
        logger.info(
            "Extraction complete: %d extracted, %d skipped",
            len(results), skipped,
        )
        return results

    ready = warm_up_model()
    items = fetch_unextracted()
    ready >> items
    extract_structure(items)


structure_extractor()
