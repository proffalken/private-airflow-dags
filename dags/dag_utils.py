"""Shared DAG utilities: LLM helpers and OTel instrumentation wrappers."""

from __future__ import annotations

import json
import logging
import re
import time

logger = logging.getLogger(__name__)

OLLAMA_BASE_URL = "http://ollama.ollama.svc.cluster.local:11434/v1"
OLLAMA_NATIVE_URL = "http://ollama.ollama.svc.cluster.local:11434"
OLLAMA_MODEL = "huihui_ai/qwen3-abliterated:4b"

# Valid content_type values — must match CONTENT_TYPES in backend/app/routes/items.py
CONTENT_TYPES = ["recipe", "project", "article", "reference", "tool", "other"]

# Shared prompt fragment added to every import DAG's LLM call.
# Keep this in sync with CONTENT_TYPES above.
CONTENT_TYPE_PROMPT_FRAGMENT = """\
- "content_type": exactly one of: recipe, project, article, reference, tool, other
  RULES:
  - recipe    → food or drink recipe (cooking instructions, ingredient lists)
  - project   → something to make or build (DIY, electronics, craft, software project)
  - reference → documentation, cheat-sheets, how-to guides kept for future lookup
  - article   → written piece to read once (news, blog post, essay)
  - tool      → software library, CLI app, framework, or service
  - other     → anything that doesn't clearly fit the above\
"""

# ---------------------------------------------------------------------------
# Time-estimation constants
# ---------------------------------------------------------------------------

VALID_ESTIMATES = {"quick", "afternoon", "full_day", "multi_day", "not_a_project"}

SIZING_PROMPT = """\
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

# ---------------------------------------------------------------------------
# Structure-extraction constants
# ---------------------------------------------------------------------------

RECIPE_PROMPT = """\
You are a recipe data extractor. Given information about a saved recipe item, \
extract structured data and return a JSON object only — no markdown, no explanation.

Be concise: list at most 15 ingredients and 10 steps. Omit keys you cannot determine.

Return exactly this structure:
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

Be concise: list at most 15 materials and 8 tools. Omit keys you cannot determine.

Return exactly this structure:
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

STRUCTURE_PROMPTS = {
    "recipe": RECIPE_PROMPT,
    "project": PROJECT_PROMPT,
}

# ---------------------------------------------------------------------------
# Classification prompt (used by bookmark_processor for post-scrape classify)
# ---------------------------------------------------------------------------

CLASSIFICATION_PROMPT = """\
You are classifying a saved item from a personal social archive.
Given the metadata below, determine the content_type and return a JSON object only.

{content_type_rules}

Return exactly:
{{"content_type": "<one of: recipe, project, article, reference, tool, other>"}}

Item metadata:
Source: {source}
Collection / folder: {source_context}
Media type: {type}
Title: {title}
Summary: {summary}
Tags: {tags}
URL: {uri}
Body excerpt:
{body}
"""


def get_llm_client():
    """Return an OpenAI-compatible client pointed at the local Ollama instance."""
    from openai import OpenAI
    return OpenAI(base_url=OLLAMA_BASE_URL, api_key="ollama")


def ollama_chat(
    messages: list[dict],
    *,
    model: str = OLLAMA_MODEL,
    max_tokens: int = 512,
    temperature: float = 0.2,
    timeout: int = 60,
) -> str:
    """Call Ollama's native /api/chat with thinking disabled.

    The OpenAI-compatible endpoint ignores extra_body think:false for complex
    prompts; the native API respects it reliably.
    """
    import requests
    body = {
        "model": model,
        "stream": False,
        "think": False,
        "options": {"num_predict": max_tokens, "temperature": temperature},
        "messages": messages,
    }
    resp = requests.post(f"{OLLAMA_NATIVE_URL}/api/chat", json=body, timeout=timeout)
    resp.raise_for_status()
    return resp.json()["message"]["content"]


def instrument_llm() -> None:
    """Activate OTel GenAI instrumentation for the OpenAI SDK (and Ollama-compatible APIs)."""
    from opentelemetry.instrumentation.openai_v2 import OpenAIInstrumentor
    OpenAIInstrumentor().instrument()


def instrument_requests() -> None:
    """Activate OTel instrumentation for the requests library (idempotent)."""
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
    RequestsInstrumentor().instrument()


def parse_llm_json(raw: str, item_id: str) -> dict:
    """
    Robustly parse a JSON object from an LLM response.

    Handles common LLM output quirks:
    - Markdown code fences (```json ... ```)
    - Leading/trailing whitespace
    - Trailing commas before closing braces/brackets
    - Responses wrapped in a single-element array
    """
    text = raw.strip()

    # Strip <think>...</think> blocks emitted by reasoning models (e.g. qwen3)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()

    # Strip markdown code fences
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()

    # Remove trailing commas before } or ]
    text = re.sub(r",\s*([}\]])", r"\1", text)

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        logger.warning("Failed to parse LLM JSON for item %s: %s — raw: %r", item_id, exc, raw[:200])
        return {}

    # Unwrap single-element arrays
    if isinstance(parsed, list) and len(parsed) == 1 and isinstance(parsed[0], dict):
        return parsed[0]

    if isinstance(parsed, dict):
        return parsed

    logger.warning("Unexpected LLM JSON structure for item %s: %r", item_id, parsed)
    return {}


# ---------------------------------------------------------------------------
# Shared processing pipeline helpers
# Called from @task wrappers in each platform DAG.
# ---------------------------------------------------------------------------

def warm_up_ollama(model: str = OLLAMA_MODEL) -> None:
    """Pull model if absent and verify it responds. Raises RuntimeError on failure."""
    import requests

    logger.info("Pulling model %s (no-op if already present)...", model)
    with requests.post(
        f"{OLLAMA_NATIVE_URL}/api/pull",
        json={"model": model},
        stream=True,
        timeout=600,
    ) as resp:
        resp.raise_for_status()
        for line in resp.iter_lines():
            if line:
                logger.info("pull: %s", line.decode())

    requests.post(
        f"{OLLAMA_NATIVE_URL}/api/generate",
        json={"model": model, "keep_alive": "30m"},
        timeout=300,
    ).raise_for_status()

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
    raise RuntimeError(f"Model {model} did not respond after pull")


def run_estimate_items(new_ids: list[int], platform: str) -> list[dict]:
    """
    Time-estimate items with the given IDs. Writes time_estimate + reasoning to DB.
    Returns [{id, estimate}, ...] for successfully estimated items.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow_otel import instrument_task_context, get_meter

    if not new_ids:
        logger.info("No new items to estimate.")
        return []

    hook = PostgresHook(postgres_conn_id="social_archive_db")

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE saved_items
                    ADD COLUMN IF NOT EXISTS time_estimate      VARCHAR(20),
                    ADD COLUMN IF NOT EXISTS estimate_reasoning TEXT,
                    ADD COLUMN IF NOT EXISTS estimated_at       TIMESTAMPTZ
            """)
            conn.commit()
            cursor.execute("""
                SELECT id, source, source_context, type, title, summary, tags, uri
                FROM saved_items
                WHERE id = ANY(%s)
                  AND time_estimate IS NULL
            """, (new_ids,))
            rows = cursor.fetchall()

    items = [
        {
            "id": r[0], "source": r[1], "source_context": r[2] or "",
            "type": r[3] or "", "title": r[4] or "", "summary": r[5] or "",
            "tags": r[6] or [], "uri": r[7] or "",
        }
        for r in rows
    ]

    if not items:
        logger.info("All new items already have time estimates.")
        return []

    results = []
    skipped = 0

    with instrument_task_context({"items.total": len(items)}) as span:
        meter = get_meter()
        estimated_counter = meter.create_counter(
            f"{platform}.items_estimated",
            description="Items sized by the LLM",
        )
        skipped_counter = meter.create_counter(
            f"{platform}.items_estimate_skipped",
            description="Items skipped due to LLM failure",
        )

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for item in items:
                    prompt = SIZING_PROMPT.format(
                        title=item["title"][:300],
                        source=item["source"],
                        source_context=item["source_context"],
                        type=item["type"],
                        summary=item["summary"][:500],
                        tags=", ".join(item["tags"]) if item["tags"] else "none",
                        uri=item["uri"][:200],
                    )
                    try:
                        raw = ollama_chat(
                            [{"role": "user", "content": prompt}],
                            max_tokens=512,
                            timeout=60,
                        )
                        parsed = parse_llm_json(raw, str(item["id"]))
                    except Exception as exc:
                        logger.warning("LLM failed for item %s: %s", item["id"], exc)
                        skipped_counter.add(1)
                        skipped += 1
                        continue

                    estimate = parsed.get("time_estimate", "").strip().lower()
                    reasoning = parsed.get("reasoning", "").strip()

                    if estimate not in VALID_ESTIMATES:
                        logger.warning(
                            "Invalid estimate %r for item %s — skipping",
                            estimate, item["id"],
                        )
                        skipped_counter.add(1)
                        skipped += 1
                        continue

                    cursor.execute("""
                        UPDATE saved_items
                           SET time_estimate      = %s,
                               estimate_reasoning = %s,
                               estimated_at       = NOW()
                         WHERE id = %s
                    """, (estimate, reasoning, item["id"]))
                    conn.commit()
                    results.append({"id": item["id"], "estimate": estimate})
                    estimated_counter.add(1)
                    logger.debug("Item %s: %s — %s", item["id"], estimate, reasoning[:80])

                    if len(results) % 20 == 0 and results:
                        logger.info("Progress: %d/%d estimated", len(results), len(items))

        span.set_attribute("items.estimated", len(results))
        logger.info(
            "Estimation complete: %d estimated, %d skipped", len(results), skipped
        )
    return results


def run_extract_structure(new_ids: list[int], platform: str) -> list[dict]:
    """
    Extract structured data for recipe/project items with the given IDs.
    Writes structured_data JSONB to DB.
    Returns [{id, content_type}, ...] for successfully extracted items.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow_otel import instrument_task_context, get_meter

    if not new_ids:
        logger.info("No new items for structure extraction.")
        return []

    hook = PostgresHook(postgres_conn_id="social_archive_db")

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE saved_items ADD COLUMN IF NOT EXISTS structured_data JSONB
            """)
            conn.commit()
            cursor.execute("""
                SELECT id, source, source_context, type, content_type,
                       title, summary, body, uri
                FROM saved_items
                WHERE id = ANY(%s)
                  AND content_type IN ('recipe', 'project')
                  AND structured_data IS NULL
            """, (new_ids,))
            rows = cursor.fetchall()

    items = [
        {
            "id": r[0], "source": r[1], "source_context": r[2] or "",
            "type": r[3] or "", "content_type": r[4],
            "title": r[5] or "", "summary": r[6] or "",
            "body": r[7] or "", "uri": r[8] or "",
        }
        for r in rows
    ]

    if not items:
        logger.info("No recipe/project items need structure extraction.")
        return []

    results = []
    skipped = 0

    with instrument_task_context({"items.total": len(items)}) as span:
        meter = get_meter()
        extracted_counter = meter.create_counter(
            f"{platform}.items_extracted",
            description="Items with structured_data populated",
        )
        skipped_counter = meter.create_counter(
            f"{platform}.items_extract_skipped",
            description="Items skipped due to LLM failure",
        )

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for item in items:
                    content_type = item["content_type"]
                    prompt_template = STRUCTURE_PROMPTS.get(content_type)
                    if not prompt_template:
                        skipped += 1
                        skipped_counter.add(1)
                        continue

                    prompt = prompt_template.format(
                        title=item["title"][:300],
                        source=item["source"],
                        source_context=item["source_context"],
                        summary=item["summary"][:500],
                        body=item["body"][:3000],
                    )

                    try:
                        raw = ollama_chat(
                            [{"role": "user", "content": prompt}],
                            max_tokens=2048,
                            temperature=0.1,
                            timeout=120,
                        )
                        parsed = parse_llm_json(raw, str(item["id"]))
                    except Exception as exc:
                        logger.warning(
                            "LLM failed for item %s (%s): %s",
                            item["id"], content_type, exc,
                        )
                        skipped += 1
                        skipped_counter.add(1)
                        continue

                    if not parsed:
                        logger.warning(
                            "Empty LLM response for item %s — skipping", item["id"]
                        )
                        skipped += 1
                        skipped_counter.add(1)
                        continue

                    cursor.execute(
                        "UPDATE saved_items SET structured_data = %s WHERE id = %s",
                        (json.dumps(parsed), item["id"]),
                    )
                    conn.commit()
                    results.append({"id": item["id"], "content_type": content_type})
                    extracted_counter.add(1)
                    logger.debug(
                        "Item %s (%s / %s): keys=%s",
                        item["id"], item["source"], content_type, list(parsed.keys()),
                    )

        span.set_attribute("items.extracted", len(results))
        logger.info(
            "Extraction complete: %d extracted, %d skipped", len(results), skipped
        )
    return results


def run_classify_items(ids: list[int], platform: str) -> list[dict]:
    """
    LLM-classify content_type for items that don't have it.
    Used by bookmark_processor after scraping (where body is now available).
    Returns [{id, content_type}, ...] for classified items.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow_otel import instrument_task_context, get_meter

    if not ids:
        logger.info("No items to classify.")
        return []

    hook = PostgresHook(postgres_conn_id="social_archive_db")

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, source, source_context, type, title, summary, tags, uri, body
                FROM saved_items
                WHERE id = ANY(%s)
                  AND content_type IS NULL
            """, (ids,))
            rows = cursor.fetchall()

    items = [
        {
            "id": r[0], "source": r[1], "source_context": r[2] or "",
            "type": r[3] or "", "title": r[4] or "", "summary": r[5] or "",
            "tags": r[6] or [], "uri": r[7] or "", "body": r[8] or "",
        }
        for r in rows
    ]

    if not items:
        logger.info("All items already have content_type set.")
        return []

    results = []
    skipped = 0

    with instrument_task_context({"items.total": len(items)}) as span:
        meter = get_meter()
        classified_counter = meter.create_counter(
            f"{platform}.items_classified",
            description="Items classified by the LLM",
        )
        skipped_counter = meter.create_counter(
            f"{platform}.items_classify_skipped",
            description="Items skipped due to LLM failure",
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
                        body=item["body"][:1500],
                    )

                    try:
                        raw = ollama_chat(
                            [{"role": "user", "content": prompt}],
                            max_tokens=64,
                            temperature=0.1,
                            timeout=30,
                        )
                        parsed = parse_llm_json(raw, str(item["id"]))
                    except Exception as exc:
                        logger.warning("LLM failed for item %s: %s", item["id"], exc)
                        skipped_counter.add(1)
                        skipped += 1
                        continue

                    raw_ct = parsed.get("content_type", "").strip().lower()
                    content_type = raw_ct if raw_ct in CONTENT_TYPES else "other"

                    cursor.execute(
                        "UPDATE saved_items SET content_type = %s WHERE id = %s",
                        (content_type, item["id"]),
                    )
                    conn.commit()
                    results.append({"id": item["id"], "content_type": content_type})
                    classified_counter.add(1)

        span.set_attribute("items.classified", len(results))
        logger.info(
            "Classification complete: %d classified, %d skipped", len(results), skipped
        )
    return results
