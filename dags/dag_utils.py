"""Shared DAG utilities: LLM helpers and OTel instrumentation wrappers."""

from __future__ import annotations

import json
import logging
import re

logger = logging.getLogger(__name__)

OLLAMA_BASE_URL = "http://ollama.ollama.svc.cluster.local:11434/v1"
OLLAMA_NATIVE_URL = "http://ollama.ollama.svc.cluster.local:11434"
OLLAMA_MODEL = "huihui_ai/qwen3-abliterated:4b"


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
