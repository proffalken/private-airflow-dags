#!/usr/bin/env python3
"""
Social Archive MCP server.

Gives Claude read/write access to the social archive so it can:
  - Search and browse saved items
  - Re-classify items (content_type) or fix tags without touching SQL
  - Export recipe/project lists for Mealie/Gridstock workflows

Talks directly to the FastAPI backend (Bearer token auth).

Environment variables:
    SOCIAL_ARCHIVE_API_URL   Backend API base URL
                             (e.g. https://api.social-archive.wallace.network)
    SOCIAL_ARCHIVE_USERNAME  Login username
    SOCIAL_ARCHIVE_PASSWORD  Login password

Claude Desktop config (~/.config/Claude/claude_desktop_config.json):
    {
      "mcpServers": {
        "social-archive": {
          "command": "python",
          "args": ["/path/to/social-archive/mcp/server.py"],
          "env": {
            "SOCIAL_ARCHIVE_API_URL": "https://api.social-archive.wallace.network",
            "SOCIAL_ARCHIVE_USERNAME": "...",
            "SOCIAL_ARCHIVE_PASSWORD": "..."
          }
        }
      }
    }
"""
from __future__ import annotations

import os
import time
from typing import Any, Optional

import httpx
from fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_BASE = os.environ["SOCIAL_ARCHIVE_API_URL"].rstrip("/")
_USERNAME = os.environ["SOCIAL_ARCHIVE_USERNAME"]
_PASSWORD = os.environ["SOCIAL_ARCHIVE_PASSWORD"]

mcp = FastMCP(
    "social-archive",
    instructions=(
        "Tools for querying and updating a personal social archive — saved items "
        "from Instagram, Reddit, YouTube, GitHub, and browser bookmarks. "
        "Items are classified by content_type (recipe, project, article, reference, "
        "tool, other) and may carry structured_data (ingredients/steps for recipes, "
        "materials/tools for projects). Use export_items to get clean lists for "
        "pushing to Mealie (recipes) or comparing against Gridstock (projects). "
        "Use update_item to correct misclassifications or fix tags."
    ),
)

# ---------------------------------------------------------------------------
# Auth — JWT with auto-refresh
# ---------------------------------------------------------------------------

_token: str | None = None
_token_expires_at: float = 0.0


def _get_token() -> str:
    global _token, _token_expires_at
    # Refresh if missing or within 60 s of expiry (JWT is 24 h)
    if _token and time.time() < _token_expires_at - 60:
        return _token
    resp = httpx.post(
        f"{_BASE}/auth/login",
        json={"username": _USERNAME, "password": _PASSWORD},
        timeout=10,
    )
    resp.raise_for_status()
    _token = resp.json()["access_token"]
    _token_expires_at = time.time() + 86400  # 24 h
    return _token


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {_get_token()}"}


def _get(path: str, params: dict | None = None) -> Any:
    resp = httpx.get(f"{_BASE}{path}", headers=_headers(), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _patch(path: str, body: dict) -> Any:
    resp = httpx.patch(f"{_BASE}{path}", headers=_headers(), json=body, timeout=10)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def search_items(
    q: Optional[str] = None,
    content_type: Optional[str] = None,
    tags: Optional[list[str]] = None,
    source_context: Optional[str] = None,
    flagged: Optional[bool] = None,
    limit: int = 20,
) -> dict:
    """Search the social archive.

    Args:
        q:              Full-text search across title and body.
        content_type:   Filter by type: recipe, project, article, reference, tool, other.
        tags:           Filter to items that have ALL of these tags.
        source_context: Filter by source context (e.g. subreddit name, Instagram
                        collection name, YouTube playlist name).
        flagged:        True = only flagged items, False = only unflagged.
        limit:          Max results to return (default 20, max 200).

    Returns a dict with `items` list and `total` count.
    """
    params: dict[str, Any] = {"limit": min(limit, 200)}
    if q:
        params["q"] = q
    if content_type:
        params["content_type"] = content_type
    if tags:
        params["tags"] = tags
    if source_context:
        params["source_context"] = source_context
    if flagged is not None:
        params["flagged"] = str(flagged).lower()
    return _get("/api/items", params)


@mcp.tool()
def get_item(item_id: int) -> dict:
    """Fetch a single item by ID, including its full structured_data.

    Args:
        item_id: The numeric ID of the item.
    """
    return _get(f"/api/items/{item_id}")


@mcp.tool()
def export_items(
    content_type: str,
    tags: Optional[list[str]] = None,
    source_context: Optional[str] = None,
    limit: int = 200,
) -> list[dict]:
    """Export a flat list of items optimised for LLM workflows — no pagination envelope.

    Excludes flagged and deleted items. Typical use:
      - export_items('recipe')  → push to Mealie meal planner
      - export_items('project') → compare materials against Gridstock inventory

    Args:
        content_type:   Required. One of: recipe, project, article, reference, tool, other.
        tags:           Optionally narrow to items with these tags.
        source_context: Optionally narrow to a specific source context.
        limit:          Max items to return (default 200, max 500).
    """
    params: dict[str, Any] = {"content_type": content_type, "limit": min(limit, 500)}
    if tags:
        params["tags"] = tags
    if source_context:
        params["source_context"] = source_context
    return _get("/api/items/export", params)


@mcp.tool()
def update_item(
    item_id: int,
    title: Optional[str] = None,
    tags: Optional[list[str]] = None,
    content_type: Optional[str] = None,
) -> dict:
    """Update an item's title, tags, and/or content_type.

    Use this to correct misclassifications or fix bad tags without going to SQL.
    Only the fields you provide are changed — omitted fields are left as-is.

    Args:
        item_id:      The numeric ID of the item to update.
        title:        New title string, or None to leave unchanged.
        tags:         Complete replacement tag list (lowercase), or None to leave unchanged.
        content_type: New content type — one of: recipe, project, article, reference,
                      tool, other. Or None to leave unchanged.
    """
    body: dict[str, Any] = {}
    if title is not None:
        body["title"] = title
    if tags is not None:
        body["tags"] = [t.strip().lower() for t in tags]
    if content_type is not None:
        body["content_type"] = content_type
    if not body:
        return {"error": "Nothing to update — supply at least one field"}
    return _patch(f"/api/items/{item_id}", body)


@mcp.tool()
def flag_item(item_id: int, flagged: bool) -> dict:
    """Flag or unflag an item for deletion.

    Args:
        item_id: The numeric ID of the item.
        flagged: True to flag, False to unflag.
    """
    return _patch(f"/api/items/{item_id}/flag", {"flagged_for_deletion": flagged})


@mcp.tool()
def list_content_types() -> list[str]:
    """Return the distinct content_type values that currently exist in the archive.

    Useful for knowing what filter values are valid before calling search_items
    or export_items.
    """
    return _get("/api/content_types")


@mcp.tool()
def list_source_contexts() -> list[str]:
    """Return the distinct source_context values in the archive.

    Source contexts are collection names (Instagram), subreddit names (Reddit),
    playlist names (YouTube), and primary languages (GitHub).
    """
    return _get("/api/source_contexts")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run()
