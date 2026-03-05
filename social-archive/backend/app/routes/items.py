from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from ..database import get_db
from ..models import EditRequest, FlagRequest, ItemResponse, ItemsResponse
from .auth import get_current_user

router = APIRouter()


@router.get("/api/items", response_model=ItemsResponse)
async def list_items(
    q: Optional[str] = None,
    tags: list[str] = Query(default=[]),
    source_context: Optional[str] = None,
    type_filter: Optional[str] = Query(default=None, alias="type"),
    flagged: Optional[bool] = None,
    limit: int = Query(default=50, le=200),
    offset: int = 0,
    db=Depends(get_db),
    _: str = Depends(get_current_user),
):
    conditions: list[str] = []
    params: list = []

    if q:
        conditions.append(
            "(to_tsvector('english', coalesce(title,'') || ' ' || coalesce(body,''))"
            " @@ plainto_tsquery('english', %s))"
        )
        params.append(q)
    if tags:
        conditions.append("tags && %s::text[]")
        params.append(tags)
    if source_context:
        conditions.append("source_context = %s")
        params.append(source_context)
    if type_filter:
        conditions.append("type = %s")
        params.append(type_filter)
    if flagged is not None:
        conditions.append("flagged_for_deletion = %s")
        params.append(flagged)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    count_sql = f"SELECT COUNT(*) FROM saved_items {where}"
    data_sql = f"""
        SELECT id, title, uri, body, source_context, type,
               summary, tags, flagged_for_deletion, saved_at
        FROM saved_items {where}
        ORDER BY saved_at DESC NULLS LAST
        LIMIT %s OFFSET %s
    """

    async with db.cursor() as cur:
        await cur.execute(count_sql, params)
        total = (await cur.fetchone())[0]

        await cur.execute(data_sql, params + [limit, offset])
        rows = await cur.fetchall()

    items = [
        ItemResponse(
            id=r[0],
            title=r[1],
            uri=r[2],
            body=r[3],
            source_context=r[4],
            type=r[5],
            summary=r[6],
            tags=r[7] or [],
            flagged_for_deletion=r[8] or False,
            saved_at=r[9],
        )
        for r in rows
    ]
    return ItemsResponse(items=items, total=total, limit=limit, offset=offset)


@router.get("/api/source_contexts")
async def list_source_contexts(
    db=Depends(get_db),
    _: str = Depends(get_current_user),
):
    async with db.cursor() as cur:
        await cur.execute(
            "SELECT DISTINCT source_context FROM saved_items"
            " WHERE source_context IS NOT NULL ORDER BY source_context"
        )
        rows = await cur.fetchall()
    return [r[0] for r in rows]


@router.patch("/api/items/{item_id}")
async def edit_item(
    item_id: int,
    body: EditRequest,
    db=Depends(get_db),
    _: str = Depends(get_current_user),
):
    updates: list[str] = []
    params: list = []
    if body.title is not None:
        updates.append("title = %s")
        params.append(body.title)
    if body.tags is not None:
        updates.append("tags = %s")
        params.append(body.tags)
    if not updates:
        raise HTTPException(status_code=400, detail="Nothing to update")
    params.append(item_id)
    async with db.cursor() as cur:
        await cur.execute(
            f"UPDATE saved_items SET {', '.join(updates)} WHERE id = %s RETURNING id",
            params,
        )
        row = await cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Item not found")
    return {"id": item_id}


@router.patch("/api/items/{item_id}/flag")
async def flag_item(
    item_id: int,
    body: FlagRequest,
    db=Depends(get_db),
    _: str = Depends(get_current_user),
):
    async with db.cursor() as cur:
        await cur.execute(
            "UPDATE saved_items SET flagged_for_deletion = %s WHERE id = %s RETURNING id",
            (body.flagged_for_deletion, item_id),
        )
        row = await cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Item not found")
    return {"id": item_id, "flagged_for_deletion": body.flagged_for_deletion}
