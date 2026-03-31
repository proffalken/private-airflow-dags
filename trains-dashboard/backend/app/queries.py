"""SQL query functions for the trains dashboard.

Each function accepts a psycopg3 AsyncConnection and returns plain
Python dicts/lists so they can be tested without a real database.
"""
from __future__ import annotations


async def get_summary(conn) -> dict:
    """Today's headline stats (UTC day boundary)."""
    async with conn.cursor() as cur:
        await cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE msg_type = '0003')                        AS total_movements,
                COUNT(*) FILTER (WHERE msg_type = '0002')                        AS cancellations,
                ROUND(
                    100.0
                    * COUNT(*) FILTER (WHERE msg_type = '0003' AND variation_status = 'ON TIME')
                    / NULLIF(COUNT(*) FILTER (WHERE msg_type = '0003'), 0),
                    1
                )                                                                AS on_time_pct,
                ROUND(
                    AVG(timetable_variation)
                        FILTER (WHERE msg_type = '0003' AND timetable_variation > 0)::numeric,
                    1
                )                                                                AS avg_delay_mins
            FROM train_movements
            WHERE msg_queue_ts >= DATE_TRUNC('day', NOW() AT TIME ZONE 'UTC')
        """)
        row = await cur.fetchone()
    return {
        "total_movements": int(row[0] or 0),
        "cancellations":   int(row[1] or 0),
        "on_time_pct":     float(row[2] or 0.0),
        "avg_delay_mins":  float(row[3] or 0.0),
    }


async def get_performance(conn) -> list[dict]:
    """On-time performance by TOC for the last 24 hours, ordered by volume."""
    async with conn.cursor() as cur:
        await cur.execute("""
            SELECT
                toc_id,
                COUNT(*)                                                     AS total,
                COUNT(*) FILTER (WHERE variation_status = 'ON TIME')         AS on_time,
                COUNT(*) FILTER (WHERE variation_status = 'LATE')            AS late,
                COUNT(*) FILTER (WHERE variation_status = 'EARLY')           AS early,
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE variation_status = 'ON TIME')
                    / NULLIF(COUNT(*), 0),
                    1
                )                                                            AS on_time_pct
            FROM train_movements
            WHERE msg_type = '0003'
              AND msg_queue_ts >= NOW() - INTERVAL '24 hours'
              AND toc_id IS NOT NULL
            GROUP BY toc_id
            ORDER BY total DESC
            LIMIT 20
        """)
        rows = await cur.fetchall()
    return [
        {
            "toc_id":      r[0],
            "total":       int(r[1]),
            "on_time":     int(r[2]),
            "late":        int(r[3]),
            "early":       int(r[4]),
            "on_time_pct": float(r[5] or 0.0),
        }
        for r in rows
    ]


async def get_recent_movements(conn, limit: int = 100) -> list[dict]:
    """Most recent movement events, newest first."""
    async with conn.cursor() as cur:
        await cur.execute("""
            SELECT
                train_id, train_uid, toc_id, event_type, loc_stanox,
                variation_status, timetable_variation,
                actual_ts, planned_ts, msg_queue_ts
            FROM train_movements
            WHERE msg_type = '0003'
            ORDER BY msg_queue_ts DESC
            LIMIT %s
        """, (limit,))
        rows = await cur.fetchall()
    return [
        {
            "train_id":            r[0],
            "train_uid":           r[1],
            "toc_id":              r[2],
            "event_type":          r[3],
            "loc_stanox":          r[4],
            "variation_status":    r[5],
            "timetable_variation": int(r[6]) if r[6] is not None else None,
            "actual_ts":           r[7].isoformat() if r[7] else None,
            "planned_ts":          r[8].isoformat() if r[8] else None,
            "msg_queue_ts":        r[9].isoformat() if r[9] else None,
        }
        for r in rows
    ]


async def get_hourly_counts(conn) -> list[dict]:
    """Movement count per hour for the last 24 hours."""
    async with conn.cursor() as cur:
        await cur.execute("""
            SELECT
                DATE_TRUNC('hour', msg_queue_ts) AS hour,
                COUNT(*)                          AS count
            FROM train_movements
            WHERE msg_queue_ts >= NOW() - INTERVAL '24 hours'
            GROUP BY 1
            ORDER BY 1
        """)
        rows = await cur.fetchall()
    return [
        {
            "hour":  r[0].isoformat() if r[0] else None,
            "count": int(r[1]),
        }
        for r in rows
    ]
