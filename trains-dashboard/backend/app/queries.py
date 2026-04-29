"""SQL query functions for the trains dashboard.

Each function accepts a psycopg3 AsyncConnection and returns plain
Python dicts/lists so they can be tested without a real database.
"""
from __future__ import annotations

from .toc_names import toc_name


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
            "toc_name":    toc_name(r[0]),
            "total":       int(r[1]),
            "on_time":     int(r[2]),
            "late":        int(r[3]),
            "early":       int(r[4]),
            "on_time_pct": float(r[5] or 0.0),
        }
        for r in rows
    ]


async def get_recent_movements(conn, limit: int = 100) -> list[dict]:
    """Most recent movement events, newest first.

    Includes:
    - loc_stanme: human-readable name for the current location STANOX
      (from stanox_locations; falls back to the STANOX code if not loaded yet)
    - next_report_stanox / next_report_stanme: next scheduled reporting point,
      extracted from the stored raw_json body
    - origin_stanox / origin_stanme: first location seen for this train in
      the last 24 hours (proxy for journey origin)
    """
    async with conn.cursor() as cur:
        await cur.execute("""
            WITH recent AS (
                SELECT
                    train_id,
                    train_uid,
                    toc_id,
                    event_type,
                    loc_stanox,
                    (raw_json::jsonb -> 'body' ->> 'next_report_stanox') AS next_report_stanox,
                    variation_status,
                    timetable_variation,
                    actual_ts,
                    planned_ts,
                    msg_queue_ts
                FROM train_movements
                WHERE msg_type = '0003'
                ORDER BY msg_queue_ts DESC
                LIMIT %s
            ),
            first_seen AS (
                SELECT DISTINCT ON (m.train_id)
                    m.train_id,
                    m.loc_stanox AS origin_stanox
                FROM train_movements m
                JOIN (SELECT DISTINCT train_id FROM recent) t USING (train_id)
                WHERE m.msg_type = '0003'
                  AND m.msg_queue_ts >= NOW() - INTERVAL '24 hours'
                ORDER BY m.train_id, m.msg_queue_ts ASC
            )
            SELECT
                r.train_id,
                r.train_uid,
                r.toc_id,
                r.event_type,
                r.loc_stanox,
                COALESCE(sl.stanme,  sl.description,  r.loc_stanox)          AS loc_stanme,
                r.next_report_stanox,
                COALESCE(nsl.stanme, nsl.description, r.next_report_stanox)  AS next_report_stanme,
                f.origin_stanox,
                COALESCE(osl.stanme, osl.description, f.origin_stanox)       AS origin_stanme,
                r.variation_status,
                r.timetable_variation,
                r.actual_ts,
                r.planned_ts,
                r.msg_queue_ts
            FROM recent r
            LEFT JOIN first_seen f    ON f.train_id    = r.train_id
            LEFT JOIN stanox_locations sl  ON sl.stanox = LPAD(r.loc_stanox, 5, '0')
            LEFT JOIN stanox_locations nsl ON nsl.stanox = LPAD(r.next_report_stanox, 5, '0')
            LEFT JOIN stanox_locations osl ON osl.stanox = LPAD(f.origin_stanox, 5, '0')
            ORDER BY r.msg_queue_ts DESC
        """, (limit,))
        rows = await cur.fetchall()
    return [
        {
            "train_id":             r[0],
            "train_uid":            r[1],
            "toc_id":               r[2],
            "toc_name":             toc_name(r[2]),
            "event_type":           r[3],
            "loc_stanox":           r[4],
            "loc_stanme":           r[5],
            "next_report_stanox":   r[6],
            "next_report_stanme":   r[7],
            "origin_stanox":        r[8],
            "origin_stanme":        r[9],
            "variation_status":     r[10],
            "timetable_variation":  int(r[11]) if r[11] is not None else None,
            "actual_ts":            r[12].isoformat() if r[12] else None,
            "planned_ts":           r[13].isoformat() if r[13] else None,
            "msg_queue_ts":         r[14].isoformat() if r[14] else None,
        }
        for r in rows
    ]


async def get_station_delays(conn) -> list[dict]:
    """Top 20 stations by late-movement count for the last 24 hours."""
    async with conn.cursor() as cur:
        await cur.execute("""
            SELECT
                m.loc_stanox,
                COALESCE(sl.stanme, sl.description, m.loc_stanox) AS station_name,
                COUNT(*)                                            AS late_count,
                COUNT(*) FILTER (WHERE m.variation_status = 'ON TIME') AS on_time_count,
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE m.variation_status = 'LATE')
                    / NULLIF(COUNT(*), 0),
                    1
                )                                                   AS late_pct
            FROM train_movements m
            LEFT JOIN stanox_locations sl
                ON sl.stanox = LPAD(m.loc_stanox, 5, '0')
            WHERE m.msg_type = '0003'
              AND m.msg_queue_ts >= NOW() - INTERVAL '24 hours'
              AND m.variation_status = 'LATE'
              AND m.loc_stanox IS NOT NULL
            GROUP BY m.loc_stanox, station_name
            ORDER BY late_count DESC
            LIMIT 20
        """)
        rows = await cur.fetchall()
    return [
        {
            "loc_stanox":   r[0],
            "station_name": r[1],
            "late_count":   int(r[2]),
            "on_time_count": int(r[3]),
            "late_pct":     float(r[4] or 0.0),
        }
        for r in rows
    ]


async def get_otp_trend(conn) -> list[dict]:
    """On-time percentage per hour for the last 24 hours."""
    async with conn.cursor() as cur:
        await cur.execute("""
            SELECT
                DATE_TRUNC('hour', msg_queue_ts) AS hour,
                ROUND(
                    100.0
                    * COUNT(*) FILTER (WHERE variation_status = 'ON TIME')
                    / NULLIF(COUNT(*), 0),
                    1
                ) AS on_time_pct,
                COUNT(*) AS total
            FROM train_movements
            WHERE msg_type = '0003'
              AND msg_queue_ts >= NOW() - INTERVAL '24 hours'
            GROUP BY 1
            ORDER BY 1
        """)
        rows = await cur.fetchall()
    return [
        {
            "hour":        r[0].isoformat() if r[0] else None,
            "on_time_pct": float(r[1] or 0.0),
            "total":       int(r[2]),
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
