"""
Rail Network Analysis DAG
=========================
Daily pipeline that ingests Network Rail Open Data (NROD) feeds and produces
network-level performance metrics stored in the rail_network PostgreSQL database.

Data flow:
  fetch_corpus ──► parse_corpus ──────────────────────────┐
                                                           ├──► analyse_by_route ──────┐
  fetch_schedule ──► parse_schedule ──────────────────────┤                           │
                          │               analyse_by_station ────────────────────────► aggregate ──► store_results ──► llm_summary
                          └──► extract_toc_list ──► analyse_by_toc ───────────────────┘

Airflow Variables required (Admin → Variables):
  NROD_USERNAME   - Network Rail Open Data portal username
  NROD_PASSWORD   - Network Rail Open Data portal password

Airflow Connection required (Admin → Connections):
  rail_network_db - PostgreSQL connection to the rail_network database
                    (host: ossway-pg-rw.default.svc.cluster.local, port: 5432,
                     schema: rail_network, login: app)
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import tempfile
from datetime import timedelta
from typing import Any

import pendulum
import requests

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import chain, dag, get_current_context, task, Variable
from airflow.traces import otel_tracer
from airflow.traces.tracer import Trace

from airflow_otel import instrument_task_context, get_meter
from dag_utils import get_llm_client, instrument_llm, instrument_requests, parse_llm_json, OLLAMA_MODEL

logger = logging.getLogger("airflow.rail_network_analysis")

NROD_BASE = "https://datafeeds.networkrail.co.uk/ntrod"
CORPUS_URL = f"{NROD_BASE}/SupportingFileAuthenticate?type=CORPUS"
CIF_URL = f"{NROD_BASE}/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full"

# Batch size for DB inserts to avoid huge transactions
INSERT_BATCH = 500


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS rail_locations (
    tiploc      VARCHAR(7)   PRIMARY KEY,
    crs         CHAR(3),
    name        TEXT,
    stanox      VARCHAR(5),
    nlc         VARCHAR(6),
    updated_at  TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rail_schedules (
    id              SERIAL PRIMARY KEY,
    run_date        DATE        NOT NULL,
    train_uid       VARCHAR(6)  NOT NULL,
    date_runs_from  DATE        NOT NULL,
    date_runs_to    DATE        NOT NULL,
    days_run        CHAR(7)     NOT NULL,
    train_status    CHAR(1),
    train_category  CHAR(2),
    toc_id          CHAR(2),
    stp_indicator   CHAR(1),
    UNIQUE (run_date, train_uid, date_runs_from, stp_indicator)
);
CREATE INDEX IF NOT EXISTS rail_schedules_run_date ON rail_schedules (run_date);
CREATE INDEX IF NOT EXISTS rail_schedules_toc ON rail_schedules (toc_id);

CREATE TABLE IF NOT EXISTS rail_schedule_stops (
    id           SERIAL  PRIMARY KEY,
    schedule_id  INTEGER NOT NULL REFERENCES rail_schedules (id) ON DELETE CASCADE,
    stop_type    CHAR(2) NOT NULL,
    tiploc       VARCHAR(7) NOT NULL,
    sequence     SMALLINT NOT NULL,
    arrive       CHAR(5),
    depart       CHAR(5),
    pass         CHAR(5),
    platform     VARCHAR(3),
    activity     VARCHAR(12)
);
CREATE INDEX IF NOT EXISTS rail_stops_schedule ON rail_schedule_stops (schedule_id);
CREATE INDEX IF NOT EXISTS rail_stops_tiploc ON rail_schedule_stops (tiploc);

CREATE TABLE IF NOT EXISTS rail_toc_metrics (
    id             SERIAL PRIMARY KEY,
    run_date       DATE       NOT NULL,
    toc_id         CHAR(2)    NOT NULL,
    service_count  INTEGER    NOT NULL DEFAULT 0,
    route_count    INTEGER    NOT NULL DEFAULT 0,
    station_count  INTEGER    NOT NULL DEFAULT 0,
    UNIQUE (run_date, toc_id)
);

CREATE TABLE IF NOT EXISTS rail_route_metrics (
    id              SERIAL PRIMARY KEY,
    run_date        DATE       NOT NULL,
    origin_tiploc   VARCHAR(7) NOT NULL,
    dest_tiploc     VARCHAR(7) NOT NULL,
    origin_crs      CHAR(3),
    dest_crs        CHAR(3),
    origin_name     TEXT,
    dest_name       TEXT,
    daily_frequency INTEGER    NOT NULL DEFAULT 0,
    toc_count       SMALLINT   NOT NULL DEFAULT 0,
    UNIQUE (run_date, origin_tiploc, dest_tiploc)
);

CREATE TABLE IF NOT EXISTS rail_station_metrics (
    id              SERIAL PRIMARY KEY,
    run_date        DATE       NOT NULL,
    tiploc          VARCHAR(7) NOT NULL,
    crs             CHAR(3),
    name            TEXT,
    call_count      INTEGER    NOT NULL DEFAULT 0,
    toc_count       SMALLINT   NOT NULL DEFAULT 0,
    terminus_count  INTEGER    NOT NULL DEFAULT 0,
    UNIQUE (run_date, tiploc)
);

CREATE TABLE IF NOT EXISTS rail_run_metrics (
    id                  SERIAL PRIMARY KEY,
    run_date            DATE        NOT NULL UNIQUE,
    corpus_locations    INTEGER,
    schedules_parsed    INTEGER,
    tocs_active         SMALLINT,
    route_pairs         INTEGER,
    stations_active     INTEGER,
    completed_at        TIMESTAMPTZ DEFAULT NOW()
);
"""


def _get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id="rail_network_db")


def _ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        for statement in SCHEMA_SQL.split(";"):
            stmt = statement.strip()
            if stmt:
                cur.execute(stmt)
    conn.commit()


def _nrod_auth() -> tuple[str, str]:
    return Variable.get("NROD_USERNAME"), Variable.get("NROD_PASSWORD")


# ---------------------------------------------------------------------------
# Fetch tasks
# ---------------------------------------------------------------------------

@task
def fetch_corpus(ti) -> str:
    """Download CORPUS location reference JSON from NROD → temp file."""
    logger.info("Fetching CORPUS reference data from NROD")
    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)

    with instrument_task_context({"nrod.feed": "CORPUS"}) as span:
        instrument_requests()
        meter = get_meter("rail.fetch")
        size_gauge = meter.create_gauge(
            "rail.corpus.download_bytes",
            unit="By",
            description="Bytes downloaded for CORPUS feed",
        )

        username, password = _nrod_auth()
        with otel_task_tracer.start_child_span(span_name="rail.fetch.corpus") as fetch_span:
            fetch_span.set_attribute("http.url", CORPUS_URL)
            resp = requests.get(CORPUS_URL, auth=(username, password), stream=True, timeout=120)
            resp.raise_for_status()

            tmp = tempfile.NamedTemporaryFile(
                suffix=".json.gz", delete=False,
                prefix=f"corpus_{ti.run_id}_"
            )
            total_bytes = 0
            for chunk in resp.iter_content(chunk_size=65536):
                tmp.write(chunk)
                total_bytes += len(chunk)
            tmp.close()

            fetch_span.set_attribute("http.status_code", resp.status_code)
            fetch_span.set_attribute("download.bytes", total_bytes)
            size_gauge.set(total_bytes)
            logger.info("CORPUS downloaded: %d bytes → %s", total_bytes, tmp.name)

    return tmp.name


@task
def fetch_schedule(ti) -> str:
    """Download CIF full daily schedule from NROD → temp file."""
    logger.info("Fetching CIF full schedule from NROD")
    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)

    with instrument_task_context({"nrod.feed": "CIF_FULL"}) as span:
        instrument_requests()
        meter = get_meter("rail.fetch")
        size_gauge = meter.create_gauge(
            "rail.schedule.download_bytes",
            unit="By",
            description="Bytes downloaded for CIF schedule feed",
        )

        username, password = _nrod_auth()
        with otel_task_tracer.start_child_span(span_name="rail.fetch.schedule") as fetch_span:
            fetch_span.set_attribute("http.url", CIF_URL)
            resp = requests.get(CIF_URL, auth=(username, password), stream=True, timeout=600)
            resp.raise_for_status()

            tmp = tempfile.NamedTemporaryFile(
                suffix=".cif.gz", delete=False,
                prefix=f"cif_{ti.run_id}_"
            )
            total_bytes = 0
            for chunk in resp.iter_content(chunk_size=65536):
                tmp.write(chunk)
                total_bytes += len(chunk)
            tmp.close()

            fetch_span.set_attribute("http.status_code", resp.status_code)
            fetch_span.set_attribute("download.bytes", total_bytes)
            size_gauge.set(total_bytes)
            logger.info("CIF downloaded: %d bytes → %s", total_bytes, tmp.name)

    return tmp.name


# ---------------------------------------------------------------------------
# Parse tasks
# ---------------------------------------------------------------------------

@task
def parse_corpus(corpus_path: str) -> dict[str, Any]:
    """Parse CORPUS JSON and upsert into rail_locations. Returns summary dict."""
    logger.info("Parsing CORPUS from %s", corpus_path)
    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)

    with instrument_task_context({"nrod.feed": "CORPUS"}) as span:
        meter = get_meter("rail.parse")
        parsed_counter = meter.create_counter(
            "rail.corpus.locations_parsed",
            unit="1",
            description="Number of CORPUS location records parsed",
        )

        with otel_task_tracer.start_child_span(span_name="rail.parse.corpus") as parse_span:
            with gzip.open(corpus_path, "rt", encoding="utf-8") as f:
                data = json.load(f)

            records = data.get("TIPLOCDATA", [])
            total = len(records)
            with_crs = sum(1 for r in records if r.get("3ALPHA", "").strip())
            parse_span.set_attribute("corpus.total_records", total)
            parse_span.set_attribute("corpus.with_crs", with_crs)
            logger.info("CORPUS: %d records, %d with CRS code", total, with_crs)

        hook = _get_hook()
        with hook.get_conn() as conn:
            _ensure_schema(conn)
            with otel_task_tracer.start_child_span(span_name="rail.parse.corpus.upsert") as upsert_span:
                rows = []
                for rec in records:
                    tiploc = rec.get("TIPLOC", "").strip()
                    if not tiploc:
                        continue
                    def _s(val) -> str | None:
                        """Coerce a CORPUS field to a stripped string, or None if empty."""
                        return str(val).strip() or None if val is not None else None

                    rows.append((
                        tiploc,
                        _s(rec.get("3ALPHA")),
                        _s(rec.get("NLCDESC16") or rec.get("NLCDESC")),
                        _s(rec.get("STANOX")),
                        _s(rec.get("NLC")),
                    ))

                with conn.cursor() as cur:
                    for i in range(0, len(rows), INSERT_BATCH):
                        batch = rows[i:i + INSERT_BATCH]
                        cur.executemany("""
                            INSERT INTO rail_locations (tiploc, crs, name, stanox, nlc)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (tiploc) DO UPDATE SET
                                crs  = EXCLUDED.crs,
                                name = EXCLUDED.name,
                                stanox = EXCLUDED.stanox,
                                nlc  = EXCLUDED.nlc,
                                updated_at = NOW()
                        """, batch)
                conn.commit()
                parsed_counter.add(len(rows))
                upsert_span.set_attribute("corpus.rows_upserted", len(rows))
                logger.info("CORPUS upserted %d rows", len(rows))

    os.unlink(corpus_path)
    return {"locations_total": total, "locations_with_crs": with_crs}



@task
def parse_schedule(schedule_path: str) -> dict[str, Any]:
    """
    Stream-parse CIF full schedule and insert into rail_schedules + rail_schedule_stops.
    Returns a summary dict including the list of TOC IDs found.
    """
    run_date: str = get_current_context()["ds"]
    logger.info("Parsing CIF schedule from %s (run_date=%s)", schedule_path, run_date)
    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)

    with instrument_task_context({"nrod.feed": "CIF_FULL", "run_date": run_date}) as span:
        meter = get_meter("rail.parse")
        schedule_counter = meter.create_counter(
            "rail.schedule.records_parsed",
            unit="1",
            description="CIF schedule records parsed by type",
        )

        hook = _get_hook()
        with hook.get_conn() as conn:
            _ensure_schema(conn)

            # Clear existing schedules for this run_date (full reload)
            with conn.cursor() as cur:
                cur.execute("DELETE FROM rail_schedules WHERE run_date = %s", (run_date,))
                deleted = cur.rowcount
            conn.commit()
            logger.info("Cleared %d existing schedules for %s", deleted, run_date)

            # Stream-parse the JSON Lines CIF file (JsonTimetableV1 format)
            schedule_count = 0
            stop_count = 0
            tocs_seen: set[str] = set()
            status_counts: dict[str, int] = {}

            with otel_task_tracer.start_child_span(span_name="rail.parse.schedule.stream") as stream_span:
                with conn.cursor() as cur:
                    with gzip.open(schedule_path, "rt", encoding="utf-8") as f:
                        lines_read = 0
                        for line in f:
                            lines_read += 1
                            if not line.strip():
                                continue

                            try:
                                obj = json.loads(line)
                            except json.JSONDecodeError:
                                continue

                            sched = obj.get("JsonScheduleV1")
                            if not sched:
                                continue

                            # Skip delete records
                            if sched.get("transaction_type") == "Delete":
                                continue

                            uid = sched.get("CIF_train_uid", "").strip()
                            date_from = sched.get("schedule_start_date")
                            date_to = sched.get("schedule_end_date")
                            days_run = sched.get("schedule_days_runs", "")
                            status = sched.get("train_status") or None
                            stp = sched.get("CIF_stp_indicator") or None
                            toc = sched.get("atoc_code") or None
                            seg = sched.get("schedule_segment") or {}
                            cat = seg.get("CIF_train_category") or None

                            if not uid or not date_from or not date_to:
                                continue

                            cur.execute("""
                                INSERT INTO rail_schedules
                                    (run_date, train_uid, date_runs_from, date_runs_to,
                                     days_run, train_status, train_category, toc_id, stp_indicator)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (run_date, train_uid, date_runs_from, stp_indicator)
                                DO UPDATE SET
                                    date_runs_to   = EXCLUDED.date_runs_to,
                                    days_run       = EXCLUDED.days_run,
                                    train_status   = EXCLUDED.train_status,
                                    train_category = EXCLUDED.train_category,
                                    toc_id         = EXCLUDED.toc_id
                                RETURNING id
                            """, (
                                run_date, uid, date_from, date_to,
                                days_run, status, cat, toc, stp,
                            ))
                            row = cur.fetchone()
                            schedule_id = row[0] if row else None
                            schedule_count += 1

                            if toc:
                                tocs_seen.add(toc)
                            status_counts[status or "?"] = status_counts.get(status or "?", 0) + 1
                            schedule_counter.add(1, {"record_type": "JsonScheduleV1"})

                            if schedule_id:
                                stops = []
                                for seq, loc in enumerate(seg.get("schedule_location", [])):
                                    loc_type = loc.get("location_type") or loc.get("record_identity")
                                    if loc_type not in ("LO", "LI", "LT"):
                                        continue
                                    stops.append((
                                        loc_type,
                                        loc.get("tiploc_code", "").strip(),
                                        seq,
                                        loc.get("arrival") or loc.get("public_arrival"),
                                        loc.get("departure") or loc.get("public_departure"),
                                        loc.get("pass"),
                                        loc.get("platform"),
                                        None,  # activity not in JSON format
                                    ))
                                if stops:
                                    cur.executemany("""
                                        INSERT INTO rail_schedule_stops
                                            (schedule_id, stop_type, tiploc, sequence,
                                             arrive, depart, pass, platform, activity)
                                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                    """, [(schedule_id, *s) for s in stops])
                                    stop_count += len(stops)

                            if schedule_count % INSERT_BATCH == 0:
                                conn.commit()
                                logger.info("Parsed %d schedules so far...", schedule_count)

                    conn.commit()
                    stream_span.set_attribute("cif.lines_read", lines_read)
                    stream_span.set_attribute("cif.schedules_parsed", schedule_count)
                    stream_span.set_attribute("cif.stops_parsed", stop_count)
                    stream_span.set_attribute("cif.toc_count", len(tocs_seen))
                    logger.info(
                        "CIF parse complete: %d schedules, %d stops, %d TOCs",
                        schedule_count, stop_count, len(tocs_seen)
                    )

        span.set_attribute("cif.schedule_count", schedule_count)
        span.set_attribute("cif.toc_count", len(tocs_seen))

    os.unlink(schedule_path)
    return {
        "schedule_count": schedule_count,
        "stop_count": stop_count,
        "toc_list": sorted(tocs_seen),
        "status_counts": status_counts,
        "run_date": run_date,
    }


# ---------------------------------------------------------------------------
# Extract TOC list
# ---------------------------------------------------------------------------

@task
def extract_toc_list(parse_result: dict[str, Any]) -> list[str]:
    """Pass through the TOC list from parse_schedule for use in analyse_by_toc."""
    tocs = parse_result["toc_list"]
    logger.info("Active TOCs this run: %s", tocs)

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    with instrument_task_context({"stage": "extract_toc_list"}) as span:
        span.set_attribute("toc.count", len(tocs))
        with otel_task_tracer.start_child_span(span_name="rail.extract.toc_list") as s:
            s.set_attribute("toc.list", ",".join(tocs))
    return tocs


# ---------------------------------------------------------------------------
# Analysis tasks
# ---------------------------------------------------------------------------

@task
def analyse_by_toc(toc_list: list[str], parse_result: dict[str, Any]) -> dict[str, Any]:
    """
    For each TOC: count services, distinct route O/D pairs, distinct stations.
    Writes to rail_toc_metrics.
    """
    run_date = parse_result["run_date"]
    logger.info("Analysing %d TOCs for %s", len(toc_list), run_date)
    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)

    with instrument_task_context({"stage": "analyse_by_toc", "run_date": run_date}) as span:
        meter = get_meter("rail.analysis")
        toc_gauge = meter.create_gauge(
            "rail.analysis.toc_services",
            unit="1",
            description="Number of scheduled services per TOC",
        )

        hook = _get_hook()
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM rail_toc_metrics WHERE run_date = %s", (run_date,)
                )

            results = {}
            with otel_task_tracer.start_child_span(span_name="rail.analyse.by_toc") as parent:
                parent.set_attribute("toc.count", len(toc_list))

                for toc in toc_list:
                    with otel_task_tracer.start_child_span(
                        span_name=f"rail.analyse.toc.{toc}"
                    ) as toc_span:
                        with conn.cursor() as cur:
                            # Service count
                            cur.execute("""
                                SELECT COUNT(*) FROM rail_schedules
                                WHERE run_date = %s AND toc_id = %s
                            """, (run_date, toc))
                            service_count = cur.fetchone()[0]

                            # Route count (distinct origin→destination pairs)
                            cur.execute("""
                                SELECT COUNT(*) FROM (
                                    SELECT DISTINCT lo.tiploc AS origin, lt.tiploc AS dest
                                    FROM rail_schedules s
                                    JOIN rail_schedule_stops lo ON lo.schedule_id = s.id AND lo.stop_type = 'LO'
                                    JOIN rail_schedule_stops lt ON lt.schedule_id = s.id AND lt.stop_type = 'LT'
                                    WHERE s.run_date = %s AND s.toc_id = %s
                                ) routes
                            """, (run_date, toc))
                            route_count = cur.fetchone()[0]

                            # Station count (distinct tiplocs called at)
                            cur.execute("""
                                SELECT COUNT(DISTINCT ss.tiploc)
                                FROM rail_schedules s
                                JOIN rail_schedule_stops ss ON ss.schedule_id = s.id
                                WHERE s.run_date = %s AND s.toc_id = %s
                            """, (run_date, toc))
                            station_count = cur.fetchone()[0]

                            cur.execute("""
                                INSERT INTO rail_toc_metrics
                                    (run_date, toc_id, service_count, route_count, station_count)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (run_date, toc_id) DO UPDATE SET
                                    service_count = EXCLUDED.service_count,
                                    route_count   = EXCLUDED.route_count,
                                    station_count = EXCLUDED.station_count
                            """, (run_date, toc, service_count, route_count, station_count))

                        toc_span.set_attribute("toc.id", toc)
                        toc_span.set_attribute("toc.service_count", service_count)
                        toc_span.set_attribute("toc.route_count", route_count)
                        toc_span.set_attribute("toc.station_count", station_count)
                        toc_gauge.set(service_count, {"toc": toc})

                        results[toc] = {
                            "services": service_count,
                            "routes": route_count,
                            "stations": station_count,
                        }
                        logger.info(
                            "TOC %s: %d services, %d routes, %d stations",
                            toc, service_count, route_count, station_count,
                        )

            conn.commit()

        span.set_attribute("tocs.analysed", len(results))
        total_services = sum(v["services"] for v in results.values())
        span.set_attribute("total.services", total_services)

    return {"toc_metrics": results, "run_date": run_date}


@task
def analyse_by_route(
    corpus_result: dict[str, Any],
    parse_result: dict[str, Any],
) -> dict[str, Any]:
    """
    For each origin→destination pair: count daily frequency and number of TOCs.
    Writes to rail_route_metrics.
    """
    run_date = parse_result["run_date"]
    logger.info("Analysing routes for %s", run_date)
    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)

    with instrument_task_context({"stage": "analyse_by_route", "run_date": run_date}) as span:
        meter = get_meter("rail.analysis")
        route_gauge = meter.create_gauge(
            "rail.analysis.route_density",
            unit="1",
            description="Daily service frequency between origin/destination pairs",
        )

        hook = _get_hook()
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM rail_route_metrics WHERE run_date = %s", (run_date,)
                )

            with otel_task_tracer.start_child_span(span_name="rail.analyse.by_route") as route_span:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT
                            lo.tiploc       AS origin,
                            lt.tiploc       AS dest,
                            COUNT(*)        AS frequency,
                            COUNT(DISTINCT s.toc_id) AS toc_count
                        FROM rail_schedules s
                        JOIN rail_schedule_stops lo
                            ON lo.schedule_id = s.id AND lo.stop_type = 'LO'
                        JOIN rail_schedule_stops lt
                            ON lt.schedule_id = s.id AND lt.stop_type = 'LT'
                        WHERE s.run_date = %s
                        GROUP BY lo.tiploc, lt.tiploc
                        ORDER BY frequency DESC
                    """, (run_date,))
                    rows = cur.fetchall()

                route_span.set_attribute("route.pair_count", len(rows))
                logger.info("Found %d distinct route pairs", len(rows))

                # Enrich with CRS codes + names from rail_locations
                with otel_task_tracer.start_child_span(span_name="rail.analyse.by_route.enrich") as enrich_span:
                    with conn.cursor() as cur:
                        batch = []
                        top_freq = 0
                        for origin, dest, freq, toc_count in rows:
                            if freq > top_freq:
                                top_freq = freq

                            cur.execute(
                                "SELECT crs, name FROM rail_locations WHERE tiploc = %s",
                                (origin,)
                            )
                            o_row = cur.fetchone() or (None, None)
                            cur.execute(
                                "SELECT crs, name FROM rail_locations WHERE tiploc = %s",
                                (dest,)
                            )
                            d_row = cur.fetchone() or (None, None)

                            batch.append((
                                run_date, origin, dest,
                                o_row[0], d_row[0],
                                o_row[1], d_row[1],
                                freq, toc_count,
                            ))

                            if len(batch) >= INSERT_BATCH:
                                cur.executemany("""
                                    INSERT INTO rail_route_metrics
                                        (run_date, origin_tiploc, dest_tiploc,
                                         origin_crs, dest_crs, origin_name, dest_name,
                                         daily_frequency, toc_count)
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                    ON CONFLICT (run_date, origin_tiploc, dest_tiploc)
                                    DO UPDATE SET
                                        daily_frequency = EXCLUDED.daily_frequency,
                                        toc_count = EXCLUDED.toc_count
                                """, batch)
                                batch = []

                        if batch:
                            cur.executemany("""
                                INSERT INTO rail_route_metrics
                                    (run_date, origin_tiploc, dest_tiploc,
                                     origin_crs, dest_crs, origin_name, dest_name,
                                     daily_frequency, toc_count)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (run_date, origin_tiploc, dest_tiploc)
                                DO UPDATE SET
                                    daily_frequency = EXCLUDED.daily_frequency,
                                    toc_count = EXCLUDED.toc_count
                            """, batch)

                    conn.commit()
                    enrich_span.set_attribute("route.rows_inserted", len(rows))
                    enrich_span.set_attribute("route.top_frequency", top_freq)
                    route_gauge.set(top_freq, {"label": "top_pair"})

        span.set_attribute("route.pairs", len(rows))

    return {"route_pairs": len(rows), "run_date": run_date}


@task
def analyse_by_station(
    corpus_result: dict[str, Any],
    parse_result: dict[str, Any],
) -> dict[str, Any]:
    """
    For each station (tiploc): count total calls, distinct TOCs, times as terminus.
    Writes to rail_station_metrics.
    """
    run_date = parse_result["run_date"]
    logger.info("Analysing stations for %s", run_date)
    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)

    with instrument_task_context({"stage": "analyse_by_station", "run_date": run_date}) as span:
        meter = get_meter("rail.analysis")
        station_gauge = meter.create_gauge(
            "rail.analysis.station_calls",
            unit="1",
            description="Total scheduled calls per station",
        )

        hook = _get_hook()
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM rail_station_metrics WHERE run_date = %s", (run_date,)
                )

            with otel_task_tracer.start_child_span(span_name="rail.analyse.by_station") as station_span:
                with conn.cursor() as cur:
                    # Total calls + distinct TOCs per tiploc
                    cur.execute("""
                        SELECT
                            ss.tiploc,
                            COUNT(*)                     AS call_count,
                            COUNT(DISTINCT s.toc_id)     AS toc_count
                        FROM rail_schedule_stops ss
                        JOIN rail_schedules s ON s.id = ss.schedule_id
                        WHERE s.run_date = %s
                          AND ss.stop_type IN ('LO','LI','LT')
                        GROUP BY ss.tiploc
                        ORDER BY call_count DESC
                    """, (run_date,))
                    call_rows = cur.fetchall()

                    # Terminus counts (LT stop_type = train terminates here)
                    cur.execute("""
                        SELECT ss.tiploc, COUNT(*) AS terminus_count
                        FROM rail_schedule_stops ss
                        JOIN rail_schedules s ON s.id = ss.schedule_id
                        WHERE s.run_date = %s AND ss.stop_type = 'LT'
                        GROUP BY ss.tiploc
                    """, (run_date,))
                    terminus_map = {row[0]: row[1] for row in cur.fetchall()}

                station_span.set_attribute("station.count", len(call_rows))
                logger.info("Found %d distinct stations", len(call_rows))
                top_station_calls = call_rows[0][1] if call_rows else 0
                station_gauge.set(top_station_calls, {"label": "busiest"})

                # Enrich with CRS / name from rail_locations and batch-insert
                with otel_task_tracer.start_child_span(
                    span_name="rail.analyse.by_station.enrich"
                ) as enrich_span:
                    with conn.cursor() as cur:
                        batch = []
                        for tiploc, call_count, toc_count in call_rows:
                            cur.execute(
                                "SELECT crs, name FROM rail_locations WHERE tiploc = %s",
                                (tiploc,)
                            )
                            loc = cur.fetchone() or (None, None)
                            batch.append((
                                run_date, tiploc,
                                loc[0], loc[1],
                                call_count, toc_count,
                                terminus_map.get(tiploc, 0),
                            ))

                            if len(batch) >= INSERT_BATCH:
                                cur.executemany("""
                                    INSERT INTO rail_station_metrics
                                        (run_date, tiploc, crs, name,
                                         call_count, toc_count, terminus_count)
                                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                                    ON CONFLICT (run_date, tiploc) DO UPDATE SET
                                        call_count     = EXCLUDED.call_count,
                                        toc_count      = EXCLUDED.toc_count,
                                        terminus_count = EXCLUDED.terminus_count
                                """, batch)
                                batch = []

                        if batch:
                            cur.executemany("""
                                INSERT INTO rail_station_metrics
                                    (run_date, tiploc, crs, name,
                                     call_count, toc_count, terminus_count)
                                VALUES (%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (run_date, tiploc) DO UPDATE SET
                                    call_count     = EXCLUDED.call_count,
                                    toc_count      = EXCLUDED.toc_count,
                                    terminus_count = EXCLUDED.terminus_count
                            """, batch)

                    conn.commit()
                    enrich_span.set_attribute("station.rows_inserted", len(call_rows))

        span.set_attribute("station.count", len(call_rows))

    return {"station_count": len(call_rows), "run_date": run_date}


# ---------------------------------------------------------------------------
# Aggregate and store
# ---------------------------------------------------------------------------

@task
def aggregate_metrics(
    toc_result: dict[str, Any],
    route_result: dict[str, Any],
    station_result: dict[str, Any],
    parse_result: dict[str, Any],
    corpus_result: dict[str, Any],
) -> dict[str, Any]:
    """Roll up totals from all three analysis tasks into a single summary."""
    run_date = parse_result["run_date"]

    summary = {
        "run_date": run_date,
        "corpus_locations": corpus_result["locations_total"],
        "schedules_parsed": parse_result["schedule_count"],
        "tocs_active": len(toc_result["toc_metrics"]),
        "route_pairs": route_result["route_pairs"],
        "stations_active": station_result["station_count"],
    }

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    with instrument_task_context({"stage": "aggregate_metrics", "run_date": run_date}) as span:
        with otel_task_tracer.start_child_span(span_name="rail.aggregate") as agg_span:
            for k, v in summary.items():
                if k != "run_date":
                    agg_span.set_attribute(k, v)

    logger.info("Aggregate for %s: %s", run_date, summary)
    return summary


@task
def store_results(summary: dict[str, Any]) -> None:
    """Upsert the run summary into rail_run_metrics."""
    run_date = summary["run_date"]
    logger.info("Storing run metrics for %s", run_date)
    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)

    with instrument_task_context({"stage": "store_results", "run_date": run_date}) as span:
        hook = _get_hook()
        with hook.get_conn() as conn:
            with otel_task_tracer.start_child_span(span_name="rail.store.run_metrics") as store_span:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO rail_run_metrics
                            (run_date, corpus_locations, schedules_parsed,
                             tocs_active, route_pairs, stations_active)
                        VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (run_date) DO UPDATE SET
                            corpus_locations = EXCLUDED.corpus_locations,
                            schedules_parsed = EXCLUDED.schedules_parsed,
                            tocs_active      = EXCLUDED.tocs_active,
                            route_pairs      = EXCLUDED.route_pairs,
                            stations_active  = EXCLUDED.stations_active,
                            completed_at     = NOW()
                    """, (
                        run_date,
                        summary["corpus_locations"],
                        summary["schedules_parsed"],
                        summary["tocs_active"],
                        summary["route_pairs"],
                        summary["stations_active"],
                    ))
                conn.commit()
                store_span.set_attribute("run_date", run_date)
                store_span.set_attribute("rows.upserted", 1)

        span.set_attribute("run_date", run_date)
    logger.info("Run metrics stored for %s", run_date)


# ---------------------------------------------------------------------------
# LLM narrative summary
# ---------------------------------------------------------------------------

@task
def llm_summary(summary: dict[str, Any], toc_result: dict[str, Any]) -> None:
    """Ask the LLM to narrate the day's rail network metrics and log the result."""
    run_date = summary["run_date"]
    logger.info("=" * 80)
    logger.info("Generating LLM narrative summary for %s", run_date)

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    client = get_llm_client()

    toc_lines = "\n".join(
        f"  {toc}: {m['services']} services, {m['routes']} routes, {m['stations']} stations"
        for toc, m in sorted(toc_result["toc_metrics"].items())
    )

    prompt = (
        f"You are a railway analyst. Write a concise, plain-English summary of the "
        f"UK rail network for {run_date} based on the following schedule data.\n\n"
        f"Network totals:\n"
        f"  Scheduled services: {summary['schedules_parsed']}\n"
        f"  Active stations: {summary['stations_active']}\n"
        f"  Unique routes: {summary['route_pairs']}\n"
        f"  Train operating companies: {summary['tocs_active']}\n"
        f"  Known locations (CORPUS): {summary['corpus_locations']}\n\n"
        f"Per-operator breakdown:\n{toc_lines}\n\n"
        f"Write 3-4 sentences highlighting the scale of operations, the busiest operators, "
        f"and anything noteworthy. Be factual and concise."
    )

    with instrument_task_context({"stage": "llm_summary", "run_date": run_date}) as span:
        instrument_requests()
        instrument_llm()
        with otel_task_tracer.start_child_span(span_name="llm.summarise_network") as llm_span:
            llm_span.set_attribute("run_date", run_date)
            llm_span.set_attribute("toc.count", summary["tocs_active"])

            response = client.chat.completions.create(
                model=OLLAMA_MODEL,
                max_tokens=512,
                messages=[{"role": "user", "content": prompt}],
            )

            narrative = (response.choices[0].message.content or "").strip()
            llm_span.set_attribute("summary.length", len(narrative))

    logger.info("=" * 80)
    logger.info("NETWORK SUMMARY — %s", run_date)
    logger.info("=" * 80)
    logger.info(narrative)
    logger.info("=" * 80)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    schedule=timedelta(days=1),
    start_date=pendulum.datetime(2026, 3, 21, tz="UTC"),
    catchup=False,
)
def rail_network_analysis():
    # Parallel fetches
    corpus_path = fetch_corpus()
    schedule_path = fetch_schedule()

    # Parse
    corpus_result = parse_corpus(corpus_path)
    parse_result = parse_schedule(schedule_path)

    # Extract TOC list from parse result
    toc_list = extract_toc_list(parse_result)

    # Fan-out: three independent analysis tasks
    toc_result = analyse_by_toc(toc_list, parse_result)
    route_result = analyse_by_route(corpus_result, parse_result)
    station_result = analyse_by_station(corpus_result, parse_result)

    # Fan-in: aggregate, store, then narrate
    summary = aggregate_metrics(toc_result, route_result, station_result, parse_result, corpus_result)
    store_results(summary) >> llm_summary(summary, toc_result)


rail_network_analysis()
