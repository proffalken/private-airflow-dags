"""CORPUS Reference Data Loader DAG.

Downloads the Network Rail CORPUS (Containing Operating Reference Point
Unified Solution) file from the NROD data feeds and upserts all STANOX
location records into the rail_network PostgreSQL database.

The CORPUS JSON maps STANOX codes to human-readable station names (STANME),
TIPLOC codes, CRS (National Rail 3-letter) codes, and NLC codes.  The
trains-dashboard backend JOINs this table to display station names instead
of raw STANOX codes.

Prerequisites
─────────────
Set the following Airflow Variables (Admin → Variables) before running:
  • NROD_USERNAME  — your Network Rail Open Data username
  • NROD_PASSWORD  — your Network Rail Open Data password

The same NROD account used by the TRUST consumer STOMP connection will work.

Schedule
────────
Weekly — CORPUS data changes only when stations open, close, or are
renamed, which is rare.  Re-running more frequently is harmless.
"""
from __future__ import annotations

import logging

import pendulum
from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

CORPUS_URL = (
    "https://publicdatafeeds.networkrail.co.uk"
    "/ntrod/SupportingFileAuthentication?type=CORPUS"
)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stanox_locations (
    stanox      VARCHAR(6)  PRIMARY KEY,
    tiploc      VARCHAR(7),
    stanme      VARCHAR(26),
    crs         VARCHAR(3),
    nlc         VARCHAR(4),
    description TEXT
);
"""

_UPSERT_SQL = """
INSERT INTO stanox_locations (stanox, tiploc, stanme, crs, nlc, description)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (stanox) DO UPDATE SET
    tiploc      = EXCLUDED.tiploc,
    stanme      = EXCLUDED.stanme,
    crs         = EXCLUDED.crs,
    nlc         = EXCLUDED.nlc,
    description = EXCLUDED.description;
"""


def _parse_corpus(data: dict) -> list[tuple]:
    """Extract and normalise records from the raw CORPUS JSON dict.

    Filters out entries with no STANOX, zero-pads STANOX to 5 digits,
    and trims all string values.
    """
    records = []
    for entry in data.get("TIPLOCDATA", []):
        raw_stanox = entry.get("STANOX", "").strip()
        if not raw_stanox or raw_stanox == "00000":
            continue
        stanox      = raw_stanox.zfill(5)
        tiploc      = (entry.get("TIPLOC")  or "").strip() or None
        stanme      = (entry.get("STANME")  or "").strip() or None
        crs         = (entry.get("CRS")     or "").strip() or None
        nlc         = (entry.get("NLC")     or "").strip() or None
        description = (entry.get("NLCDESC") or "").strip() or None
        records.append((stanox, tiploc, stanme, crs, nlc, description))
    return records


@dag(
    dag_id="corpus_loader",
    schedule="@weekly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["reference-data", "rail"],
    doc_md=__doc__,
)
def corpus_loader():

    @task
    def download_corpus() -> dict:
        """Fetch the CORPUS JSON from Network Rail."""
        import requests

        username = Variable.get("NROD_USERNAME")
        password = Variable.get("NROD_PASSWORD")

        log.info("Downloading CORPUS from %s", CORPUS_URL)
        resp = requests.get(
            CORPUS_URL,
            auth=(username, password),
            timeout=120,
            headers={"Accept-Encoding": "gzip"},
        )
        resp.raise_for_status()
        data = resp.json()
        entry_count = len(data.get("TIPLOCDATA", []))
        log.info("Downloaded %d CORPUS entries", entry_count)
        return data

    @task
    def load_to_db(data: dict) -> dict:
        """Parse and upsert CORPUS records into stanox_locations."""
        records = _parse_corpus(data)
        log.info("Parsed %d valid STANOX records", len(records))

        hook = PostgresHook(postgres_conn_id="rail_network_db")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(_CREATE_TABLE_SQL)
                conn.commit()

                # Batch in chunks of 500 to avoid very large transactions
                chunk_size = 500
                for i in range(0, len(records), chunk_size):
                    chunk = records[i : i + chunk_size]
                    cur.executemany(_UPSERT_SQL, chunk)
                    conn.commit()

        log.info("Upserted %d STANOX locations into rail_network_db", len(records))
        return {"loaded": len(records)}

    corpus_data = download_corpus()
    load_to_db(corpus_data)


corpus_loader()
