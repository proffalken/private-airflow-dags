"""Trains Dashboard — FastAPI backend.

Exposes four read-only endpoints that the Next.js frontend fetches
server-side.  All queries target the rail_network PostgreSQL database
populated by the trust_movements_loader Airflow DAG.
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from .database import close_pool, get_connection, open_pool
from .queries import get_hourly_counts, get_otp_trend, get_performance, get_recent_movements, get_station_delays, get_summary

logger = logging.getLogger("trains_dashboard")


def _setup_otel(app: FastAPI) -> None:
    """Wire up OTel tracing if OTEL_EXPORTER_OTLP_ENDPOINT is set."""
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        logger.info("OTEL_EXPORTER_OTLP_ENDPOINT not set — OTel disabled")
        return
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource({SERVICE_NAME: os.getenv("OTEL_SERVICE_NAME", "trains-dashboard")})
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(provider)
        FastAPIInstrumentor.instrument_app(app)
        logger.info("OTel initialised, exporting to %s", endpoint)
    except Exception as exc:  # noqa: BLE001
        logger.warning("OTel setup failed (non-fatal): %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await open_pool()
    yield
    await close_pool()


app = FastAPI(title="Trains Dashboard API", version="1.0.0", lifespan=lifespan)
_setup_otel(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/api/summary")
async def summary():
    """Today's headline stats (total movements, cancellations, OTP, avg delay)."""
    async with get_connection() as conn:
        return await get_summary(conn)


@app.get("/api/performance")
async def performance():
    """On-time performance by operator (TOC) for the last 24 hours."""
    async with get_connection() as conn:
        return await get_performance(conn)


@app.get("/api/movements/recent")
async def recent_movements(limit: int = Query(default=100, le=500)):
    """Most recent movement events, newest first."""
    async with get_connection() as conn:
        return await get_recent_movements(conn, limit)


@app.get("/api/stations/delays")
async def station_delays():
    """Top 20 stations by late-movement count for the last 24 hours."""
    async with get_connection() as conn:
        return await get_station_delays(conn)


@app.get("/api/movements/otp-trend")
async def otp_trend():
    """On-time percentage per hour for the last 24 hours."""
    async with get_connection() as conn:
        return await get_otp_trend(conn)


@app.get("/api/movements/hourly")
async def hourly_counts():
    """Movement count per hour for the last 24 hours."""
    async with get_connection() as conn:
        return await get_hourly_counts(conn)
