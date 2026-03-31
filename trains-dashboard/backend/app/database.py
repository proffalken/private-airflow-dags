import os
from contextlib import asynccontextmanager

from psycopg_pool import AsyncConnectionPool

_pool: AsyncConnectionPool | None = None

DATABASE_URL = os.environ.get("DATABASE_URL", "")

_CREATE_STANOX_TABLE = """
    CREATE TABLE IF NOT EXISTS stanox_locations (
        stanox      VARCHAR(6)  PRIMARY KEY,
        tiploc      VARCHAR(7),
        stanme      VARCHAR(26),
        crs         VARCHAR(3),
        nlc         VARCHAR(4),
        description TEXT
    )
"""


async def open_pool() -> None:
    global _pool
    conninfo = DATABASE_URL.replace("postgresql+psycopg://", "postgresql://")
    _pool = AsyncConnectionPool(conninfo=conninfo, min_size=2, max_size=10, open=False)
    await _pool.open()
    # Ensure reference table exists even before the CORPUS loader DAG has run.
    async with _pool.connection() as conn:
        await conn.execute(_CREATE_STANOX_TABLE)
        await conn.commit()


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()


@asynccontextmanager
async def get_connection():
    async with _pool.connection() as conn:
        yield conn
