import os
from contextlib import asynccontextmanager

from psycopg_pool import AsyncConnectionPool

_pool: AsyncConnectionPool | None = None

DATABASE_URL = os.environ.get("DATABASE_URL", "")


async def open_pool() -> None:
    global _pool
    conninfo = DATABASE_URL.replace("postgresql+psycopg://", "postgresql://")
    _pool = AsyncConnectionPool(conninfo=conninfo, min_size=2, max_size=10, open=False)
    await _pool.open()


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()


@asynccontextmanager
async def get_connection():
    async with _pool.connection() as conn:
        yield conn
