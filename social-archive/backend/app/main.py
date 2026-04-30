import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .auth import hash_password
from .database import close_pool, get_connection, open_pool
from .routes import auth as auth_router
from .routes import items as items_router


async def _ensure_schema() -> None:
    async with get_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id          SERIAL PRIMARY KEY,
                    username    TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            # flagged_for_deletion is included in the DAG's CREATE TABLE;
            # this is a no-op safety net for any older rows.
            await cur.execute("""
                ALTER TABLE saved_items
                ADD COLUMN IF NOT EXISTS flagged_for_deletion BOOLEAN NOT NULL DEFAULT FALSE
            """)
            # Widen external_id to TEXT — original VARCHAR(50) is too short for URLs
            # used as external IDs by the bookmark sync. Safe: widening never loses data.
            await cur.execute("""
                ALTER TABLE saved_items
                ALTER COLUMN external_id TYPE text
            """)
            await cur.execute("""
                ALTER TABLE saved_items
                    ADD COLUMN IF NOT EXISTS time_estimate      VARCHAR(20),
                    ADD COLUMN IF NOT EXISTS estimate_reasoning TEXT,
                    ADD COLUMN IF NOT EXISTS estimated_at       TIMESTAMPTZ
            """)
            await cur.execute("""
                ALTER TABLE saved_items
                    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ
            """)
            # Seed default admin if no users exist
            await cur.execute("SELECT COUNT(*) FROM users")
            count = (await cur.fetchone())[0]
            if count == 0:
                admin_user = os.environ.get("ADMIN_USERNAME", "admin")
                admin_pass = os.environ.get("ADMIN_PASSWORD", "changeme")
                await cur.execute(
                    "INSERT INTO users (username, password_hash) VALUES (%s, %s)",
                    (admin_user, hash_password(admin_pass)),
                )


@asynccontextmanager
async def lifespan(app: FastAPI):
    await open_pool()
    await _ensure_schema()
    yield
    await close_pool()


app = FastAPI(title="Social Archive API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router.router)
app.include_router(items_router.router)
