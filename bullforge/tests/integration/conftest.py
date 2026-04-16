# bullforge/tests/integration/conftest.py
from __future__ import annotations

from pathlib import Path

import asyncpg
import pytest_asyncio

from shared.config import Settings
from shared.db import Database

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def _raw_conn():
    settings = Settings()
    conn = await asyncpg.connect(
        host=settings.timescaledb_host,
        port=settings.timescaledb_port,
        user=settings.timescaledb_user,
        password=settings.timescaledb_password,
        database=settings.timescaledb_db,
    )
    try:
        # Clear any legacy rows (e.g. raw Alpaca statuses like "new"/"accepted"
        # left in the volume from a prior test session) before migration 011
        # adds the CHECK constraint; otherwise ALTER TABLE would fail.
        orders_exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name='orders')"
        )
        if orders_exists:
            await conn.execute(
                "TRUNCATE trades, orders RESTART IDENTITY CASCADE"
            )
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            await conn.execute(path.read_text())
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture(loop_scope="session")
async def real_db(_raw_conn):
    await _raw_conn.execute("TRUNCATE trades, orders RESTART IDENTITY CASCADE")
    settings = Settings()
    db = Database(settings)
    await db.connect()
    try:
        yield db
    finally:
        await db.disconnect()
