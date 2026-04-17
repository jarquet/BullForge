"""
Integration tests for `Database._sync_sandbox_schema`.

Verifies the live-pool connect path auto-repairs drift between
`public.<table>` and `backtest.<table>` that arises when a migration
ALTERs a public-schema table without an explicit schema-qualified mirror.

Requires a running Postgres (matches the other `test_*.py` integration tests
under `bullforge/tests/`).
"""
from __future__ import annotations

import pytest

from shared.config import Settings
from shared.db import (
    Database,
    _build_add_column_ddl,
    _index_body,
    _rewrite_index_for_backtest,
)


# --- Pure-function tests (no DB required) --------------------------------


def test_index_body_strips_prefix():
    idxdef = (
        "CREATE UNIQUE INDEX my_idx ON public.my_table "
        "USING btree (a, b) WHERE (x = 1)"
    )
    body = _index_body(idxdef)
    assert body == "USING btree (a, b) WHERE (x = 1)"


def test_index_body_matches_across_schemas():
    """Same index on two schemas has identical bodies."""
    a = "CREATE UNIQUE INDEX foo ON public.t USING btree (c)"
    b = "CREATE UNIQUE INDEX foo ON backtest.t USING btree (c)"
    assert _index_body(a) == _index_body(b)


def test_rewrite_index_points_at_backtest():
    idxdef = (
        "CREATE UNIQUE INDEX idx_model_registry_meta_active "
        "ON public.model_registry USING btree "
        "(symbol, timeframe, horizon_minutes, model_type, estimator_type) "
        "WHERE ((is_active = true) AND (estimator_type IS NOT NULL))"
    )
    out = _rewrite_index_for_backtest(
        idxdef, "idx_model_registry_meta_active", "model_registry"
    )
    assert out is not None
    assert "IF NOT EXISTS backtest_idx_model_registry_meta_active" in out
    assert 'backtest."model_registry"' in out
    assert "WHERE ((is_active = true) AND (estimator_type IS NOT NULL))" in out


def test_rewrite_index_returns_none_for_garbage():
    assert _rewrite_index_for_backtest("not an index ddl", "x", "t") is None


def test_build_add_column_ddl_text_nullable():
    col = _fake_col(column_name="estimator_type", udt_name="text",
                    data_type="text", is_nullable="YES")
    stmt = _build_add_column_ddl("model_registry", col)
    assert stmt == (
        'ALTER TABLE backtest."model_registry" '
        'ADD COLUMN IF NOT EXISTS "estimator_type" text'
    )


def test_build_add_column_ddl_not_null_with_default():
    col = _fake_col(column_name="is_active", udt_name="bool",
                    data_type="boolean", is_nullable="NO",
                    column_default="true")
    stmt = _build_add_column_ddl("model_registry", col)
    assert stmt == (
        'ALTER TABLE backtest."model_registry" '
        'ADD COLUMN IF NOT EXISTS "is_active" bool DEFAULT true NOT NULL'
    )


def test_build_add_column_ddl_varchar_with_length():
    col = _fake_col(column_name="code", udt_name="varchar",
                    data_type="character varying", is_nullable="YES",
                    character_maximum_length=32)
    stmt = _build_add_column_ddl("t", col)
    assert "varchar(32)" in stmt


def test_build_add_column_ddl_numeric_with_precision():
    col = _fake_col(column_name="price", udt_name="numeric",
                    data_type="numeric", is_nullable="YES",
                    numeric_precision=18, numeric_scale=4)
    stmt = _build_add_column_ddl("t", col)
    assert "numeric(18,4)" in stmt


def test_build_add_column_ddl_array():
    col = _fake_col(column_name="tags", udt_name="_text",
                    data_type="ARRAY", is_nullable="YES")
    stmt = _build_add_column_ddl("t", col)
    assert "text[]" in stmt


def _fake_col(**overrides) -> dict:
    base = {
        "column_name": "x",
        "udt_name": "text",
        "data_type": "text",
        "is_nullable": "YES",
        "column_default": None,
        "character_maximum_length": None,
        "numeric_precision": None,
        "numeric_scale": None,
    }
    base.update(overrides)
    return base


# --- DB-backed tests -----------------------------------------------------

pytestmark_db = pytest.mark.asyncio


@pytest.mark.asyncio
async def test_sync_adds_missing_column_to_sandbox():
    """
    Simulates the exact bug from commit 65cb90f: public gets a new column
    via unqualified ALTER, sandbox is left behind. Sync must repair it.
    """
    settings = Settings()
    live = Database(settings)
    await live.connect()
    try:
        async with live.pool.acquire() as conn:
            # Manually drop the column from backtest.model_registry to
            # simulate pre-sync drift. (It may or may not exist depending
            # on prior test runs — IF EXISTS keeps this idempotent.)
            await conn.execute(
                "ALTER TABLE backtest.model_registry "
                "DROP COLUMN IF EXISTS sync_probe_col"
            )
            # Add the drift marker only to public.
            await conn.execute(
                "ALTER TABLE public.model_registry "
                "ADD COLUMN IF NOT EXISTS sync_probe_col TEXT"
            )

        # Re-run sync (already ran once during connect(), but run again
        # explicitly to verify idempotence and that the drift we just
        # introduced gets repaired).
        await live._sync_sandbox_schema()

        async with live.pool.acquire() as conn:
            present = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'backtest'
                      AND table_name = 'model_registry'
                      AND column_name = 'sync_probe_col'
                )
                """
            )
        assert present, (
            "sync_sandbox_schema did not propagate public column to backtest"
        )
    finally:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "ALTER TABLE backtest.model_registry "
                "DROP COLUMN IF EXISTS sync_probe_col"
            )
            await conn.execute(
                "ALTER TABLE public.model_registry "
                "DROP COLUMN IF EXISTS sync_probe_col"
            )
        await live.disconnect()


@pytest.mark.asyncio
async def test_sync_adds_missing_index_to_sandbox():
    settings = Settings()
    live = Database(settings)
    await live.connect()
    try:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "DROP INDEX IF EXISTS public.idx_sync_probe_index"
            )
            await conn.execute(
                "DROP INDEX IF EXISTS backtest.backtest_idx_sync_probe_index"
            )
            await conn.execute(
                "CREATE INDEX idx_sync_probe_index "
                "ON public.model_registry (model_type)"
            )

        await live._sync_sandbox_schema()

        async with live.pool.acquire() as conn:
            present = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM pg_indexes
                    WHERE schemaname = 'backtest'
                      AND tablename = 'model_registry'
                      AND indexdef ILIKE '%model_type%'
                      AND indexname LIKE '%sync_probe_index%'
                )
                """
            )
        assert present, (
            "sync_sandbox_schema did not propagate public index to backtest"
        )
    finally:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "DROP INDEX IF EXISTS public.idx_sync_probe_index"
            )
            await conn.execute(
                "DROP INDEX IF EXISTS backtest.backtest_idx_sync_probe_index"
            )
        await live.disconnect()


@pytest.mark.asyncio
async def test_sync_is_idempotent():
    """Two back-to-back syncs must not error or duplicate anything."""
    settings = Settings()
    live = Database(settings)
    await live.connect()
    try:
        await live._sync_sandbox_schema()
        await live._sync_sandbox_schema()
    finally:
        await live.disconnect()


@pytest.mark.asyncio
async def test_sync_runs_automatically_on_connect_and_repairs_estimator_type():
    """
    End-to-end: remove the column from backtest, connect a fresh Database,
    and verify the auto-sync during connect() puts it back. Reproduces the
    original bug end-to-end.
    """
    settings = Settings()
    prep = Database(settings)
    await prep.connect()
    try:
        async with prep.pool.acquire() as conn:
            await conn.execute(
                "ALTER TABLE backtest.model_registry "
                "DROP COLUMN IF EXISTS estimator_type"
            )
        await prep.disconnect()

        live = Database(settings)
        await live.connect()
        try:
            async with live.pool.acquire() as conn:
                present = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'backtest'
                          AND table_name = 'model_registry'
                          AND column_name = 'estimator_type'
                    )
                    """
                )
            assert present, (
                "connect() did not auto-repair missing estimator_type"
            )
        finally:
            await live.disconnect()
    finally:
        # Ensure the column is present regardless of how the test exits,
        # so subsequent tests and the worker don't fail on it.
        restore = Database(settings)
        await restore.connect()
        await restore.disconnect()
