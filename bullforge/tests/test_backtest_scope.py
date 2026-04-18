import pytest

from shared.models import BacktestScope


def test_backtest_scope_is_frozen():
    s = BacktestScope(backtest_run_id=1, fold_index=0)
    with pytest.raises((AttributeError, Exception)):
        s.backtest_run_id = 2  # type: ignore[misc]


def test_backtest_scope_equality_and_hash():
    a = BacktestScope(backtest_run_id=1, fold_index=0)
    b = BacktestScope(backtest_run_id=1, fold_index=0)
    c = BacktestScope(backtest_run_id=1, fold_index=1)
    assert a == b
    assert a != c
    assert hash(a) == hash(b)
    assert {a, b, c} == {a, c}


from datetime import datetime, timezone

from shared.config import Settings
from shared.db import Database
from shared.models import ModelMetadata


def _meta(**overrides) -> ModelMetadata:
    base = dict(
        symbol="SCOPETEST",
        timeframe="1m",
        horizon_minutes=5,
        model_type="gradient_boost",
        artifact_path="/tmp/fake.joblib",
        metrics={"accuracy": 0.5},
        feature_columns=["a"],
        trained_at=datetime.now(timezone.utc),
        train_samples=1,
        test_samples=1,
        is_active=True,
    )
    base.update(overrides)
    return ModelMetadata(**base)


@pytest.mark.asyncio
async def test_insert_model_metadata_live_writes_nulls():
    settings = Settings()
    live = Database(settings)
    await live.connect()
    try:
        row_id = await live.insert_model_metadata(_meta())
        async with live.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT backtest_run_id, fold_index FROM public.model_registry WHERE id=$1",
                row_id,
            )
        assert row["backtest_run_id"] is None
        assert row["fold_index"] is None
    finally:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM public.model_registry WHERE symbol='SCOPETEST'"
            )
        await live.disconnect()


@pytest.mark.asyncio
async def test_insert_model_metadata_sandbox_writes_scope():
    settings = Settings()
    live = Database(settings)
    sandbox = Database(settings, sandbox=True)
    await live.connect()
    await sandbox.connect()
    try:
        scope = BacktestScope(backtest_run_id=999_001, fold_index=3)
        row_id = await sandbox.insert_model_metadata(_meta(), scope=scope)
        async with live.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT backtest_run_id, fold_index FROM backtest.model_registry WHERE id=$1",
                row_id,
            )
        assert row["backtest_run_id"] == 999_001
        assert row["fold_index"] == 3
    finally:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM backtest.model_registry WHERE symbol='SCOPETEST'"
            )
        await sandbox.disconnect()
        await live.disconnect()


@pytest.mark.asyncio
async def test_query_all_active_models_scoped_filters_to_run():
    settings = Settings()
    live = Database(settings)
    sandbox = Database(settings, sandbox=True)
    await live.connect()
    await sandbox.connect()
    try:
        scope_a = BacktestScope(backtest_run_id=999_101, fold_index=0)
        scope_b = BacktestScope(backtest_run_id=999_102, fold_index=0)
        await sandbox.insert_model_metadata(_meta(symbol="SCOPEA"), scope=scope_a)
        await sandbox.insert_model_metadata(_meta(symbol="SCOPEB"), scope=scope_b)

        a_models = await sandbox.query_all_active_models(scope=scope_a)
        b_models = await sandbox.query_all_active_models(scope=scope_b)
        assert {m.symbol for m in a_models} == {"SCOPEA"}
        assert {m.symbol for m in b_models} == {"SCOPEB"}
    finally:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM backtest.model_registry WHERE symbol IN ('SCOPEA','SCOPEB')"
            )
        await sandbox.disconnect()
        await live.disconnect()


@pytest.mark.asyncio
async def test_deactivate_models_scoped_does_not_touch_other_runs():
    settings = Settings()
    live = Database(settings)
    sandbox = Database(settings, sandbox=True)
    await live.connect()
    await sandbox.connect()
    try:
        scope_a = BacktestScope(backtest_run_id=999_201, fold_index=0)
        scope_b = BacktestScope(backtest_run_id=999_202, fold_index=0)
        await sandbox.insert_model_metadata(_meta(symbol="SCOPEX"), scope=scope_a)
        await sandbox.insert_model_metadata(_meta(symbol="SCOPEX"), scope=scope_b)

        await sandbox.deactivate_models("SCOPEX", "1m", 5, scope=scope_a)

        a_active = await sandbox.query_all_active_models(scope=scope_a)
        b_active = await sandbox.query_all_active_models(scope=scope_b)
        assert a_active == []
        assert len(b_active) == 1 and b_active[0].symbol == "SCOPEX"
    finally:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM backtest.model_registry WHERE symbol='SCOPEX'"
            )
        await sandbox.disconnect()
        await live.disconnect()
