"""Tests for the /api/backtests/* extra DB query methods.

Uses the real_db fixture from tests/integration/conftest.py.
"""
from datetime import datetime, timedelta, timezone

import pytest

from shared.models import BacktestScope


pytestmark = pytest.mark.asyncio(loop_scope="session")


async def test_set_backtest_tags(real_db):
    # Insert a minimal run.
    async with real_db.pool.acquire() as conn:
        run_id = await conn.fetchval(
            """
            INSERT INTO backtest_runs (name, symbols, timeframe, horizons,
                range_start, range_end, train_days, test_days, embargo_min,
                mode, config_json, status)
            VALUES ('t', ARRAY['AAPL'], '1m', ARRAY[5],
                NOW(), NOW() + INTERVAL '1 day', 30, 5, 30,
                'rolling', '{}'::jsonb, 'completed')
            RETURNING id
            """
        )
    await real_db.set_backtest_tags(run_id, ["tagA", "tagB"])
    tags = await real_db.get_backtest_tags(run_id)
    assert set(tags) == {"tagA", "tagB"}


async def test_set_backtest_notes(real_db):
    async with real_db.pool.acquire() as conn:
        run_id = await conn.fetchval(
            """
            INSERT INTO backtest_runs (name, symbols, timeframe, horizons,
                range_start, range_end, train_days, test_days, embargo_min,
                mode, config_json, status)
            VALUES ('n', ARRAY['AAPL'], '1m', ARRAY[5],
                NOW(), NOW() + INTERVAL '1 day', 30, 5, 30,
                'rolling', '{}'::jsonb, 'completed')
            RETURNING id
            """
        )
    await real_db.set_backtest_notes(run_id, "this run is the v2 baseline")
    notes = await real_db.get_backtest_notes(run_id)
    assert notes == "this run is the v2 baseline"


async def test_query_backtest_symbols_aggregates_by_symbol(real_db):
    async with real_db.pool.acquire() as conn:
        run_id = await conn.fetchval(
            """
            INSERT INTO backtest_runs (name, symbols, timeframe, horizons,
                range_start, range_end, train_days, test_days, embargo_min,
                mode, config_json, status)
            VALUES ('sym', ARRAY['AAPL','MSFT'], '1m', ARRAY[5],
                NOW(), NOW() + INTERVAL '1 day', 30, 5, 30,
                'rolling', '{}'::jsonb, 'completed')
            RETURNING id
            """
        )
        fold_id = await conn.fetchval(
            """
            INSERT INTO backtest_folds (run_id, fold_index,
                train_start, train_end, test_start, test_end)
            VALUES ($1, 0, NOW(), NOW(), NOW(), NOW()) RETURNING id
            """, run_id,
        )
        for (sym, pnl) in [("AAPL", 10.0), ("AAPL", -2.0), ("MSFT", 5.0)]:
            await conn.execute(
                """
                INSERT INTO backtest_trades (run_id, fold_id, symbol, side,
                    entry_time, entry_price, exit_time, exit_price,
                    quantity, pnl, signal_strength)
                VALUES ($1, $2, $3, 'long', NOW(), 100.0,
                    NOW() + INTERVAL '10 minutes', 100.0 + $4,
                    1, $4, 0.7)
                """, run_id, fold_id, sym, pnl,
            )

    rows = await real_db.query_backtest_symbols(run_id=run_id)
    by_sym = {r["symbol"]: r for r in rows}
    assert by_sym["AAPL"]["trades"] == 2
    assert by_sym["AAPL"]["total_pnl"] == pytest.approx(8.0)
    assert by_sym["MSFT"]["trades"] == 1


async def test_query_backtest_distributions_scoped_to_run(real_db):
    async with real_db.pool.acquire() as conn:
        run_id = await conn.fetchval(
            """
            INSERT INTO backtest_runs (name, symbols, timeframe, horizons,
                range_start, range_end, train_days, test_days, embargo_min,
                mode, config_json, status)
            VALUES ('dist', ARRAY['X'], '1m', ARRAY[5],
                NOW(), NOW() + INTERVAL '1 day', 30, 5, 30,
                'rolling', '{}'::jsonb, 'completed')
            RETURNING id
            """
        )
        fold_id = await conn.fetchval(
            """
            INSERT INTO backtest_folds (run_id, fold_index,
                train_start, train_end, test_start, test_end)
            VALUES ($1, 0, NOW(), NOW(), NOW(), NOW()) RETURNING id
            """, run_id,
        )
        for pnl in [5.0, -2.0, 3.0, -1.0, 7.0]:
            await conn.execute(
                """
                INSERT INTO backtest_trades (run_id, fold_id, symbol, side,
                    entry_time, entry_price, exit_time, exit_price,
                    quantity, pnl, signal_strength)
                VALUES ($1, $2, 'X', 'long', NOW(), 100.0,
                    NOW() + INTERVAL '10 minutes', 100.0, 1, $3, 0.7)
                """, run_id, fold_id, pnl,
            )
    dist = await real_db.query_backtest_distributions(run_id=run_id)
    assert dist["trade_pnl_histogram"]["count"] == 5 or \
        sum(dist["trade_pnl_histogram"]["counts"]) == 5


async def test_query_backtest_live_overlay_returns_both_paths(real_db):
    # Seed one backtest run and a public trade inside its window.
    async with real_db.pool.acquire() as conn:
        base = datetime(2026, 3, 1, tzinfo=timezone.utc)
        run_id = await conn.fetchval(
            """
            INSERT INTO backtest_runs (name, symbols, timeframe, horizons,
                range_start, range_end, train_days, test_days, embargo_min,
                mode, config_json, status)
            VALUES ('lv', ARRAY['OVL'], '1m', ARRAY[5],
                $1::TIMESTAMPTZ, $1::TIMESTAMPTZ + INTERVAL '5 days', 30, 5, 30,
                'rolling', '{}'::jsonb, 'completed')
            RETURNING id
            """, base,
        )
        fold_id = await conn.fetchval(
            """
            INSERT INTO backtest_folds (run_id, fold_index,
                train_start, train_end, test_start, test_end)
            VALUES ($1, 0, $2::TIMESTAMPTZ, $2::TIMESTAMPTZ, $2::TIMESTAMPTZ,
                $2::TIMESTAMPTZ + INTERVAL '1 day') RETURNING id
            """, run_id, base,
        )
        # Backtest trade for symbol OVL
        await conn.execute(
            """
            INSERT INTO backtest_trades (run_id, fold_id, symbol, side,
                entry_time, entry_price, exit_time, exit_price,
                quantity, pnl, signal_strength)
            VALUES ($1, $2, 'OVL', 'long', $3, 100.0, $4, 103.0,
                1, 3.0, 0.7)
            """, run_id, fold_id, base, base + timedelta(hours=1),
        )
        # Live trade for symbol OVL in the same window
        order_id = await conn.fetchval(
            """
            INSERT INTO orders (symbol, side, quantity, order_type,
                submitted_at, signal_strength, alpaca_order_id, status,
                filled_at, filled_price, filled_quantity)
            VALUES ('OVL', 'buy', 1, 'market', $1, 0.7,
                'ovl-1', 'filled', $1, 100.0, 1)
            RETURNING id
            """, base + timedelta(hours=2),
        )
        await conn.execute(
            """
            INSERT INTO trades (symbol, side, entry_time, entry_price,
                quantity, entry_order_id, exit_time, exit_price, pnl,
                hold_duration_seconds)
            VALUES ('OVL', 'long', $1, 100.0, 1, $2, $3, 102.0, 2.0, 3600)
            """, base + timedelta(hours=2), order_id, base + timedelta(hours=3),
        )

    overlay = await real_db.query_backtest_live_overlay(run_id=run_id)
    assert "backtest_series" in overlay
    assert "live_series" in overlay
    assert overlay["has_overlap"] is True


async def test_query_backtest_funnel_counts_attrition_per_stage(real_db):
    # Seed a run with predictions at varying confidence and a few orders.
    async with real_db.pool.acquire() as conn:
        # Docker test volume persists predictions across runs; insert_prediction
        # ON CONFLICT DO UPDATE doesn't overwrite backtest_run_id, so stale rows
        # with the old run_id would mask this run's scoped query.
        await conn.execute("DELETE FROM predictions WHERE symbol = 'F'")
        run_id = await conn.fetchval(
            """
            INSERT INTO backtest_runs (name, symbols, timeframe, horizons,
                range_start, range_end, train_days, test_days, embargo_min,
                mode, config_json, status)
            VALUES ('fn', ARRAY['F'], '1m', ARRAY[5],
                NOW(), NOW() + INTERVAL '1 day', 30, 5, 30,
                'rolling', '{}'::jsonb, 'completed')
            RETURNING id
            """
        )
    # Insert predictions scoped to this run.
    scope = BacktestScope(backtest_run_id=run_id, fold_index=0)
    from shared.models import Prediction
    base = datetime(2026, 3, 1, tzinfo=timezone.utc)
    sandbox = real_db  # live pool also sees backtest.* via ALTER
    # Use the existing scope-aware insert.
    for i, conf in enumerate([0.8, 0.7, 0.65, 0.5, 0.4]):
        await sandbox.insert_prediction(Prediction(
            symbol="F", timestamp=base + timedelta(minutes=i),
            timeframe="1m", horizon_minutes=5, model_id=1,
            predicted_direction=1, confidence=conf,
        ), scope=scope)

    funnel = await real_db.query_backtest_funnel(
        run_id=run_id, min_confidence=0.6,
    )
    assert funnel["signals"] == 5
    assert funnel["confident"] == 3
    # Orders placed / filled / closed columns default to 0 when no
    # backtest.orders rows exist for the run.
    assert funnel["orders_placed"] == 0
    assert funnel["orders_filled"] == 0


async def test_query_backtest_compare_returns_metrics_for_each_run(real_db):
    async with real_db.pool.acquire() as conn:
        ids = []
        for name in ["a", "b"]:
            rid = await conn.fetchval(
                """
                INSERT INTO backtest_runs (name, symbols, timeframe, horizons,
                    range_start, range_end, train_days, test_days, embargo_min,
                    mode, config_json, status)
                VALUES ($1, ARRAY['C'], '1m', ARRAY[5],
                    NOW(), NOW() + INTERVAL '1 day', 30, 5, 30,
                    'rolling', '{}'::jsonb, 'completed')
                RETURNING id
                """, name,
            )
            ids.append(rid)
            fold_id = await conn.fetchval(
                """
                INSERT INTO backtest_folds (run_id, fold_index,
                    train_start, train_end, test_start, test_end, metrics_json)
                VALUES ($1, 0, NOW(), NOW(), NOW(), NOW(),
                    '{"pnl": 10, "sharpe": 1.2}'::jsonb)
                RETURNING id
                """, rid,
            )
            await conn.execute(
                """
                INSERT INTO backtest_equity (run_id, fold_id, timestamp, equity, cash)
                VALUES ($1, $2, NOW(), 100100.0, 90000.0)
                """, rid, fold_id,
            )

    compare = await real_db.query_backtest_compare(run_ids=ids)
    assert len(compare["runs"]) == 2
    for r in compare["runs"]:
        assert "equity_series" in r
        assert "metrics" in r


async def test_query_backtest_sensitivity_groups_by_swept_param(real_db):
    async with real_db.pool.acquire() as conn:
        # Three runs with same grouping key (symbols + timeframe + horizons)
        # but different `min_confidence` (the swept parameter).
        for conf in [0.5, 0.6, 0.7]:
            await conn.execute(
                """
                INSERT INTO backtest_runs (name, symbols, timeframe, horizons,
                    range_start, range_end, train_days, test_days, embargo_min,
                    mode, config_json, status)
                VALUES ('sens-' || $1, ARRAY['S'], '1m', ARRAY[5],
                    NOW(), NOW() + INTERVAL '1 day', 30, 5, 30, 'rolling',
                    jsonb_build_object('min_confidence', $2::DOUBLE PRECISION),
                    'completed')
                """, f"{conf}", conf,
            )
    sens = await real_db.query_backtest_sensitivity(
        group_by=["symbols", "timeframe"],
        swept_param="min_confidence",
    )
    assert len(sens["points"]) >= 3
    for p in sens["points"]:
        assert "param_value" in p
        assert "metrics" in p
