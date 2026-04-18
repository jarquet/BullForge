"""Tests for the /api/trading/* DB query methods.

Uses the real_db fixture from tests/integration/conftest.py. Skipped by
ConnectionRefusedError when Postgres isn't running locally.
"""
from datetime import datetime, timedelta, timezone

import pytest

from shared.models import Bar, Order, Trade


@pytest.mark.asyncio
async def test_query_equity_curve_walks_closed_trades(real_db):
    base = datetime(2026, 4, 1, 14, 0, 0, tzinfo=timezone.utc)
    # Two trades: +$100, -$30. Expect equity series = [start, start+100, start+70].
    oid1 = await real_db.insert_order(Order(
        symbol="EQ", side="buy", quantity=10, order_type="market",
        submitted_at=base, signal_strength=0.7,
        alpaca_order_id="eq-entry-1", status="filled",
        filled_at=base, filled_price=100.0, filled_quantity=10,
    ))
    oid_exit1 = await real_db.insert_order(Order(
        symbol="EQ", side="sell", quantity=10, order_type="market",
        submitted_at=base + timedelta(hours=1), signal_strength=0.7,
        alpaca_order_id="eq-exit-1", status="filled",
        filled_at=base + timedelta(hours=1),
        filled_price=110.0, filled_quantity=10,
    ))
    await real_db.insert_trade_with_exit(Trade(
        symbol="EQ", side="long",
        entry_time=base, entry_price=100.0, quantity=10,
        entry_order_id=oid1,
        exit_time=base + timedelta(hours=1), exit_price=110.0,
        pnl=100.0, hold_duration_seconds=3600,
        exit_order_id=oid_exit1,
    ))
    oid2 = await real_db.insert_order(Order(
        symbol="EQ", side="buy", quantity=3, order_type="market",
        submitted_at=base + timedelta(hours=2), signal_strength=0.6,
        alpaca_order_id="eq-entry-2", status="filled",
        filled_at=base + timedelta(hours=2),
        filled_price=50.0, filled_quantity=3,
    ))
    oid_exit2 = await real_db.insert_order(Order(
        symbol="EQ", side="sell", quantity=3, order_type="market",
        submitted_at=base + timedelta(hours=3), signal_strength=0.6,
        alpaca_order_id="eq-exit-2", status="filled",
        filled_at=base + timedelta(hours=3),
        filled_price=40.0, filled_quantity=3,
    ))
    await real_db.insert_trade_with_exit(Trade(
        symbol="EQ", side="long",
        entry_time=base + timedelta(hours=2),
        entry_price=50.0, quantity=3, entry_order_id=oid2,
        exit_time=base + timedelta(hours=3), exit_price=40.0,
        pnl=-30.0, hold_duration_seconds=3600,
        exit_order_id=oid_exit2,
    ))

    series = await real_db.query_equity_curve(
        starting_equity=100_000.0,
        from_dt=base - timedelta(hours=1),
        to_dt=base + timedelta(hours=4),
    )
    assert len(series) >= 2
    assert series[-1]["realized_equity"] == pytest.approx(100_070.0)
    # Drawdown at the -30 point = (100070 - 100100) / 100100 ≈ -0.0003
    assert series[-1]["drawdown"] <= 0.0


@pytest.mark.asyncio
async def test_query_distributions_returns_daily_histogram_holdtime_side(real_db):
    base = datetime(2026, 4, 5, 14, 0, 0, tzinfo=timezone.utc)
    # 4 trades: 2 long wins, 1 long loss, 1 short win.
    trades = [
        ("long", 100.0, 105.0, 5.0, 600),
        ("long", 100.0, 98.0, -2.0, 900),
        ("long", 100.0, 103.0, 3.0, 1200),
        ("short", 100.0, 97.0, 3.0, 1800),
    ]
    for i, (side, entry, exit_, pnl, dur) in enumerate(trades):
        oid = await real_db.insert_order(Order(
            symbol="DIST", side="buy" if side == "long" else "sell",
            quantity=1, order_type="market",
            submitted_at=base + timedelta(minutes=i*10),
            signal_strength=0.7,
            alpaca_order_id=f"dist-{i}", status="filled",
            filled_at=base + timedelta(minutes=i*10),
            filled_price=entry, filled_quantity=1,
        ))
        await real_db.insert_trade_with_exit(Trade(
            symbol="DIST", side=side,
            entry_time=base + timedelta(minutes=i*10),
            entry_price=entry, quantity=1, entry_order_id=oid,
            exit_time=base + timedelta(minutes=i*10, seconds=dur),
            exit_price=exit_, pnl=pnl, hold_duration_seconds=dur,
            exit_order_id=None,
        ))

    dist = await real_db.query_distributions(
        from_dt=base - timedelta(hours=1),
        to_dt=base + timedelta(hours=2),
    )
    assert "daily_pnl" in dist
    assert "trade_pnl_histogram" in dist
    assert "hold_time_buckets" in dist
    assert "side_breakdown" in dist

    sides = {s["side"]: s for s in dist["side_breakdown"]}
    assert sides["long"]["trades"] == 3
    assert sides["long"]["wins"] == 2
    assert sides["short"]["trades"] == 1
    assert sides["short"]["wins"] == 1


@pytest.mark.asyncio
async def test_query_streaks_returns_current_and_longest(real_db):
    base = datetime(2026, 4, 5, 14, 0, 0, tzinfo=timezone.utc)
    # 3 wins, 2 losses, 1 win (current) — longest win streak = 3, longest loss streak = 2.
    pnls = [10.0, 5.0, 2.0, -3.0, -1.0, 4.0]
    for i, pnl in enumerate(pnls):
        oid = await real_db.insert_order(Order(
            symbol="STREAK", side="buy", quantity=1, order_type="market",
            submitted_at=base + timedelta(minutes=i),
            signal_strength=0.7,
            alpaca_order_id=f"streak-{i}", status="filled",
            filled_at=base + timedelta(minutes=i),
            filled_price=100.0, filled_quantity=1,
        ))
        await real_db.insert_trade_with_exit(Trade(
            symbol="STREAK", side="long",
            entry_time=base + timedelta(minutes=i),
            entry_price=100.0, quantity=1, entry_order_id=oid,
            exit_time=base + timedelta(minutes=i, seconds=60),
            exit_price=100.0 + pnl, pnl=pnl, hold_duration_seconds=60,
            exit_order_id=None,
        ))
    result = await real_db.query_streaks(
        from_dt=base - timedelta(hours=1),
        to_dt=base + timedelta(hours=1),
    )
    assert result["current_streak_kind"] == "win"
    assert result["current_streak_length"] == 1
    assert result["longest_win_streak"] == 3
    assert result["longest_loss_streak"] == 2
    assert len(result["ribbon"]) == 6


@pytest.mark.asyncio
async def test_query_heatmap_time_of_day_buckets_by_weekday_halfhour(real_db):
    base = datetime(2026, 4, 6, 14, 0, 0, tzinfo=timezone.utc)  # Mon 14:00 UTC = 10:00 ET
    oid = await real_db.insert_order(Order(
        symbol="HEAT", side="buy", quantity=1, order_type="market",
        submitted_at=base, signal_strength=0.7,
        alpaca_order_id="heat-1", status="filled",
        filled_at=base, filled_price=100.0, filled_quantity=1,
    ))
    await real_db.insert_trade_with_exit(Trade(
        symbol="HEAT", side="long",
        entry_time=base, entry_price=100.0, quantity=1, entry_order_id=oid,
        exit_time=base + timedelta(minutes=10), exit_price=101.0,
        pnl=1.0, hold_duration_seconds=600, exit_order_id=None,
    ))
    cells = await real_db.query_heatmap_time_of_day(
        from_dt=base - timedelta(hours=1), to_dt=base + timedelta(hours=1),
    )
    # 5 weekday rows × N half-hour columns; trade should appear in Mon row.
    assert "x_labels" in cells
    assert "y_labels" in cells
    assert cells["y_labels"][0] == "Mon"
    assert "cells" in cells
    total = sum(1 for row in cells["cells"] for v in row if v is not None and v != 0)
    assert total >= 1


@pytest.mark.asyncio
async def test_query_exposure_timeline_sums_concurrent_positions(real_db):
    base = datetime(2026, 4, 6, 14, 0, 0, tzinfo=timezone.utc)
    # Two overlapping trades on different symbols.
    for i, sym in enumerate(["EXPA", "EXPB"]):
        oid = await real_db.insert_order(Order(
            symbol=sym, side="buy", quantity=10, order_type="market",
            submitted_at=base + timedelta(minutes=i * 5),
            signal_strength=0.7,
            alpaca_order_id=f"exp-{i}", status="filled",
            filled_at=base + timedelta(minutes=i * 5),
            filled_price=100.0, filled_quantity=10,
        ))
        await real_db.insert_trade_with_exit(Trade(
            symbol=sym, side="long",
            entry_time=base + timedelta(minutes=i * 5),
            entry_price=100.0, quantity=10, entry_order_id=oid,
            exit_time=base + timedelta(minutes=30),
            exit_price=101.0, pnl=10.0,
            hold_duration_seconds=(30 - i * 5) * 60, exit_order_id=None,
        ))
    timeline = await real_db.query_exposure_timeline(
        equity=100_000.0,
        from_dt=base - timedelta(hours=1),
        to_dt=base + timedelta(hours=1),
        resolution_minutes=5,
    )
    assert isinstance(timeline, list)
    # At some point we had both positions open → exposure from both.
    exposure_values = [p["total_exposure_pct"] for p in timeline]
    assert max(exposure_values) > 0


@pytest.mark.asyncio
async def test_query_benchmark_computes_stats_vs_spy(real_db):
    base = datetime(2026, 4, 1, tzinfo=timezone.utc)
    # Seed SPY daily bars rising from 400 to 410 over 10 days.
    for day in range(10):
        await real_db.insert_bar(Bar(
            symbol="SPY", timestamp=base + timedelta(days=day),
            open=400 + day, high=401 + day,
            low=399 + day, close=400 + day,
            volume=1000, timeframe="1d",
        ))
    # Add 5 trades so the strategy has some P&L.
    for i in range(5):
        oid = await real_db.insert_order(Order(
            symbol="BEN", side="buy", quantity=1, order_type="market",
            submitted_at=base + timedelta(days=i),
            signal_strength=0.7,
            alpaca_order_id=f"ben-{i}", status="filled",
            filled_at=base + timedelta(days=i),
            filled_price=100.0, filled_quantity=1,
        ))
        await real_db.insert_trade_with_exit(Trade(
            symbol="BEN", side="long",
            entry_time=base + timedelta(days=i),
            entry_price=100.0, quantity=1, entry_order_id=oid,
            exit_time=base + timedelta(days=i, hours=1),
            exit_price=101.0, pnl=1.0,
            hold_duration_seconds=3600, exit_order_id=None,
        ))
    bench = await real_db.query_benchmark(
        starting_equity=100_000.0,
        from_dt=base, to_dt=base + timedelta(days=11),
    )
    assert "strategy_series" in bench
    assert "spy_series" in bench
    assert "stats" in bench
    assert "annualized_return" in bench["stats"]
    assert "sharpe" in bench["stats"]
    assert "max_drawdown" in bench["stats"]
    assert "beta" in bench["stats"]
    assert "alpha" in bench["stats"]


@pytest.mark.asyncio
async def test_query_symbol_drilldown_payload_scoped_to_one_symbol(real_db):
    base = datetime(2026, 4, 10, 14, 0, 0, tzinfo=timezone.utc)
    oid = await real_db.insert_order(Order(
        symbol="DRILL", side="buy", quantity=1, order_type="market",
        submitted_at=base, signal_strength=0.7,
        alpaca_order_id="drill-1", status="filled",
        filled_at=base, filled_price=100.0, filled_quantity=1,
    ))
    await real_db.insert_trade_with_exit(Trade(
        symbol="DRILL", side="long",
        entry_time=base, entry_price=100.0, quantity=1, entry_order_id=oid,
        exit_time=base + timedelta(minutes=30), exit_price=105.0,
        pnl=5.0, hold_duration_seconds=1800, exit_order_id=None,
    ))
    payload = await real_db.query_symbol_drilldown(
        symbol="DRILL",
        from_dt=base - timedelta(hours=1),
        to_dt=base + timedelta(hours=1),
    )
    assert payload["symbol"] == "DRILL"
    assert payload["trade_count"] == 1
    assert "equity_series" in payload
    assert "trades" in payload
    assert "side_breakdown" in payload
    assert "win_rate_over_time" in payload
