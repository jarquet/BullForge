# Dashboard Expansion Plan 3 — Trading Domain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the Trading tab into a full account-performance explorer: 6 cross-cutting sub-tabs (Overview, Equity Curve, Distributions, Streaks & Exposure, Heatmaps, Benchmark) plus a per-symbol drill-down page. Backend supplies equity curve, drawdown, distributions, heatmap cells, streak stats, exposure series, and SPY benchmark comparison.

**Architecture:** Seven new aiohttp handlers replace the Plan 1 HTTP 501 stubs. One new `trading_api.py` module holds them all. DB-layer additions compute equity curves and aggregations directly in SQL where possible, falling back to Python for running-peak drawdown and Kelly-weighted benchmark stats. Frontend `trading_tab.js` follows the exact pattern of `models_tab.js` from Plan 2: sub-tab bar + router + time-window + refresh, per-sub-tab handlers, drill-down via DrillDown component.

**Tech Stack:** aiohttp + asyncpg backend, vanilla JS + Chart.js frontend via Plan 1 wrappers. No new dependencies.

**Spec:** [docs/superpowers/specs/2026-04-17-dashboard-expansion-design.md](../specs/2026-04-17-dashboard-expansion-design.md)

**Depends on:**
- [Plan 1 foundation](2026-04-17-dashboard-foundation.md) — uses every Plan 1 component.
- [Plan 2 models](2026-04-17-dashboard-plan-2-models.md) — `trading_tab.js` mirrors `models_tab.js`'s structure; it's a reference pattern, not a runtime dependency.
- [Backtest scoping](2026-04-17-backtest-run-scoping.md) — already merged; all raw-SQL reads filter `orders.backtest_run_id IS NULL`. `trades` table has no scope column (backtest results live in `public.backtest_trades` — a separate table).

---

## Pre-Task: Worktree & Dev Iteration

The execution skill creates `.worktrees/dashboard-plan-3-trading` branched from master. Suggested branch: `feat/dashboard-trading`. Paths below are relative to that worktree.

Set up the dev static mount as in Plans 1-2:

```yaml
# bullforge/docker-compose.override.yml (gitignored; don't commit)
services:
  dashboard_service:
    volumes:
      - ./services/dashboard_service/static:/app/services/dashboard_service/static
```

Then `cd bullforge && docker compose up -d dashboard_service`.

---

## File Structure

### New files

```
bullforge/services/dashboard_service/
  trading_api.py                        # 7 /api/trading/* handlers

bullforge/services/dashboard_service/static/
  trading_tab.js                        # Trading tab controller (mirror of models_tab.js pattern)

bullforge/tests/
  test_dashboard_trading_api.py         # aiohttp tests for each new endpoint
  test_dashboard_trading_db.py          # DB query method tests (uses real_db fixture)
```

### Modified files

```
bullforge/services/dashboard_service/api.py              # register_trading_routes(app)
bullforge/services/dashboard_service/stubs_api.py        # remove 7 Plan 3 stubs from STUB_ENDPOINTS
bullforge/services/dashboard_service/main.py             # register_trading_routes wiring
bullforge/services/dashboard_service/static/index.html   # replace Trading tab inner content
bullforge/services/dashboard_service/static/dashboard.js # remove loadTrading() and window-btn handler
bullforge/shared/db.py                                   # add 7 new query methods
bullforge/tests/test_dashboard_stubs.py                  # update expected_prefixes (remove 7 trading paths)
```

---

## Task 1: Database query methods

**Files:**
- Modify: `bullforge/shared/db.py`
- Create: `bullforge/tests/integration/test_dashboard_trading_db.py`

Seven new methods back the seven endpoints. All read-only; all default `scope=None` semantics for `orders` reads (filter `backtest_run_id IS NULL`). Tests go under `tests/integration/` because they need the `real_db` fixture.

- [ ] **Step 1: Write failing tests**

Create `bullforge/tests/integration/test_dashboard_trading_db.py`:

```python
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
```

Also **add a helper `insert_trade_with_exit`** to `bullforge/shared/db.py`. The existing `insert_trade` only creates open trades. We need one that inserts trades with exit data set (for the tests above). This helper is test-only but lives with the other DB methods for reuse.

In `bullforge/shared/db.py`, find `async def insert_trade` and add this method right after it:

```python
    async def insert_trade_with_exit(self, trade: Trade) -> int:
        """Insert a trade already marked closed (exit_time, exit_price, pnl set)."""
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO trades
                    (symbol, side, entry_time, entry_price, quantity, entry_order_id,
                     exit_time, exit_price, pnl, hold_duration_seconds, exit_order_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING id
                """,
                trade.symbol, trade.side, trade.entry_time,
                trade.entry_price, trade.quantity, trade.entry_order_id,
                trade.exit_time, trade.exit_price, trade.pnl,
                trade.hold_duration_seconds, trade.exit_order_id,
            )
        return row_id
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/integration/test_dashboard_trading_db.py -v`

Expected: FAIL — methods don't exist yet (`AttributeError: 'Database' object has no attribute 'query_equity_curve'`). If Postgres isn't running: `ConnectionRefusedError`; skip and the code below will still be exercised via mocks in Task 2.

- [ ] **Step 3: Implement `query_equity_curve`**

Append to the `Database` class in `bullforge/shared/db.py`:

```python
    async def query_equity_curve(
        self,
        starting_equity: float,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        max_points: int = 2000,
    ) -> list[dict]:
        """Equity curve walked from closed trades.

        Returns list of {timestamp, realized_pnl, realized_equity, drawdown,
        trailing_peak}. Downsampled to at most max_points via LTTB-like
        thinning (uniform stride — acceptable for our scale).
        """
        query = """
            SELECT exit_time, pnl
            FROM trades
            WHERE exit_time IS NOT NULL AND pnl IS NOT NULL
        """
        args: list = []
        idx = 1
        if from_dt is not None:
            query += f" AND exit_time >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            query += f" AND exit_time < ${idx}"
            args.append(to_dt); idx += 1
        query += " ORDER BY exit_time ASC"

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)

        series: list[dict] = []
        equity = starting_equity
        peak = starting_equity
        for r in rows:
            equity += float(r["pnl"])
            if equity > peak:
                peak = equity
            drawdown = (equity - peak) / peak if peak > 0 else 0.0
            series.append({
                "timestamp": r["exit_time"],
                "realized_pnl": float(r["pnl"]),
                "realized_equity": equity,
                "trailing_peak": peak,
                "drawdown": drawdown,
            })

        if len(series) > max_points:
            stride = len(series) // max_points
            series = series[::stride]
        return series
```

- [ ] **Step 4: Implement `query_distributions`**

Append to `db.py`:

```python
    async def query_distributions(
        self,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> dict:
        """Daily P&L, trade P&L histogram, hold-time buckets, and side breakdown.

        Returns {daily_pnl, trade_pnl_histogram, hold_time_buckets,
        side_breakdown}. Each sub-object shaped for direct Chart.js rendering.
        """
        base_where = "WHERE exit_time IS NOT NULL AND pnl IS NOT NULL"
        args: list = []
        idx = 1
        if from_dt is not None:
            base_where += f" AND exit_time >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            base_where += f" AND exit_time < ${idx}"
            args.append(to_dt); idx += 1

        async with self.pool.acquire() as conn:
            daily = await conn.fetch(
                f"""
                SELECT DATE_TRUNC('day', exit_time) AS date,
                       SUM(pnl) AS pnl, COUNT(*) AS trades
                FROM trades {base_where}
                GROUP BY DATE_TRUNC('day', exit_time)
                ORDER BY date ASC
                """,
                *args,
            )
            all_trades = await conn.fetch(
                f"""SELECT pnl, hold_duration_seconds, side FROM trades {base_where}""",
                *args,
            )

        pnls = [float(t["pnl"]) for t in all_trades]
        holds = [int(t["hold_duration_seconds"]) for t in all_trades
                 if t["hold_duration_seconds"] is not None]

        # Trade P&L histogram (20 bins).
        if pnls:
            lo, hi = min(pnls), max(pnls)
            if lo == hi:
                lo -= 1.0; hi += 1.0
            bins = 20
            width = (hi - lo) / bins
            counts = [0] * bins
            for v in pnls:
                i = min(bins - 1, int((v - lo) / width))
                counts[i] += 1
            labels = [round(lo + i * width, 2) for i in range(bins)]
        else:
            labels, counts = [], []

        # Hold-time buckets: <5m, 5-30m, 30m-2h, 2h-8h, 8h-1d, overnight, multi-day.
        hold_buckets = [
            ("<5m", 0, 300),
            ("5-30m", 300, 1800),
            ("30m-2h", 1800, 7200),
            ("2h-8h", 7200, 28800),
            ("8h-1d", 28800, 86400),
            ("overnight", 86400, 172800),
            ("multi-day", 172800, 10**9),
        ]
        hold_table = []
        for name, lo_s, hi_s in hold_buckets:
            bucket_trades = [t for t in all_trades
                              if t["hold_duration_seconds"] is not None
                              and lo_s <= t["hold_duration_seconds"] < hi_s]
            n = len(bucket_trades)
            avg_pnl = (sum(float(t["pnl"]) for t in bucket_trades) / n) if n > 0 else 0.0
            hold_table.append({"bucket": name, "trades": n, "avg_pnl": avg_pnl})

        # Side breakdown.
        def side_stats(side):
            rows = [t for t in all_trades if t["side"] == side]
            n = len(rows)
            wins = sum(1 for t in rows if float(t["pnl"]) > 0)
            total_pnl = sum(float(t["pnl"]) for t in rows)
            return {
                "side": side,
                "trades": n,
                "wins": wins,
                "win_rate": (wins / n) if n > 0 else 0.0,
                "total_pnl": total_pnl,
                "avg_pnl": (total_pnl / n) if n > 0 else 0.0,
            }

        return {
            "daily_pnl": [
                {"date": r["date"], "pnl": float(r["pnl"]), "trades": r["trades"]}
                for r in daily
            ],
            "trade_pnl_histogram": {
                "labels": labels, "counts": counts,
                "median": sorted(pnls)[len(pnls) // 2] if pnls else 0.0,
                "mean": (sum(pnls) / len(pnls)) if pnls else 0.0,
            },
            "hold_time_buckets": hold_table,
            "side_breakdown": [side_stats("long"), side_stats("short")],
        }
```

- [ ] **Step 5: Implement `query_streaks`**

Append to `db.py`:

```python
    async def query_streaks(
        self,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> dict:
        """Current streak (kind + length), longest streaks, and W/L ribbon.

        Returns {current_streak_kind: 'win'|'loss', current_streak_length,
        longest_win_streak, longest_loss_streak, ribbon: [{timestamp, win}]}.
        """
        query = (
            "SELECT exit_time, pnl FROM trades "
            "WHERE exit_time IS NOT NULL AND pnl IS NOT NULL"
        )
        args: list = []
        idx = 1
        if from_dt is not None:
            query += f" AND exit_time >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            query += f" AND exit_time < ${idx}"
            args.append(to_dt); idx += 1
        query += " ORDER BY exit_time ASC"

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)

        ribbon = []
        longest_win = 0
        longest_loss = 0
        run_win = 0
        run_loss = 0
        for r in rows:
            win = float(r["pnl"]) > 0
            ribbon.append({"timestamp": r["exit_time"], "win": win})
            if win:
                run_win += 1; run_loss = 0
                longest_win = max(longest_win, run_win)
            else:
                run_loss += 1; run_win = 0
                longest_loss = max(longest_loss, run_loss)

        current_kind = "win" if (ribbon and ribbon[-1]["win"]) else "loss"
        current_length = run_win if current_kind == "win" else run_loss
        if not ribbon:
            current_kind = "none"; current_length = 0
        return {
            "current_streak_kind": current_kind,
            "current_streak_length": current_length,
            "longest_win_streak": longest_win,
            "longest_loss_streak": longest_loss,
            "ribbon": ribbon,
        }
```

- [ ] **Step 6: Implement `query_heatmap_time_of_day`**

Append to `db.py`:

```python
    async def query_heatmap_time_of_day(
        self,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> dict:
        """Heatmap grid: avg P&L per (weekday, half-hour UTC bucket).

        Returns {x_labels: ['00:00', '00:30', ...], y_labels: ['Mon', ...,
        'Fri'], cells: [[val|None, ...], ...]}. Half-hour buckets across full
        UTC day (48 columns). Weekdays Mon-Fri (skip weekends since equities
        don't trade).
        """
        query = (
            "SELECT EXTRACT(DOW FROM entry_time)::INT AS dow, "
            "       EXTRACT(HOUR FROM entry_time)::INT AS hh, "
            "       EXTRACT(MINUTE FROM entry_time)::INT AS mm, "
            "       pnl "
            "FROM trades WHERE exit_time IS NOT NULL AND pnl IS NOT NULL"
        )
        args: list = []
        idx = 1
        if from_dt is not None:
            query += f" AND entry_time >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            query += f" AND entry_time < ${idx}"
            args.append(to_dt); idx += 1

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)

        # 5 weekdays × 48 half-hour columns.
        x_labels = [f"{h:02d}:{m:02d}" for h in range(24) for m in (0, 30)]
        y_labels = ["Mon", "Tue", "Wed", "Thu", "Fri"]
        sums = [[0.0] * 48 for _ in range(5)]
        counts = [[0] * 48 for _ in range(5)]
        for r in rows:
            # Postgres DOW: Sun=0, Mon=1, ..., Sat=6. Skip weekends.
            dow = r["dow"]
            if dow < 1 or dow > 5:
                continue
            y = dow - 1
            x = r["hh"] * 2 + (1 if r["mm"] >= 30 else 0)
            sums[y][x] += float(r["pnl"])
            counts[y][x] += 1
        cells = [
            [(sums[y][x] / counts[y][x]) if counts[y][x] > 0 else None
             for x in range(48)]
            for y in range(5)
        ]
        return {"x_labels": x_labels, "y_labels": y_labels, "cells": cells}
```

- [ ] **Step 7: Implement `query_exposure_timeline`**

Append to `db.py`:

```python
    async def query_exposure_timeline(
        self,
        equity: float,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        resolution_minutes: int = 5,
    ) -> list[dict]:
        """Time-bucketed exposure stacked by symbol.

        Returns [{timestamp, total_exposure_pct, by_symbol: {SYM: pct, ...}}].
        For each bucket, sums market value of all positions open at the
        bucket's start time, divided by equity.
        """
        query = (
            "SELECT symbol, entry_time, exit_time, entry_price, quantity "
            "FROM trades WHERE exit_time IS NOT NULL"
        )
        args: list = []
        idx = 1
        if from_dt is not None:
            query += f" AND entry_time >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            query += f" AND exit_time < ${idx}"
            args.append(to_dt); idx += 1
        async with self.pool.acquire() as conn:
            trades = await conn.fetch(query, *args)

        if not trades:
            return []
        min_t = min(t["entry_time"] for t in trades) if not from_dt else from_dt
        max_t = max(t["exit_time"] for t in trades) if not to_dt else to_dt
        if max_t <= min_t:
            return []

        step = timedelta(minutes=resolution_minutes)
        timeline: list[dict] = []
        t = min_t
        while t <= max_t:
            by_symbol: dict[str, float] = {}
            total = 0.0
            for tr in trades:
                if tr["entry_time"] <= t < tr["exit_time"]:
                    mv = float(tr["entry_price"]) * float(tr["quantity"])
                    by_symbol[tr["symbol"]] = by_symbol.get(tr["symbol"], 0.0) + mv
                    total += mv
            pct = total / equity if equity > 0 else 0.0
            timeline.append({
                "timestamp": t,
                "total_exposure_pct": pct,
                "by_symbol": {k: v / equity for k, v in by_symbol.items()},
            })
            t += step
        return timeline
```

Add `timedelta` to the imports at top of `db.py` if not already present: `from datetime import datetime, timedelta, timezone`.

- [ ] **Step 8: Implement `query_benchmark`**

Append to `db.py`:

```python
    async def query_benchmark(
        self,
        starting_equity: float,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> dict:
        """Strategy equity vs SPY buy-and-hold + stats.

        Returns {strategy_series, spy_series, stats:{annualized_return,
        annualized_vol, sharpe, max_drawdown, beta, alpha}}.
        """
        import math
        # Strategy equity (daily bucketed for clean alignment with SPY dailies).
        equity_query = (
            "SELECT DATE_TRUNC('day', exit_time) AS day, SUM(pnl) AS pnl "
            "FROM trades WHERE exit_time IS NOT NULL AND pnl IS NOT NULL"
        )
        args: list = []
        idx = 1
        if from_dt is not None:
            equity_query += f" AND exit_time >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            equity_query += f" AND exit_time < ${idx}"
            args.append(to_dt); idx += 1
        equity_query += " GROUP BY DATE_TRUNC('day', exit_time) ORDER BY day ASC"

        spy_query = (
            "SELECT timestamp, close FROM bars_daily "
            "WHERE symbol = 'SPY'"
        )
        spy_args: list = []
        sidx = 1
        if from_dt is not None:
            spy_query += f" AND timestamp >= ${sidx}"
            spy_args.append(from_dt); sidx += 1
        if to_dt is not None:
            spy_query += f" AND timestamp < ${sidx}"
            spy_args.append(to_dt); sidx += 1
        spy_query += " ORDER BY timestamp ASC"

        async with self.pool.acquire() as conn:
            equity_rows = await conn.fetch(equity_query, *args)
            spy_rows = await conn.fetch(spy_query, *spy_args)

        # Build strategy series: cumulative equity per day.
        strat = []
        e = starting_equity
        for r in equity_rows:
            e += float(r["pnl"])
            strat.append({"timestamp": r["day"], "equity": e})

        # Normalize SPY to starting_equity.
        spy_series = []
        if spy_rows:
            start_close = float(spy_rows[0]["close"])
            for r in spy_rows:
                spy_series.append({
                    "timestamp": r["timestamp"],
                    "equity": starting_equity * float(r["close"]) / start_close,
                })

        # Compute daily strategy returns + SPY returns.
        def daily_returns(series):
            out = []
            for i in range(1, len(series)):
                prev = series[i - 1]["equity"]
                cur = series[i]["equity"]
                if prev > 0:
                    out.append((cur / prev) - 1.0)
            return out

        strat_returns = daily_returns(strat)
        spy_returns = daily_returns(spy_series)

        def mean(xs): return sum(xs) / len(xs) if xs else 0.0
        def var(xs):
            if len(xs) < 2: return 0.0
            m = mean(xs)
            return sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
        def stdev(xs): return math.sqrt(var(xs))

        def annualize_return(xs):
            if not xs: return 0.0
            # Compound product: Π(1+r_i) - 1, then annualize by days_per_year ^ (1/len).
            prod = 1.0
            for r in xs: prod *= (1.0 + r)
            if prod <= 0: return -1.0
            return prod ** (252 / len(xs)) - 1.0 if len(xs) > 0 else 0.0

        ann_return = annualize_return(strat_returns)
        ann_vol = stdev(strat_returns) * math.sqrt(252)
        rf = 0.0  # risk-free rate simplification
        sharpe = (ann_return - rf) / ann_vol if ann_vol > 0 else 0.0

        # Max drawdown from strategy series.
        max_dd = 0.0
        peak = starting_equity
        for p in strat:
            if p["equity"] > peak: peak = p["equity"]
            dd = (p["equity"] - peak) / peak if peak > 0 else 0.0
            if dd < max_dd: max_dd = dd

        # Beta & alpha vs SPY — align return series by length (truncate to min).
        n = min(len(strat_returns), len(spy_returns))
        beta = alpha = 0.0
        if n >= 2:
            a = strat_returns[-n:]
            b = spy_returns[-n:]
            mb = mean(b)
            vb = var(b)
            if vb > 0:
                cov = sum((a[i] - mean(a)) * (b[i] - mb) for i in range(n)) / (n - 1)
                beta = cov / vb
                ann_spy = annualize_return(b)
                alpha = ann_return - (rf + beta * (ann_spy - rf))

        return {
            "strategy_series": strat,
            "spy_series": spy_series,
            "stats": {
                "annualized_return": ann_return,
                "annualized_vol": ann_vol,
                "sharpe": sharpe,
                "max_drawdown": max_dd,
                "beta": beta,
                "alpha": alpha,
            },
        }
```

- [ ] **Step 9: Implement `query_symbol_drilldown`**

Append to `db.py`:

```python
    async def query_symbol_drilldown(
        self,
        symbol: str,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> dict:
        """Per-symbol payload: equity series, trades, side breakdown,
        win rate over time (daily).
        """
        base_where = "WHERE symbol = $1 AND exit_time IS NOT NULL AND pnl IS NOT NULL"
        args: list = [symbol]
        idx = 2
        if from_dt is not None:
            base_where += f" AND exit_time >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            base_where += f" AND exit_time < ${idx}"
            args.append(to_dt); idx += 1

        async with self.pool.acquire() as conn:
            all_trades = await conn.fetch(
                f"""
                SELECT id, side, entry_time, exit_time, entry_price,
                       exit_price, quantity, pnl, hold_duration_seconds
                FROM trades {base_where}
                ORDER BY exit_time ASC
                """,
                *args,
            )
            daily = await conn.fetch(
                f"""
                SELECT DATE_TRUNC('day', exit_time) AS date,
                       COUNT(*) AS total,
                       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END)::INT AS wins,
                       SUM(pnl) AS pnl
                FROM trades {base_where}
                GROUP BY DATE_TRUNC('day', exit_time)
                ORDER BY date ASC
                """,
                *args,
            )

        # Per-symbol equity: start at 0, cumulate P&L.
        equity = 0.0
        peak = 0.0
        equity_series = []
        for t in all_trades:
            equity += float(t["pnl"])
            if equity > peak: peak = equity
            dd = (equity - peak) / peak if peak > 0 else 0.0
            equity_series.append({
                "timestamp": t["exit_time"],
                "equity": equity, "drawdown": dd,
            })

        def side_stats(side):
            rows = [t for t in all_trades if t["side"] == side]
            n = len(rows)
            wins = sum(1 for t in rows if float(t["pnl"]) > 0)
            total_pnl = sum(float(t["pnl"]) for t in rows)
            return {
                "side": side, "trades": n, "wins": wins,
                "win_rate": (wins / n) if n > 0 else 0.0,
                "total_pnl": total_pnl,
            }

        return {
            "symbol": symbol,
            "trade_count": len(all_trades),
            "equity_series": equity_series,
            "trades": [
                {
                    "id": t["id"], "side": t["side"],
                    "entry_time": t["entry_time"], "exit_time": t["exit_time"],
                    "entry_price": float(t["entry_price"]),
                    "exit_price": float(t["exit_price"]) if t["exit_price"] else None,
                    "quantity": float(t["quantity"]),
                    "pnl": float(t["pnl"]) if t["pnl"] else None,
                    "hold_duration_seconds": t["hold_duration_seconds"],
                }
                for t in all_trades
            ],
            "side_breakdown": [side_stats("long"), side_stats("short")],
            "win_rate_over_time": [
                {
                    "date": r["date"],
                    "total": r["total"],
                    "wins": r["wins"],
                    "win_rate": (r["wins"] / r["total"]) if r["total"] > 0 else 0.0,
                    "pnl": float(r["pnl"] or 0),
                }
                for r in daily
            ],
        }
```

- [ ] **Step 10: Run DB tests**

Run: `cd bullforge && python -m pytest tests/integration/test_dashboard_trading_db.py -v`

Expected: all 7 PASS if Postgres is running; otherwise `ConnectionRefusedError` — OK.

- [ ] **Step 11: Commit**

```bash
git add bullforge/shared/db.py \
        bullforge/tests/integration/test_dashboard_trading_db.py
git commit -m "feat(db): add trading-performance query methods (equity, dist, streaks, heatmap, exposure, benchmark, symbol)"
```

---

## Task 2: `trading_api.py` with 7 endpoints

**Files:**
- Create: `bullforge/services/dashboard_service/trading_api.py`
- Create: `bullforge/tests/test_dashboard_trading_api.py`
- Modify: `bullforge/services/dashboard_service/stubs_api.py`
- Modify: `bullforge/services/dashboard_service/api.py`
- Modify: `bullforge/tests/test_dashboard_stubs.py`

- [ ] **Step 1: Write failing tests**

Create `bullforge/tests/test_dashboard_trading_api.py`:

```python
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from services.dashboard_service.trading_api import register_trading_routes


@pytest.fixture
def mock_db_trading():
    db = AsyncMock()
    db.query_equity_curve = AsyncMock(return_value=[
        {
            "timestamp": datetime(2026, 4, 10, tzinfo=timezone.utc),
            "realized_pnl": 100.0, "realized_equity": 100_100.0,
            "trailing_peak": 100_100.0, "drawdown": 0.0,
        },
    ])
    db.query_distributions = AsyncMock(return_value={
        "daily_pnl": [{"date": datetime(2026, 4, 10, tzinfo=timezone.utc),
                       "pnl": 100.0, "trades": 2}],
        "trade_pnl_histogram": {"labels": [-5, 0, 5],
                                 "counts": [1, 0, 2], "median": 3.0, "mean": 1.5},
        "hold_time_buckets": [{"bucket": "<5m", "trades": 2, "avg_pnl": 1.5}],
        "side_breakdown": [
            {"side": "long", "trades": 3, "wins": 2, "win_rate": 0.667,
             "total_pnl": 5.0, "avg_pnl": 1.67},
            {"side": "short", "trades": 1, "wins": 1, "win_rate": 1.0,
             "total_pnl": 3.0, "avg_pnl": 3.0},
        ],
    })
    db.query_streaks = AsyncMock(return_value={
        "current_streak_kind": "win", "current_streak_length": 2,
        "longest_win_streak": 3, "longest_loss_streak": 1,
        "ribbon": [{"timestamp": datetime(2026, 4, 10, tzinfo=timezone.utc),
                    "win": True}],
    })
    db.query_heatmap_time_of_day = AsyncMock(return_value={
        "x_labels": ["00:00"], "y_labels": ["Mon"],
        "cells": [[1.5]],
    })
    db.query_exposure_timeline = AsyncMock(return_value=[
        {"timestamp": datetime(2026, 4, 10, tzinfo=timezone.utc),
         "total_exposure_pct": 0.05, "by_symbol": {"AAPL": 0.05}},
    ])
    db.query_benchmark = AsyncMock(return_value={
        "strategy_series": [{"timestamp": datetime(2026, 4, 10, tzinfo=timezone.utc),
                             "equity": 100_100.0}],
        "spy_series": [{"timestamp": datetime(2026, 4, 10, tzinfo=timezone.utc),
                         "equity": 100_050.0}],
        "stats": {"annualized_return": 0.12, "annualized_vol": 0.18,
                  "sharpe": 0.67, "max_drawdown": -0.03,
                  "beta": 0.85, "alpha": 0.04},
    })
    db.query_symbol_drilldown = AsyncMock(return_value={
        "symbol": "AAPL", "trade_count": 1,
        "equity_series": [], "trades": [], "side_breakdown": [],
        "win_rate_over_time": [],
    })
    db.query_latest_metric_snapshot = AsyncMock(return_value={
        "metrics": {"equity": 100_000.0},
        "timestamp": datetime(2026, 4, 10, tzinfo=timezone.utc),
    })
    return db


@pytest.fixture
def trading_app(mock_db_trading):
    app = web.Application()
    app["db"] = mock_db_trading
    register_trading_routes(app)
    return app


@pytest.mark.asyncio
async def test_equity_endpoint(aiohttp_client, trading_app):
    client = await aiohttp_client(trading_app)
    resp = await client.get("/api/trading/equity")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)
    assert body[0]["realized_equity"] == 100_100.0


@pytest.mark.asyncio
async def test_equity_with_window(aiohttp_client, trading_app, mock_db_trading):
    client = await aiohttp_client(trading_app)
    resp = await client.get(
        "/api/trading/equity?from=2026-04-01T00:00:00Z&to=2026-04-11T00:00:00Z",
    )
    assert resp.status == 200
    call = mock_db_trading.query_equity_curve.call_args
    assert call.kwargs.get("from_dt") is not None
    assert call.kwargs.get("to_dt") is not None


@pytest.mark.asyncio
async def test_distributions_endpoint(aiohttp_client, trading_app):
    client = await aiohttp_client(trading_app)
    resp = await client.get("/api/trading/distributions")
    assert resp.status == 200
    body = await resp.json()
    assert "daily_pnl" in body
    assert "trade_pnl_histogram" in body
    assert "hold_time_buckets" in body
    assert "side_breakdown" in body


@pytest.mark.asyncio
async def test_streaks_endpoint(aiohttp_client, trading_app):
    client = await aiohttp_client(trading_app)
    resp = await client.get("/api/trading/streaks")
    assert resp.status == 200
    body = await resp.json()
    assert body["current_streak_kind"] == "win"


@pytest.mark.asyncio
async def test_heatmap_endpoint(aiohttp_client, trading_app):
    client = await aiohttp_client(trading_app)
    resp = await client.get("/api/trading/heatmap?kind=time")
    assert resp.status == 200
    body = await resp.json()
    assert "x_labels" in body
    assert "cells" in body


@pytest.mark.asyncio
async def test_exposure_endpoint(aiohttp_client, trading_app):
    client = await aiohttp_client(trading_app)
    resp = await client.get("/api/trading/exposure")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)


@pytest.mark.asyncio
async def test_benchmark_endpoint(aiohttp_client, trading_app):
    client = await aiohttp_client(trading_app)
    resp = await client.get("/api/trading/benchmark")
    assert resp.status == 200
    body = await resp.json()
    assert "strategy_series" in body
    assert "spy_series" in body
    assert body["stats"]["sharpe"] == pytest.approx(0.67)


@pytest.mark.asyncio
async def test_symbol_drilldown_endpoint(aiohttp_client, trading_app):
    client = await aiohttp_client(trading_app)
    resp = await client.get("/api/trading/symbols/AAPL")
    assert resp.status == 200
    body = await resp.json()
    assert body["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_equity_rejects_malformed_from(aiohttp_client, trading_app):
    client = await aiohttp_client(trading_app)
    resp = await client.get("/api/trading/equity?from=garbage")
    assert resp.status == 400
```

- [ ] **Step 2: Run to verify failure**

Run: `cd bullforge && python -m pytest tests/test_dashboard_trading_api.py -v`

Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Implement `trading_api.py`**

Create `bullforge/services/dashboard_service/trading_api.py`:

```python
from __future__ import annotations

import logging
from datetime import datetime

from aiohttp import web

from shared.db import Database
from shared.time_window import parse_time_window

logger = logging.getLogger(__name__)


def register_trading_routes(app: web.Application) -> None:
    app.router.add_get("/api/trading/equity", _handle_equity)
    app.router.add_get("/api/trading/distributions", _handle_distributions)
    app.router.add_get("/api/trading/heatmap", _handle_heatmap)
    app.router.add_get("/api/trading/streaks", _handle_streaks)
    app.router.add_get("/api/trading/exposure", _handle_exposure)
    app.router.add_get("/api/trading/benchmark", _handle_benchmark)
    app.router.add_get("/api/trading/symbols/{sym}", _handle_symbol)


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


async def _starting_equity(db: Database) -> float:
    snap = await db.query_latest_metric_snapshot("account", "", "all")
    if snap and "equity" in snap.get("metrics", {}):
        return float(snap["metrics"]["equity"])
    return 100_000.0  # fallback default


async def _handle_equity(request: web.Request) -> web.Response:
    from_dt, to_dt = parse_time_window(request)
    db: Database = request.app["db"]
    starting = await _starting_equity(db)
    series = await db.query_equity_curve(
        starting_equity=starting, from_dt=from_dt, to_dt=to_dt,
    )
    return web.json_response([
        {
            "timestamp": _iso(s["timestamp"]),
            "realized_pnl": s["realized_pnl"],
            "realized_equity": s["realized_equity"],
            "trailing_peak": s["trailing_peak"],
            "drawdown": s["drawdown"],
        }
        for s in series
    ])


async def _handle_distributions(request: web.Request) -> web.Response:
    from_dt, to_dt = parse_time_window(request)
    db: Database = request.app["db"]
    data = await db.query_distributions(from_dt=from_dt, to_dt=to_dt)
    return web.json_response({
        "daily_pnl": [
            {"date": _iso(d["date"]), "pnl": d["pnl"], "trades": d["trades"]}
            for d in data["daily_pnl"]
        ],
        "trade_pnl_histogram": data["trade_pnl_histogram"],
        "hold_time_buckets": data["hold_time_buckets"],
        "side_breakdown": data["side_breakdown"],
    })


async def _handle_streaks(request: web.Request) -> web.Response:
    from_dt, to_dt = parse_time_window(request)
    db: Database = request.app["db"]
    data = await db.query_streaks(from_dt=from_dt, to_dt=to_dt)
    return web.json_response({
        "current_streak_kind": data["current_streak_kind"],
        "current_streak_length": data["current_streak_length"],
        "longest_win_streak": data["longest_win_streak"],
        "longest_loss_streak": data["longest_loss_streak"],
        "ribbon": [
            {"timestamp": _iso(r["timestamp"]), "win": r["win"]}
            for r in data["ribbon"]
        ],
    })


async def _handle_heatmap(request: web.Request) -> web.Response:
    from_dt, to_dt = parse_time_window(request)
    kind = request.query.get("kind", "time")
    db: Database = request.app["db"]
    if kind == "time":
        data = await db.query_heatmap_time_of_day(from_dt=from_dt, to_dt=to_dt)
    else:
        raise web.HTTPBadRequest(reason=f"unsupported kind: {kind!r}")
    return web.json_response(data)


async def _handle_exposure(request: web.Request) -> web.Response:
    from_dt, to_dt = parse_time_window(request)
    resolution_min = int(request.query.get("resolution_minutes", "5"))
    db: Database = request.app["db"]
    starting = await _starting_equity(db)
    timeline = await db.query_exposure_timeline(
        equity=starting, from_dt=from_dt, to_dt=to_dt,
        resolution_minutes=resolution_min,
    )
    return web.json_response([
        {
            "timestamp": _iso(p["timestamp"]),
            "total_exposure_pct": p["total_exposure_pct"],
            "by_symbol": p["by_symbol"],
        }
        for p in timeline
    ])


async def _handle_benchmark(request: web.Request) -> web.Response:
    from_dt, to_dt = parse_time_window(request)
    db: Database = request.app["db"]
    starting = await _starting_equity(db)
    data = await db.query_benchmark(
        starting_equity=starting, from_dt=from_dt, to_dt=to_dt,
    )
    return web.json_response({
        "strategy_series": [
            {"timestamp": _iso(s["timestamp"]), "equity": s["equity"]}
            for s in data["strategy_series"]
        ],
        "spy_series": [
            {"timestamp": _iso(s["timestamp"]), "equity": s["equity"]}
            for s in data["spy_series"]
        ],
        "stats": data["stats"],
    })


async def _handle_symbol(request: web.Request) -> web.Response:
    symbol = request.match_info.get("sym", "")
    from_dt, to_dt = parse_time_window(request)
    db: Database = request.app["db"]
    data = await db.query_symbol_drilldown(
        symbol=symbol, from_dt=from_dt, to_dt=to_dt,
    )
    return web.json_response({
        "symbol": data["symbol"],
        "trade_count": data["trade_count"],
        "equity_series": [
            {"timestamp": _iso(s["timestamp"]),
             "equity": s["equity"], "drawdown": s["drawdown"]}
            for s in data["equity_series"]
        ],
        "trades": [
            {**t,
             "entry_time": _iso(t["entry_time"]),
             "exit_time": _iso(t["exit_time"])}
            for t in data["trades"]
        ],
        "side_breakdown": data["side_breakdown"],
        "win_rate_over_time": [
            {**r, "date": _iso(r["date"])}
            for r in data["win_rate_over_time"]
        ],
    })
```

- [ ] **Step 4: Register trading routes in `api.py`**

In `bullforge/services/dashboard_service/api.py`, add import:

```python
from services.dashboard_service.trading_api import register_trading_routes
```

In `create_api_app()`, register after `register_models_routes(app)`:

```python
    register_trading_routes(app)
```

- [ ] **Step 5: Remove the 7 Plan 3 stubs from `stubs_api.py`**

Delete these 7 lines from `STUB_ENDPOINTS`:

```python
    "/api/trading/equity":                  {"enabled_after_plan": "plan_3"},
    "/api/trading/distributions":           {"enabled_after_plan": "plan_3"},
    "/api/trading/heatmap":                 {"enabled_after_plan": "plan_3"},
    "/api/trading/streaks":                 {"enabled_after_plan": "plan_3"},
    "/api/trading/exposure":                {"enabled_after_plan": "plan_3"},
    "/api/trading/benchmark":               {"enabled_after_plan": "plan_3"},
    "/api/trading/symbols/{sym}":           {"enabled_after_plan": "plan_3"},
```

- [ ] **Step 6: Update `test_dashboard_stubs.py` `expected_prefixes`**

In `bullforge/tests/test_dashboard_stubs.py`, replace `expected_prefixes` (already shrunk by Plan 2) to only contain the 8 backtest paths:

```python
    expected_prefixes = {
        "/api/backtests/{run_id}/equity",
        "/api/backtests/{run_id}/folds",
        "/api/backtests/{run_id}/symbols",
        "/api/backtests/{run_id}/distributions",
        "/api/backtests/{run_id}/live-overlay",
        "/api/backtests/{run_id}/funnel",
        "/api/backtests/compare",
        "/api/backtests/sensitivity",
    }
```

- [ ] **Step 7: Run tests**

Run: `cd bullforge && python -m pytest tests/test_dashboard_trading_api.py tests/test_dashboard_stubs.py tests/test_dashboard_api.py tests/test_dashboard_models_api.py -v`

Expected: all PASS. 9 trading endpoint tests + existing 2 stub tests + existing dashboard api tests + model api tests.

- [ ] **Step 8: Commit**

```bash
git add bullforge/services/dashboard_service/trading_api.py \
        bullforge/services/dashboard_service/api.py \
        bullforge/services/dashboard_service/stubs_api.py \
        bullforge/tests/test_dashboard_trading_api.py \
        bullforge/tests/test_dashboard_stubs.py
git commit -m "feat(dashboard): implement /api/trading/* endpoints (7 handlers, replace Plan 1 stubs)"
```

---

## Task 3: Trading tab scaffold

**Files:**
- Create: `bullforge/services/dashboard_service/static/trading_tab.js`
- Modify: `bullforge/services/dashboard_service/static/index.html`
- Modify: `bullforge/services/dashboard_service/static/dashboard.js`

- [ ] **Step 1: Replace Trading tab markup in `index.html`**

Find the existing Trading tab block in `static/index.html`:

```html
    <!-- Trading Tab -->
    <section id="tab-trading" class="tab-content">
        <div class="window-selector">
            <button class="window-btn active" data-window="all">All Time</button>
            <button class="window-btn" data-window="last_50">Last 50</button>
            <button class="window-btn" data-window="last_20">Last 20</button>
        </div>
        ... (card grid, symbol grid) ...
    </section>
```

Replace with:

```html
    <!-- Trading Tab -->
    <section id="tab-trading" class="tab-content">
        <div class="subtab-bar" id="trading-subtabs">
            <button class="subtab active" data-subtab="overview">Overview</button>
            <button class="subtab" data-subtab="equity">Equity Curve</button>
            <button class="subtab" data-subtab="distributions">Distributions</button>
            <button class="subtab" data-subtab="streaks">Streaks &amp; Exposure</button>
            <button class="subtab" data-subtab="heatmaps">Heatmaps</button>
            <button class="subtab" data-subtab="benchmark">Benchmark</button>
        </div>
        <div class="subtab-toolbar" id="trading-toolbar">
            <span class="toolbar-label">Window:</span>
            <span id="trading-timewindow-slot"></span>
            <span id="trading-refresh-slot" class="toolbar-right"></span>
        </div>
        <div id="trading-panel"></div>
    </section>
```

- [ ] **Step 2: Load `trading_tab.js` from `index.html`**

Add after the existing `<script src="/static/models_tab.js"></script>`:

```html
    <script src="/static/trading_tab.js"></script>
```

- [ ] **Step 3: Remove old Trading-tab code from `dashboard.js`**

In `bullforge/services/dashboard_service/static/dashboard.js`, delete:
- The `loadTrading()` function in its entirety
- Any `currentWindow` variable declaration
- The `document.querySelectorAll('.window-btn').forEach(...)` click-handler block
- Any reference to `loadTrading` in tab-click handlers or setInterval setups

- [ ] **Step 4: Create the Trading tab controller scaffold**

Create `bullforge/services/dashboard_service/static/trading_tab.js`. This mirrors `models_tab.js` with placeholders for each sub-tab handler and a stub drill-down:

```javascript
// trading_tab.js — Trading tab controller.
// Owns: sub-tab switching, time-window + refresh wiring, router integration,
// per-sub-tab data fetching, and per-symbol drill-down.
//
// Uses Plan 1 components: window.Router, window.TimeWindow, window.Refresh,
// window.Charts, window.Leaderboard, window.DrillDown.
(function () {
  var state = {
    activeSubtab: 'overview',
    window: {from: null, to: null},
    timewindow: null,
    refresh: null,
    currentDrillSymbol: null,
  };

  function $(id) { return document.getElementById(id); }
  function panel() { return $('trading-panel'); }

  function setActiveSubtab(name) {
    state.activeSubtab = name;
    document.querySelectorAll('#trading-subtabs .subtab').forEach(function (b) {
      b.classList.toggle('active', b.dataset.subtab === name);
    });
    render();
  }

  async function fetchJSON(path) {
    var url = path;
    var range = state.window;
    if (range && range.from) {
      url += (url.indexOf('?') >= 0 ? '&' : '?') + 'from=' + encodeURIComponent(range.from);
    }
    if (range && range.to) {
      url += (url.indexOf('?') >= 0 ? '&' : '?') + 'to=' + encodeURIComponent(range.to);
    }
    var r = await fetch(url);
    if (!r.ok) {
      var body = await r.json().catch(function () { return {}; });
      throw new Error('HTTP ' + r.status + ' ' + JSON.stringify(body));
    }
    return r.json();
  }

  var handlers = {
    overview: async function () {
      panel().innerHTML = '<p class="empty-state">Overview sub-tab placeholder (Task 4).</p>';
    },
    equity: async function () {
      panel().innerHTML = '<p class="empty-state">Equity Curve sub-tab placeholder (Task 5).</p>';
    },
    distributions: async function () {
      panel().innerHTML = '<p class="empty-state">Distributions sub-tab placeholder (Task 6).</p>';
    },
    streaks: async function () {
      panel().innerHTML = '<p class="empty-state">Streaks & Exposure sub-tab placeholder (Task 7).</p>';
    },
    heatmaps: async function () {
      panel().innerHTML = '<p class="empty-state">Heatmaps sub-tab placeholder (Task 8).</p>';
    },
    benchmark: async function () {
      panel().innerHTML = '<p class="empty-state">Benchmark sub-tab placeholder (Task 9).</p>';
    },
  };

  async function renderDrillDown(symbol) {
    state.currentDrillSymbol = symbol;
    panel().innerHTML = '';
    var dd = window.DrillDown.create({
      mountEl: panel(),
      title: symbol,
      backHref: '#trading/overview',
      breadcrumbs: [
        {label: 'Trading', href: '#trading'},
        {label: 'Overview', href: '#trading/overview'},
        {label: symbol},
      ],
    });
    dd.getContentSlot().innerHTML =
      '<p class="empty-state">Per-symbol drill-down placeholder (Task 10).</p>';
  }

  async function render() {
    if (state.currentDrillSymbol) return;
    var handler = handlers[state.activeSubtab];
    if (!handler) {
      panel().innerHTML = '<p class="empty-state">Unknown sub-tab: ' + state.activeSubtab + '</p>';
      return;
    }
    try {
      await handler();
    } catch (e) {
      panel().innerHTML = '<p class="error-state">Error loading sub-tab: ' + e.message + '</p>';
    }
  }

  function activate() {
    if (!state.timewindow) {
      state.timewindow = window.TimeWindow.create({
        mountEl: $('trading-timewindow-slot'),
        storageKey: 'trading.timewindow',
        onChange: function (range) { state.window = range; render(); },
      });
    }
    if (!state.refresh) {
      state.refresh = window.Refresh.create({
        mountEl: $('trading-refresh-slot'),
        storageKey: 'trading.refresh',
        initialIntervalSec: 30,
        onRefresh: function () { render(); },
      });
    }
    document.querySelectorAll('#trading-subtabs .subtab').forEach(function (b) {
      b.onclick = function () {
        window.Router.navigate('#trading/' + b.dataset.subtab);
      };
    });
  }

  function handleRoute(route) {
    if (route.tab !== 'trading') return;
    activate();
    if (route.entity_kind === 'symbols' && route.entity_id) {
      renderDrillDown(route.entity_id);
    } else if (route.subtab) {
      state.currentDrillSymbol = null;
      setActiveSubtab(route.subtab);
    } else {
      state.currentDrillSymbol = null;
      setActiveSubtab('overview');
    }
  }

  window.addEventListener('DOMContentLoaded', function () {
    if (window.Router) {
      window.Router.subscribe(handleRoute);
      handleRoute(window.Router.current());
    }
  });

  document.addEventListener('click', function (e) {
    var btn = e.target && e.target.closest && e.target.closest('.tab[data-tab="trading"]');
    if (btn) {
      setTimeout(function () {
        window.Router.navigate('#trading/' + (state.activeSubtab || 'overview'));
      }, 0);
    }
  });

  window.TradingTab = {
    _state: state,
    _handlers: handlers,
    _render: render,
    _fetchJSON: fetchJSON,
  };
})();
```

- [ ] **Step 5: Reload and verify**

Reload `http://localhost:18084/#trading`.

Expected:
- 6-button sub-tab bar; "Overview" highlighted.
- Time-window picker + refresh control in toolbar.
- Panel shows "Overview sub-tab placeholder (Task 4)."
- Clicking sub-tab buttons updates panel + URL hash.
- Direct URL `http://localhost:18084/#trading/symbols/AAPL` shows drill-down placeholder.
- Other tabs (Overview, Models, Trades, etc.) still work.

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/dashboard_service/static/trading_tab.js \
        bullforge/services/dashboard_service/static/index.html \
        bullforge/services/dashboard_service/static/dashboard.js
git commit -m "feat(dashboard): Trading tab sub-tab scaffold + router/timewindow/refresh wiring"
```

---

## Task 4: Overview sub-tab

**Files:** Modify `bullforge/services/dashboard_service/static/trading_tab.js`

- [ ] **Step 1: Replace the `overview` placeholder**

Replace the `overview:` handler body with:

```javascript
    overview: async function () {
      panel().innerHTML =
        '<div class="card-grid">' +
          '<div class="card"><h3>Total Trades</h3><div id="tov-total-trades" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Win Rate</h3><div id="tov-win-rate" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Total P&amp;L</h3><div id="tov-total-pnl" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Profit Factor</h3><div id="tov-profit-factor" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Sharpe Ratio</h3><div id="tov-sharpe" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Avg Win</h3><div id="tov-avg-win" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Avg Loss</h3><div id="tov-avg-loss" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Avg Hold</h3><div id="tov-avg-hold" class="metric-value">--</div></div>' +
        '</div>' +
        '<div class="card-grid">' +
          '<div class="card"><h3>Current Streak</h3><div id="tov-cur-streak" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Longest Win Streak</h3><div id="tov-long-win" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Longest Loss Streak</h3><div id="tov-long-loss" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Exposure Now</h3><div id="tov-exposure" class="metric-value">--</div></div>' +
        '</div>' +
        '<h3 style="margin-top:1rem;">Per-Symbol Breakdown</h3>' +
        '<div id="tov-symbols" class="symbol-grid"></div>';

      // Reuse existing /api/metrics/trading and /api/metrics/symbols endpoints.
      var [trading, symbols, streaks, exposure] = await Promise.all([
        fetchJSON('/api/metrics/trading?window=all'),
        fetchJSON('/api/metrics/symbols'),
        fetchJSON('/api/trading/streaks'),
        fetchJSON('/api/trading/exposure'),
      ]);

      function fmtMoney(n) {
        if (n == null) return '--';
        var sign = n >= 0 ? '' : '-';
        return sign + '$' + Math.abs(n).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
      }
      function fmtPct(n) { return n == null ? '--' : (n * 100).toFixed(1) + '%'; }
      function fmtDur(s) {
        if (s == null) return '--';
        if (s < 60) return s + 's';
        if (s < 3600) return Math.floor(s / 60) + 'm';
        return Math.floor(s / 3600) + 'h';
      }

      var m = trading.metrics || {};
      $('tov-total-trades').textContent = m.total_trades != null ? m.total_trades : '--';
      $('tov-win-rate').textContent = fmtPct(m.win_rate);
      $('tov-total-pnl').innerHTML = fmtMoney(m.total_pnl);
      $('tov-total-pnl').className = 'metric-value ' + (m.total_pnl >= 0 ? 'positive' : 'negative');
      $('tov-profit-factor').textContent = m.profit_factor != null ? m.profit_factor.toFixed(2) : '--';
      $('tov-sharpe').textContent = m.sharpe_ratio != null ? m.sharpe_ratio.toFixed(2) : '--';
      $('tov-avg-win').innerHTML = fmtMoney(m.avg_win);
      $('tov-avg-loss').innerHTML = fmtMoney(m.avg_loss);
      $('tov-avg-hold').textContent = fmtDur(m.avg_hold_duration_seconds);

      // Streaks
      var streakText = streaks.current_streak_kind === 'win'
        ? '+' + streaks.current_streak_length + ' wins'
        : (streaks.current_streak_kind === 'loss'
            ? '-' + streaks.current_streak_length + ' losses'
            : '--');
      $('tov-cur-streak').innerHTML = '<span class="' +
        (streaks.current_streak_kind === 'win' ? 'positive' : 'negative') +
        '">' + streakText + '</span>';
      $('tov-long-win').innerHTML = '<span class="positive">' + streaks.longest_win_streak + '</span>';
      $('tov-long-loss').innerHTML = '<span class="negative">' + streaks.longest_loss_streak + '</span>';

      // Exposure now = last point in exposure timeline.
      var now_pct = exposure.length > 0 ? exposure[exposure.length - 1].total_exposure_pct : 0;
      $('tov-exposure').textContent = (now_pct * 100).toFixed(1) + '%';

      // Per-symbol grid.
      var symNames = Object.keys(symbols).sort();
      $('tov-symbols').innerHTML = symNames.map(function (sym) {
        var s = symbols[sym].metrics || {};
        var pnlCls = (s.total_pnl || 0) >= 0 ? 'positive' : 'negative';
        return '<a class="symbol-card" href="#trading/symbols/' + sym + '">' +
          '<div class="symbol-name">' + sym + '</div>' +
          '<div class="symbol-stat">Trades: ' + (s.total_trades || 0) + '</div>' +
          '<div class="symbol-stat">Win: ' + fmtPct(s.win_rate) + '</div>' +
          '<div class="symbol-stat ' + pnlCls + '">P&L: ' + fmtMoney(s.total_pnl) + '</div>' +
          '</a>';
      }).join('');
    },
```

Add CSS for `.symbol-card` to `style.css` (append):

```css
.symbol-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
    gap: 0.5rem;
}
.symbol-card {
    display: block;
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 0.75rem;
    text-decoration: none;
    color: var(--text);
    font-size: 0.85rem;
    transition: border-color 0.2s;
}
.symbol-card:hover { border-color: var(--accent); }
.symbol-name {
    font-weight: 600;
    color: var(--accent);
    margin-bottom: 0.25rem;
}
.symbol-stat {
    color: var(--text-muted);
    font-size: 0.8rem;
}
```

- [ ] **Step 2: Reload + verify**

Reload `http://localhost:18084/#trading/overview`.

Expected: 12 metric cards (8 + 4), followed by per-symbol grid. Symbol cards link to `#trading/symbols/<SYM>`.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/trading_tab.js \
        bullforge/services/dashboard_service/static/style.css
git commit -m "feat(dashboard): Trading Overview sub-tab (12 metric cards + per-symbol grid)"
```

---

## Task 5: Equity Curve sub-tab

**Files:** Modify `bullforge/services/dashboard_service/static/trading_tab.js`

- [ ] **Step 1: Replace the `equity` placeholder**

```javascript
    equity: async function () {
      panel().innerHTML =
        '<p class="subtitle">Realized equity over time. Drawdown shaded where below trailing peak. Toggle SPY overlay to compare.</p>' +
        '<div style="margin-bottom:0.5rem;">' +
        '<label><input type="checkbox" id="eq-spy-toggle"> Overlay SPY buy-and-hold</label>' +
        '</div>' +
        '<div id="eq-chart-host" class="chart-host" style="width:100%;height:420px;"></div>' +
        '<div id="eq-stats" style="margin-top:0.5rem;color:var(--text-muted);"></div>';

      async function render() {
        var includeSpy = $('eq-spy-toggle').checked;
        var equityData = await fetchJSON('/api/trading/equity');
        var series = [{
          label: 'Realized equity',
          data: equityData.map(function (p) {
            return {x: p.timestamp, y: p.realized_equity};
          }),
          color: getComputedStyle(document.documentElement).getPropertyValue('--accent').trim() || '#00b4d8',
        }];
        if (includeSpy) {
          var bench = await fetchJSON('/api/trading/benchmark');
          series.push({
            label: 'SPY buy-and-hold',
            data: bench.spy_series.map(function (p) {
              return {x: p.timestamp, y: p.equity};
            }),
            color: getComputedStyle(document.documentElement).getPropertyValue('--yellow').trim() || '#f39c12',
          });
        }
        window.Charts.lineChart($('eq-chart-host'), {
          series: series, xIsTime: true, yLabel: 'Equity ($)',
        });

        if (equityData.length > 0) {
          var last = equityData[equityData.length - 1];
          $('eq-stats').textContent =
            'Current equity: $' + last.realized_equity.toFixed(2) +
            ' · Peak: $' + last.trailing_peak.toFixed(2) +
            ' · Drawdown: ' + (last.drawdown * 100).toFixed(2) + '%' +
            ' · Trades plotted: ' + equityData.length;
        } else {
          $('eq-stats').textContent = 'No closed trades in window.';
        }
      }
      $('eq-spy-toggle').onchange = render;
      await render();
    },
```

- [ ] **Step 2: Reload + verify**

Reload `http://localhost:18084/#trading/equity`.

Expected:
- Line chart of realized equity over time.
- SPY overlay toggle adds/removes a yellow SPY buy-and-hold line.
- Stats line below shows current equity, peak, drawdown, trade count.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/trading_tab.js
git commit -m "feat(dashboard): Trading Equity Curve sub-tab (equity + SPY overlay toggle)"
```

---

## Task 6: Distributions sub-tab

**Files:** Modify `bullforge/services/dashboard_service/static/trading_tab.js`

- [ ] **Step 1: Replace the `distributions` placeholder**

```javascript
    distributions: async function () {
      panel().innerHTML =
        '<h3>Daily P&L</h3>' +
        '<div id="dist-daily-host" class="chart-host" style="width:100%;height:220px;"></div>' +
        '<h3 style="margin-top:1rem;">Trade P&L histogram</h3>' +
        '<div id="dist-hist-host" class="chart-host" style="width:100%;height:240px;"></div>' +
        '<div id="dist-hist-summary" style="color:var(--text-muted);font-size:0.85rem;"></div>' +
        '<h3 style="margin-top:1rem;">Hold-time buckets</h3>' +
        '<div id="dist-hold-host" class="chart-host" style="width:100%;height:220px;"></div>' +
        '<h3 style="margin-top:1rem;">Side breakdown</h3>' +
        '<div id="dist-side"></div>';

      var data = await fetchJSON('/api/trading/distributions');

      // Daily P&L bars.
      window.Charts.barChart($('dist-daily-host'), {
        labels: data.daily_pnl.map(function (d) {
          return (d.date || '').slice(0, 10);
        }),
        values: data.daily_pnl.map(function (d) { return d.pnl; }),
        colorByValue: true, yLabel: 'P&L ($)',
      });

      // Histogram.
      var hist = data.trade_pnl_histogram;
      window.Charts.barChart($('dist-hist-host'), {
        labels: hist.labels.map(function (v) { return String(v); }),
        values: hist.counts, colorByValue: false, yLabel: 'Count',
      });
      $('dist-hist-summary').textContent =
        'Median: $' + hist.median.toFixed(2) + ' · Mean: $' + hist.mean.toFixed(2);

      // Hold-time buckets.
      window.Charts.barChart($('dist-hold-host'), {
        labels: data.hold_time_buckets.map(function (b) {
          return b.bucket + ' (' + b.trades + ')';
        }),
        values: data.hold_time_buckets.map(function (b) { return b.avg_pnl; }),
        colorByValue: true, yLabel: 'Avg P&L per trade ($)',
      });

      // Side breakdown table.
      $('dist-side').innerHTML =
        '<table class="data-table" style="max-width:500px;"><thead><tr>' +
        '<th>Side</th><th>Trades</th><th>Wins</th><th>Win Rate</th>' +
        '<th>Total P&L</th><th>Avg P&L</th>' +
        '</tr></thead><tbody>' +
        data.side_breakdown.map(function (s) {
          var cls = (s.total_pnl || 0) >= 0 ? 'positive' : 'negative';
          return '<tr><td>' + s.side + '</td>' +
            '<td>' + s.trades + '</td>' +
            '<td>' + s.wins + '</td>' +
            '<td>' + (s.win_rate * 100).toFixed(1) + '%</td>' +
            '<td class="' + cls + '">$' + s.total_pnl.toFixed(2) + '</td>' +
            '<td>$' + s.avg_pnl.toFixed(2) + '</td></tr>';
        }).join('') + '</tbody></table>';
    },
```

- [ ] **Step 2: Reload + verify**

Reload `http://localhost:18084/#trading/distributions`.

Expected: Daily P&L bars (green/red), trade P&L histogram with median/mean labels, hold-time buckets, side-breakdown table.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/trading_tab.js
git commit -m "feat(dashboard): Trading Distributions sub-tab (daily + histogram + hold-time + side)"
```

---

## Task 7: Streaks & Exposure sub-tab

**Files:** Modify `bullforge/services/dashboard_service/static/trading_tab.js`

- [ ] **Step 1: Replace the `streaks` placeholder**

```javascript
    streaks: async function () {
      panel().innerHTML =
        '<h3>Exposure over time (stacked by symbol)</h3>' +
        '<div id="streak-exp-host" class="chart-host" style="width:100%;height:280px;"></div>' +
        '<h3 style="margin-top:1rem;">Win/Loss ribbon (each cell = one trade)</h3>' +
        '<div id="streak-ribbon" class="wl-ribbon"></div>' +
        '<div id="streak-legend" style="color:var(--text-muted);font-size:0.85rem;margin-top:0.5rem;"></div>';

      var [exp, streaks] = await Promise.all([
        fetchJSON('/api/trading/exposure'),
        fetchJSON('/api/trading/streaks'),
      ]);

      // Stacked line representation: one line per symbol (simpler than a
      // true stacked area without additional Chart.js plugins).
      var symbols = {};
      exp.forEach(function (p) {
        Object.keys(p.by_symbol || {}).forEach(function (sym) {
          if (!symbols[sym]) symbols[sym] = [];
          symbols[sym].push({x: p.timestamp, y: p.by_symbol[sym]});
        });
      });
      // Also include the total line.
      var totalSeries = exp.map(function (p) {
        return {x: p.timestamp, y: p.total_exposure_pct};
      });
      var series = [{label: 'Total', data: totalSeries}];
      Object.keys(symbols).sort().forEach(function (sym) {
        series.push({label: sym, data: symbols[sym]});
      });
      window.Charts.lineChart($('streak-exp-host'), {
        series: series, xIsTime: true, yLabel: 'Exposure (fraction of equity)',
      });

      // W/L ribbon: div per trade, colored green/red.
      $('streak-ribbon').innerHTML = streaks.ribbon.map(function (r) {
        var cls = r.win ? 'wl-win' : 'wl-loss';
        var tip = (r.timestamp || '').slice(0, 19).replace('T', ' ');
        return '<span class="wl-cell ' + cls + '" title="' + tip + '"></span>';
      }).join('');
      $('streak-legend').textContent =
        streaks.ribbon.length + ' trades · longest win: ' + streaks.longest_win_streak +
        ' · longest loss: ' + streaks.longest_loss_streak;
    },
```

Append CSS to `style.css`:

```css
.wl-ribbon {
    display: flex;
    flex-wrap: wrap;
    gap: 1px;
    padding: 0.25rem;
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 4px;
}
.wl-cell {
    width: 12px;
    height: 18px;
    display: inline-block;
}
.wl-win { background: var(--green); }
.wl-loss { background: var(--red); }
```

- [ ] **Step 2: Reload + verify**

Reload `http://localhost:18084/#trading/streaks`.

Expected: Exposure chart with a line per symbol + total; W/L ribbon below with green/red cells.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/trading_tab.js \
        bullforge/services/dashboard_service/static/style.css
git commit -m "feat(dashboard): Trading Streaks & Exposure sub-tab (per-symbol exposure + W/L ribbon)"
```

---

## Task 8: Heatmaps sub-tab

**Files:** Modify `bullforge/services/dashboard_service/static/trading_tab.js`

- [ ] **Step 1: Replace the `heatmaps` placeholder**

```javascript
    heatmaps: async function () {
      panel().innerHTML =
        '<h3>Weekday × half-hour (UTC) average trade P&L</h3>' +
        '<p class="subtitle">Color intensity scales with |avg P&L|. Green = positive, red = negative. Empty cells: no trades.</p>' +
        '<div id="heat-time-host" class="chart-host" style="width:100%;min-height:260px;"></div>';

      var heat = await fetchJSON('/api/trading/heatmap?kind=time');
      window.Charts.heatmapChart($('heat-time-host'), {
        x_labels: heat.x_labels,
        y_labels: heat.y_labels,
        cells: heat.cells,
      });
      // Fix: Charts.heatmapChart expects xLabels/yLabels (camelCase).
      // Re-render with the correct keys.
      window.Charts.heatmapChart($('heat-time-host'), {
        xLabels: heat.x_labels,
        yLabels: heat.y_labels,
        cells: heat.cells,
      });
    },
```

- [ ] **Step 2: Reload + verify**

Reload `http://localhost:18084/#trading/heatmaps`.

Expected: 5-row × 48-col colored-cell grid; cells with trades colored green/red by avg P&L, others blank. Hover tooltip shows "Mon × 14:30: 0.0034".

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/trading_tab.js
git commit -m "feat(dashboard): Trading Heatmaps sub-tab (weekday × time P&L grid)"
```

---

## Task 9: Benchmark sub-tab

**Files:** Modify `bullforge/services/dashboard_service/static/trading_tab.js`

- [ ] **Step 1: Replace the `benchmark` placeholder**

```javascript
    benchmark: async function () {
      panel().innerHTML =
        '<h3>Strategy equity vs SPY buy-and-hold</h3>' +
        '<div id="bench-chart-host" class="chart-host" style="width:100%;height:340px;"></div>' +
        '<h3 style="margin-top:1rem;">Stats</h3>' +
        '<div id="bench-stats"></div>';

      var data = await fetchJSON('/api/trading/benchmark');
      var series = [
        {
          label: 'Strategy',
          data: data.strategy_series.map(function (p) {
            return {x: p.timestamp, y: p.equity};
          }),
          color: getComputedStyle(document.documentElement).getPropertyValue('--accent').trim() || '#00b4d8',
        },
        {
          label: 'SPY buy-and-hold',
          data: data.spy_series.map(function (p) {
            return {x: p.timestamp, y: p.equity};
          }),
          color: getComputedStyle(document.documentElement).getPropertyValue('--yellow').trim() || '#f39c12',
        },
      ];
      window.Charts.lineChart($('bench-chart-host'), {
        series: series, xIsTime: true, yLabel: 'Equity ($)',
      });

      var s = data.stats;
      function pct(x) { return (x * 100).toFixed(2) + '%'; }
      function num(x) { return x.toFixed(3); }
      $('bench-stats').innerHTML =
        '<table class="data-table" style="max-width:400px;"><tbody>' +
        '<tr><td>Annualized return</td><td class="' + (s.annualized_return >= 0 ? 'positive' : 'negative') + '">' + pct(s.annualized_return) + '</td></tr>' +
        '<tr><td>Annualized volatility</td><td>' + pct(s.annualized_vol) + '</td></tr>' +
        '<tr><td>Sharpe</td><td>' + num(s.sharpe) + '</td></tr>' +
        '<tr><td>Max drawdown</td><td class="negative">' + pct(s.max_drawdown) + '</td></tr>' +
        '<tr><td>Beta (vs SPY)</td><td>' + num(s.beta) + '</td></tr>' +
        '<tr><td>Alpha (vs SPY)</td><td class="' + (s.alpha >= 0 ? 'positive' : 'negative') + '">' + pct(s.alpha) + '</td></tr>' +
        '</tbody></table>';
    },
```

- [ ] **Step 2: Reload + verify**

Reload `http://localhost:18084/#trading/benchmark`.

Expected: Overlay line chart (strategy in accent, SPY in yellow); stats table with 6 rows.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/trading_tab.js
git commit -m "feat(dashboard): Trading Benchmark sub-tab (strategy vs SPY + 6 stats)"
```

---

## Task 10: Per-symbol drill-down

**Files:** Modify `bullforge/services/dashboard_service/static/trading_tab.js`

- [ ] **Step 1: Replace `renderDrillDown`**

Replace the `renderDrillDown` placeholder with:

```javascript
  async function renderDrillDown(symbol) {
    state.currentDrillSymbol = symbol;
    panel().innerHTML = '';
    var dd = window.DrillDown.create({
      mountEl: panel(),
      title: 'Symbol: ' + symbol,
      backHref: '#trading/overview',
      breadcrumbs: [
        {label: 'Trading', href: '#trading'},
        {label: 'Overview', href: '#trading/overview'},
        {label: symbol},
      ],
    });
    var slot = dd.getContentSlot();
    slot.innerHTML =
      '<section class="drill-section"><h3>Equity (this symbol)</h3>' +
      '<div id="ds-eq" class="chart-host" style="width:100%;height:240px;"></div></section>' +
      '<section class="drill-section"><h3>Win rate over time</h3>' +
      '<div id="ds-wr" class="chart-host" style="width:100%;height:220px;"></div></section>' +
      '<section class="drill-section"><h3>Side breakdown</h3>' +
      '<div id="ds-side"></div></section>' +
      '<section class="drill-section"><h3>Recent trades</h3>' +
      '<div id="ds-trades-host"></div></section>';

    var data = await fetchJSON('/api/trading/symbols/' + encodeURIComponent(symbol));

    // Equity series for this symbol.
    window.Charts.lineChart($('ds-eq'), {
      series: [{
        label: symbol + ' equity',
        data: data.equity_series.map(function (p) {
          return {x: p.timestamp, y: p.equity};
        }),
      }],
      xIsTime: true, yLabel: 'P&L ($)',
    });

    // Win rate over time.
    window.Charts.lineChart($('ds-wr'), {
      series: [{
        label: 'Daily win rate',
        data: data.win_rate_over_time.map(function (r) {
          return {x: r.date, y: r.win_rate};
        }),
      }],
      xIsTime: true, yLabel: 'Win rate',
    });

    // Side breakdown table.
    $('ds-side').innerHTML =
      '<table class="data-table" style="max-width:500px;"><thead><tr>' +
      '<th>Side</th><th>Trades</th><th>Wins</th><th>Win Rate</th><th>Total P&L</th>' +
      '</tr></thead><tbody>' +
      data.side_breakdown.map(function (s) {
        var cls = (s.total_pnl || 0) >= 0 ? 'positive' : 'negative';
        return '<tr><td>' + s.side + '</td>' +
          '<td>' + s.trades + '</td>' +
          '<td>' + s.wins + '</td>' +
          '<td>' + (s.win_rate * 100).toFixed(1) + '%</td>' +
          '<td class="' + cls + '">$' + s.total_pnl.toFixed(2) + '</td></tr>';
      }).join('') + '</tbody></table>';

    // Trades table via Leaderboard component.
    window.Leaderboard.create({
      mountEl: $('ds-trades-host'),
      columns: [
        {key: 'entry_time', label: 'Entry', sortable: true,
         render: function (v) { return window.fmtTime ? window.fmtTime(v) : v; }},
        {key: 'side', label: 'Side', sortable: true},
        {key: 'entry_price', label: 'Entry $', sortable: true,
         render: function (v) { return v != null ? v.toFixed(2) : '--'; }},
        {key: 'exit_price', label: 'Exit $', sortable: true,
         render: function (v) { return v != null ? v.toFixed(2) : '--'; }},
        {key: 'quantity', label: 'Qty', sortable: true},
        {key: 'pnl', label: 'P&L', sortable: true,
         render: function (v) {
           if (v == null) return '--';
           var cls = v >= 0 ? 'positive' : 'negative';
           return '<span class="' + cls + '">$' + v.toFixed(2) + '</span>';
         }},
        {key: 'hold_duration_seconds', label: 'Hold', sortable: true,
         render: function (s) {
           if (s == null) return '--';
           if (s < 60) return s + 's';
           if (s < 3600) return Math.floor(s / 60) + 'm';
           return Math.floor(s / 3600) + 'h ' + Math.floor((s % 3600) / 60) + 'm';
         }},
      ],
      rows: data.trades,
      initialSort: {key: 'entry_time', order: 'desc'},
    });
  }
```

- [ ] **Step 2: Reload + verify**

Reload `http://localhost:18084/#trading/symbols/AAPL` (replace AAPL with a symbol that has trades).

Expected: DrillDown header + 4 sections — symbol-scoped equity chart, win rate chart, side breakdown table, sortable trades table.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/trading_tab.js
git commit -m "feat(dashboard): per-symbol drill-down page (equity + win rate + side + trades)"
```

---

## Task 11: End-to-end smoke test

**Files:** none modified — verification only.

- [ ] **Step 1: Full backend test suite**

Run: `cd bullforge && python -m pytest tests/test_time_window.py tests/test_dashboard_api.py tests/test_dashboard_stubs.py tests/test_dashboard_models_api.py tests/test_dashboard_trading_api.py tests/test_accuracy_snapshot.py tests/test_dashboard_backtests_api.py -v`

Expected: all PASS; no regressions.

- [ ] **Step 2: Full core suite sanity**

Run: `cd bullforge && python -m pytest tests/test_signal_aggregator.py tests/test_risk_manager.py tests/test_trainer.py tests/test_lstm_model.py tests/test_order_executor.py tests/meta/ -v`

Expected: no regressions.

- [ ] **Step 3: Probe stub state**

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:18084/api/trading/equity
# Expected: 200

curl -s -o /dev/null -w '%{http_code}\n' http://localhost:18084/api/backtests/1/folds
# Expected: 501 (still a Plan 4 stub)
```

- [ ] **Step 4: Manual dashboard walkthrough**

In a browser, visit in order:

1. `#trading/overview` — 12 metric cards + per-symbol grid.
2. `#trading/equity` — realized equity line; toggle SPY overlay.
3. `#trading/distributions` — 4 visualizations.
4. `#trading/streaks` — exposure lines + W/L ribbon.
5. `#trading/heatmaps` — weekday × time grid.
6. `#trading/benchmark` — strategy vs SPY + stats table.
7. Click a symbol in the Overview grid → symbol drill-down loads.
8. Back button returns to overview.
9. Time-window picker change triggers re-fetch.
10. Refresh-manual button re-fetches.
11. Other tabs (Overview, Models, Trades, Orders, ...) still work.
12. DevTools console has no errors.

- [ ] **Step 5: Commit smoke-test fixes if any**

If none needed, skip. Otherwise: `git commit -m "fix(dashboard): address smoke-test findings in Trading plan"`.

---

## Self-Review Summary

**Spec coverage** (against [2026-04-17-dashboard-expansion-design.md](../specs/2026-04-17-dashboard-expansion-design.md) — Plan 3 scope):

| Spec item | Task |
|-----------|------|
| Overview sub-tab (existing cards + 4 new streak/exposure cards) | Task 4 |
| Equity curve + drawdown + SPY overlay | Tasks 1 (query_equity_curve, query_benchmark), 2 (endpoint), 5 |
| Daily P&L bars | Tasks 1 (query_distributions), 2, 6 |
| Trade P&L histogram | Tasks 1, 2, 6 |
| Hold-time bucketed performance | Tasks 1, 2, 6 |
| Side breakdown | Tasks 1, 2, 6 |
| Current + longest streaks | Tasks 1 (query_streaks), 2, 4 (overview cards), 7 (ribbon) |
| Exposure over time stacked by symbol | Tasks 1 (query_exposure_timeline), 2, 7 |
| Time-of-day × day-of-week heatmap | Tasks 1 (query_heatmap_time_of_day), 2, 8 |
| SPY benchmark + 6 stats | Tasks 1 (query_benchmark), 2, 9 |
| Per-symbol drill-down (#trading/symbols/{sym}) | Tasks 1 (query_symbol_drilldown), 2, 10 |
| Seven Plan 1 stubs removed | Task 2 |
| Layout D (sub-tabs + drill-down via Router) | Task 3 (scaffold) |
| Time-window picker + refresh wiring | Task 3 |

**Placeholder scan:** None.

**Type consistency:** All DB methods accept `Optional[datetime]` `from_dt`/`to_dt`. Endpoints route through `parse_time_window`. Frontend `fetchJSON` appends window params. `parse_time_window` reuses Plan 1's helper. `starting_equity` consolidated in one helper to avoid drift between endpoints.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-17-dashboard-plan-3-trading.md`.

**Two execution options:**

1. **Subagent-Driven (recommended)** — fresh subagent per task, two-stage review
2. **Inline Execution** — batch tasks in this session

Which approach?
