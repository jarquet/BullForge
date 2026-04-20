# Backtest Performance — Three Wins Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut postgres round-trips from the backtest hot path (one INSERT per equity point, one per rejected order) and stop submitting orders we already know will be rejected by the volume cap.

**Architecture:**
1. Replace per-row awaited inserts at fold-end with a single `executemany` per table (asyncpg pattern already used in `shared/db.py:insert_bars`).
2. In backtest mode (`scope is not None`), peek the simulated broker's order status *before* writing to `orders`; skip the insert and return `None` when the broker rejected.
3. Cap requested qty at `next_bar.volume * max_volume_pct` inside `OrderExecutor.process_signal` (when caller passes `max_qty_cap`); the engine computes the cap from the next bar it already has on hand. If capped qty falls below the existing $1-notional floor, skip submission entirely.

**Tech Stack:** asyncpg, pytest-asyncio, existing `SimulatedBroker` / `BacktestResultsDB` / `OrderExecutor` / `RiskManager` / `FillModel`.

**Out of scope:** changing live-trading behavior, changing fill-model semantics from reject-to-partial-fill, parallelism inside a single fold, postgres-side tuning.

---

## Task 1: Batch equity & trade inserts at fold end

**Files:**
- Modify: `bullforge/services/backtest_service/results_db.py` (add `insert_trades_many`, `insert_equity_points_many`)
- Modify: `bullforge/services/backtest_service/engine.py:255-263` (replace per-row awaits with batch calls)
- Test: `bullforge/tests/test_backtest_results_db_batch.py` (new — pure asyncpg-mock unit tests, no real DB)

- [ ] **Step 1: Write failing unit tests for the two new batch methods**

```python
# bullforge/tests/test_backtest_results_db_batch.py
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.backtest_service.results_db import BacktestResultsDB


UTC = timezone.utc


def _mock_db():
    conn = AsyncMock()
    conn.executemany = AsyncMock()
    pool_ctx = MagicMock()
    pool_ctx.__aenter__ = AsyncMock(return_value=conn)
    pool_ctx.__aexit__ = AsyncMock(return_value=False)
    db = MagicMock()
    db.pool.acquire = MagicMock(return_value=pool_ctx)
    return db, conn


@pytest.mark.asyncio
async def test_insert_equity_points_many_batches_in_one_executemany():
    db, conn = _mock_db()
    rdb = BacktestResultsDB(db)
    points = [
        {"timestamp": datetime(2026, 4, 20, 14, i, tzinfo=UTC),
         "equity": 100_000.0 + i, "cash": 95_000.0 + i}
        for i in range(5)
    ]
    await rdb.insert_equity_points_many(run_id=7, fold_id=11, points=points)

    conn.executemany.assert_awaited_once()
    sql, rows = conn.executemany.await_args.args
    assert "INSERT INTO backtest_equity" in sql
    assert "ON CONFLICT" in sql
    assert len(rows) == 5
    assert rows[0] == (7, 11, points[0]["timestamp"], 100_000.0, 95_000.0)


@pytest.mark.asyncio
async def test_insert_equity_points_many_noop_on_empty():
    db, conn = _mock_db()
    rdb = BacktestResultsDB(db)
    await rdb.insert_equity_points_many(run_id=7, fold_id=11, points=[])
    conn.executemany.assert_not_called()


@pytest.mark.asyncio
async def test_insert_trades_many_batches_in_one_executemany():
    db, conn = _mock_db()
    rdb = BacktestResultsDB(db)
    trades = [
        {
            "symbol": "SPY", "side": "long",
            "entry_time": datetime(2026, 4, 20, 14, 0, tzinfo=UTC),
            "entry_price": 420.0, "quantity": 10.0,
            "exit_time": datetime(2026, 4, 20, 15, 0, tzinfo=UTC),
            "exit_price": 421.0, "pnl": 10.0, "signal_strength": 0.7,
        },
        {
            "symbol": "BTC/USD", "side": "short",
            "entry_time": datetime(2026, 4, 20, 14, 1, tzinfo=UTC),
            "entry_price": 60000.0, "quantity": 0.001,
            "exit_time": None, "exit_price": None,
            "pnl": None, "signal_strength": 0.5,
        },
    ]
    await rdb.insert_trades_many(run_id=3, fold_id=4, trades=trades)

    conn.executemany.assert_awaited_once()
    sql, rows = conn.executemany.await_args.args
    assert "INSERT INTO backtest_trades" in sql
    assert len(rows) == 2
    assert rows[0][0] == 3 and rows[0][1] == 4 and rows[0][2] == "SPY"


@pytest.mark.asyncio
async def test_insert_trades_many_noop_on_empty():
    db, conn = _mock_db()
    rdb = BacktestResultsDB(db)
    await rdb.insert_trades_many(run_id=3, fold_id=4, trades=[])
    conn.executemany.assert_not_called()
```

- [ ] **Step 2: Run the tests; confirm 4 failures (methods not defined)**

Run: `cd bullforge && pytest tests/test_backtest_results_db_batch.py -v`
Expected: 4 errors / `AttributeError: 'BacktestResultsDB' object has no attribute 'insert_equity_points_many'`

- [ ] **Step 3: Add batch methods to `results_db.py`** (right after the existing singular methods at the end of the writes block, before the `# ---- reads ----` divider):

```python
    async def insert_trades_many(
        self, *, run_id: int, fold_id: int, trades: list[dict],
    ) -> None:
        if not trades:
            return
        rows = [
            (
                run_id, fold_id, t["symbol"], t["side"],
                t["entry_time"], t["entry_price"], t["quantity"],
                t["exit_time"], t["exit_price"], t["pnl"], t["signal_strength"],
            )
            for t in trades
        ]
        async with self._db.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO backtest_trades (
                    run_id, fold_id, symbol, side,
                    entry_time, entry_price, quantity,
                    exit_time, exit_price, pnl, signal_strength
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                """,
                rows,
            )

    async def insert_equity_points_many(
        self, *, run_id: int, fold_id: int, points: list[dict],
    ) -> None:
        if not points:
            return
        rows = [
            (run_id, fold_id, p["timestamp"], p["equity"], p["cash"])
            for p in points
        ]
        async with self._db.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO backtest_equity (run_id, fold_id, timestamp, equity, cash)
                VALUES ($1,$2,$3,$4,$5)
                ON CONFLICT (run_id, fold_id, timestamp) DO UPDATE
                SET equity = EXCLUDED.equity, cash = EXCLUDED.cash
                """,
                rows,
            )
```

- [ ] **Step 4: Re-run unit tests; expect 4 PASS**

Run: `cd bullforge && pytest tests/test_backtest_results_db_batch.py -v`
Expected: 4 passed

- [ ] **Step 5: Wire engine to call batch methods**

Replace `bullforge/services/backtest_service/engine.py:255-263`:

```python
        # 5) Persist equity + trades + metrics for the fold.
        await self._results.insert_equity_points_many(
            run_id=run_id, fold_id=fold_id,
            points=[
                {"timestamp": pt.timestamp, "equity": pt.equity, "cash": pt.cash}
                for pt in equity.points()
            ],
        )
        await self._results.insert_trades_many(
            run_id=run_id, fold_id=fold_id, trades=completed_trades,
        )
```

- [ ] **Step 6: Run engine + results_db unit tests; expect green**

Run: `cd bullforge && pytest tests/test_backtest_engine_record_trade.py tests/test_backtest_results_db_batch.py -v`
Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add bullforge/services/backtest_service/results_db.py \
        bullforge/services/backtest_service/engine.py \
        bullforge/tests/test_backtest_results_db_batch.py
git commit -m "perf(backtest): batch equity/trade inserts at fold end"
```

---

## Task 2: Skip orders INSERT for rejected backtest orders

**Files:**
- Modify: `bullforge/services/execution_service/order_executor.py` (`_execute_open`, `_execute_close`)
- Modify: `bullforge/tests/test_order_executor.py` (add backtest-mode rejection tests)

**Why this is safe in backtest only:** the backtest stack uses `_NoopReconciler` and a synchronous `SimulatedBroker` whose `submit_market_order` returns the terminal status (`filled` or `rejected`) on the same await. There is no stream race like in live trading. Live trading still inserts immediately and lets the reconciler resolve later.

- [ ] **Step 1: Write failing tests**

Add at end of `bullforge/tests/test_order_executor.py`:

```python
from shared.models import BacktestScope


@pytest.mark.asyncio
async def test_backtest_mode_skips_db_insert_when_broker_rejects(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    mock_broker.get_order = AsyncMock(return_value={
        "id": "alp-order-123", "symbol": "BTC/USD", "side": "buy",
        "qty": 0.5, "order_type": "market",
        "submitted_at": datetime(2026, 4, 20, 14, 0, 0, tzinfo=timezone.utc),
        "status": "rejected",
        "filled_qty": None, "filled_avg_price": None, "filled_at": None,
    })
    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
        scope=BacktestScope(backtest_run_id=1, fold_index=0),
    )

    order = await executor.process_signal(
        _signal(symbol="BTC/USD"), current_price=60_000.0,
    )

    assert order is None
    mock_broker.submit_market_order.assert_awaited_once()
    mock_db.insert_order_if_new.assert_not_called()
    mock_core.apply_alpaca_order.assert_not_called()


@pytest.mark.asyncio
async def test_backtest_mode_inserts_when_broker_fills(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    mock_broker.get_order = AsyncMock(return_value={
        "id": "alp-order-123", "symbol": "SPY", "side": "buy",
        "qty": 10.0, "order_type": "market",
        "submitted_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        "status": "filled",
        "filled_qty": 10.0, "filled_avg_price": 420.05,
        "filled_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
    })
    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
        scope=BacktestScope(backtest_run_id=1, fold_index=0),
    )

    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is not None
    mock_db.insert_order_if_new.assert_awaited_once()


@pytest.mark.asyncio
async def test_backtest_close_skips_db_insert_when_broker_rejects(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    mock_tracker.get_position = MagicMock(
        return_value={"symbol": "SPY", "qty": 10.0, "market_value": 4200.0},
    )
    mock_risk.check_exit = MagicMock(return_value=(True, "Opposing signal"))
    mock_broker.get_order = AsyncMock(return_value={
        "id": "alp-close-456", "symbol": "SPY", "side": "sell",
        "qty": 10.0, "order_type": "market",
        "submitted_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        "status": "rejected",
        "filled_qty": None, "filled_avg_price": None, "filled_at": None,
    })
    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
        scope=BacktestScope(backtest_run_id=1, fold_index=0),
    )

    order = await executor.process_signal(
        _signal(direction=-1), current_price=420.0,
    )

    assert order is None
    mock_broker.close_position.assert_awaited_once()
    mock_db.insert_order_if_new.assert_not_called()
```

- [ ] **Step 2: Run; expect 3 failures (current code always inserts)**

Run: `cd bullforge && pytest tests/test_order_executor.py -v`

- [ ] **Step 3: Modify `_execute_open` and `_execute_close` to peek-then-insert in backtest mode**

In `bullforge/services/execution_service/order_executor.py`, replace the body of `_execute_open` after the `submit_market_order` block (lines 70-96) with:

```python
        now = datetime.now(timezone.utc)
        order = Order(
            symbol=signal.symbol, side=side, quantity=qty,
            order_type="market", submitted_at=now,
            signal_strength=signal.strength,
            alpaca_order_id=alpaca_id, status="submitted",
        )

        if self._scope is not None:
            # Backtest: simulated broker is synchronous and the reconciler is
            # a no-op, so the terminal status is already known. Skip the DB
            # insert entirely for rejected orders to avoid wasted round-trips.
            snap = await self._broker.get_order(alpaca_id)
            if snap.get("status") != "filled":
                logger.info(
                    "skipping order insert for %s %s: broker status=%s",
                    signal.symbol, alpaca_id, snap.get("status"),
                )
                return None

        order_id = await self._db.insert_order_if_new(order, scope=self._scope)

        try:
            snap = await self._broker.get_order(alpaca_id)
            snap.setdefault("symbol", signal.symbol)
            snap.setdefault("side", side)
            snap.setdefault("qty", qty)
            snap.setdefault("order_type", "market")
            snap.setdefault("submitted_at", now)
            await self._core.apply_alpaca_order(snap, source="submit")
        except Exception:
            logger.exception(
                "submit-path reconciliation failed for %s; stream/sweeper will recover",
                alpaca_id,
            )

        if order_id is None:
            return await self._db.get_order_by_alpaca_id(alpaca_id, scope=self._scope)
        return replace(order, id=order_id)
```

Apply the equivalent peek-then-skip to `_execute_close` (insert the same `if self._scope is not None: ...` block right before the `order_id = await self._db.insert_order_if_new(...)` call at line 114).

- [ ] **Step 4: Re-run executor tests; expect green**

Run: `cd bullforge && pytest tests/test_order_executor.py -v`

- [ ] **Step 5: Update the engine's `_record_trade` log to DEBUG (now a degenerate path: only fires if a non-scope caller hits it)**

In `bullforge/services/backtest_service/engine.py`, change `logger.info` at line 290 to `logger.debug`. (The orders-insert path no longer reaches this for rejected orders, but the function still defends against the case.)

- [ ] **Step 6: Run engine record-trade tests; expect green**

Run: `cd bullforge && pytest tests/test_backtest_engine_record_trade.py -v`

- [ ] **Step 7: Commit**

```bash
git add bullforge/services/execution_service/order_executor.py \
        bullforge/services/backtest_service/engine.py \
        bullforge/tests/test_order_executor.py
git commit -m "perf(backtest): skip orders INSERT for rejected sim broker fills"
```

---

## Task 3: Preflight volume cap in `process_signal`

**Files:**
- Modify: `bullforge/services/execution_service/order_executor.py` (`process_signal` signature + cap logic)
- Modify: `bullforge/services/backtest_service/engine.py` (compute cap from `nxt["volume"]` and pass it)
- Modify: `bullforge/tests/test_order_executor.py` (cap behavior tests)

**Why this lives in `process_signal`, not `RiskManager`:** the volume cap is a market-microstructure constraint specific to backtest fills, not a portfolio-risk concern. Threading it through the risk manager would conflate the two and require live callers to pass a meaningless arg. Keeping it as an executor kwarg (default `None` = no cap) leaves live trading untouched.

- [ ] **Step 1: Write failing tests**

Add to `bullforge/tests/test_order_executor.py`:

```python
@pytest.mark.asyncio
async def test_max_qty_cap_shrinks_order_to_cap(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    mock_risk.check_entry = AsyncMock(return_value=(True, "Approved", 10.0))
    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
    )
    await executor.process_signal(
        _signal(), current_price=420.0, max_qty_cap=2.5,
    )
    mock_broker.submit_market_order.assert_awaited_once_with("SPY", 2.5, "buy")


@pytest.mark.asyncio
async def test_max_qty_cap_skips_when_cap_below_one_dollar_notional(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    mock_risk.check_entry = AsyncMock(return_value=(True, "Approved", 10.0))
    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
    )
    result = await executor.process_signal(
        _signal(), current_price=420.0, max_qty_cap=0.001,  # 0.001 * 420 = $0.42
    )
    assert result is None
    mock_broker.submit_market_order.assert_not_called()


@pytest.mark.asyncio
async def test_max_qty_cap_no_op_when_risk_qty_already_below_cap(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    mock_risk.check_entry = AsyncMock(return_value=(True, "Approved", 1.0))
    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
    )
    await executor.process_signal(
        _signal(), current_price=420.0, max_qty_cap=5.0,
    )
    mock_broker.submit_market_order.assert_awaited_once_with("SPY", 1.0, "buy")
```

- [ ] **Step 2: Run; expect 3 failures (`process_signal` doesn't accept `max_qty_cap`)**

Run: `cd bullforge && pytest tests/test_order_executor.py -v`

- [ ] **Step 3: Add the cap kwarg + logic to `process_signal`**

In `bullforge/services/execution_service/order_executor.py`, modify the signature and the post-`check_entry` block:

```python
    async def process_signal(
        self, signal: TradingSignal, current_price: float,
        *, max_qty_cap: Optional[float] = None,
    ) -> Optional[Order]:
        await self._tracker.refresh()
        existing = self._tracker.get_position(signal.symbol)

        if existing:
            should_exit, _reason = self._risk.check_exit(signal, existing)
            if should_exit:
                return await self._execute_close(signal, existing)
            return None

        if signal.direction == 0:
            return None

        account = self._tracker.account
        positions = list(self._tracker.positions.values())
        approved, reason, qty = await self._risk.check_entry(
            signal, account, positions, current_price,
        )
        if not approved:
            logger.info("Entry rejected for %s: %s", signal.symbol, reason)
            return None

        if max_qty_cap is not None and qty > max_qty_cap:
            qty = max_qty_cap
            if qty * current_price < 1.0:
                logger.info(
                    "Entry capped below $1 notional for %s; skipping submit",
                    signal.symbol,
                )
                return None

        return await self._execute_open(signal, qty)
```

- [ ] **Step 4: Re-run executor tests; expect green**

Run: `cd bullforge && pytest tests/test_order_executor.py -v`

- [ ] **Step 5: Wire engine to compute the cap from the next bar**

In `bullforge/services/backtest_service/engine.py`, replace the `if signal is not None and nxt is not None:` block (around line 235-241):

```python
            if signal is not None and nxt is not None:
                cap = float(nxt["volume"]) * cfg.max_volume_pct
                if cap <= 0:
                    continue
                order = await executor.process_signal(
                    signal, bar["close"], max_qty_cap=cap,
                )
                if order is not None:
                    await self._record_trade(
                        symbol, order, signal, ts, open_trade,
                        completed_trades, broker,
                    )
```

- [ ] **Step 6: Run the full backtest test suite**

Run: `cd bullforge && pytest tests/test_backtest_engine_record_trade.py tests/test_order_executor.py tests/test_backtest_results_db_batch.py tests/test_simulated_broker.py tests/test_fill_model.py tests/test_risk_manager.py -v`
Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add bullforge/services/execution_service/order_executor.py \
        bullforge/services/backtest_service/engine.py \
        bullforge/tests/test_order_executor.py
git commit -m "perf(backtest): preflight volume cap in process_signal"
```
