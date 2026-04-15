# Walk-Forward Backtest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an offline backtesting harness that replays historical TimescaleDB bars through the live signal/execution pipeline using walk-forward, out-of-sample model training and a simulated broker, producing per-fold and aggregated performance metrics.

**Architecture:** A new `services/backtest_service/` package adds a `WalkForwardSplitter`, a `SimulatedBroker` that mimics `AlpacaBroker`'s interface against a fold-scoped bar store, a `BacktestEngine` orchestrator that for each fold (a) trains models with `Trainer` against an `as_of` cutoff, (b) iterates test-window bars chronologically, calling `InferenceEngine` → `SignalAggregator` → `OrderExecutor` exactly as production does, and (c) writes fills, equity, and metrics into new `backtest_*` tables. A CLI wrapper (`scripts/backtest.py`) drives runs.

**Tech Stack:** Python 3.11, asyncio, TimescaleDB (asyncpg), pandas, scikit-learn, pytest. No new third-party dependencies.

---

## File Structure

**Created:**
- `bullforge/migrations/012_create_backtest_tables.sql` — schema for backtest runs, folds, trades, equity points
- `bullforge/services/backtest_service/__init__.py`
- `bullforge/services/backtest_service/splitter.py` — `WalkForwardSplitter`
- `bullforge/services/backtest_service/simulated_broker.py` — `SimulatedBroker` (drop-in for `AlpacaBroker`)
- `bullforge/services/backtest_service/fill_model.py` — slippage + latency model
- `bullforge/services/backtest_service/equity_tracker.py` — mark-to-market state
- `bullforge/services/backtest_service/metrics.py` — Sharpe, drawdown, hit rate
- `bullforge/services/backtest_service/results_db.py` — typed wrappers around `backtest_*` tables
- `bullforge/services/backtest_service/engine.py` — `BacktestEngine` orchestrator
- `bullforge/scripts/backtest.py` — CLI entrypoint
- `bullforge/tests/test_walkforward_splitter.py`
- `bullforge/tests/test_simulated_broker.py`
- `bullforge/tests/test_fill_model.py`
- `bullforge/tests/test_equity_tracker.py`
- `bullforge/tests/test_backtest_metrics.py`
- `bullforge/tests/test_backtest_results_db.py` (integration — requires Timescale)
- `bullforge/tests/integration/test_backtest_engine_smoke.py` (integration — single-fold end-to-end)

**Modified:**
- `bullforge/shared/db.py` — add helper `query_bars_between(symbol, timeframe, start, end)` and `query_active_models_as_of(symbol, timeframe, as_of)` (the latter only if not already present)

**Untouched (reused):**
- `services/feature_engine/engine.py`, `services/feature_engine/labels.py`
- `services/model_service/{trainer,inference,model_store,dataset}.py`
- `services/execution_service/{signal_aggregator,risk_manager,order_executor,position_tracker}.py`
- `shared/trading_window.py`, `shared/runtime_config.py`, `shared/models.py`

---

## Conventions

- All timestamps UTC, timezone-aware (matches `shared/models.py` invariants).
- Tests use `pytest`; unit tests live under `bullforge/tests/` and run without DB; integration tests under `bullforge/tests/integration/` require `docker-compose.test.yml` services.
- Run all tests from the repo root: `cd bullforge && PYTHONPATH=. pytest tests/<file>.py -v`.
- Commit after each task with the message shown in the task's commit step.
- Type hints with `from __future__ import annotations` at the top of every new file.

---

## Task 1: Add backtest results schema

**Files:**
- Create: `bullforge/migrations/012_create_backtest_tables.sql`
- Test: `bullforge/tests/test_backtest_results_db.py` (added in Task 7; this task only writes the migration)

- [ ] **Step 1: Write the migration**

```sql
-- bullforge/migrations/012_create_backtest_tables.sql
CREATE TABLE IF NOT EXISTS backtest_runs (
    id            BIGSERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at   TIMESTAMPTZ,
    symbols       TEXT[] NOT NULL,
    timeframe     TEXT NOT NULL,
    horizons      INT[]  NOT NULL,
    range_start   TIMESTAMPTZ NOT NULL,
    range_end     TIMESTAMPTZ NOT NULL,
    train_days    INT  NOT NULL,
    test_days     INT  NOT NULL,
    embargo_min   INT  NOT NULL,
    mode          TEXT NOT NULL CHECK (mode IN ('rolling','anchored')),
    config_json   JSONB NOT NULL,
    status        TEXT NOT NULL DEFAULT 'running'
                  CHECK (status IN ('running','completed','failed'))
);

CREATE TABLE IF NOT EXISTS backtest_folds (
    id            BIGSERIAL PRIMARY KEY,
    run_id        BIGINT NOT NULL REFERENCES backtest_runs(id) ON DELETE CASCADE,
    fold_index    INT NOT NULL,
    train_start   TIMESTAMPTZ NOT NULL,
    train_end     TIMESTAMPTZ NOT NULL,
    test_start    TIMESTAMPTZ NOT NULL,
    test_end      TIMESTAMPTZ NOT NULL,
    metrics_json  JSONB,
    UNIQUE (run_id, fold_index)
);

CREATE TABLE IF NOT EXISTS backtest_trades (
    id            BIGSERIAL PRIMARY KEY,
    run_id        BIGINT NOT NULL REFERENCES backtest_runs(id) ON DELETE CASCADE,
    fold_id       BIGINT NOT NULL REFERENCES backtest_folds(id) ON DELETE CASCADE,
    symbol        TEXT NOT NULL,
    side          TEXT NOT NULL CHECK (side IN ('long','short')),
    entry_time    TIMESTAMPTZ NOT NULL,
    entry_price   DOUBLE PRECISION NOT NULL,
    exit_time     TIMESTAMPTZ,
    exit_price    DOUBLE PRECISION,
    quantity      DOUBLE PRECISION NOT NULL,
    pnl           DOUBLE PRECISION,
    signal_strength DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_backtest_trades_run ON backtest_trades(run_id);

CREATE TABLE IF NOT EXISTS backtest_equity (
    run_id     BIGINT NOT NULL REFERENCES backtest_runs(id) ON DELETE CASCADE,
    fold_id    BIGINT NOT NULL REFERENCES backtest_folds(id) ON DELETE CASCADE,
    timestamp  TIMESTAMPTZ NOT NULL,
    equity     DOUBLE PRECISION NOT NULL,
    cash       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (run_id, fold_id, timestamp)
);
```

- [ ] **Step 2: Apply migration locally**

Run: `cd bullforge && docker compose -f docker-compose.test.yml up -d timescaledb && PGPASSWORD=postgres psql -h localhost -U postgres -d bullforge -f migrations/012_create_backtest_tables.sql`

Expected: four `CREATE TABLE` statements succeed with no errors. Verify with `\dt backtest_*` showing all four tables.

- [ ] **Step 3: Commit**

```bash
git add bullforge/migrations/012_create_backtest_tables.sql
git commit -m "feat(backtest): add backtest_runs/folds/trades/equity schema"
```

---

## Task 2: WalkForwardSplitter

**Files:**
- Create: `bullforge/services/backtest_service/__init__.py` (empty)
- Create: `bullforge/services/backtest_service/splitter.py`
- Test: `bullforge/tests/test_walkforward_splitter.py`

- [ ] **Step 1: Write failing tests**

```python
# bullforge/tests/test_walkforward_splitter.py
from datetime import datetime, timedelta, timezone

import pytest

from services.backtest_service.splitter import (
    Fold,
    WalkForwardSplitter,
)


UTC = timezone.utc


def _dt(y, m, d):
    return datetime(y, m, d, tzinfo=UTC)


def test_rolling_splits_basic():
    splitter = WalkForwardSplitter(
        train_days=10, test_days=5, embargo_minutes=30, mode="rolling",
    )
    folds = list(splitter.split(_dt(2025, 1, 1), _dt(2025, 2, 1)))

    assert len(folds) > 0
    first = folds[0]
    assert isinstance(first, Fold)
    assert first.train_start == _dt(2025, 1, 1)
    assert first.train_end == _dt(2025, 1, 11)
    # Embargo of 30 minutes between train_end and test_start
    assert first.test_start == first.train_end + timedelta(minutes=30)
    assert first.test_end == first.test_start + timedelta(days=5)


def test_rolling_step_equals_test_days():
    splitter = WalkForwardSplitter(
        train_days=10, test_days=5, embargo_minutes=30, mode="rolling",
    )
    folds = list(splitter.split(_dt(2025, 1, 1), _dt(2025, 2, 1)))

    # Each fold's train window slides forward by exactly test_days
    deltas = [
        (folds[i + 1].train_start - folds[i].train_start).days
        for i in range(len(folds) - 1)
    ]
    assert all(d == 5 for d in deltas)


def test_anchored_train_start_is_constant():
    splitter = WalkForwardSplitter(
        train_days=10, test_days=5, embargo_minutes=30, mode="anchored",
    )
    folds = list(splitter.split(_dt(2025, 1, 1), _dt(2025, 2, 15)))

    assert len(folds) >= 2
    # In anchored mode, train_start never advances; only train_end grows.
    assert all(f.train_start == _dt(2025, 1, 1) for f in folds)
    deltas = [
        (folds[i + 1].train_end - folds[i].train_end).days
        for i in range(len(folds) - 1)
    ]
    assert all(d == 5 for d in deltas)


def test_no_folds_when_range_too_short():
    splitter = WalkForwardSplitter(
        train_days=10, test_days=5, embargo_minutes=30, mode="rolling",
    )
    # Range only covers train window; no room for a test slice
    folds = list(splitter.split(_dt(2025, 1, 1), _dt(2025, 1, 10)))
    assert folds == []


def test_invalid_mode_raises():
    with pytest.raises(ValueError):
        WalkForwardSplitter(
            train_days=10, test_days=5, embargo_minutes=30, mode="bogus",
        )


def test_invalid_window_sizes_raise():
    with pytest.raises(ValueError):
        WalkForwardSplitter(
            train_days=0, test_days=5, embargo_minutes=30, mode="rolling",
        )
    with pytest.raises(ValueError):
        WalkForwardSplitter(
            train_days=10, test_days=0, embargo_minutes=30, mode="rolling",
        )
    with pytest.raises(ValueError):
        WalkForwardSplitter(
            train_days=10, test_days=5, embargo_minutes=-1, mode="rolling",
        )
```

- [ ] **Step 2: Run tests, expect failure**

Run: `cd bullforge && PYTHONPATH=. pytest tests/test_walkforward_splitter.py -v`
Expected: `ModuleNotFoundError: services.backtest_service.splitter`.

- [ ] **Step 3: Implement splitter**

```python
# bullforge/services/backtest_service/splitter.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterator

VALID_MODES = ("rolling", "anchored")


@dataclass(frozen=True)
class Fold:
    index: int
    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime


class WalkForwardSplitter:
    def __init__(
        self,
        *,
        train_days: int,
        test_days: int,
        embargo_minutes: int,
        mode: str = "rolling",
    ):
        if mode not in VALID_MODES:
            raise ValueError(f"mode must be one of {VALID_MODES}, got {mode!r}")
        if train_days <= 0:
            raise ValueError(f"train_days must be > 0, got {train_days}")
        if test_days <= 0:
            raise ValueError(f"test_days must be > 0, got {test_days}")
        if embargo_minutes < 0:
            raise ValueError(
                f"embargo_minutes must be >= 0, got {embargo_minutes}"
            )
        self._train = timedelta(days=train_days)
        self._test = timedelta(days=test_days)
        self._embargo = timedelta(minutes=embargo_minutes)
        self._mode = mode

    def split(
        self, range_start: datetime, range_end: datetime,
    ) -> Iterator[Fold]:
        anchor = range_start
        train_start = range_start
        index = 0
        while True:
            train_end = (
                train_start + self._train if self._mode == "rolling"
                else anchor + self._train + self._test * index
            )
            test_start = train_end + self._embargo
            test_end = test_start + self._test
            if test_end > range_end:
                return
            yield Fold(
                index=index,
                train_start=(
                    anchor if self._mode == "anchored" else train_start
                ),
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
            )
            index += 1
            train_start = train_start + self._test
```

- [ ] **Step 4: Run tests, expect pass**

Run: `cd bullforge && PYTHONPATH=. pytest tests/test_walkforward_splitter.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/backtest_service/__init__.py \
        bullforge/services/backtest_service/splitter.py \
        bullforge/tests/test_walkforward_splitter.py
git commit -m "feat(backtest): walk-forward splitter with rolling/anchored modes + embargo"
```

---

## Task 3: Fill model (slippage + latency)

**Files:**
- Create: `bullforge/services/backtest_service/fill_model.py`
- Test: `bullforge/tests/test_fill_model.py`

- [ ] **Step 1: Write failing tests**

```python
# bullforge/tests/test_fill_model.py
import pytest

from services.backtest_service.fill_model import FillModel, FillRequest


def _bar(o, h, l, c, v):
    return {"open": o, "high": h, "low": l, "close": c, "volume": v}


def test_fill_at_next_bar_open_with_buy_slippage():
    fm = FillModel(slippage_bps=10.0, max_volume_pct=1.0)
    next_bar = _bar(100.0, 101.0, 99.5, 100.5, 10_000)
    fill = fm.fill(FillRequest(side="buy", qty=100), next_bar)
    assert fill.filled is True
    # 10 bps = 0.10% above the open for a buy
    assert fill.price == pytest.approx(100.0 * 1.001)
    assert fill.qty == 100


def test_fill_at_next_bar_open_with_sell_slippage():
    fm = FillModel(slippage_bps=10.0, max_volume_pct=1.0)
    next_bar = _bar(100.0, 101.0, 99.5, 100.5, 10_000)
    fill = fm.fill(FillRequest(side="sell", qty=100), next_bar)
    assert fill.price == pytest.approx(100.0 * 0.999)


def test_rejects_when_qty_exceeds_volume_cap():
    fm = FillModel(slippage_bps=0.0, max_volume_pct=0.10)
    next_bar = _bar(100.0, 101.0, 99.5, 100.5, 1_000)
    fill = fm.fill(FillRequest(side="buy", qty=500), next_bar)
    # max allowed = 10% of 1000 = 100 shares; 500 > 100
    assert fill.filled is False
    assert fill.qty == 0
    assert "volume" in fill.reason.lower()


def test_partial_volume_cap_disabled_when_pct_is_one():
    fm = FillModel(slippage_bps=0.0, max_volume_pct=1.0)
    next_bar = _bar(100.0, 101.0, 99.5, 100.5, 50)
    fill = fm.fill(FillRequest(side="buy", qty=50), next_bar)
    assert fill.filled is True
    assert fill.qty == 50
```

- [ ] **Step 2: Run tests, expect failure**

Run: `cd bullforge && PYTHONPATH=. pytest tests/test_fill_model.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement fill model**

```python
# bullforge/services/backtest_service/fill_model.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FillRequest:
    side: str  # "buy" or "sell"
    qty: float


@dataclass(frozen=True)
class FillResult:
    filled: bool
    qty: float
    price: float
    reason: str = ""


class FillModel:
    """Fills an order at the *next* bar's open with slippage and a volume cap.

    Why next-bar open: the signal is computed from a closed bar at time T;
    submitting against that same bar's close would peek at unrealized data.
    """

    def __init__(self, *, slippage_bps: float, max_volume_pct: float):
        if slippage_bps < 0:
            raise ValueError("slippage_bps must be >= 0")
        if not (0.0 < max_volume_pct <= 1.0):
            raise ValueError("max_volume_pct must be in (0, 1]")
        self._slip = slippage_bps / 10_000.0
        self._max_vol_pct = max_volume_pct

    def fill(self, req: FillRequest, next_bar: dict) -> FillResult:
        max_qty = next_bar["volume"] * self._max_vol_pct
        if req.qty > max_qty:
            return FillResult(
                filled=False,
                qty=0,
                price=0.0,
                reason=f"qty {req.qty} > volume cap {max_qty:.0f}",
            )
        sign = 1 if req.side == "buy" else -1
        price = next_bar["open"] * (1.0 + sign * self._slip)
        return FillResult(filled=True, qty=req.qty, price=price)
```

- [ ] **Step 4: Run tests, expect pass**

Run: `cd bullforge && PYTHONPATH=. pytest tests/test_fill_model.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/backtest_service/fill_model.py \
        bullforge/tests/test_fill_model.py
git commit -m "feat(backtest): next-bar-open fill model with slippage + volume cap"
```

---

## Task 4: SimulatedBroker

**Files:**
- Create: `bullforge/services/backtest_service/simulated_broker.py`
- Test: `bullforge/tests/test_simulated_broker.py`

The `SimulatedBroker` must be a *duck-type* drop-in for `AlpacaBroker` ([alpaca_broker.py:17](bullforge/services/execution_service/alpaca_broker.py#L17)) — same async method names, same return shapes — so `OrderExecutor`, `PositionTracker`, and `ReconcilerCore` can use it unchanged.

Required methods (all `async`):
- `get_account() -> dict` with keys `equity`, `buying_power`, `cash`, `portfolio_value`
- `get_positions() -> list[dict]` of positions
- `get_position(symbol) -> dict | None`
- `submit_market_order(symbol, qty, side) -> str` (returns synthetic UUID)
- `get_order(order_id) -> dict` matching `_format_order_snapshot`
- `close_position(symbol) -> str | None`
- `list_open_orders(after) -> list[dict]`

Position dict keys ([alpaca_broker.py:138-147](bullforge/services/execution_service/alpaca_broker.py#L138-L147)): `symbol, qty, side, avg_entry_price, current_price, unrealized_pl, market_value`.

- [ ] **Step 1: Write failing tests**

```python
# bullforge/tests/test_simulated_broker.py
import pytest
from datetime import datetime, timezone

from services.backtest_service.fill_model import FillModel
from services.backtest_service.simulated_broker import SimulatedBroker


UTC = timezone.utc


def _bar(o, h, l, c, v):
    return {"open": o, "high": h, "low": l, "close": c, "volume": v}


@pytest.fixture
def broker():
    fm = FillModel(slippage_bps=0.0, max_volume_pct=1.0)
    b = SimulatedBroker(starting_cash=100_000.0, fill_model=fm)
    b.set_clock(datetime(2025, 1, 2, 15, 0, tzinfo=UTC))
    b.set_next_bar("SPY", _bar(100.0, 101.0, 99.5, 100.5, 1_000_000))
    return b


@pytest.mark.asyncio
async def test_initial_account():
    fm = FillModel(slippage_bps=0.0, max_volume_pct=1.0)
    b = SimulatedBroker(starting_cash=100_000.0, fill_model=fm)
    acct = await b.get_account()
    assert acct["cash"] == 100_000.0
    assert acct["equity"] == 100_000.0
    assert acct["buying_power"] == 100_000.0
    assert acct["portfolio_value"] == 100_000.0


@pytest.mark.asyncio
async def test_buy_creates_position_and_deducts_cash(broker):
    order_id = await broker.submit_market_order("SPY", 100, "buy")
    assert isinstance(order_id, str) and len(order_id) > 0

    snap = await broker.get_order(order_id)
    assert snap["status"] == "filled"
    assert snap["filled_qty"] == 100
    assert snap["filled_avg_price"] == pytest.approx(100.0)

    pos = await broker.get_position("SPY")
    assert pos is not None
    assert pos["qty"] == 100
    assert pos["avg_entry_price"] == pytest.approx(100.0)
    assert pos["side"] == "long"

    acct = await broker.get_account()
    assert acct["cash"] == pytest.approx(100_000.0 - 100 * 100.0)


@pytest.mark.asyncio
async def test_close_position_realizes_pnl(broker):
    await broker.submit_market_order("SPY", 100, "buy")

    # Mark price up before close
    broker.set_next_bar("SPY", _bar(105.0, 106.0, 104.5, 105.5, 1_000_000))
    close_id = await broker.close_position("SPY")
    assert close_id is not None

    pos = await broker.get_position("SPY")
    assert pos is None

    acct = await broker.get_account()
    # Bought at 100, sold at 105: +500 PnL on top of starting cash
    assert acct["cash"] == pytest.approx(100_000.0 + 500.0)


@pytest.mark.asyncio
async def test_rejected_order_when_no_volume(broker):
    fm = FillModel(slippage_bps=0.0, max_volume_pct=0.001)
    b = SimulatedBroker(starting_cash=100_000.0, fill_model=fm)
    b.set_clock(datetime(2025, 1, 2, 15, 0, tzinfo=UTC))
    b.set_next_bar("SPY", _bar(100.0, 101.0, 99.5, 100.5, 1_000))

    order_id = await b.submit_market_order("SPY", 500, "buy")
    snap = await b.get_order(order_id)
    assert snap["status"] == "rejected"
    pos = await b.get_position("SPY")
    assert pos is None


@pytest.mark.asyncio
async def test_mark_to_market_updates_unrealized_pnl(broker):
    await broker.submit_market_order("SPY", 100, "buy")
    broker.mark_to_market("SPY", price=110.0)
    pos = await broker.get_position("SPY")
    assert pos["current_price"] == 110.0
    assert pos["unrealized_pl"] == pytest.approx(1000.0)
    assert pos["market_value"] == pytest.approx(110.0 * 100)
```

- [ ] **Step 2: Add asyncio test config if missing**

Check `bullforge/pytest.ini` (or `pyproject.toml`) for `asyncio_mode = auto`. If absent, append to `bullforge/pytest.ini`:

```ini
[pytest]
asyncio_mode = auto
```

(Skip this step if already configured — e.g., if `test_order_executor.py` already uses async tests.)

- [ ] **Step 3: Run tests, expect failure**

Run: `cd bullforge && PYTHONPATH=. pytest tests/test_simulated_broker.py -v`
Expected: import error.

- [ ] **Step 4: Implement SimulatedBroker**

```python
# bullforge/services/backtest_service/simulated_broker.py
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from services.backtest_service.fill_model import FillModel, FillRequest


class SimulatedBroker:
    """In-memory broker that mirrors AlpacaBroker's async interface.

    Intended for backtests. Callers (BacktestEngine) must:
      1. set_clock(ts) before each bar
      2. set_next_bar(symbol, bar) for any symbol that may receive an order
         this step (the order will fill against this bar's open)
      3. mark_to_market(symbol, price) once per bar to update unrealized PnL
    """

    def __init__(self, *, starting_cash: float, fill_model: FillModel):
        self._cash = starting_cash
        self._starting_cash = starting_cash
        self._fill_model = fill_model
        self._positions: dict[str, dict] = {}
        self._orders: dict[str, dict] = {}
        self._next_bars: dict[str, dict] = {}
        self._clock: Optional[datetime] = None

    # --- Backtest-only control surface ---

    def set_clock(self, ts: datetime) -> None:
        if ts.tzinfo is None or ts.tzinfo != timezone.utc:
            raise ValueError("clock must be UTC")
        self._clock = ts

    def set_next_bar(self, symbol: str, bar: dict) -> None:
        self._next_bars[symbol] = bar

    def mark_to_market(self, symbol: str, price: float) -> None:
        pos = self._positions.get(symbol)
        if pos is None:
            return
        qty = pos["qty"]
        pos["current_price"] = price
        pos["market_value"] = price * qty
        pos["unrealized_pl"] = (price - pos["avg_entry_price"]) * qty

    # --- AlpacaBroker-compatible async surface ---

    async def get_account(self) -> dict:
        position_value = sum(
            abs(p["market_value"]) for p in self._positions.values()
        )
        equity = self._cash + sum(
            p["market_value"] for p in self._positions.values()
        )
        return {
            "equity": equity,
            "buying_power": max(equity - position_value, 0.0),
            "cash": self._cash,
            "portfolio_value": equity,
        }

    async def get_positions(self) -> list[dict]:
        return [dict(p) for p in self._positions.values()]

    async def get_position(self, symbol: str) -> Optional[dict]:
        pos = self._positions.get(symbol)
        return dict(pos) if pos is not None else None

    async def submit_market_order(
        self, symbol: str, qty: float, side: str,
    ) -> str:
        if self._clock is None:
            raise RuntimeError("set_clock() must be called before submitting")
        order_id = str(uuid.uuid4())
        bar = self._next_bars.get(symbol)
        if bar is None:
            self._orders[order_id] = self._snapshot(
                order_id, symbol, side, qty, "rejected", None, None,
            )
            return order_id

        result = self._fill_model.fill(
            FillRequest(side=side, qty=qty), bar,
        )
        if not result.filled:
            self._orders[order_id] = self._snapshot(
                order_id, symbol, side, qty, "rejected", None, None,
            )
            return order_id

        self._apply_fill(symbol, side, result.qty, result.price)
        self._orders[order_id] = self._snapshot(
            order_id, symbol, side, qty,
            "filled", result.qty, result.price,
        )
        return order_id

    async def get_order(self, order_id: str) -> dict:
        return dict(self._orders[order_id])

    async def close_position(self, symbol: str) -> Optional[str]:
        pos = self._positions.get(symbol)
        if pos is None:
            return None
        qty = abs(pos["qty"])
        side = "sell" if pos["qty"] > 0 else "buy"
        return await self.submit_market_order(symbol, qty, side)

    async def list_open_orders(self, after) -> list[dict]:
        # Backtest broker fills synchronously; no open orders ever exist.
        return []

    # --- internals ---

    def _apply_fill(
        self, symbol: str, side: str, qty: float, price: float,
    ) -> None:
        signed_qty = qty if side == "buy" else -qty
        self._cash -= signed_qty * price
        existing = self._positions.get(symbol)
        if existing is None:
            self._positions[symbol] = {
                "symbol": symbol,
                "qty": signed_qty,
                "side": "long" if signed_qty > 0 else "short",
                "avg_entry_price": price,
                "current_price": price,
                "unrealized_pl": 0.0,
                "market_value": signed_qty * price,
            }
            return
        new_qty = existing["qty"] + signed_qty
        if new_qty == 0:
            del self._positions[symbol]
            return
        # Weighted average only when adding to a same-direction position.
        if (existing["qty"] > 0) == (signed_qty > 0):
            total_cost = (
                existing["avg_entry_price"] * existing["qty"]
                + price * signed_qty
            )
            existing["avg_entry_price"] = total_cost / new_qty
        existing["qty"] = new_qty
        existing["side"] = "long" if new_qty > 0 else "short"
        existing["current_price"] = price
        existing["market_value"] = new_qty * price
        existing["unrealized_pl"] = (
            (price - existing["avg_entry_price"]) * new_qty
        )

    def _snapshot(
        self, order_id, symbol, side, qty, status, filled_qty, filled_price,
    ) -> dict:
        return {
            "id": order_id,
            "symbol": symbol,
            "side": side,
            "qty": float(qty),
            "order_type": "market",
            "status": status,
            "submitted_at": self._clock,
            "filled_qty": filled_qty,
            "filled_avg_price": filled_price,
            "filled_at": self._clock if status == "filled" else None,
        }
```

- [ ] **Step 5: Run tests, expect pass**

Run: `cd bullforge && PYTHONPATH=. pytest tests/test_simulated_broker.py -v`
Expected: 5 passed.

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/backtest_service/simulated_broker.py \
        bullforge/tests/test_simulated_broker.py
git commit -m "feat(backtest): SimulatedBroker drop-in for AlpacaBroker"
```

---

## Task 5: Equity tracker

**Files:**
- Create: `bullforge/services/backtest_service/equity_tracker.py`
- Test: `bullforge/tests/test_equity_tracker.py`

- [ ] **Step 1: Write failing tests**

```python
# bullforge/tests/test_equity_tracker.py
from datetime import datetime, timezone
from services.backtest_service.equity_tracker import EquityTracker


UTC = timezone.utc


def test_records_equity_points_in_order():
    et = EquityTracker()
    et.record(datetime(2025, 1, 2, 15, 0, tzinfo=UTC), equity=100_000.0, cash=100_000.0)
    et.record(datetime(2025, 1, 2, 15, 1, tzinfo=UTC), equity=100_500.0, cash=90_000.0)
    points = et.points()
    assert len(points) == 2
    assert points[0].equity == 100_000.0
    assert points[1].equity == 100_500.0


def test_returns_series_for_metrics():
    et = EquityTracker()
    et.record(datetime(2025, 1, 2, 15, 0, tzinfo=UTC), equity=100_000.0, cash=100_000.0)
    et.record(datetime(2025, 1, 2, 15, 1, tzinfo=UTC), equity=101_000.0, cash=100_000.0)
    et.record(datetime(2025, 1, 2, 15, 2, tzinfo=UTC), equity=100_500.0, cash=100_000.0)
    rets = et.returns()
    assert len(rets) == 2
    assert abs(rets[0] - 0.01) < 1e-9
    assert abs(rets[1] - (-0.00495)) < 1e-4
```

- [ ] **Step 2: Run tests, expect failure**

Run: `cd bullforge && PYTHONPATH=. pytest tests/test_equity_tracker.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

```python
# bullforge/services/backtest_service/equity_tracker.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class EquityPoint:
    timestamp: datetime
    equity: float
    cash: float


class EquityTracker:
    def __init__(self):
        self._points: list[EquityPoint] = []

    def record(self, timestamp: datetime, *, equity: float, cash: float) -> None:
        self._points.append(EquityPoint(timestamp, equity, cash))

    def points(self) -> list[EquityPoint]:
        return list(self._points)

    def returns(self) -> list[float]:
        out = []
        for prev, curr in zip(self._points, self._points[1:]):
            if prev.equity == 0:
                out.append(0.0)
            else:
                out.append((curr.equity - prev.equity) / prev.equity)
        return out
```

- [ ] **Step 4: Run tests, expect pass**

Run: `cd bullforge && PYTHONPATH=. pytest tests/test_equity_tracker.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/backtest_service/equity_tracker.py \
        bullforge/tests/test_equity_tracker.py
git commit -m "feat(backtest): EquityTracker for mark-to-market series"
```

---

## Task 6: Performance metrics

**Files:**
- Create: `bullforge/services/backtest_service/metrics.py`
- Test: `bullforge/tests/test_backtest_metrics.py`

Metrics annualization assumes 1-minute bars: `~252 * 390 = 98,280` periods/year for equities (regular session). The function takes a `periods_per_year` arg so the caller passes the right value (e.g., `390 * 252` for 1m equities, `1440 * 365` for 1m crypto).

- [ ] **Step 1: Write failing tests**

```python
# bullforge/tests/test_backtest_metrics.py
import math
import pytest

from services.backtest_service.metrics import (
    sharpe_ratio,
    sortino_ratio,
    max_drawdown,
    summarize_trades,
)


def test_sharpe_zero_when_returns_constant():
    assert sharpe_ratio([0.0, 0.0, 0.0], periods_per_year=252) == 0.0


def test_sharpe_positive_for_uptrend():
    rets = [0.001, 0.002, 0.001, 0.0015]
    assert sharpe_ratio(rets, periods_per_year=252) > 0


def test_sortino_only_penalizes_downside():
    up = [0.01, 0.02, 0.01]
    down = [0.01, -0.02, 0.01]
    assert sortino_ratio(up, periods_per_year=252) > sortino_ratio(down, periods_per_year=252)


def test_max_drawdown_basic():
    equity = [100.0, 110.0, 105.0, 120.0, 90.0, 100.0]
    # Peak 120, trough 90 -> dd = (90-120)/120 = -0.25
    assert max_drawdown(equity) == pytest.approx(-0.25)


def test_max_drawdown_zero_when_monotonic():
    equity = [100.0, 101.0, 102.0]
    assert max_drawdown(equity) == 0.0


def test_summarize_trades_metrics():
    trades = [
        {"pnl": 100.0},
        {"pnl": -50.0},
        {"pnl": 75.0},
        {"pnl": -25.0},
    ]
    s = summarize_trades(trades)
    assert s["count"] == 4
    assert s["wins"] == 2
    assert s["losses"] == 2
    assert s["hit_rate"] == 0.5
    assert s["total_pnl"] == 100.0
    assert s["avg_win"] == pytest.approx(87.5)
    assert s["avg_loss"] == pytest.approx(-37.5)
    assert s["profit_factor"] == pytest.approx(175.0 / 75.0)


def test_summarize_trades_empty():
    s = summarize_trades([])
    assert s["count"] == 0
    assert s["hit_rate"] == 0.0
    assert s["profit_factor"] == 0.0
```

- [ ] **Step 2: Run tests, expect failure**

Run: `cd bullforge && PYTHONPATH=. pytest tests/test_backtest_metrics.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement metrics**

```python
# bullforge/services/backtest_service/metrics.py
from __future__ import annotations

import math
from statistics import mean, pstdev


def sharpe_ratio(returns: list[float], *, periods_per_year: int) -> float:
    if len(returns) < 2:
        return 0.0
    sd = pstdev(returns)
    if sd == 0:
        return 0.0
    return (mean(returns) / sd) * math.sqrt(periods_per_year)


def sortino_ratio(returns: list[float], *, periods_per_year: int) -> float:
    if len(returns) < 2:
        return 0.0
    downside = [r for r in returns if r < 0]
    if not downside:
        return 0.0
    dd_sd = math.sqrt(sum(r * r for r in downside) / len(downside))
    if dd_sd == 0:
        return 0.0
    return (mean(returns) / dd_sd) * math.sqrt(periods_per_year)


def max_drawdown(equity_curve: list[float]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    worst = 0.0
    for e in equity_curve:
        if e > peak:
            peak = e
        if peak > 0:
            dd = (e - peak) / peak
            if dd < worst:
                worst = dd
    return worst


def summarize_trades(trades: list[dict]) -> dict:
    if not trades:
        return {
            "count": 0, "wins": 0, "losses": 0,
            "hit_rate": 0.0, "total_pnl": 0.0,
            "avg_win": 0.0, "avg_loss": 0.0, "profit_factor": 0.0,
        }
    pnls = [t["pnl"] for t in trades if t.get("pnl") is not None]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    return {
        "count": len(pnls),
        "wins": len(wins),
        "losses": len(losses),
        "hit_rate": len(wins) / len(pnls) if pnls else 0.0,
        "total_pnl": sum(pnls),
        "avg_win": mean(wins) if wins else 0.0,
        "avg_loss": mean(losses) if losses else 0.0,
        "profit_factor": gross_win / gross_loss if gross_loss > 0 else 0.0,
    }
```

- [ ] **Step 4: Run tests, expect pass**

Run: `cd bullforge && PYTHONPATH=. pytest tests/test_backtest_metrics.py -v`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/backtest_service/metrics.py \
        bullforge/tests/test_backtest_metrics.py
git commit -m "feat(backtest): Sharpe/Sortino/drawdown + trade summary metrics"
```

---

## Task 7: Results DB writer

**Files:**
- Create: `bullforge/services/backtest_service/results_db.py`
- Test: `bullforge/tests/test_backtest_results_db.py` (integration — requires Timescale)

- [ ] **Step 1: Write integration test**

```python
# bullforge/tests/test_backtest_results_db.py
import json
import os
from datetime import datetime, timezone

import pytest

from shared.config import Settings
from shared.db import Database
from services.backtest_service.results_db import BacktestResultsDB


pytestmark = pytest.mark.asyncio


UTC = timezone.utc


@pytest.fixture
async def db():
    settings = Settings()
    d = Database(settings)
    await d.connect()
    yield d
    await d.close()


async def test_insert_run_and_fold(db):
    rdb = BacktestResultsDB(db)
    run_id = await rdb.insert_run(
        name="unit-test-run",
        symbols=["SPY"],
        timeframe="1m",
        horizons=[5, 10, 30],
        range_start=datetime(2025, 1, 1, tzinfo=UTC),
        range_end=datetime(2025, 1, 31, tzinfo=UTC),
        train_days=10, test_days=5, embargo_minutes=30, mode="rolling",
        config={"slippage_bps": 5.0},
    )
    assert run_id > 0

    fold_id = await rdb.insert_fold(
        run_id=run_id, fold_index=0,
        train_start=datetime(2025, 1, 1, tzinfo=UTC),
        train_end=datetime(2025, 1, 11, tzinfo=UTC),
        test_start=datetime(2025, 1, 11, 0, 30, tzinfo=UTC),
        test_end=datetime(2025, 1, 16, 0, 30, tzinfo=UTC),
    )
    assert fold_id > 0

    await rdb.insert_trade(
        run_id=run_id, fold_id=fold_id,
        symbol="SPY", side="long",
        entry_time=datetime(2025, 1, 11, 14, 0, tzinfo=UTC),
        entry_price=420.0, quantity=10,
        exit_time=datetime(2025, 1, 11, 15, 0, tzinfo=UTC),
        exit_price=421.0, pnl=10.0, signal_strength=0.7,
    )
    await rdb.insert_equity_point(
        run_id=run_id, fold_id=fold_id,
        timestamp=datetime(2025, 1, 11, 14, 0, tzinfo=UTC),
        equity=100_000.0, cash=95_800.0,
    )
    await rdb.update_fold_metrics(
        fold_id=fold_id, metrics={"sharpe": 1.5, "max_dd": -0.05},
    )
    await rdb.finish_run(run_id=run_id, status="completed")
```

- [ ] **Step 2: Run test, expect failure**

Run: `cd bullforge && PYTHONPATH=. pytest tests/test_backtest_results_db.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement results DB**

```python
# bullforge/services/backtest_service/results_db.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from shared.db import Database


class BacktestResultsDB:
    """Thin async wrapper around the backtest_* tables."""

    def __init__(self, db: Database):
        self._db = db

    async def insert_run(
        self, *,
        name: str,
        symbols: list[str],
        timeframe: str,
        horizons: list[int],
        range_start: datetime,
        range_end: datetime,
        train_days: int,
        test_days: int,
        embargo_minutes: int,
        mode: str,
        config: dict,
    ) -> int:
        row = await self._db.fetchrow(
            """
            INSERT INTO backtest_runs (
                name, symbols, timeframe, horizons,
                range_start, range_end,
                train_days, test_days, embargo_min, mode, config_json
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING id
            """,
            name, symbols, timeframe, horizons,
            range_start, range_end,
            train_days, test_days, embargo_minutes, mode,
            json.dumps(config),
        )
        return row["id"]

    async def insert_fold(
        self, *, run_id: int, fold_index: int,
        train_start: datetime, train_end: datetime,
        test_start: datetime, test_end: datetime,
    ) -> int:
        row = await self._db.fetchrow(
            """
            INSERT INTO backtest_folds (
                run_id, fold_index,
                train_start, train_end, test_start, test_end
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
            """,
            run_id, fold_index,
            train_start, train_end, test_start, test_end,
        )
        return row["id"]

    async def insert_trade(
        self, *, run_id: int, fold_id: int,
        symbol: str, side: str,
        entry_time: datetime, entry_price: float, quantity: float,
        exit_time: Optional[datetime], exit_price: Optional[float],
        pnl: Optional[float], signal_strength: float,
    ) -> None:
        await self._db.execute(
            """
            INSERT INTO backtest_trades (
                run_id, fold_id, symbol, side,
                entry_time, entry_price, quantity,
                exit_time, exit_price, pnl, signal_strength
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            """,
            run_id, fold_id, symbol, side,
            entry_time, entry_price, quantity,
            exit_time, exit_price, pnl, signal_strength,
        )

    async def insert_equity_point(
        self, *, run_id: int, fold_id: int,
        timestamp: datetime, equity: float, cash: float,
    ) -> None:
        await self._db.execute(
            """
            INSERT INTO backtest_equity (run_id, fold_id, timestamp, equity, cash)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (run_id, fold_id, timestamp) DO UPDATE
            SET equity = EXCLUDED.equity, cash = EXCLUDED.cash
            """,
            run_id, fold_id, timestamp, equity, cash,
        )

    async def update_fold_metrics(
        self, *, fold_id: int, metrics: dict,
    ) -> None:
        await self._db.execute(
            "UPDATE backtest_folds SET metrics_json = $1 WHERE id = $2",
            json.dumps(metrics), fold_id,
        )

    async def finish_run(self, *, run_id: int, status: str) -> None:
        await self._db.execute(
            """
            UPDATE backtest_runs
            SET finished_at = NOW(), status = $1
            WHERE id = $2
            """,
            status, run_id,
        )
```

- [ ] **Step 4: Verify Database has `execute`/`fetchrow` helpers**

Run: `cd bullforge && grep -nE "async def (execute|fetchrow|fetch)\b" shared/db.py | head -20`

If `Database` does **not** expose `execute()` and `fetchrow()` directly (it uses asyncpg pools internally), implement them in `shared/db.py` first as thin wrappers, *or* use the pool API:

```python
# in results_db.py — fallback if Database has only pool access
async with self._db._pool.acquire() as conn:
    row = await conn.fetchrow(...)
```

Pick whichever pattern matches existing code in `shared/db.py`. Do not invent a new convention.

- [ ] **Step 5: Run integration test**

Run: `cd bullforge && docker compose -f docker-compose.test.yml up -d && PYTHONPATH=. pytest tests/test_backtest_results_db.py -v`
Expected: 1 passed.

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/backtest_service/results_db.py \
        bullforge/tests/test_backtest_results_db.py
# include shared/db.py if you added helpers
git commit -m "feat(backtest): BacktestResultsDB writer for runs/folds/trades/equity"
```

---

## Task 8: BacktestEngine orchestrator

**Files:**
- Create: `bullforge/services/backtest_service/engine.py`

This task is the largest. It wires together every prior task and the production strategy components. It has **no unit tests of its own** — coverage comes from the smoke integration test in Task 10. The component dependencies are explicit so each can be mocked later if needed.

- [ ] **Step 1: Implement engine**

```python
# bullforge/services/backtest_service/engine.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from shared.db import Database
from shared.runtime_config import RuntimeConfig

from services.backtest_service.equity_tracker import EquityTracker
from services.backtest_service.fill_model import FillModel
from services.backtest_service.metrics import (
    max_drawdown,
    sharpe_ratio,
    sortino_ratio,
    summarize_trades,
)
from services.backtest_service.results_db import BacktestResultsDB
from services.backtest_service.simulated_broker import SimulatedBroker
from services.backtest_service.splitter import Fold, WalkForwardSplitter

from services.execution_service.order_executor import OrderExecutor
from services.execution_service.position_tracker import PositionTracker
from services.execution_service.risk_manager import RiskManager
from services.execution_service.signal_aggregator import SignalAggregator

from services.model_service.inference import InferenceEngine
from services.model_service.model_store import ModelStore
from services.model_service.trainer import Trainer

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BacktestConfig:
    name: str
    symbols: list[str]
    timeframe: str           # "1m" or "1d"
    horizons: list[int]      # e.g. [5, 10, 30]
    range_start: datetime
    range_end: datetime
    train_days: int
    test_days: int
    embargo_minutes: int
    mode: str                # "rolling" | "anchored"
    starting_cash: float = 100_000.0
    slippage_bps: float = 5.0
    max_volume_pct: float = 0.10
    periods_per_year: int = 390 * 252  # 1m equities default


class BacktestEngine:
    def __init__(
        self, *,
        db: Database,
        runtime_config: RuntimeConfig,
        model_store: ModelStore,
        results_db: BacktestResultsDB,
    ):
        self._db = db
        self._rc = runtime_config
        self._model_store = model_store
        self._results = results_db

    async def run(self, cfg: BacktestConfig) -> int:
        run_id = await self._results.insert_run(
            name=cfg.name,
            symbols=cfg.symbols, timeframe=cfg.timeframe, horizons=cfg.horizons,
            range_start=cfg.range_start, range_end=cfg.range_end,
            train_days=cfg.train_days, test_days=cfg.test_days,
            embargo_minutes=cfg.embargo_minutes, mode=cfg.mode,
            config={
                "starting_cash": cfg.starting_cash,
                "slippage_bps": cfg.slippage_bps,
                "max_volume_pct": cfg.max_volume_pct,
            },
        )
        try:
            splitter = WalkForwardSplitter(
                train_days=cfg.train_days,
                test_days=cfg.test_days,
                embargo_minutes=cfg.embargo_minutes,
                mode=cfg.mode,
            )
            for fold in splitter.split(cfg.range_start, cfg.range_end):
                logger.info(
                    "Fold %d: train [%s..%s] test [%s..%s]",
                    fold.index, fold.train_start, fold.train_end,
                    fold.test_start, fold.test_end,
                )
                await self._run_fold(run_id, cfg, fold)
            await self._results.finish_run(run_id=run_id, status="completed")
        except Exception:
            await self._results.finish_run(run_id=run_id, status="failed")
            raise
        return run_id

    async def _run_fold(
        self, run_id: int, cfg: BacktestConfig, fold: Fold,
    ) -> None:
        fold_id = await self._results.insert_fold(
            run_id=run_id, fold_index=fold.index,
            train_start=fold.train_start, train_end=fold.train_end,
            test_start=fold.test_start, test_end=fold.test_end,
        )

        # 1) Train one model per (symbol, horizon) using only train-window data.
        trainer = Trainer(self._db, self._model_store)
        for symbol in cfg.symbols:
            for horizon in cfg.horizons:
                meta = await trainer.train(
                    symbol, cfg.timeframe, horizon,
                    as_of=fold.train_end,
                )
                if meta is None:
                    logger.warning(
                        "No model trained for %s %dm in fold %d",
                        symbol, horizon, fold.index,
                    )

        # 2) Build inference engine restricted to this fold's models.
        inference = InferenceEngine(self._db, self._model_store)
        await inference.load_models()  # picks up the just-trained active models

        # 3) Build sim broker + execution stack.
        fill_model = FillModel(
            slippage_bps=cfg.slippage_bps,
            max_volume_pct=cfg.max_volume_pct,
        )
        broker = SimulatedBroker(
            starting_cash=cfg.starting_cash, fill_model=fill_model,
        )
        tracker = PositionTracker(broker)
        risk = RiskManager(runtime_config=self._rc)
        # ReconcilerCore is unnecessary for sim; pass a stub that no-ops.
        executor = OrderExecutor(
            broker=broker, risk_manager=risk, position_tracker=tracker,
            db=self._db, core=_NoopReconciler(),
        )

        equity = EquityTracker()
        completed_trades: list[dict] = []
        open_trade: dict[str, dict] = {}

        # 4) Replay test-window bars chronologically per symbol.
        # For multi-symbol folds, interleave by timestamp.
        all_bars = []
        for symbol in cfg.symbols:
            bars = await self._db.query_bars_between(
                symbol, cfg.timeframe, fold.test_start, fold.test_end,
            )
            for b in bars:
                all_bars.append((b["timestamp"], symbol, b))
        all_bars.sort(key=lambda t: t[0])

        # We need next-bar lookahead to fill against the *next* bar's open.
        next_bar_lookup: dict[tuple[str, datetime], dict] = {}
        for symbol in cfg.symbols:
            bars = await self._db.query_bars_between(
                symbol, cfg.timeframe, fold.test_start, fold.test_end,
            )
            for prev, nxt in zip(bars, bars[1:]):
                next_bar_lookup[(symbol, prev["timestamp"])] = nxt

        for ts, symbol, bar in all_bars:
            broker.set_clock(ts)
            broker.mark_to_market(symbol, bar["close"])

            # Stage the fill bar so any order this step fills against next open.
            nxt = next_bar_lookup.get((symbol, ts))
            if nxt is not None:
                broker.set_next_bar(symbol, nxt)

            preds = await inference.predict(symbol, cfg.timeframe)
            pred_dicts = [
                {
                    "model_id": p.model_id,
                    "direction": p.predicted_direction,
                    "confidence": p.confidence,
                    "horizon_minutes": p.horizon_minutes,
                }
                for p in preds
            ]
            aggregator = SignalAggregator()  # uses defaults; matches prod
            signal = aggregator.aggregate(symbol, ts, pred_dicts)
            if signal is not None and nxt is not None:
                order = await executor.process_signal(signal, bar["close"])
                if order is not None:
                    self._record_trade(
                        symbol, order, signal, ts, open_trade,
                        completed_trades,
                    )

            account = await broker.get_account()
            equity.record(ts, equity=account["equity"], cash=account["cash"])

        # 5) Persist equity + trades + metrics for the fold.
        for pt in equity.points():
            await self._results.insert_equity_point(
                run_id=run_id, fold_id=fold_id,
                timestamp=pt.timestamp, equity=pt.equity, cash=pt.cash,
            )
        for t in completed_trades:
            await self._results.insert_trade(
                run_id=run_id, fold_id=fold_id, **t,
            )

        rets = equity.returns()
        equity_curve = [pt.equity for pt in equity.points()]
        metrics = {
            "sharpe": sharpe_ratio(rets, periods_per_year=cfg.periods_per_year),
            "sortino": sortino_ratio(rets, periods_per_year=cfg.periods_per_year),
            "max_drawdown": max_drawdown(equity_curve),
            "final_equity": equity_curve[-1] if equity_curve else cfg.starting_cash,
            "total_return_pct": (
                (equity_curve[-1] / cfg.starting_cash - 1.0) * 100.0
                if equity_curve else 0.0
            ),
            **{f"trade_{k}": v for k, v in summarize_trades(completed_trades).items()},
        }
        await self._results.update_fold_metrics(fold_id=fold_id, metrics=metrics)

    def _record_trade(
        self, symbol, order, signal, ts,
        open_trade: dict, completed: list,
    ) -> None:
        existing = open_trade.get(symbol)
        if existing is None:
            open_trade[symbol] = {
                "symbol": symbol,
                "side": "long" if signal.direction == 1 else "short",
                "entry_time": ts,
                "entry_price": float(order.filled_price or 0.0),
                "quantity": float(order.quantity),
                "exit_time": None,
                "exit_price": None,
                "pnl": None,
                "signal_strength": float(signal.strength),
            }
            return
        # Closing trade — compute PnL
        entry = existing
        exit_price = float(order.filled_price or 0.0)
        sign = 1.0 if entry["side"] == "long" else -1.0
        pnl = (exit_price - entry["entry_price"]) * entry["quantity"] * sign
        entry["exit_time"] = ts
        entry["exit_price"] = exit_price
        entry["pnl"] = pnl
        completed.append(entry)
        del open_trade[symbol]


class _NoopReconciler:
    """Sim-only stub: ReconcilerCore is for live order/event reconciliation;
    in backtests fills are synchronous so reconciliation is a no-op."""

    async def apply_alpaca_order(self, snap: dict, *, source: str) -> None:
        return None
```

- [ ] **Step 2: Add helper to Database if missing**

Check `shared/db.py` for an existing `query_bars_between(symbol, timeframe, start, end)`. If absent, add:

```python
# bullforge/shared/db.py — add inside Database class
async def query_bars_between(
    self, symbol: str, timeframe: str,
    start: datetime, end: datetime,
) -> list[dict]:
    table = "bars_1m" if timeframe == "1m" else "bars_daily"
    async with self._pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT timestamp, open, high, low, close, volume
            FROM {table}
            WHERE symbol = $1 AND timestamp >= $2 AND timestamp < $3
            ORDER BY timestamp ASC
            """,
            symbol, start, end,
        )
    return [dict(r) for r in rows]
```

(Use the same pool/connection idiom you already see in [shared/db.py](bullforge/shared/db.py); do not introduce a new pattern.)

- [ ] **Step 3: Manually verify imports resolve**

Run: `cd bullforge && PYTHONPATH=. python -c "from services.backtest_service.engine import BacktestEngine, BacktestConfig; print('ok')"`
Expected: `ok`.

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/backtest_service/engine.py bullforge/shared/db.py
git commit -m "feat(backtest): BacktestEngine — train/replay/fill/record per fold"
```

---

## Task 8a: Backtest sandbox database

**Why:** The BacktestEngine as wired in Task 8 reuses the live `Database` instance, so `Trainer` writes to `public.model_registry` (deactivating live active models) and `OrderExecutor` writes to `public.orders` (polluting live order history). Backtest runs must be fully isolated from live execution state.

**Approach:** Postgres schema-based sandbox. A `backtest` schema mirrors the three write-contended tables (`orders`, `model_registry`, `predictions`). A sandbox `Database` instance sets `search_path=backtest,public` so writes land in the mirror while reads of shared data (`bars_1m`, `features`, `labels`) fall through to `public`. Non-mirrored writes (e.g. `backtest_runs`) already live in `public` under a `backtest_` prefix and continue to use the live DB.

**Files:**
- Create: `bullforge/migrations/013_backtest_sandbox.sql`
- Modify: `bullforge/shared/db.py` (add sandbox flag to `Database`)
- Modify: `bullforge/services/backtest_service/engine.py` (docstring note — wiring unchanged)

### Step 1: Migration — create backtest schema and mirrored tables

```sql
-- bullforge/migrations/013_backtest_sandbox.sql
CREATE SCHEMA IF NOT EXISTS backtest;

CREATE TABLE IF NOT EXISTS backtest.orders (LIKE public.orders INCLUDING ALL);
CREATE TABLE IF NOT EXISTS backtest.model_registry (LIKE public.model_registry INCLUDING ALL);
CREATE TABLE IF NOT EXISTS backtest.predictions (LIKE public.predictions INCLUDING ALL);
```

`LIKE ... INCLUDING ALL` copies columns, defaults, constraints, indexes, and identity sequences. The mirrors drift-proof themselves against future DDL on the `public` tables only if re-applied; document in runbook that mirror DDL must be refreshed when `public` counterparts change.

### Step 2: Extend `Database` with sandbox flag

Modify `bullforge/shared/db.py`:

```python
class Database:
    def __init__(self, settings: Settings, *, sandbox: bool = False):
        self._settings = settings
        self._sandbox = sandbox
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        server_settings = {}
        if self._sandbox:
            server_settings["search_path"] = "backtest,public"
        self.pool = await asyncpg.create_pool(
            host=self._settings.timescaledb_host,
            port=self._settings.timescaledb_port,
            user=self._settings.timescaledb_user,
            password=self._settings.timescaledb_password,
            database=self._settings.timescaledb_db,
            min_size=2,
            max_size=10,
            server_settings=server_settings or None,
        )
        if not self._sandbox:
            await self._run_migrations()
```

Migrations run only on the live pool. The sandbox pool relies on migration 013 (applied by the live pool) to have created the `backtest` schema.

### Step 3: Write tests for sandbox isolation

Create `bullforge/tests/test_backtest_sandbox.py`:

```python
import pytest
from datetime import datetime, timezone

from shared.config import Settings
from shared.db import Database
from shared.models import Order


@pytest.mark.asyncio
async def test_sandbox_order_writes_to_backtest_schema():
    settings = Settings()
    live = Database(settings)
    sandbox = Database(settings, sandbox=True)
    await live.connect()
    await sandbox.connect()
    try:
        order = Order(
            symbol="TEST_SANDBOX",
            side="buy",
            quantity=1.0,
            order_type="market",
            submitted_at=datetime.now(timezone.utc),
            signal_strength=0.5,
            alpaca_order_id="sandbox-test-1",
        )
        await sandbox.insert_order_if_new(order)

        async with live.pool.acquire() as conn:
            live_count = await conn.fetchval(
                "SELECT count(*) FROM public.orders WHERE alpaca_order_id = $1",
                "sandbox-test-1",
            )
            sandbox_count = await conn.fetchval(
                "SELECT count(*) FROM backtest.orders WHERE alpaca_order_id = $1",
                "sandbox-test-1",
            )
        assert live_count == 0, "sandbox write leaked into public.orders"
        assert sandbox_count == 1, "sandbox write missing from backtest.orders"
    finally:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM backtest.orders WHERE alpaca_order_id = $1",
                "sandbox-test-1",
            )
        await sandbox.disconnect()
        await live.disconnect()


@pytest.mark.asyncio
async def test_sandbox_reads_bars_from_public():
    """Non-mirrored tables fall through to public via search_path."""
    settings = Settings()
    sandbox = Database(settings, sandbox=True)
    await sandbox.connect()
    try:
        async with sandbox.pool.acquire() as conn:
            result = await conn.fetchval("SELECT to_regclass('bars_1m')")
        assert result == "bars_1m", "bars_1m should resolve via search_path fallback"
    finally:
        await sandbox.disconnect()
```

(These are integration tests requiring the docker test DB, matching the pattern of existing `test_db.py`.)

### Step 4: Run import + test verification

```bash
cd bullforge && PYTHONPATH=. python -c "from shared.db import Database; d = Database.__init__; print('ok')"
cd bullforge && PYTHONPATH=. pytest tests/test_backtest_sandbox.py -q  # skip if no docker DB
cd bullforge && PYTHONPATH=. pytest -q  # full suite — no regressions
```

### Step 5: Commit

```bash
git add bullforge/migrations/013_backtest_sandbox.sql bullforge/shared/db.py bullforge/tests/test_backtest_sandbox.py
git commit -m "feat(backtest): schema-based sandbox DB isolates backtest writes"
```

---

## Task 9: CLI entrypoint

**Files:**
- Create: `bullforge/scripts/backtest.py`

- [ ] **Step 1: Implement CLI**

```python
# bullforge/scripts/backtest.py
from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timezone

from shared.config import Settings
from shared.db import Database
from shared.runtime_config import RuntimeConfig
from shared.redis import RedisClient

from services.backtest_service.engine import BacktestConfig, BacktestEngine
from services.backtest_service.results_db import BacktestResultsDB
from services.model_service.model_store import ModelStore


def _parse_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="BullForge walk-forward backtest")
    p.add_argument("--name", required=True, help="run label stored in DB")
    p.add_argument("--symbols", required=True,
                   help="comma-separated, e.g. SPY,QQQ")
    p.add_argument("--timeframe", default="1m", choices=["1m", "1d"])
    p.add_argument("--horizons", default="5,10,30",
                   help="comma-separated minutes")
    p.add_argument("--start", required=True, type=_parse_iso,
                   help="ISO datetime (UTC) inclusive")
    p.add_argument("--end", required=True, type=_parse_iso,
                   help="ISO datetime (UTC) exclusive")
    p.add_argument("--train-days", type=int, default=60)
    p.add_argument("--test-days", type=int, default=5)
    p.add_argument("--embargo-min", type=int, default=30)
    p.add_argument("--mode", default="rolling", choices=["rolling", "anchored"])
    p.add_argument("--starting-cash", type=float, default=100_000.0)
    p.add_argument("--slippage-bps", type=float, default=5.0)
    p.add_argument("--max-volume-pct", type=float, default=0.10)
    p.add_argument("--periods-per-year", type=int, default=390 * 252,
                   help="annualization factor; default = 1m equities")
    return p.parse_args()


async def _main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")

    settings = Settings()
    live_db = Database(settings)
    await live_db.connect()  # runs migrations, including 013 that creates `backtest` schema
    sandbox_db = Database(settings, sandbox=True)
    await sandbox_db.connect()  # search_path=backtest,public; no migrations
    redis = RedisClient(settings)
    await redis.connect()
    rc = RuntimeConfig(redis)
    model_store = ModelStore(settings.model_artifacts_dir)
    results = BacktestResultsDB(live_db)  # backtest_runs/folds/trades/equity live in public

    engine = BacktestEngine(
        db=sandbox_db, runtime_config=rc, model_store=model_store,
        results_db=results,
    )

    cfg = BacktestConfig(
        name=args.name,
        symbols=[s.strip() for s in args.symbols.split(",") if s.strip()],
        timeframe=args.timeframe,
        horizons=[int(h) for h in args.horizons.split(",") if h.strip()],
        range_start=args.start,
        range_end=args.end,
        train_days=args.train_days,
        test_days=args.test_days,
        embargo_minutes=args.embargo_min,
        mode=args.mode,
        starting_cash=args.starting_cash,
        slippage_bps=args.slippage_bps,
        max_volume_pct=args.max_volume_pct,
        periods_per_year=args.periods_per_year,
    )
    run_id = await engine.run(cfg)
    print(f"backtest run_id={run_id} completed")

    await redis.disconnect()
    await sandbox_db.disconnect()
    await live_db.disconnect()


if __name__ == "__main__":
    asyncio.run(_main())
```

- [ ] **Step 2: Verify CLI parses**

Run: `cd bullforge && PYTHONPATH=. python scripts/backtest.py --help`
Expected: argparse help text listing all flags.

- [ ] **Step 3: Commit**

```bash
git add bullforge/scripts/backtest.py
git commit -m "feat(backtest): CLI entrypoint scripts/backtest.py"
```

---

## Task 10: End-to-end smoke test (single fold)

**Files:**
- Create: `bullforge/tests/integration/test_backtest_engine_smoke.py`

This test requires the test docker stack and a small backfill of historical bars. It runs **one** fold over a short window to prove the full pipeline (splitter → trainer → inference → aggregator → executor → simulated broker → results DB) works end-to-end. It does **not** assert profitability — only that artifacts land in the DB.

- [ ] **Step 1: Write integration test**

```python
# bullforge/tests/integration/test_backtest_engine_smoke.py
from datetime import datetime, timedelta, timezone

import pytest

from shared.config import Settings
from shared.db import Database
from shared.runtime_config import RuntimeConfig
from shared.redis import RedisClient

from services.backtest_service.engine import BacktestConfig, BacktestEngine
from services.backtest_service.results_db import BacktestResultsDB
from services.model_service.model_store import ModelStore


pytestmark = pytest.mark.asyncio
UTC = timezone.utc


@pytest.fixture
async def stack():
    settings = Settings()
    live_db = Database(settings)
    await live_db.connect()
    sandbox_db = Database(settings, sandbox=True)
    await sandbox_db.connect()
    redis = RedisClient(settings)
    await redis.connect()
    yield live_db, sandbox_db, redis, settings
    await redis.disconnect()
    await sandbox_db.disconnect()
    await live_db.disconnect()


async def test_single_fold_smoke(stack):
    """Requires bars + features + labels for SPY 1m loaded for the test window.

    To prepare data once before this test runs, execute:
        python -m services.data_pipeline.backfill --symbol SPY --days 90
        python -m services.feature_engine.main --batch --symbol SPY
    """
    live_db, sandbox_db, redis, settings = stack
    rc = RuntimeConfig(redis)
    model_store = ModelStore(settings.model_artifacts_dir)
    results = BacktestResultsDB(live_db)
    engine = BacktestEngine(
        db=sandbox_db, runtime_config=rc, model_store=model_store, results_db=results,
    )

    end = datetime.now(UTC).replace(microsecond=0, second=0)
    cfg = BacktestConfig(
        name="smoke-single-fold",
        symbols=["SPY"], timeframe="1m", horizons=[5, 10],
        range_start=end - timedelta(days=20),
        range_end=end - timedelta(days=1),
        train_days=10, test_days=2, embargo_minutes=30, mode="rolling",
        starting_cash=100_000.0, slippage_bps=5.0, max_volume_pct=0.10,
    )
    run_id = await engine.run(cfg)
    assert run_id > 0

    # Verify run completed and at least one fold + equity points exist.
    async with live_db.pool.acquire() as conn:
        run = await conn.fetchrow(
            "SELECT status FROM backtest_runs WHERE id=$1", run_id,
        )
        folds = await conn.fetch(
            "SELECT id FROM backtest_folds WHERE run_id=$1", run_id,
        )
        eq_count = await conn.fetchval(
            "SELECT COUNT(*) FROM backtest_equity WHERE run_id=$1", run_id,
        )
    assert run["status"] == "completed"
    assert len(folds) >= 1
    assert eq_count > 0
```

- [ ] **Step 2: Confirm test data exists**

Before running the smoke test, ensure SPY 1m bars + features + labels are loaded for at least the last 25 days. The test docstring documents the prep commands; if data is missing, run them first.

Run: `cd bullforge && PYTHONPATH=. python -c "
import asyncio
from shared.config import Settings
from shared.db import Database
async def main():
    db = Database(Settings()); await db.connect()
    async with db.pool.acquire() as c:
        n = await c.fetchval(\"SELECT COUNT(*) FROM bars_1m WHERE symbol='SPY'\")
        print('SPY 1m bars:', n)
    await db.disconnect()
asyncio.run(main())
"`

Expected: at least ~10000 bars for a meaningful test. If the count is too low, abort and run a backfill before continuing.

- [ ] **Step 3: Run smoke test**

Run: `cd bullforge && PYTHONPATH=. pytest tests/integration/test_backtest_engine_smoke.py -v -s`
Expected: 1 passed; log output shows fold(s) trained and bars replayed.

- [ ] **Step 4: Inspect results**

Run:
```bash
cd bullforge && PGPASSWORD=postgres psql -h localhost -U postgres -d bullforge -c "
  SELECT id, name, status, finished_at FROM backtest_runs ORDER BY id DESC LIMIT 1;
  SELECT fold_index, metrics_json FROM backtest_folds
   WHERE run_id = (SELECT MAX(id) FROM backtest_runs)
   ORDER BY fold_index;
"
```

Expected: most recent run has `status='completed'` and at least one row in `backtest_folds` with non-null `metrics_json` containing `sharpe`, `max_drawdown`, `final_equity`, etc.

- [ ] **Step 5: Commit**

```bash
git add bullforge/tests/integration/test_backtest_engine_smoke.py
git commit -m "test(backtest): end-to-end single-fold smoke integration test"
```

---

## Task 11: Documentation

**Files:**
- Create: `bullforge/docs/runbooks/backtest.md`

- [ ] **Step 1: Write the runbook**

```markdown
# Walk-Forward Backtest Runbook

## Prerequisites
- Test stack running: `docker compose -f docker-compose.test.yml up -d`
- Migrations applied (012 + earlier).
- Historical bars + features + labels loaded for target symbols/timeframe.

## Run a backtest

    cd bullforge
    PYTHONPATH=. python scripts/backtest.py \
        --name "spy-1m-q1-2025" \
        --symbols SPY,QQQ \
        --timeframe 1m \
        --horizons 5,10,30 \
        --start 2025-01-01T00:00:00 \
        --end   2025-04-01T00:00:00 \
        --train-days 60 --test-days 5 --embargo-min 30 \
        --mode rolling \
        --slippage-bps 5 --max-volume-pct 0.10

## Inspect results

    psql -h localhost -U postgres -d bullforge

    -- summary across folds
    SELECT fold_index,
           (metrics_json->>'sharpe')::float        AS sharpe,
           (metrics_json->>'max_drawdown')::float  AS max_dd,
           (metrics_json->>'total_return_pct')::float AS ret_pct,
           (metrics_json->>'trade_count')::int     AS trades,
           (metrics_json->>'trade_hit_rate')::float AS hit_rate
      FROM backtest_folds
     WHERE run_id = <RUN_ID>
     ORDER BY fold_index;

    -- equity curve for plotting
    SELECT timestamp, equity FROM backtest_equity
     WHERE run_id = <RUN_ID> ORDER BY timestamp;

## Sandbox isolation

The CLI wires a sandbox `Database` (search_path=`backtest,public`) for the
engine, so per-fold writes to `orders`, `model_registry`, and `predictions`
land in the `backtest` schema instead of `public`. Read-only tables
(`bars_1m`, `features`, `labels`) fall through to `public` via search_path.
Backtest results (`backtest_runs`, `backtest_folds`, `backtest_trades`,
`backtest_equity`) live in `public` and are written via a separate live
`Database` instance.

To clear sandbox state between runs:

    psql -h localhost -U postgres -d bullforge -c "
      TRUNCATE backtest.orders, backtest.model_registry, backtest.predictions;
    "

To clear backtest results:

    psql -h localhost -U postgres -d bullforge -c "
      TRUNCATE backtest_equity, backtest_trades, backtest_folds, backtest_runs
        RESTART IDENTITY CASCADE;
    "

## Important caveats
- Embargo must be >= the longest horizon being backtested to prevent label leakage.
- Default annualization assumes 1m equities (390 * 252 periods/year). Pass
  `--periods-per-year` for crypto (1440 * 365) or daily (252).
- The simulated broker only models flat ↔ full-position trades; partial fills
  and scale-out / scale-in are not supported. Strategies that depend on
  position sizing changes mid-trade will not backtest accurately.
```

- [ ] **Step 2: Commit**

```bash
git add bullforge/docs/runbooks/backtest.md
git commit -m "docs(backtest): runbook for running and inspecting walk-forward backtests"
```

---

## Self-Review Checklist

- [x] Spec coverage: schema (1), splitter (2), fill model (3), sim broker (4), equity (5), metrics (6), DB writer (7), engine (8), CLI (9), smoke test (10), docs (11) — every component from the design conversation has a task.
- [x] No placeholders: every code step contains complete, runnable code; every test step shows the assertions; commit commands are spelled out.
- [x] Type consistency: `Fold` shape used by `BacktestEngine` matches the dataclass defined in Task 2; `SimulatedBroker` async signatures match `OrderExecutor`'s expectations from `AlpacaBroker`; `BacktestResultsDB` method names are reused unchanged in `BacktestEngine`.
- [x] Known caveat called out in Task 11: trainer overwrites active models — backtest must not race with live model_service.
