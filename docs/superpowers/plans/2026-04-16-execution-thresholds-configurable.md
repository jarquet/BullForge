# Editable Execution Thresholds Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the 7 trade-entry threshold parameters from env vars into runtime_config so the dashboard config page can edit them live, and extend the backtest modal to override them per-run (snapshot-on-creation for reproducibility).

**Architecture:** `SignalAggregator` and `RiskManager` accept a `RuntimeConfig` and an optional `overrides` dict; every relevant parameter is read via a small `_param(key)` helper on each call (overrides first, else `runtime_config`). The `/api/backtests` POST validates optional override fields and fills blanks from runtime_config before persisting. The config page picks up the new schema keys automatically.

**Tech Stack:** Python 3.11 (asyncio, aiohttp, dataclasses), Redis (runtime_config hash), vanilla JS (dashboard modal), pytest-asyncio.

---

## Spec reference

See [`docs/superpowers/specs/2026-04-16-execution-thresholds-configurable-design.md`](../specs/2026-04-16-execution-thresholds-configurable-design.md).

## File structure

Modified:
- `bullforge/shared/runtime_config.py` — schema additions (7 keys)
- `bullforge/shared/config.py` — drop 6 `Settings` fields
- `bullforge/services/execution_service/signal_aggregator.py` — async, runtime_config + overrides, no hardcoded band
- `bullforge/services/execution_service/risk_manager.py` — overrides, no threshold kwargs
- `bullforge/services/execution_service/main.py` — simplify construction, await aggregate
- `bullforge/services/backtest_service/engine.py` — extend `BacktestConfig`, pass overrides
- `bullforge/services/backtest_service/worker.py` — read override fields in `_build_config`
- `bullforge/services/dashboard_service/backtests_api.py` — validate + snapshot-fill overrides
- `bullforge/services/dashboard_service/static/backtests.js` — Advanced section in modal
- `bullforge/.env.example` — drop 6 env vars
- `bullforge/tests/test_signal_aggregator.py` — rewrite for async + runtime_config
- `bullforge/tests/test_risk_manager.py` — rewrite thresholds into fake RC
- `bullforge/tests/test_config.py` — drop execution_* assertions
- `bullforge/tests/test_runtime_config.py` — add coverage for new keys
- `bullforge/tests/test_backtest_worker.py` — add `_build_config` override test

Extended (not created):
- `bullforge/tests/test_dashboard_backtests_api.py` — add overrides validation + snapshot-fill + happy path (existing file, existing fixture pattern reused)

---

## Task 1: Add 7 execution keys to `CONFIG_SCHEMA`

**Files:**
- Modify: `bullforge/shared/runtime_config.py:13-102`
- Test: `bullforge/tests/test_runtime_config.py`

- [ ] **Step 1: Write the failing test**

Append to `bullforge/tests/test_runtime_config.py`:
```python
import pytest

from shared.runtime_config import CONFIG_SCHEMA


EXECUTION_KEYS = (
    "execution_min_confidence",
    "execution_min_agreement",
    "execution_direction_band",
    "execution_min_signal_strength",
    "execution_max_position_pct",
    "execution_max_positions",
    "execution_max_exposure",
)


def test_execution_keys_present_in_schema():
    for key in EXECUTION_KEYS:
        assert key in CONFIG_SCHEMA, f"{key} missing from CONFIG_SCHEMA"
        meta = CONFIG_SCHEMA[key]
        assert "default" in meta and "type" in meta
        assert meta.get("label"), f"{key} needs a label"
        assert meta.get("description"), f"{key} needs a description"


def test_execution_key_defaults_and_bounds():
    assert CONFIG_SCHEMA["execution_min_confidence"]["default"] == 0.6
    assert CONFIG_SCHEMA["execution_min_agreement"]["default"] == 2
    assert CONFIG_SCHEMA["execution_min_agreement"]["max"] == 10000
    assert CONFIG_SCHEMA["execution_direction_band"]["default"] == 0.3
    assert CONFIG_SCHEMA["execution_min_signal_strength"]["default"] == 0.5
    assert CONFIG_SCHEMA["execution_max_position_pct"]["default"] == 0.02
    assert CONFIG_SCHEMA["execution_max_positions"]["default"] == 5
    assert CONFIG_SCHEMA["execution_max_exposure"]["default"] == 0.2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest bullforge/tests/test_runtime_config.py::test_execution_keys_present_in_schema bullforge/tests/test_runtime_config.py::test_execution_key_defaults_and_bounds -v`
Expected: FAIL with `KeyError: 'execution_min_confidence'` or similar.

- [ ] **Step 3: Add the 7 entries to `CONFIG_SCHEMA`**

In `bullforge/shared/runtime_config.py`, insert these entries into `CONFIG_SCHEMA` (place them after `equity_window_close_buffer_minutes` and before `label_horizons_1m`; the exact location doesn't affect behavior, it only affects display order in the config page):

```python
    "execution_min_confidence": {
        "default": 0.6, "type": "float", "min": 0.0, "max": 1.0,
        "label": "Min Prediction Confidence",
        "description": (
            "Minimum per-prediction confidence (max-class probability from "
            "the model) required for a model's vote to count. Predictions "
            "below this are dropped before agreement is tallied. Raising it "
            "makes each model's vote harder to earn; lowering it lets weaker "
            "signals through."
        ),
        "reference": "https://en.wikipedia.org/wiki/Probabilistic_classification",
    },
    "execution_min_agreement": {
        "default": 2, "type": "int", "min": 1, "max": 10000,
        "label": "Min Agreeing Models",
        "description": (
            "Minimum number of confident predictions — after filtering by "
            "Min Prediction Confidence and dropping disabled models — "
            "required to emit a trading signal. With fewer survivors than "
            "this, no trade is considered. Set close to your active model "
            "count for near-unanimity, or lower to allow a plurality vote."
        ),
        "reference": "https://en.wikipedia.org/wiki/Ensemble_learning",
    },
    "execution_direction_band": {
        "default": 0.3, "type": "float", "min": 0.0, "max": 1.0,
        "label": "Direction Neutral Band",
        "description": (
            "Dead-zone around zero for the confidence-weighted average "
            "direction. A weighted average between -band and +band produces "
            "a flat signal (no trade); above +band is buy, below -band is "
            "sell. Widening the band filters out low-conviction crowd votes."
        ),
        "reference": "https://en.wikipedia.org/wiki/Dead_zone_(signal_processing)",
    },
    "execution_min_signal_strength": {
        "default": 0.5, "type": "float", "min": 0.0, "max": 1.0,
        "label": "Min Signal Strength",
        "description": (
            "Minimum |weighted_avg_direction| (0-1) required by the Risk "
            "Manager to approve an entry after the Direction Neutral Band "
            "check passes. Example: two models agreeing at confidence 0.6 "
            "produce strength=0.6 — just clearing the default 0.5."
        ),
        "reference": "https://en.wikipedia.org/wiki/Signal-to-noise_ratio",
    },
    "execution_max_position_pct": {
        "default": 0.02, "type": "float", "min": 0.0, "max": 1.0,
        "label": "Max Position Size (fraction of equity)",
        "description": (
            "Fraction of account equity allocated to a single new position. "
            "0.02 = 2% of equity per entry. Share quantity is "
            "floor(equity * pct / current_price); entries that would size "
            "to zero shares are rejected."
        ),
        "reference": "https://en.wikipedia.org/wiki/Position_sizing",
    },
    "execution_max_positions": {
        "default": 5, "type": "int", "min": 1, "max": 100,
        "label": "Max Concurrent Positions",
        "description": (
            "Maximum number of simultaneous open positions across all "
            "symbols. New entries beyond this count are rejected until an "
            "existing position exits."
        ),
        "reference": "https://en.wikipedia.org/wiki/Diversification_(finance)",
    },
    "execution_max_exposure": {
        "default": 0.2, "type": "float", "min": 0.0, "max": 1.0,
        "label": "Max Portfolio Exposure (fraction of equity)",
        "description": (
            "Upper bound on total portfolio exposure as a fraction of "
            "equity (sum of |market_value| across open positions divided by "
            "equity). New entries that would push exposure to or past this "
            "value are rejected. 0.2 = up to 20% of equity deployed at any "
            "time."
        ),
        "reference": "https://en.wikipedia.org/wiki/Market_exposure",
    },
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest bullforge/tests/test_runtime_config.py -v`
Expected: PASS for both new tests plus any pre-existing tests.

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/runtime_config.py bullforge/tests/test_runtime_config.py
git commit -m "feat(runtime_config): add 7 execution-threshold keys to schema"
```

---

## Task 2: Refactor `SignalAggregator` to read from runtime_config + overrides (async)

**Files:**
- Modify: `bullforge/services/execution_service/signal_aggregator.py` (full rewrite)
- Test: `bullforge/tests/test_signal_aggregator.py` (full rewrite)

- [ ] **Step 1: Write the failing test**

Replace the full contents of `bullforge/tests/test_signal_aggregator.py` with:

```python
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from services.execution_service.signal_aggregator import SignalAggregator


def _rc(values: dict | None = None):
    """Build a fake RuntimeConfig whose .get returns `values[key]` or a default."""
    defaults = {
        "execution_min_confidence": 0.6,
        "execution_min_agreement": 2,
        "execution_direction_band": 0.3,
    }
    data = {**defaults, **(values or {})}
    rc = AsyncMock()

    async def get(key):
        return data[key]

    rc.get = get
    return rc


def _ts():
    return datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_aggregate_strong_buy():
    agg = SignalAggregator(runtime_config=_rc())
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.80},
        {"horizon_minutes": 10, "direction": 1, "confidence": 0.75},
        {"horizon_minutes": 30, "direction": 1, "confidence": 0.70},
    ]
    signal = await agg.aggregate("SPY", _ts(), predictions)
    assert signal is not None
    assert signal.direction == 1
    assert signal.strength > 0.5
    assert signal.contributing_predictions == 3


@pytest.mark.asyncio
async def test_aggregate_strong_sell():
    agg = SignalAggregator(runtime_config=_rc())
    predictions = [
        {"horizon_minutes": 5, "direction": -1, "confidence": 0.80},
        {"horizon_minutes": 10, "direction": -1, "confidence": 0.70},
    ]
    signal = await agg.aggregate("SPY", _ts(), predictions)
    assert signal is not None
    assert signal.direction == -1
    assert signal.strength > 0.5


@pytest.mark.asyncio
async def test_aggregate_mixed_produces_flat():
    agg = SignalAggregator(runtime_config=_rc())
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.75},
        {"horizon_minutes": 10, "direction": -1, "confidence": 0.75},
    ]
    signal = await agg.aggregate("SPY", _ts(), predictions)
    assert signal is not None
    assert signal.direction == 0


@pytest.mark.asyncio
async def test_aggregate_filters_low_confidence():
    agg = SignalAggregator(runtime_config=_rc())
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.80},
        {"horizon_minutes": 10, "direction": 1, "confidence": 0.70},
        {"horizon_minutes": 30, "direction": -1, "confidence": 0.40},
    ]
    signal = await agg.aggregate("SPY", _ts(), predictions)
    assert signal is not None
    assert signal.direction == 1
    assert signal.contributing_predictions == 2


@pytest.mark.asyncio
async def test_aggregate_insufficient_agreement_returns_none():
    agg = SignalAggregator(runtime_config=_rc())
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.80},
        {"horizon_minutes": 10, "direction": -1, "confidence": 0.40},
    ]
    signal = await agg.aggregate("SPY", _ts(), predictions)
    assert signal is None


@pytest.mark.asyncio
async def test_aggregate_empty_returns_none():
    agg = SignalAggregator(runtime_config=_rc())
    assert await agg.aggregate("SPY", _ts(), []) is None


@pytest.mark.asyncio
async def test_aggregate_all_below_confidence_returns_none():
    agg = SignalAggregator(runtime_config=_rc())
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.40},
        {"horizon_minutes": 10, "direction": 1, "confidence": 0.50},
    ]
    assert await agg.aggregate("SPY", _ts(), predictions) is None


@pytest.mark.asyncio
async def test_aggregate_filters_disabled_models():
    agg = SignalAggregator(runtime_config=_rc())
    agg.update_disabled_models({2})
    predictions = [
        {"direction": 1, "confidence": 0.8, "model_id": 1},
        {"direction": 1, "confidence": 0.7, "model_id": 2},
        {"direction": 1, "confidence": 0.9, "model_id": 3},
    ]
    result = await agg.aggregate("SPY", _ts(), predictions)
    assert result is not None
    assert result.contributing_predictions == 2


@pytest.mark.asyncio
async def test_aggregate_disabled_models_cause_no_signal():
    agg = SignalAggregator(runtime_config=_rc())
    agg.update_disabled_models({1, 2})
    predictions = [
        {"direction": 1, "confidence": 0.8, "model_id": 1},
        {"direction": 1, "confidence": 0.7, "model_id": 2},
        {"direction": 1, "confidence": 0.9, "model_id": 3},
    ]
    assert await agg.aggregate("SPY", _ts(), predictions) is None


@pytest.mark.asyncio
async def test_aggregate_no_model_id_not_filtered():
    agg = SignalAggregator(runtime_config=_rc())
    agg.update_disabled_models({99})
    predictions = [
        {"direction": 1, "confidence": 0.8},
        {"direction": 1, "confidence": 0.7},
    ]
    result = await agg.aggregate("SPY", _ts(), predictions)
    assert result is not None
    assert result.contributing_predictions == 2


@pytest.mark.asyncio
async def test_direction_band_wide_produces_flat():
    """Same inputs that would produce direction=1 at band=0.3 produce 0 at band=0.9."""
    agg = SignalAggregator(
        runtime_config=_rc({"execution_direction_band": 0.9}),
    )
    predictions = [
        {"direction": 1, "confidence": 0.65},
        {"direction": 1, "confidence": 0.65},
    ]
    signal = await agg.aggregate("SPY", _ts(), predictions)
    assert signal is not None
    assert signal.direction == 0


@pytest.mark.asyncio
async def test_direction_band_narrow_produces_buy():
    """Same inputs that are flat at band=0.3 buy at band=0.05."""
    agg = SignalAggregator(
        runtime_config=_rc({"execution_direction_band": 0.05}),
    )
    predictions = [
        {"direction": 1, "confidence": 0.7},
        {"direction": 0, "confidence": 0.7},
    ]
    signal = await agg.aggregate("SPY", _ts(), predictions)
    assert signal is not None
    assert signal.direction == 1


@pytest.mark.asyncio
async def test_overrides_win_over_runtime_config():
    """If overrides={'min_confidence': 0.9}, the 0.6 from runtime_config is ignored."""
    agg = SignalAggregator(
        runtime_config=_rc(),
        overrides={"min_confidence": 0.9},
    )
    predictions = [
        {"direction": 1, "confidence": 0.80},
        {"direction": 1, "confidence": 0.75},
    ]
    assert await agg.aggregate("SPY", _ts(), predictions) is None


@pytest.mark.asyncio
async def test_override_none_falls_back_to_runtime_config():
    """overrides={'min_confidence': None} = unset; use runtime_config."""
    agg = SignalAggregator(
        runtime_config=_rc(),
        overrides={"min_confidence": None, "min_agreement": None, "direction_band": None},
    )
    predictions = [
        {"direction": 1, "confidence": 0.80},
        {"direction": 1, "confidence": 0.70},
    ]
    signal = await agg.aggregate("SPY", _ts(), predictions)
    assert signal is not None
    assert signal.direction == 1


@pytest.mark.asyncio
async def test_live_reload_uses_latest_runtime_config_value():
    """Changing runtime_config between calls changes behavior."""
    data = {
        "execution_min_confidence": 0.5,
        "execution_min_agreement": 2,
        "execution_direction_band": 0.3,
    }
    rc = AsyncMock()

    async def get(key):
        return data[key]

    rc.get = get
    agg = SignalAggregator(runtime_config=rc)
    predictions = [
        {"direction": 1, "confidence": 0.55},
        {"direction": 1, "confidence": 0.55},
    ]
    first = await agg.aggregate("SPY", _ts(), predictions)
    assert first is not None and first.direction == 1

    data["execution_min_confidence"] = 0.9
    second = await agg.aggregate("SPY", _ts(), predictions)
    assert second is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest bullforge/tests/test_signal_aggregator.py -v`
Expected: FAIL — constructor signature rejects `runtime_config` kwarg (or all `aggregate` calls return coroutines that aren't awaited properly against the old sync implementation).

- [ ] **Step 3: Rewrite the aggregator**

Replace the full contents of `bullforge/services/execution_service/signal_aggregator.py` with:

```python
from __future__ import annotations

from datetime import datetime
from typing import Optional, Set

from shared.models import TradingSignal
from shared.runtime_config import RuntimeConfig


class SignalAggregator:
    def __init__(
        self,
        *,
        runtime_config: RuntimeConfig,
        overrides: Optional[dict] = None,
    ):
        self._rc = runtime_config
        self._overrides = overrides or {}
        self._disabled_model_ids: Set[int] = set()

    def update_disabled_models(self, disabled_ids: Set[int]) -> None:
        self._disabled_model_ids = disabled_ids

    async def _param(self, key: str):
        v = self._overrides.get(key)
        if v is not None:
            return v
        return await self._rc.get(f"execution_{key}")

    async def aggregate(
        self,
        symbol: str,
        timestamp: datetime,
        predictions: list[dict],
    ) -> Optional[TradingSignal]:
        if not predictions:
            return None

        active_preds = [
            p for p in predictions
            if p.get("model_id") not in self._disabled_model_ids
        ]

        min_confidence = await self._param("min_confidence")
        confident = [
            p for p in active_preds if p["confidence"] >= min_confidence
        ]
        min_agreement = await self._param("min_agreement")
        if len(confident) < min_agreement:
            return None

        weighted_sum = sum(
            p["direction"] * p["confidence"] for p in confident
        )
        total_weight = sum(p["confidence"] for p in confident)
        avg = weighted_sum / total_weight

        band = await self._param("direction_band")
        if avg > band:
            direction = 1
        elif avg < -band:
            direction = -1
        else:
            direction = 0

        strength = min(abs(avg), 1.0)

        return TradingSignal(
            symbol=symbol,
            timestamp=timestamp,
            direction=direction,
            strength=strength,
            contributing_predictions=len(confident),
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest bullforge/tests/test_signal_aggregator.py -v`
Expected: PASS for all 14 tests.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/execution_service/signal_aggregator.py bullforge/tests/test_signal_aggregator.py
git commit -m "refactor(signal_aggregator): read thresholds from runtime_config, support overrides"
```

---

## Task 3: Refactor `RiskManager` to read thresholds from runtime_config + overrides

**Files:**
- Modify: `bullforge/services/execution_service/risk_manager.py`
- Test: `bullforge/tests/test_risk_manager.py` (rewrite)

- [ ] **Step 1: Write the failing test**

Replace the contents of `bullforge/tests/test_risk_manager.py` with:

```python
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from shared.models import TradingSignal
from services.execution_service.risk_manager import RiskManager


def _rc(values: dict | None = None):
    defaults = {
        "equity_window_open_buffer_minutes": 60,
        "equity_window_close_buffer_minutes": 60,
        "execution_min_signal_strength": 0.5,
        "execution_max_position_pct": 0.10,
        "execution_max_positions": 3,
        "execution_max_exposure": 0.5,
    }
    data = {**defaults, **(values or {})}
    rc = AsyncMock()

    async def get(key):
        return data[key]

    rc.get = get
    return rc


def _signal(direction=1, strength=0.8, symbol="SPY", hour_utc=14):
    return TradingSignal(
        symbol=symbol,
        timestamp=datetime(2026, 4, 13, hour_utc, 0, 0, tzinfo=timezone.utc),
        direction=direction,
        strength=strength,
        contributing_predictions=3,
    )


def _account(equity=100000.0, buying_power=200000.0):
    return {"equity": equity, "buying_power": buying_power}


def _positions(*symbols):
    return [
        {"symbol": s, "qty": 10.0, "market_value": 4200.0} for s in symbols
    ]


@pytest.mark.asyncio
async def test_check_entry_approved():
    rm = RiskManager(runtime_config=_rc())
    approved, reason, qty = await rm.check_entry(
        _signal(hour_utc=16), _account(), [], current_price=420.0,
    )
    assert approved is True
    assert qty == 23
    assert reason == "Approved"


@pytest.mark.asyncio
async def test_check_entry_qty_scales_with_equity():
    rm = RiskManager(runtime_config=_rc())
    approved, _, qty = await rm.check_entry(
        _signal(hour_utc=16), _account(equity=200000.0), [], current_price=420.0,
    )
    assert approved is True
    assert qty == 47


@pytest.mark.asyncio
async def test_check_entry_weak_signal():
    rm = RiskManager(runtime_config=_rc())
    approved, reason, qty = await rm.check_entry(
        _signal(strength=0.3, hour_utc=16), _account(), [], current_price=420.0,
    )
    assert approved is False
    assert "weak" in reason.lower()
    assert qty == 0


@pytest.mark.asyncio
async def test_check_entry_flat_signal():
    rm = RiskManager(runtime_config=_rc())
    approved, reason, qty = await rm.check_entry(
        _signal(direction=0, hour_utc=16), _account(), [], current_price=420.0,
    )
    assert approved is False
    assert qty == 0


@pytest.mark.asyncio
async def test_check_entry_short_allowed():
    rm = RiskManager(runtime_config=_rc())
    approved, reason, qty = await rm.check_entry(
        _signal(direction=-1, hour_utc=16), _account(), [], current_price=420.0,
    )
    assert approved is True
    assert qty == 23


@pytest.mark.asyncio
async def test_check_entry_existing_position():
    rm = RiskManager(runtime_config=_rc())
    approved, reason, qty = await rm.check_entry(
        _signal(hour_utc=16), _account(), _positions("SPY"), current_price=420.0,
    )
    assert approved is False
    assert "position" in reason.lower()


@pytest.mark.asyncio
async def test_check_entry_max_positions():
    rm = RiskManager(runtime_config=_rc())
    approved, reason, qty = await rm.check_entry(
        _signal(hour_utc=16), _account(), _positions("QQQ", "AAPL", "MSFT"),
        current_price=420.0,
    )
    assert approved is False
    assert "max" in reason.lower()


@pytest.mark.asyncio
async def test_check_entry_max_exposure():
    rm = RiskManager(runtime_config=_rc())
    big_positions = [
        {"symbol": "QQQ", "qty": 100.0, "market_value": 30000.0},
        {"symbol": "AAPL", "qty": 100.0, "market_value": 25000.0},
    ]
    approved, reason, _ = await rm.check_entry(
        _signal(hour_utc=16), _account(), big_positions, current_price=420.0,
    )
    assert approved is False
    assert "exposure" in reason.lower()


def test_check_exit_opposing_signal():
    rm = RiskManager(runtime_config=_rc())
    position = {"symbol": "SPY", "qty": 10.0}
    should_exit, reason = rm.check_exit(_signal(direction=-1), position)
    assert should_exit is True
    assert "opposing" in reason.lower()


def test_check_exit_flat_signal():
    rm = RiskManager(runtime_config=_rc())
    position = {"symbol": "SPY", "qty": 10.0}
    should_exit, reason = rm.check_exit(_signal(direction=0, strength=0.0), position)
    assert should_exit is True
    assert "flat" in reason.lower()


def test_check_exit_same_direction():
    rm = RiskManager(runtime_config=_rc())
    position = {"symbol": "SPY", "qty": 10.0}
    should_exit, _ = rm.check_exit(_signal(direction=1), position)
    assert should_exit is False


@pytest.mark.asyncio
async def test_overrides_win_over_runtime_config():
    """overrides={'min_signal_strength': 0.9} rejects a 0.8 strength signal."""
    rm = RiskManager(
        runtime_config=_rc(),
        overrides={"min_signal_strength": 0.9},
    )
    approved, reason, _ = await rm.check_entry(
        _signal(strength=0.8, hour_utc=16), _account(), [], current_price=420.0,
    )
    assert approved is False
    assert "weak" in reason.lower()


@pytest.mark.asyncio
async def test_override_none_falls_back_to_runtime_config():
    rm = RiskManager(
        runtime_config=_rc({"execution_min_signal_strength": 0.1}),
        overrides={"min_signal_strength": None},
    )
    approved, _, qty = await rm.check_entry(
        _signal(strength=0.2, hour_utc=16), _account(), [], current_price=420.0,
    )
    assert approved is True
    assert qty > 0


@pytest.mark.asyncio
async def test_max_position_pct_override():
    rm = RiskManager(
        runtime_config=_rc(),
        overrides={"max_position_pct": 0.5},
    )
    approved, _, qty = await rm.check_entry(
        _signal(hour_utc=16), _account(), [], current_price=420.0,
    )
    assert approved is True
    assert qty == 119


@pytest.mark.asyncio
async def test_window_rejects_equity_before_buffer():
    rm = RiskManager(runtime_config=_rc())
    approved, reason, qty = await rm.check_entry(
        _signal(hour_utc=13), _account(), [], current_price=420.0,
    )
    assert approved is False
    assert "window" in reason.lower()
    assert qty == 0


@pytest.mark.asyncio
async def test_window_rejects_equity_after_close_buffer():
    rm = RiskManager(runtime_config=_rc())
    approved, reason, _ = await rm.check_entry(
        _signal(hour_utc=19), _account(), [], current_price=420.0,
    )
    assert approved is False
    assert "window" in reason.lower()


@pytest.mark.asyncio
async def test_window_rejects_equity_weekend():
    rm = RiskManager(runtime_config=_rc())
    sig = TradingSignal(
        symbol="SPY",
        timestamp=datetime(2026, 4, 11, 16, 0, tzinfo=timezone.utc),
        direction=1, strength=0.8, contributing_predictions=3,
    )
    approved, reason, _ = await rm.check_entry(
        sig, _account(), [], current_price=420.0,
    )
    assert approved is False
    assert "window" in reason.lower()


@pytest.mark.asyncio
async def test_window_allows_equity_midday():
    rm = RiskManager(runtime_config=_rc())
    approved, _, qty = await rm.check_entry(
        _signal(hour_utc=16), _account(), [], current_price=420.0,
    )
    assert approved is True
    assert qty == 23


@pytest.mark.asyncio
async def test_window_allows_crypto_weekend():
    rm = RiskManager(runtime_config=_rc())
    sig = TradingSignal(
        symbol="BTC/USD",
        timestamp=datetime(2026, 4, 12, 3, 0, tzinfo=timezone.utc),
        direction=1, strength=0.8, contributing_predictions=3,
    )
    approved, _, _ = await rm.check_entry(
        sig, _account(), [], current_price=67.0,
    )
    assert approved is True


@pytest.mark.asyncio
async def test_window_allows_crypto_weekday_midnight():
    rm = RiskManager(runtime_config=_rc())
    sig = TradingSignal(
        symbol="ETH/USD",
        timestamp=datetime(2026, 4, 15, 0, 0, tzinfo=timezone.utc),
        direction=1, strength=0.8, contributing_predictions=3,
    )
    approved, _, _ = await rm.check_entry(
        sig, _account(), [], current_price=3500.0,
    )
    assert approved is True


@pytest.mark.asyncio
async def test_window_gate_precedes_other_checks():
    rm = RiskManager(runtime_config=_rc())
    sig = TradingSignal(
        symbol="SPY",
        timestamp=datetime(2026, 4, 12, 16, 0, tzinfo=timezone.utc),
        direction=0, strength=0.1, contributing_predictions=1,
    )
    approved, reason, _ = await rm.check_entry(
        sig, _account(), [], current_price=420.0,
    )
    assert approved is False
    assert "window" in reason.lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest bullforge/tests/test_risk_manager.py -v`
Expected: FAIL — threshold kwargs still accepted in old constructor but `_rc` now returns `execution_*` keys that old RiskManager doesn't read.

- [ ] **Step 3: Rewrite the RiskManager**

Replace the full contents of `bullforge/services/execution_service/risk_manager.py` with:

```python
from __future__ import annotations

import logging
from typing import Optional, Tuple

from shared.models import TradingSignal
from shared.runtime_config import RuntimeConfig
from shared.trading_window import is_trading_allowed

logger = logging.getLogger(__name__)


class RiskManager:
    def __init__(
        self,
        *,
        runtime_config: RuntimeConfig,
        overrides: Optional[dict] = None,
    ):
        if runtime_config is None:
            raise ValueError(
                "RiskManager requires a RuntimeConfig; the equity "
                "trading-window gate depends on it and must not be bypassed.",
            )
        self._rc = runtime_config
        self._overrides = overrides or {}

    async def _param(self, key: str):
        v = self._overrides.get(key)
        if v is not None:
            return v
        return await self._rc.get(f"execution_{key}")

    async def check_entry(
        self,
        signal: TradingSignal,
        account: dict,
        positions: list[dict],
        current_price: float,
    ) -> Tuple[bool, str, int]:
        """
        Check whether a new position entry is allowed.
        Returns (approved, reason, quantity).
        """
        open_buf = int(await self._rc.get("equity_window_open_buffer_minutes"))
        close_buf = int(await self._rc.get("equity_window_close_buffer_minutes"))
        allowed, reason = is_trading_allowed(
            signal.symbol, signal.timestamp,
            open_buffer_min=open_buf, close_buffer_min=close_buf,
        )
        if not allowed:
            return False, f"trading window: {reason}", 0

        if signal.direction == 0:
            return False, "Flat signal, no entry", 0

        min_strength = await self._param("min_signal_strength")
        if signal.strength < min_strength:
            return False, f"Signal too weak ({signal.strength:.2f})", 0

        for pos in positions:
            if pos["symbol"] == signal.symbol:
                return False, "Already have position in this symbol", 0

        max_positions = await self._param("max_positions")
        if len(positions) >= max_positions:
            return False, "Max positions reached", 0

        max_exposure = await self._param("max_exposure")
        total_exposure = sum(abs(p["market_value"]) for p in positions)
        equity = account["equity"]
        if equity > 0 and total_exposure / equity >= max_exposure:
            return False, "Max portfolio exposure reached", 0

        max_position_pct = await self._param("max_position_pct")
        qty = int((equity * max_position_pct) / current_price)
        if qty <= 0:
            return False, "Position value too small for one share", 0

        return True, "Approved", qty

    def check_exit(
        self,
        signal: TradingSignal,
        position: dict,
    ) -> Tuple[bool, str]:
        """
        Check whether an existing position should be closed.
        Returns (should_exit, reason).
        """
        pos_qty = float(position["qty"])
        pos_direction = 1 if pos_qty > 0 else -1

        if signal.direction == -pos_direction:
            return True, "Opposing signal"

        if signal.direction == 0:
            return True, "Flat signal"

        return False, "Signal matches position"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest bullforge/tests/test_risk_manager.py -v`
Expected: PASS for all tests.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/execution_service/risk_manager.py bullforge/tests/test_risk_manager.py
git commit -m "refactor(risk_manager): read thresholds from runtime_config, support overrides"
```

---

## Task 4: Wire new constructors into `execution_service/main.py`

**Files:**
- Modify: `bullforge/services/execution_service/main.py:52-63, 112-116`

- [ ] **Step 1: Replace the aggregator and risk_manager construction**

Find in `bullforge/services/execution_service/main.py` the block:

```python
    aggregator = SignalAggregator(
        min_confidence=settings.execution_min_confidence,
        min_agreement=settings.execution_min_agreement,
    )
    runtime_config = RuntimeConfig(redis_client)
    risk_manager = RiskManager(
        max_position_pct=settings.execution_max_position_pct,
        max_total_positions=settings.execution_max_positions,
        max_portfolio_exposure=settings.execution_max_exposure,
        min_signal_strength=settings.execution_min_signal_strength,
        runtime_config=runtime_config,
    )
```

Replace with:

```python
    aggregator = SignalAggregator(runtime_config=runtime_config)
    risk_manager = RiskManager(runtime_config=runtime_config)
```

(The outer `runtime_config = RuntimeConfig(redis_client)` at line 44 is the canonical one; delete the duplicate on line 56.)

- [ ] **Step 2: Await the aggregator call**

Find in the same file:

```python
            signal_obj = aggregator.aggregate(
                symbol=symbol,
                timestamp=datetime.now(timezone.utc),
                predictions=predictions,
            )
```

Replace with:

```python
            signal_obj = await aggregator.aggregate(
                symbol=symbol,
                timestamp=datetime.now(timezone.utc),
                predictions=predictions,
            )
```

- [ ] **Step 3: Verify the module imports are unchanged**

The file already imports `SignalAggregator`, `RiskManager`, `RuntimeConfig` — no new imports needed.

- [ ] **Step 4: Run the execution-service smoke tests**

Run: `pytest bullforge/tests/test_signal_aggregator.py bullforge/tests/test_risk_manager.py -v`
Expected: PASS (the main.py change has no test yet; it's exercised by the backtest engine test in Task 5).

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/execution_service/main.py
git commit -m "refactor(execution_service): wire runtime_config into aggregator and risk_manager"
```

---

## Task 5: Extend `BacktestConfig` + wire overrides into backtest engine

**Files:**
- Modify: `bullforge/services/backtest_service/engine.py:35-51, 144-151, 199`
- Test: `bullforge/tests/test_backtest_engine_overrides.py` (new file)

- [ ] **Step 1: Write the failing test**

Create `bullforge/tests/test_backtest_engine_overrides.py`:

```python
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from services.backtest_service.engine import BacktestConfig
from services.execution_service.signal_aggregator import SignalAggregator
from services.execution_service.risk_manager import RiskManager


def _rc():
    defaults = {
        "execution_min_confidence": 0.6,
        "execution_min_agreement": 2,
        "execution_direction_band": 0.3,
        "execution_min_signal_strength": 0.5,
        "execution_max_position_pct": 0.10,
        "execution_max_positions": 5,
        "execution_max_exposure": 0.5,
        "equity_window_open_buffer_minutes": 60,
        "equity_window_close_buffer_minutes": 60,
    }
    rc = AsyncMock()

    async def get(key):
        return defaults[key]

    rc.get = get
    return rc


def test_backtest_config_has_all_seven_override_fields():
    cfg = BacktestConfig(
        name="t",
        symbols=["SPY"],
        timeframe="1m",
        horizons=[5],
        range_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        range_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        train_days=1, test_days=1, embargo_minutes=5, mode="rolling",
        min_confidence=0.7,
        min_agreement=3,
        direction_band=0.4,
        min_signal_strength=0.6,
        max_position_pct=0.05,
        max_positions=2,
        max_exposure=0.1,
    )
    assert cfg.min_confidence == 0.7
    assert cfg.min_agreement == 3
    assert cfg.direction_band == 0.4
    assert cfg.min_signal_strength == 0.6
    assert cfg.max_position_pct == 0.05
    assert cfg.max_positions == 2
    assert cfg.max_exposure == 0.1


def test_backtest_config_override_fields_default_none():
    cfg = BacktestConfig(
        name="t",
        symbols=["SPY"],
        timeframe="1m",
        horizons=[5],
        range_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        range_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        train_days=1, test_days=1, embargo_minutes=5, mode="rolling",
    )
    assert cfg.min_confidence is None
    assert cfg.min_agreement is None
    assert cfg.direction_band is None
    assert cfg.min_signal_strength is None
    assert cfg.max_position_pct is None
    assert cfg.max_positions is None
    assert cfg.max_exposure is None


@pytest.mark.asyncio
async def test_signal_aggregator_honors_backtest_overrides_end_to_end():
    """Mimics how engine._run_fold builds overrides from cfg."""
    cfg = BacktestConfig(
        name="t",
        symbols=["SPY"],
        timeframe="1m",
        horizons=[5],
        range_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        range_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        train_days=1, test_days=1, embargo_minutes=5, mode="rolling",
        min_confidence=0.9,
    )
    overrides = {
        "min_confidence": cfg.min_confidence,
        "min_agreement": cfg.min_agreement,
        "direction_band": cfg.direction_band,
        "min_signal_strength": cfg.min_signal_strength,
        "max_position_pct": cfg.max_position_pct,
        "max_positions": cfg.max_positions,
        "max_exposure": cfg.max_exposure,
    }
    agg = SignalAggregator(runtime_config=_rc(), overrides=overrides)
    predictions = [
        {"direction": 1, "confidence": 0.80},
        {"direction": 1, "confidence": 0.75},
    ]
    ts = datetime(2026, 4, 16, 14, 0, tzinfo=timezone.utc)
    # min_confidence=0.9 override filters out both → no signal.
    assert await agg.aggregate("SPY", ts, predictions) is None


@pytest.mark.asyncio
async def test_risk_manager_honors_backtest_overrides_end_to_end():
    cfg = BacktestConfig(
        name="t",
        symbols=["SPY"],
        timeframe="1m",
        horizons=[5],
        range_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        range_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        train_days=1, test_days=1, embargo_minutes=5, mode="rolling",
        min_signal_strength=0.95,
    )
    overrides = {
        "min_confidence": cfg.min_confidence,
        "min_agreement": cfg.min_agreement,
        "direction_band": cfg.direction_band,
        "min_signal_strength": cfg.min_signal_strength,
        "max_position_pct": cfg.max_position_pct,
        "max_positions": cfg.max_positions,
        "max_exposure": cfg.max_exposure,
    }
    from shared.models import TradingSignal
    rm = RiskManager(runtime_config=_rc(), overrides=overrides)
    sig = TradingSignal(
        symbol="SPY",
        timestamp=datetime(2026, 4, 13, 16, 0, tzinfo=timezone.utc),
        direction=1, strength=0.9, contributing_predictions=3,
    )
    # min_signal_strength=0.95 override rejects a 0.9 strength signal.
    approved, reason, _ = await rm.check_entry(
        sig, {"equity": 100000.0, "buying_power": 200000.0}, [], current_price=420.0,
    )
    assert approved is False
    assert "weak" in reason.lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest bullforge/tests/test_backtest_engine_overrides.py -v`
Expected: FAIL — `BacktestConfig` has no `min_confidence` keyword (TypeError at construction).

- [ ] **Step 3: Extend `BacktestConfig`**

In `bullforge/services/backtest_service/engine.py`, locate the `BacktestConfig` dataclass (around line 35) and add 7 fields after `periods_per_year`:

```python
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
    custom_name: str | None = None
    starting_cash: float = 100_000.0
    slippage_bps: float = 5.0
    max_volume_pct: float = 0.10
    periods_per_year: int = 390 * 252  # 1m equities default
    min_confidence: Optional[float] = None
    min_agreement: Optional[int] = None
    direction_band: Optional[float] = None
    min_signal_strength: Optional[float] = None
    max_position_pct: Optional[float] = None
    max_positions: Optional[int] = None
    max_exposure: Optional[float] = None
```

- [ ] **Step 4: Run test to verify the dataclass tests pass**

Run: `pytest bullforge/tests/test_backtest_engine_overrides.py::test_backtest_config_has_all_seven_override_fields bullforge/tests/test_backtest_engine_overrides.py::test_backtest_config_override_fields_default_none -v`
Expected: PASS.

- [ ] **Step 5: Pass `overrides` into aggregator and risk**

In the same file, locate `_run_fold` around line 144. Replace:

```python
        tracker = PositionTracker(broker)
        risk = RiskManager(runtime_config=self._rc)
        executor = OrderExecutor(
            broker=broker, risk_manager=risk, position_tracker=tracker,
            db=self._db, core=_NoopReconciler(),
        )

        equity = EquityTracker()
        aggregator = SignalAggregator()
```

with:

```python
        tracker = PositionTracker(broker)
        overrides = {
            "min_confidence": cfg.min_confidence,
            "min_agreement": cfg.min_agreement,
            "direction_band": cfg.direction_band,
            "min_signal_strength": cfg.min_signal_strength,
            "max_position_pct": cfg.max_position_pct,
            "max_positions": cfg.max_positions,
            "max_exposure": cfg.max_exposure,
        }
        risk = RiskManager(runtime_config=self._rc, overrides=overrides)
        executor = OrderExecutor(
            broker=broker, risk_manager=risk, position_tracker=tracker,
            db=self._db, core=_NoopReconciler(),
        )

        equity = EquityTracker()
        aggregator = SignalAggregator(
            runtime_config=self._rc, overrides=overrides,
        )
```

- [ ] **Step 6: Await the aggregator call inside the bar loop**

In the same file, locate line 199:

```python
            signal = aggregator.aggregate(symbol, ts, pred_dicts)
```

Replace with:

```python
            signal = await aggregator.aggregate(symbol, ts, pred_dicts)
```

- [ ] **Step 7: Run the end-to-end override tests**

Run: `pytest bullforge/tests/test_backtest_engine_overrides.py -v`
Expected: PASS for all 4 tests.

- [ ] **Step 8: Commit**

```bash
git add bullforge/services/backtest_service/engine.py bullforge/tests/test_backtest_engine_overrides.py
git commit -m "feat(backtest): threshold overrides on BacktestConfig, plumb into engine"
```

---

## Task 6: Extend `worker._build_config` to read override fields

**Files:**
- Modify: `bullforge/services/backtest_service/worker.py:25-38`
- Test: `bullforge/tests/test_backtest_worker.py`

- [ ] **Step 1: Write the failing test**

Append to `bullforge/tests/test_backtest_worker.py`:

```python
from services.backtest_service.worker import _build_config


def test_build_config_copies_threshold_overrides():
    cfg = _build_config({
        "name": "t",
        "symbols": ["SPY"], "timeframe": "1m", "horizons": [5],
        "range_start": "2025-01-01T00:00:00+00:00",
        "range_end": "2025-01-02T00:00:00+00:00",
        "train_days": 1, "test_days": 1, "embargo_min": 5,
        "mode": "rolling",
        "min_confidence": 0.75,
        "min_agreement": 3,
        "direction_band": 0.4,
        "min_signal_strength": 0.6,
        "max_position_pct": 0.05,
        "max_positions": 2,
        "max_exposure": 0.1,
    })
    assert cfg.min_confidence == 0.75
    assert cfg.min_agreement == 3
    assert cfg.direction_band == 0.4
    assert cfg.min_signal_strength == 0.6
    assert cfg.max_position_pct == 0.05
    assert cfg.max_positions == 2
    assert cfg.max_exposure == 0.1


def test_build_config_missing_thresholds_stay_none():
    cfg = _build_config({
        "name": "t",
        "symbols": ["SPY"], "timeframe": "1m", "horizons": [5],
        "range_start": "2025-01-01T00:00:00+00:00",
        "range_end": "2025-01-02T00:00:00+00:00",
        "train_days": 1, "test_days": 1, "embargo_min": 5,
        "mode": "rolling",
    })
    assert cfg.min_confidence is None
    assert cfg.min_agreement is None
    assert cfg.direction_band is None
    assert cfg.min_signal_strength is None
    assert cfg.max_position_pct is None
    assert cfg.max_positions is None
    assert cfg.max_exposure is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest bullforge/tests/test_backtest_worker.py::test_build_config_copies_threshold_overrides -v`
Expected: FAIL — `cfg.min_confidence` attribute exists (Task 5) but equals `None` because `_build_config` didn't forward it.

- [ ] **Step 3: Update `_build_config`**

Replace `_build_config` in `bullforge/services/backtest_service/worker.py` with:

```python
def _build_config(cfg: dict) -> BacktestConfig:
    return BacktestConfig(
        name=cfg.get("name", "dashboard-run"),
        custom_name=cfg.get("custom_name"),
        symbols=list(cfg["symbols"]),
        timeframe=cfg["timeframe"],
        horizons=list(cfg["horizons"]),
        range_start=datetime.fromisoformat(cfg["range_start"]),
        range_end=datetime.fromisoformat(cfg["range_end"]),
        train_days=int(cfg["train_days"]),
        test_days=int(cfg["test_days"]),
        embargo_minutes=int(cfg["embargo_min"]),
        mode=cfg["mode"],
        min_confidence=cfg.get("min_confidence"),
        min_agreement=cfg.get("min_agreement"),
        direction_band=cfg.get("direction_band"),
        min_signal_strength=cfg.get("min_signal_strength"),
        max_position_pct=cfg.get("max_position_pct"),
        max_positions=cfg.get("max_positions"),
        max_exposure=cfg.get("max_exposure"),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest bullforge/tests/test_backtest_worker.py -v`
Expected: PASS for all tests (2 new + 3 existing).

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/backtest_service/worker.py bullforge/tests/test_backtest_worker.py
git commit -m "feat(backtest_worker): forward threshold overrides from job JSON to BacktestConfig"
```

---

## Task 7: `/api/backtests` accepts, validates, and snapshots threshold overrides

**Files:**
- Modify: `bullforge/services/dashboard_service/backtests_api.py`
- Test: `bullforge/tests/test_dashboard_backtests_api.py` (append tests)

- [ ] **Step 1: Write the failing test**

Append these test functions to the end of `bullforge/tests/test_dashboard_backtests_api.py` (reuse the file's existing imports and style):

```python
_DEFAULT_EXEC_RC = {
    "label_horizons_1m": [1, 2, 3, 5, 8, 10, 20, 40, 60, 80, 100, 120],
    "label_horizons_1d": [1, 2, 3, 5, 12, 15],
    "execution_min_confidence": 0.6,
    "execution_min_agreement": 2,
    "execution_direction_band": 0.3,
    "execution_min_signal_strength": 0.5,
    "execution_max_position_pct": 0.02,
    "execution_max_positions": 5,
    "execution_max_exposure": 0.2,
}


def _use_exec_rc(app, overrides=None):
    """Replace the app's runtime_config mock to return execution_* keys too."""
    data = {**_DEFAULT_EXEC_RC, **(overrides or {})}
    app["runtime_config"].get = AsyncMock(side_effect=lambda key: data[key])


def _base_new_job_body(**extra):
    body = {
        "name": "",
        "symbols": ["SPY"],
        "timeframe": "1m",
        "horizons": [5],
        "range_start": "2025-01-01T00:00:00+00:00",
        "range_end":   "2025-01-02T00:00:00+00:00",
        "train_days": 1,
        "test_days": 1,
        "embargo_min": 5,
        "mode": "rolling",
    }
    body.update(extra)
    return body


@pytest.mark.asyncio
async def test_create_without_overrides_snapshots_runtime_config(
    aiohttp_client, app,
):
    _use_exec_rc(app)
    app["jobs_db"].insert_job = AsyncMock(return_value=1)
    client = await aiohttp_client(app)
    resp = await client.post("/api/backtests", json=_base_new_job_body())
    assert resp.status == 201
    call_kwargs = app["jobs_db"].insert_job.call_args.kwargs
    config = call_kwargs["config"]
    assert config["min_confidence"] == 0.6
    assert config["min_agreement"] == 2
    assert config["direction_band"] == 0.3
    assert config["min_signal_strength"] == 0.5
    assert config["max_position_pct"] == 0.02
    assert config["max_positions"] == 5
    assert config["max_exposure"] == 0.2


@pytest.mark.asyncio
async def test_create_with_valid_overrides_persists_them(aiohttp_client, app):
    _use_exec_rc(app)
    app["jobs_db"].insert_job = AsyncMock(return_value=2)
    client = await aiohttp_client(app)
    body = _base_new_job_body(
        min_confidence=0.8,
        min_agreement=4,
        direction_band=0.25,
        min_signal_strength=0.7,
        max_position_pct=0.05,
        max_positions=3,
        max_exposure=0.1,
    )
    resp = await client.post("/api/backtests", json=body)
    assert resp.status == 201
    config = app["jobs_db"].insert_job.call_args.kwargs["config"]
    assert config["min_confidence"] == 0.8
    assert config["min_agreement"] == 4
    assert config["direction_band"] == 0.25
    assert config["min_signal_strength"] == 0.7
    assert config["max_position_pct"] == 0.05
    assert config["max_positions"] == 3
    assert config["max_exposure"] == 0.1


@pytest.mark.asyncio
async def test_create_rejects_out_of_range_override(aiohttp_client, app):
    _use_exec_rc(app)
    client = await aiohttp_client(app)
    resp = await client.post(
        "/api/backtests", json=_base_new_job_body(min_confidence=1.5),
    )
    assert resp.status == 400
    payload = await resp.json()
    assert "min_confidence" in payload["errors"]


@pytest.mark.asyncio
async def test_create_rejects_wrong_type_override(aiohttp_client, app):
    _use_exec_rc(app)
    client = await aiohttp_client(app)
    resp = await client.post(
        "/api/backtests", json=_base_new_job_body(min_agreement=3.5),
    )
    assert resp.status == 400
    payload = await resp.json()
    assert "min_agreement" in payload["errors"]


@pytest.mark.asyncio
async def test_create_partial_overrides_fill_rest_from_runtime_config(
    aiohttp_client, app,
):
    _use_exec_rc(app)
    app["jobs_db"].insert_job = AsyncMock(return_value=3)
    client = await aiohttp_client(app)
    resp = await client.post(
        "/api/backtests", json=_base_new_job_body(min_confidence=0.9),
    )
    assert resp.status == 201
    config = app["jobs_db"].insert_job.call_args.kwargs["config"]
    assert config["min_confidence"] == 0.9         # user-supplied
    assert config["min_agreement"] == 2            # snapshot
    assert config["direction_band"] == 0.3         # snapshot
    assert config["max_position_pct"] == 0.02      # snapshot
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest bullforge/tests/test_dashboard_backtests_api.py -k "overrides or snapshot" -v`
Expected: FAIL — the 7 threshold keys aren't in the persisted `config` (they're stripped or ignored).

- [ ] **Step 3: Add threshold validation + snapshot-fill in `backtests_api.py`**

In `bullforge/services/dashboard_service/backtests_api.py`, at the top of the file (after existing imports), add:

```python
from shared.runtime_config import CONFIG_SCHEMA

THRESHOLD_KEYS = (
    "min_confidence",
    "min_agreement",
    "direction_band",
    "min_signal_strength",
    "max_position_pct",
    "max_positions",
    "max_exposure",
)


def _validate_threshold_overrides(body: dict) -> dict:
    errors: dict[str, str] = {}
    for key in THRESHOLD_KEYS:
        v = body.get(key)
        if v is None:
            continue
        meta = CONFIG_SCHEMA[f"execution_{key}"]
        t = meta["type"]
        if t == "int":
            if not isinstance(v, int) or isinstance(v, bool):
                errors[key] = "must be integer"
                continue
        elif t == "float":
            if not isinstance(v, (int, float)) or isinstance(v, bool):
                errors[key] = "must be number"
                continue
        if "min" in meta and v < meta["min"]:
            errors[key] = f"must be >= {meta['min']}"
        elif "max" in meta and v > meta["max"]:
            errors[key] = f"must be <= {meta['max']}"
    return errors
```

Then locate the `_create` handler and modify it so that, after the existing `if errors: return ...` block, it validates threshold overrides, snapshots blanks from runtime_config, and includes the keys in `config`. Replace the whole `_create` handler (lines 165-200) with:

```python
async def _create(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"error": "bad json"}, status=400)

    rc = request.app["runtime_config"]
    timeframe = body.get("timeframe", "1m")
    horizons_key = (
        f"label_horizons_{timeframe}"
        if timeframe in VALID_TIMEFRAMES
        else "label_horizons_1m"
    )
    valid_horizons = set(await rc.get(horizons_key))
    errors = _validate_new_job(body, valid_horizons)
    if errors:
        return web.json_response({"errors": errors}, status=400)

    threshold_errors = _validate_threshold_overrides(body)
    if threshold_errors:
        return web.json_response({"errors": threshold_errors}, status=400)

    # Snapshot blanks from runtime_config at creation time — reproducibility.
    for key in THRESHOLD_KEYS:
        if body.get(key) is None:
            body[key] = await rc.get(f"execution_{key}")

    jobs = request.app["jobs_db"]
    config = {k: body[k] for k in (
        "symbols", "timeframe", "horizons",
        "range_start", "range_end",
        "train_days", "test_days", "embargo_min", "mode",
        *THRESHOLD_KEYS,
    )}
    custom_name = (body.get("name") or "").strip() or _random_custom_name()
    full_name = _compose_full_name(
        custom=custom_name,
        symbols=config["symbols"],
        timeframe=config["timeframe"],
        horizons=config["horizons"],
        range_start=config["range_start"],
        range_end=config["range_end"],
    )
    job_id = await jobs.insert_job(
        name=full_name, custom_name=custom_name, config=config,
    )
    return web.json_response(
        {"job_id": job_id, "name": full_name, "custom_name": custom_name},
        status=201,
    )
```

- [ ] **Step 4: Run tests to verify pass**

Run: `pytest bullforge/tests/test_dashboard_backtests_api.py -v`
Expected: PASS for all tests (5 new + existing).

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/dashboard_service/backtests_api.py bullforge/tests/test_dashboard_backtests_api.py
git commit -m "feat(backtests_api): accept threshold overrides, snapshot-fill from runtime_config"
```

---

## Task 8: Add "Advanced" threshold overrides section to the new-backtest modal

**Files:**
- Modify: `bullforge/services/dashboard_service/static/backtests.js`

- [ ] **Step 1: Add the Advanced section to the modal HTML**

In `bullforge/services/dashboard_service/static/backtests.js`, locate the `backdrop.innerHTML = ...` block inside `openNewModal()` (around line 147-191). Immediately after the `<div class="bt-form-row">` that contains `#f-train / #f-test / #f-embargo` (ends at line 185) and before `<div id="f-errors">`, add:

```html
        <details class="bt-advanced">
          <summary>Advanced — Entry thresholds (optional overrides)</summary>
          <p class="bt-advanced-help">
            Blank fields snapshot the current live values at submit time, so
            the backtest reproduces those exact thresholds even if live config
            changes later.
          </p>
          <div class="bt-form-row">
            <label>Min Prediction Confidence (0-1)
              <input id="f-min_confidence" type="number" step="0.01" min="0" max="1">
            </label>
            <label>Min Agreeing Models
              <input id="f-min_agreement" type="number" step="1" min="1" max="10000">
            </label>
          </div>
          <div class="bt-form-row">
            <label>Direction Neutral Band (0-1)
              <input id="f-direction_band" type="number" step="0.01" min="0" max="1">
            </label>
            <label>Min Signal Strength (0-1)
              <input id="f-min_signal_strength" type="number" step="0.01" min="0" max="1">
            </label>
          </div>
          <div class="bt-form-row">
            <label>Max Position Size (fraction of equity)
              <input id="f-max_position_pct" type="number" step="0.01" min="0" max="1">
            </label>
            <label>Max Concurrent Positions
              <input id="f-max_positions" type="number" step="1" min="1" max="100">
            </label>
            <label>Max Portfolio Exposure (fraction of equity)
              <input id="f-max_exposure" type="number" step="0.01" min="0" max="1">
            </label>
          </div>
        </details>
```

- [ ] **Step 2: Pre-fill placeholders from `/api/config`**

Still inside `openNewModal()`, after the existing `const cfg = await fetchJSON('/api/config');` block (around lines 128-135), extract the live threshold values. Find:

```javascript
    const horizonsByTf = { '1m': HORIZON_FALLBACK_1M, '1d': HORIZON_FALLBACK_1D };
    try {
      const cfg = await fetchJSON('/api/config');
      const v = cfg.values || {};
      if (Array.isArray(v.label_horizons_1m) && v.label_horizons_1m.length)
        horizonsByTf['1m'] = v.label_horizons_1m;
      if (Array.isArray(v.label_horizons_1d) && v.label_horizons_1d.length)
        horizonsByTf['1d'] = v.label_horizons_1d;
    } catch (_) { /* keep fallbacks */ }
```

Replace with:

```javascript
    const horizonsByTf = { '1m': HORIZON_FALLBACK_1M, '1d': HORIZON_FALLBACK_1D };
    let liveThresholds = {};
    try {
      const cfg = await fetchJSON('/api/config');
      const v = cfg.values || {};
      if (Array.isArray(v.label_horizons_1m) && v.label_horizons_1m.length)
        horizonsByTf['1m'] = v.label_horizons_1m;
      if (Array.isArray(v.label_horizons_1d) && v.label_horizons_1d.length)
        horizonsByTf['1d'] = v.label_horizons_1d;
      liveThresholds = {
        min_confidence: v.execution_min_confidence,
        min_agreement: v.execution_min_agreement,
        direction_band: v.execution_direction_band,
        min_signal_strength: v.execution_min_signal_strength,
        max_position_pct: v.execution_max_position_pct,
        max_positions: v.execution_max_positions,
        max_exposure: v.execution_max_exposure,
      };
    } catch (_) { /* keep fallbacks */ }
```

- [ ] **Step 3: Apply placeholders after the modal is in the DOM**

In `openNewModal()`, after `document.body.appendChild(backdrop);` (around line 192) and before the existing `const close = ...` line, add:

```javascript
    const THRESHOLD_KEYS = [
      'min_confidence', 'min_agreement', 'direction_band',
      'min_signal_strength', 'max_position_pct', 'max_positions',
      'max_exposure',
    ];
    THRESHOLD_KEYS.forEach(k => {
      const el = backdrop.querySelector(`#f-${k}`);
      if (el && liveThresholds[k] !== undefined && liveThresholds[k] !== null) {
        el.placeholder = `live: ${liveThresholds[k]}`;
      }
    });
```

- [ ] **Step 4: Include filled overrides in the submit payload**

Find the submit block (around lines 227-239):

```javascript
    backdrop.querySelector('#f-submit').onclick = async () => {
      const body = {
        name: backdrop.querySelector('#f-name').value.trim(),
        symbols: [...backdrop.querySelector('#f-symbols').selectedOptions].map(o => o.value),
        timeframe: backdrop.querySelector('#f-timeframe').value,
        horizons: [...horizonsSel.selectedOptions].map(o => Number(o.value)),
        range_start: new Date(backdrop.querySelector('#f-start').value + 'Z').toISOString(),
        range_end:   new Date(backdrop.querySelector('#f-end').value + 'Z').toISOString(),
        train_days: Number(backdrop.querySelector('#f-train').value),
        test_days:  Number(backdrop.querySelector('#f-test').value),
        embargo_min: Number(embargoIn.value),
        mode: backdrop.querySelector('#f-mode').value,
      };
```

Immediately after the `};` that closes the `body` object, add:

```javascript
      THRESHOLD_KEYS.forEach(k => {
        const el = backdrop.querySelector(`#f-${k}`);
        const raw = el && el.value.trim();
        if (raw !== undefined && raw !== '') {
          body[k] = Number(raw);
        }
      });
```

- [ ] **Step 5: Manual smoke-check (no unit test for JS here — covered by Task 7's API tests)**

Start the dashboard locally, open New Backtest modal, expand Advanced section:
- Placeholders should show "live: 0.6", "live: 2", "live: 0.3", etc.
- Leave blank → submit → verify created job in DB has the live snapshot values in config_json.
- Fill `Min Prediction Confidence` = 0.85 → submit → verify that field equals 0.85 in config_json.

If local infra isn't running, skip manual smoke and rely on the API layer tests from Task 7 to prove snapshot-fill works. Note in PR description that modal was not manually verified if so.

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/dashboard_service/static/backtests.js
git commit -m "feat(dashboard): threshold overrides in new-backtest modal (Advanced section)"
```

---

## Task 9: Remove env vars and cleanup

**Files:**
- Modify: `bullforge/shared/config.py:119-136` (drop 6 fields)
- Modify: `bullforge/.env.example:26-31` (drop 6 lines)
- Modify: `bullforge/tests/test_config.py:57-80` (drop 2 tests)

- [ ] **Step 1: Delete the 6 execution_* fields from `Settings`**

In `bullforge/shared/config.py`, locate the Execution service block (lines 119-136). Remove these field declarations:

```python
    execution_max_position_pct: float = field(
        default_factory=lambda: float(os.getenv("EXECUTION_MAX_POSITION_PCT", "0.10"))
    )
    execution_max_positions: int = field(
        default_factory=lambda: int(os.getenv("EXECUTION_MAX_POSITIONS", "5"))
    )
    execution_max_exposure: float = field(
        default_factory=lambda: float(os.getenv("EXECUTION_MAX_EXPOSURE", "0.5"))
    )
    execution_min_signal_strength: float = field(
        default_factory=lambda: float(os.getenv("EXECUTION_MIN_SIGNAL_STRENGTH", "0.5"))
    )
    execution_min_confidence: float = field(
        default_factory=lambda: float(os.getenv("EXECUTION_MIN_CONFIDENCE", "0.6"))
    )
    execution_min_agreement: int = field(
        default_factory=lambda: int(os.getenv("EXECUTION_MIN_AGREEMENT", "2"))
    )
```

Leave the three `execution_reconciler_*` fields below them intact.

- [ ] **Step 2: Delete the 6 env lines from `.env.example`**

In `bullforge/.env.example`, delete lines 26-31:

```
EXECUTION_MAX_POSITION_PCT=0.10
EXECUTION_MAX_POSITIONS=5
EXECUTION_MAX_EXPOSURE=0.5
EXECUTION_MIN_SIGNAL_STRENGTH=0.5
EXECUTION_MIN_CONFIDENCE=0.6
EXECUTION_MIN_AGREEMENT=2
```

- [ ] **Step 3: Delete the 2 tests in `test_config.py`**

In `bullforge/tests/test_config.py`, delete these two test functions entirely:

```python
def test_execution_defaults(settings):
    assert settings.execution_max_position_pct == 0.10
    assert settings.execution_max_positions == 5
    assert settings.execution_max_exposure == 0.5
    assert settings.execution_min_signal_strength == 0.5
    assert settings.execution_min_confidence == 0.6
    assert settings.execution_min_agreement == 2


def test_execution_env_override(monkeypatch):
    monkeypatch.setenv("EXECUTION_MAX_POSITION_PCT", "0.20")
    monkeypatch.setenv("EXECUTION_MAX_POSITIONS", "10")
    monkeypatch.setenv("EXECUTION_MAX_EXPOSURE", "0.8")
    monkeypatch.setenv("EXECUTION_MIN_SIGNAL_STRENGTH", "0.7")
    monkeypatch.setenv("EXECUTION_MIN_CONFIDENCE", "0.75")
    monkeypatch.setenv("EXECUTION_MIN_AGREEMENT", "3")
    from shared.config import Settings
    s = Settings()
    assert s.execution_max_position_pct == 0.20
    assert s.execution_max_positions == 10
    assert s.execution_max_exposure == 0.8
    assert s.execution_min_signal_strength == 0.7
    assert s.execution_min_confidence == 0.75
    assert s.execution_min_agreement == 3
```

- [ ] **Step 4: Run the full test suite for the affected modules**

Run:
```
pytest bullforge/tests/test_config.py bullforge/tests/test_signal_aggregator.py bullforge/tests/test_risk_manager.py bullforge/tests/test_runtime_config.py bullforge/tests/test_backtest_worker.py bullforge/tests/test_backtest_engine_overrides.py bullforge/tests/test_dashboard_backtests_api.py -v
```
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/config.py bullforge/.env.example bullforge/tests/test_config.py
git commit -m "chore: remove EXECUTION_* env vars (migrated to runtime_config)"
```

---

## Post-implementation verification

Outside the TDD loop, run these whole-repo checks before opening a PR:

- [ ] Full test suite passes (or the only failures are the same integration tests that fail without local Timescale/Redis infra — compare against `master` to confirm no regression): `pytest bullforge/tests/ -q`
- [ ] Grep for any stragglers:
  - `grep -rn "execution_min_confidence\|execution_min_agreement\|execution_min_signal_strength\|execution_max_position_pct\|execution_max_positions\|execution_max_exposure" bullforge/` — should show only `runtime_config.py` schema, the `_param` lookups in signal_aggregator / risk_manager, the test files, and the backtest engine/worker/api.
  - `grep -rn "EXECUTION_MIN\|EXECUTION_MAX" bullforge/` — should return nothing.
- [ ] Config page loads and shows the 7 new keys with their labels and descriptions (manual if infra available; otherwise note in PR).
- [ ] New-backtest modal Advanced section expands, placeholders show live values, submit succeeds (manual).

When green: push the branch and open a PR to `master`.
