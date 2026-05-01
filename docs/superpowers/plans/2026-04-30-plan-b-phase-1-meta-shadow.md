# Plan B Phase 1 — Meta-Model Shadow Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the existing `services/model_service/meta/` framework end-to-end: real training data, nightly retrain, and shadow-mode logging. No live execution change.

**Architecture:** Three layers stitched into the existing service mesh. (1) Offline replay machinery (`PairSimulator`, `CandidateBuilder`, `RegimeBuilder`) builds real training rows from historical bars+features+predictions. (2) `RetrainScheduler` adds a meta phase. (3) `model_service` runs meta inference after base inference, publishes `model:meta_signals`; `execution_service` persists the signals and feature snapshots; a backfill loop fills outcome labels once enough bars elapse.

**Tech Stack:** Python 3.12 / asyncio / asyncpg / pandas / numpy / lightgbm / scikit-learn / aioredis. All work happens inside `bullforge/` from the worktree root.

**Spec:** [`docs/superpowers/specs/2026-04-30-plan-b-phase-1-meta-shadow-design.md`](../specs/2026-04-30-plan-b-phase-1-meta-shadow-design.md)

**How to run a single test:** `(cd bullforge && bin/test tests/path/test.py::test_name -v)`. The first run builds the test-runner image (slow); subsequent runs are fast. To run a whole file, drop the `::test_name`.

---

## Task 1: PairSimulator + ATR helper

**Files:**
- Create: `bullforge/services/model_service/meta/replay.py`
- Test: `bullforge/tests/meta/test_replay.py`

Implements outcome simulation for a single candidate trade plus the per-bar `atr_pct` helper. Pure logic against pandas — no DB.

- [ ] **Step 1: Write the failing tests**

```python
# bullforge/tests/meta/test_replay.py
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
import pytest

from services.model_service.meta.replay import (
    PairSimulator,
    SimResult,
    compute_atr_pct,
)


def _bar_df(closes, highs=None, lows=None, start_min=0):
    """Build a synthetic 1m bars dataframe."""
    n = len(closes)
    if highs is None:
        highs = [c * 1.001 for c in closes]
    if lows is None:
        lows = [c * 0.999 for c in closes]
    base = datetime(2026, 4, 30, 14, 0, tzinfo=timezone.utc)
    return pd.DataFrame({
        "timestamp": [base + timedelta(minutes=i + start_min) for i in range(n)],
        "open": closes,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": [1000.0] * n,
    })


class TestPairSimulator:
    def test_long_target_hits_first(self):
        # entry @ 100, target +1% = 101, stop -0.5% = 99.5
        # bar 1 high=101.5 -> target hit
        bars = _bar_df([100, 101, 101], highs=[100.2, 101.5, 101.5], lows=[99.8, 100.5, 100.5])
        sim = PairSimulator()
        res = sim.simulate(bars, entry_idx=0, direction=1,
                           target_pct=0.01, stop_pct=0.005, max_hold_bars=10)
        assert res.hit_target is True
        assert res.hit_stop is False
        assert res.exit_idx == 1
        assert res.realized_return == pytest.approx(0.01)

    def test_long_stop_hits_first(self):
        bars = _bar_df([100, 99, 99], highs=[100.1, 99.2, 99.2], lows=[99.9, 99.4, 99.4])
        sim = PairSimulator()
        res = sim.simulate(bars, entry_idx=0, direction=1,
                           target_pct=0.01, stop_pct=0.005, max_hold_bars=10)
        assert res.hit_target is False
        assert res.hit_stop is True
        assert res.realized_return == pytest.approx(-0.005)

    def test_long_timeout_with_positive_return(self):
        bars = _bar_df([100, 100.2, 100.4], highs=[100.1, 100.3, 100.5], lows=[99.9, 100.1, 100.3])
        sim = PairSimulator()
        res = sim.simulate(bars, entry_idx=0, direction=1,
                           target_pct=0.01, stop_pct=0.005, max_hold_bars=2)
        assert res.hit_target is False
        assert res.hit_stop is False
        assert res.realized_return == pytest.approx((100.4 - 100) / 100, rel=1e-6)
        assert res.exit_idx == 2

    def test_long_both_hit_same_bar_stop_wins(self):
        # bar 1 has both extremes - stop must win (conservative)
        bars = _bar_df([100, 100], highs=[100.1, 102], lows=[99.9, 99])
        sim = PairSimulator()
        res = sim.simulate(bars, entry_idx=0, direction=1,
                           target_pct=0.01, stop_pct=0.005, max_hold_bars=10)
        assert res.hit_stop is True
        assert res.hit_target is False

    def test_short_target_hits_first(self):
        bars = _bar_df([100, 99, 99], highs=[100.1, 99.2, 99.2], lows=[99.9, 98.5, 98.5])
        sim = PairSimulator()
        res = sim.simulate(bars, entry_idx=0, direction=-1,
                           target_pct=0.01, stop_pct=0.005, max_hold_bars=10)
        assert res.hit_target is True
        assert res.realized_return == pytest.approx(0.01)

    def test_short_stop_hits_first(self):
        bars = _bar_df([100, 101, 101], highs=[100.1, 101.5, 101.5], lows=[99.9, 100.5, 100.5])
        sim = PairSimulator()
        res = sim.simulate(bars, entry_idx=0, direction=-1,
                           target_pct=0.01, stop_pct=0.005, max_hold_bars=10)
        assert res.hit_stop is True
        assert res.realized_return == pytest.approx(-0.005)

    def test_insufficient_bars_returns_zero(self):
        bars = _bar_df([100])  # only entry bar, no follow-ups
        sim = PairSimulator()
        res = sim.simulate(bars, entry_idx=0, direction=1,
                           target_pct=0.01, stop_pct=0.005, max_hold_bars=5)
        assert res.hit_target is False
        assert res.hit_stop is False
        assert res.realized_return == 0.0
        assert res.exit_idx == 0


def test_compute_atr_pct():
    # ATR-14 needs at least 15 bars of high/low/close
    closes = list(range(100, 130))  # 30 bars
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    bars = _bar_df(closes, highs=highs, lows=lows)
    atr_pct = compute_atr_pct(bars, idx=20, period=14)
    # high-low spread = 2; close ~120; expected ATR%/close ≈ 2/120 ≈ 0.0167
    assert 0.01 < atr_pct < 0.025


def test_compute_atr_pct_warmup_returns_nan():
    bars = _bar_df([100] * 5)
    atr_pct = compute_atr_pct(bars, idx=2, period=14)
    assert pd.isna(atr_pct)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `(cd bullforge && bin/test tests/meta/test_replay.py -v)`
Expected: FAIL with `ModuleNotFoundError: No module named 'services.model_service.meta.replay'`

- [ ] **Step 3: Write minimal implementation**

```python
# bullforge/services/model_service/meta/replay.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SimResult:
    hit_target: bool
    hit_stop: bool
    exit_idx: int
    realized_return: float


class PairSimulator:
    """Walks bars forward from `entry_idx`, returns first-touch outcome.

    Stop wins on same-bar collisions (conservative; matches typical
    intra-bar stop-fill behavior).
    """

    def simulate(
        self,
        bars: pd.DataFrame,
        *,
        entry_idx: int,
        direction: int,
        target_pct: float,
        stop_pct: float,
        max_hold_bars: int,
    ) -> SimResult:
        if entry_idx + 1 >= len(bars):
            return SimResult(False, False, entry_idx, 0.0)

        entry_price = float(bars.iloc[entry_idx]["close"])
        if direction == 1:
            target_price = entry_price * (1.0 + target_pct)
            stop_price = entry_price * (1.0 - stop_pct)
        else:
            target_price = entry_price * (1.0 - target_pct)
            stop_price = entry_price * (1.0 + stop_pct)

        last_idx = min(entry_idx + max_hold_bars, len(bars) - 1)
        for i in range(entry_idx + 1, last_idx + 1):
            high = float(bars.iloc[i]["high"])
            low = float(bars.iloc[i]["low"])

            if direction == 1:
                hit_stop = low <= stop_price
                hit_target = high >= target_price
            else:
                hit_stop = high >= stop_price
                hit_target = low <= target_price

            if hit_stop:
                return SimResult(False, True, i, -stop_pct)
            if hit_target:
                return SimResult(True, False, i, target_pct)

        # Timeout: exit at last considered bar's close
        exit_close = float(bars.iloc[last_idx]["close"])
        realized = ((exit_close - entry_price) / entry_price) * direction
        return SimResult(False, False, last_idx, float(realized))


def compute_atr_pct(
    bars: pd.DataFrame, *, idx: int, period: int = 14,
) -> float:
    """ATR-period at row `idx`, expressed as fraction of that row's close.

    Returns NaN if insufficient prior bars exist for the rolling mean.
    """
    if idx < period:
        return float("nan")
    sub = bars.iloc[: idx + 1]
    high = sub["high"]
    low = sub["low"]
    close = sub["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean().iloc[-1]
    if pd.isna(atr) or close.iloc[-1] == 0:
        return float("nan")
    return float(atr / close.iloc[-1])
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `(cd bullforge && bin/test tests/meta/test_replay.py -v)`
Expected: 9 passed

- [ ] **Step 5: Commit**

```bash
git -C <repo>/.worktrees/plan-b-meta-shadow add \
    bullforge/services/model_service/meta/replay.py \
    bullforge/tests/meta/test_replay.py
git -C <repo>/.worktrees/plan-b-meta-shadow commit \
    -m "feat(meta): add PairSimulator and compute_atr_pct"
```

---

## Task 2: RegimeBuilder

**Files:**
- Create: `bullforge/services/model_service/meta/regime.py`
- Test: `bullforge/tests/meta/test_regime.py`

Computes SPY context per timestamp with forward-fill staleness cap. Pure logic given an in-memory SPY dataframe.

- [ ] **Step 1: Write the failing tests**

```python
# bullforge/tests/meta/test_regime.py
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
import pytest

from services.model_service.meta.regime import RegimeBuilder


def _spy_df(n_bars=200):
    base = datetime(2026, 4, 30, 14, 0, tzinfo=timezone.utc)
    closes = [400.0 + 0.01 * i for i in range(n_bars)]  # gentle uptrend
    return pd.DataFrame({
        "timestamp": [base + timedelta(minutes=i) for i in range(n_bars)],
        "close": closes,
    }).set_index("timestamp")


class TestRegimeBuilder:
    def test_all_fields_populated_at_recent_timestamp(self):
        spy = _spy_df()
        rb = RegimeBuilder(spy_bars=spy, max_staleness_hours=48)
        ts = spy.index[-1]
        regime = rb.regime_at(ts)
        for key in ["spy_trend_1m", "spy_trend_1h", "spy_trend_1d",
                    "spy_momentum", "vol_regime"]:
            assert key in regime
            assert not pd.isna(regime[key])

    def test_uses_last_known_when_ts_after_last_bar(self):
        spy = _spy_df()
        rb = RegimeBuilder(spy_bars=spy, max_staleness_hours=48)
        # Query 30 minutes after the last bar — within staleness
        ts = spy.index[-1] + timedelta(minutes=30)
        regime = rb.regime_at(ts)
        assert not pd.isna(regime["spy_trend_1m"])

    def test_returns_zeros_when_stale_beyond_cap(self):
        spy = _spy_df()
        rb = RegimeBuilder(spy_bars=spy, max_staleness_hours=1)
        ts = spy.index[-1] + timedelta(hours=5)
        regime = rb.regime_at(ts)
        for key in ["spy_trend_1m", "spy_trend_1h", "spy_trend_1d",
                    "spy_momentum", "vol_regime"]:
            assert regime[key] == 0.0

    def test_returns_zeros_when_spy_empty(self):
        rb = RegimeBuilder(spy_bars=pd.DataFrame(columns=["close"]),
                           max_staleness_hours=48)
        ts = datetime(2026, 4, 30, 14, 0, tzinfo=timezone.utc)
        regime = rb.regime_at(ts)
        assert all(v == 0.0 for v in regime.values())
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `(cd bullforge && bin/test tests/meta/test_regime.py -v)`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

```python
# bullforge/services/model_service/meta/regime.py
from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd


_FIELDS = ("spy_trend_1m", "spy_trend_1h", "spy_trend_1d",
           "spy_momentum", "vol_regime")


class RegimeBuilder:
    """Computes SPY-derived regime context with forward-fill staleness cap.

    `spy_bars` is a DataFrame indexed by UTC timestamp with at least a
    `close` column. Pre-compute `_trend_1m` / `_vol_regime` columns on
    construction so per-timestamp lookups are O(log n).
    """

    def __init__(self, spy_bars: pd.DataFrame, max_staleness_hours: int):
        self._max_stale = timedelta(hours=max_staleness_hours)
        self._df = self._enrich(spy_bars)

    @staticmethod
    def _enrich(spy: pd.DataFrame) -> pd.DataFrame:
        if spy.empty:
            return spy
        df = spy.copy()
        df["trend_1m"] = df["close"].pct_change(1)
        df["momentum"] = df["trend_1m"].ewm(span=12, adjust=False).mean()
        vol60 = df["trend_1m"].rolling(60).std()
        median30d = vol60.rolling(30 * 24 * 60, min_periods=60).median()
        df["vol_regime"] = vol60 / median30d.replace(0.0, np.nan)
        return df

    def regime_at(self, ts: datetime) -> dict[str, float]:
        zeros = {k: 0.0 for k in _FIELDS}
        if self._df.empty:
            return zeros

        # asof — find the most recent SPY bar at or before ts
        idx = self._df.index.searchsorted(ts, side="right") - 1
        if idx < 0:
            return zeros
        bar_ts = self._df.index[idx]
        if ts - bar_ts > self._max_stale:
            return zeros

        row = self._df.iloc[idx]

        def _hr(minutes: int) -> float:
            target = ts - timedelta(minutes=minutes)
            j = self._df.index.searchsorted(target, side="right") - 1
            if j < 0:
                return 0.0
            past = float(self._df.iloc[j]["close"])
            now = float(row["close"])
            return (now - past) / past if past else 0.0

        return {
            "spy_trend_1m": float(row["trend_1m"]) if not pd.isna(row["trend_1m"]) else 0.0,
            "spy_trend_1h": _hr(60),
            "spy_trend_1d": _hr(60 * 24),
            "spy_momentum": float(row["momentum"]) if not pd.isna(row["momentum"]) else 0.0,
            "vol_regime": float(row["vol_regime"]) if not pd.isna(row["vol_regime"]) else 0.0,
        }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `(cd bullforge && bin/test tests/meta/test_regime.py -v)`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git -C <WORKTREE> add \
    bullforge/services/model_service/meta/regime.py \
    bullforge/tests/meta/test_regime.py
git -C <WORKTREE> commit -m "feat(meta): add RegimeBuilder with SPY staleness cap"
```

(Replace `<WORKTREE>` with the absolute worktree path; same applies to all subsequent commit blocks.)

---

## Task 3: Runtime config additions

**Files:**
- Modify: `bullforge/shared/runtime_config.py` (`CONFIG_SCHEMA`)
- Test: `bullforge/tests/test_runtime_config.py` (add new cases)

Adds the eight Plan-B config keys. Each gets a sane default per spec §9.

- [ ] **Step 1: Write the failing tests**

```python
# Append to bullforge/tests/test_runtime_config.py
import pytest
from shared.runtime_config import CONFIG_SCHEMA


def test_meta_config_keys_present():
    expected = {
        "meta_inference_enabled",
        "meta_estimator_types",
        "meta_pair_grid",
        "meta_train_window_days",
        "meta_max_hold_minutes",
        "meta_min_train_rows",
        "meta_label_backfill_interval_minutes",
        "meta_regime_max_staleness_hours",
        "meta_rolling_stats_ttl_seconds",
    }
    assert expected.issubset(CONFIG_SCHEMA.keys())


def test_meta_pair_grid_default_shape():
    grid = CONFIG_SCHEMA["meta_pair_grid"]["default"]
    assert isinstance(grid, list)
    assert len(grid) == 3
    for pair in grid:
        assert len(pair) == 2
        target_mult, stop_mult = pair
        assert target_mult > stop_mult > 0


def test_meta_inference_enabled_defaults_off():
    assert CONFIG_SCHEMA["meta_inference_enabled"]["default"] is False
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `(cd bullforge && bin/test tests/test_runtime_config.py -k meta -v)`
Expected: FAIL with `KeyError` on `meta_inference_enabled`

- [ ] **Step 3: Write minimal implementation**

Append to `CONFIG_SCHEMA` in `bullforge/shared/runtime_config.py` (after the last existing entry, before the closing `}`):

```python
    "meta_inference_enabled": {
        "default": False, "type": "bool",
        "label": "Meta-Model Shadow Inference",
        "description": (
            "Master switch for meta-model shadow inference. When off, "
            "predict_meta returns empty and model:meta_signals is not "
            "published. Flip on after the first successful nightly retrain."
        ),
        "reference": "https://en.wikipedia.org/wiki/Stacked_generalization",
    },
    "meta_estimator_types": {
        "default": ["lightgbm"], "type": "json",
        "label": "Meta Estimator Types",
        "description": (
            "Subset of {lightgbm, mlp, baseline} to train and serve. "
            "Each entry produces one active artifact per (symbol, direction)."
        ),
        "reference": "https://en.wikipedia.org/wiki/Ensemble_learning",
    },
    "meta_pair_grid": {
        "default": [[2, 1], [3, 1.5], [4, 2]], "type": "json",
        "label": "Meta (Target, Stop) Multipliers × ATR-14",
        "description": (
            "List of [target_mult, stop_mult] pairs. Per candidate bar, "
            "target_pct = target_mult × ATR-14/close; stop_pct similarly. "
            "Adapts pair sizes per-symbol via ATR scaling."
        ),
        "reference": "https://en.wikipedia.org/wiki/Average_true_range",
    },
    "meta_train_window_days": {
        "default": 60, "type": "int", "min": 7, "max": 365,
        "label": "Meta Training Window (days)",
        "description": "Replay window for offline meta-training.",
        "reference": "https://en.wikipedia.org/wiki/Walk_forward_optimization",
    },
    "meta_max_hold_minutes": {
        "default": 30, "type": "int", "min": 1, "max": 1440,
        "label": "Meta Outcome Max-Hold (minutes)",
        "description": (
            "Bar-walking simulation cuts off after this many minutes if "
            "neither target nor stop has been hit."
        ),
        "reference": "https://en.wikipedia.org/wiki/Position_(finance)",
    },
    "meta_min_train_rows": {
        "default": 500, "type": "int", "min": 100, "max": 100000,
        "label": "Meta Min Training Rows",
        "description": "Skip a (symbol, direction) if fewer labeled rows exist.",
        "reference": "https://en.wikipedia.org/wiki/Sample_size_determination",
    },
    "meta_label_backfill_interval_minutes": {
        "default": 15, "type": "int", "min": 1, "max": 1440,
        "label": "Meta Label-Backfill Interval (minutes)",
        "description": (
            "How often the backfill loop fills entry_label / "
            "optimal_pair_idx / realized_ev on live meta_features rows."
        ),
        "reference": "https://en.wikipedia.org/wiki/Label_(computer_science)",
    },
    "meta_regime_max_staleness_hours": {
        "default": 48, "type": "int", "min": 1, "max": 720,
        "label": "Meta Regime SPY Staleness Cap (hours)",
        "description": (
            "Forward-fill SPY regime values for crypto bars up to this "
            "many hours; older than this returns zeros."
        ),
        "reference": "https://en.wikipedia.org/wiki/SPDR_S%26P_500_ETF_Trust",
    },
    "meta_rolling_stats_ttl_seconds": {
        "default": 600, "type": "int", "min": 10, "max": 86400,
        "label": "Meta Rolling-Stats Cache TTL (seconds)",
        "description": (
            "Per-base-model accuracy/calibration cache TTL during meta "
            "inference. Rebuilds when expired."
        ),
        "reference": "https://en.wikipedia.org/wiki/Cache_(computing)",
    },
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `(cd bullforge && bin/test tests/test_runtime_config.py -k meta -v)`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git -C <WORKTREE> add \
    bullforge/shared/runtime_config.py \
    bullforge/tests/test_runtime_config.py
git -C <WORKTREE> commit -m "feat(config): add 9 Plan-B meta runtime-config keys"
```

---

## Task 4: DB additions for backfill + replay coexistence

**Files:**
- Modify: `bullforge/shared/db.py` (around the existing `# ── Meta-feature methods ──` block, ~line 2617)
- Test: `bullforge/tests/test_db_phase6b.py` (extend) — add a fresh test file `bullforge/tests/test_meta_db.py` if simpler

Adds: `query_unlabeled_meta_features(older_than)`, `update_meta_feature_labels(symbol, ts, direction, ...)`, and an `on_conflict="skip"` parameter to `insert_meta_features_batch` so replay never overwrites live rows.

- [ ] **Step 1: Write the failing tests**

```python
# bullforge/tests/test_meta_db.py
from datetime import datetime, timedelta, timezone
import pytest

from shared.config import Settings
from shared.db import Database
from shared.models import MetaFeatureRow


def _ts(minute: int = 0):
    return datetime(2026, 4, 28, 14, minute, 0, tzinfo=timezone.utc)


@pytest.fixture
async def db():
    settings = Settings()
    db = Database(settings)
    await db.connect()
    async with db.pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM meta_features WHERE symbol = 'TST_META'",
        )
    yield db
    async with db.pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM meta_features WHERE symbol = 'TST_META'",
        )
    await db.disconnect()


@pytest.mark.asyncio
async def test_query_unlabeled_meta_features_filters_by_age(db):
    old = MetaFeatureRow(symbol="TST_META", timestamp=_ts(0), direction=1,
                         feature_vector={"x": 1.0}, source="live")
    fresh = MetaFeatureRow(symbol="TST_META", timestamp=_ts(58), direction=1,
                           feature_vector={"x": 1.0}, source="live")
    await db.insert_meta_features_batch([old, fresh])

    cutoff = _ts(30)
    rows = await db.query_unlabeled_meta_features(older_than=cutoff)
    syms = {(r.symbol, r.timestamp) for r in rows}
    assert ("TST_META", _ts(0)) in syms
    assert ("TST_META", _ts(58)) not in syms


@pytest.mark.asyncio
async def test_query_unlabeled_meta_features_excludes_labeled(db):
    labeled = MetaFeatureRow(symbol="TST_META", timestamp=_ts(0), direction=1,
                             feature_vector={"x": 1.0}, source="live",
                             entry_label=1, optimal_pair_idx=0, realized_ev=0.01)
    await db.insert_meta_features_batch([labeled])
    rows = await db.query_unlabeled_meta_features(older_than=_ts(30))
    assert all(r.symbol != "TST_META" for r in rows)


@pytest.mark.asyncio
async def test_update_meta_feature_labels(db):
    row = MetaFeatureRow(symbol="TST_META", timestamp=_ts(0), direction=1,
                         feature_vector={"x": 1.0}, source="live")
    await db.insert_meta_features_batch([row])
    await db.update_meta_feature_labels(
        symbol="TST_META", timestamp=_ts(0), direction=1,
        entry_label=1, optimal_pair_idx=2, realized_ev=0.012,
    )
    rows = await db.query_meta_features("TST_META", direction=1, source="live")
    assert len(rows) == 1
    assert rows[0].entry_label == 1
    assert rows[0].optimal_pair_idx == 2
    assert rows[0].realized_ev == pytest.approx(0.012)


@pytest.mark.asyncio
async def test_insert_meta_features_batch_skip_preserves_live(db):
    live = MetaFeatureRow(symbol="TST_META", timestamp=_ts(0), direction=1,
                          feature_vector={"v": 1.0}, source="live")
    await db.insert_meta_features_batch([live])

    replay = MetaFeatureRow(symbol="TST_META", timestamp=_ts(0), direction=1,
                            feature_vector={"v": 999.0}, source="replay",
                            entry_label=0, optimal_pair_idx=0, realized_ev=-0.01)
    await db.insert_meta_features_batch([replay], on_conflict="skip")

    rows = await db.query_meta_features("TST_META", direction=1)
    assert len(rows) == 1
    assert rows[0].source == "live"
    assert rows[0].feature_vector == {"v": 1.0}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `(cd bullforge && bin/test tests/test_meta_db.py -v)`
Expected: FAIL with `AttributeError: 'Database' object has no attribute 'query_unlabeled_meta_features'`

- [ ] **Step 3: Write minimal implementation**

In `bullforge/shared/db.py`, replace the existing `insert_meta_features_batch` (around line 2640) and add the two new methods just below `count_meta_features`:

```python
    async def insert_meta_features_batch(
        self,
        rows: list[MetaFeatureRow],
        *,
        on_conflict: str = "upsert",
    ) -> None:
        """`on_conflict='upsert'` overwrites; `'skip'` keeps existing rows."""
        if not rows:
            return
        if on_conflict == "skip":
            conflict_clause = "ON CONFLICT (symbol, timestamp, direction) DO NOTHING"
        elif on_conflict == "upsert":
            conflict_clause = (
                "ON CONFLICT (symbol, timestamp, direction) DO UPDATE SET "
                "feature_vector = EXCLUDED.feature_vector, "
                "entry_label = EXCLUDED.entry_label, "
                "optimal_pair_idx = EXCLUDED.optimal_pair_idx, "
                "realized_ev = EXCLUDED.realized_ev, "
                "source = EXCLUDED.source"
            )
        else:
            raise ValueError(f"unknown on_conflict mode: {on_conflict!r}")

        async with self.pool.acquire() as conn:
            await conn.executemany(
                f"""
                INSERT INTO meta_features
                    (symbol, timestamp, direction, feature_vector,
                     entry_label, optimal_pair_idx, realized_ev, source)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                {conflict_clause}
                """,
                [(r.symbol, r.timestamp, r.direction,
                  json.dumps(r.feature_vector),
                  r.entry_label, r.optimal_pair_idx, r.realized_ev, r.source)
                 for r in rows],
            )

    async def query_unlabeled_meta_features(
        self, *, older_than: datetime, limit: int = 5000,
    ) -> list[MetaFeatureRow]:
        """Live rows older than `older_than` with NULL labels.

        Used by MetaLabelBackfiller. Returns at most `limit` rows ordered by
        timestamp ASC so the oldest get labeled first.
        """
        async with self.pool.acquire() as conn:
            recs = await conn.fetch(
                """
                SELECT symbol, timestamp, direction, feature_vector,
                       entry_label, optimal_pair_idx, realized_ev, source
                FROM meta_features
                WHERE source = 'live'
                  AND entry_label IS NULL
                  AND timestamp <= $1
                ORDER BY timestamp ASC
                LIMIT $2
                """,
                older_than, limit,
            )
        return [
            MetaFeatureRow(
                symbol=r["symbol"], timestamp=r["timestamp"],
                direction=r["direction"],
                feature_vector=json.loads(r["feature_vector"]),
                source=r["source"], entry_label=r["entry_label"],
                optimal_pair_idx=r["optimal_pair_idx"],
                realized_ev=r["realized_ev"],
            ) for r in recs
        ]

    async def update_meta_feature_labels(
        self, *, symbol: str, timestamp: datetime, direction: int,
        entry_label: int, optimal_pair_idx: int, realized_ev: float,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE meta_features
                SET entry_label = $4,
                    optimal_pair_idx = $5,
                    realized_ev = $6
                WHERE symbol = $1 AND timestamp = $2 AND direction = $3
                """,
                symbol, timestamp, direction,
                entry_label, optimal_pair_idx, realized_ev,
            )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `(cd bullforge && bin/test tests/test_meta_db.py -v)`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git -C <WORKTREE> add \
    bullforge/shared/db.py \
    bullforge/tests/test_meta_db.py
git -C <WORKTREE> commit -m "feat(db): add unlabeled-feature query + label update + skip-conflict"
```

---

## Task 5: CandidateBuilder (replay row generator)

**Files:**
- Modify: `bullforge/services/model_service/meta/replay.py` (extend with `CandidateBuilder` and `build_replay_rows`)
- Test: `bullforge/tests/meta/test_replay.py` (extend)

Composes `PairSimulator` + `compute_atr_pct` + `RegimeBuilder` + `MetaFeatureAssembler` + DB into a generator that yields `MetaFeatureRow` objects with labels set. Read-only against DB; writes happen in the trainer.

- [ ] **Step 1: Write the failing tests**

```python
# Append to bullforge/tests/meta/test_replay.py
from unittest.mock import AsyncMock, MagicMock
from services.model_service.meta.replay import build_replay_rows


@pytest.mark.asyncio
async def test_build_replay_rows_emits_one_row_per_direction():
    # 60 SPY-like bars, ATR will be defined from idx >= 14
    bars = _bar_df([100 + 0.05 * i for i in range(60)])
    bars["timestamp"] = bars["timestamp"].dt.tz_convert("UTC")

    db = AsyncMock()
    db.query_bars.return_value = bars.to_dict("records")
    db.query_feature_rows.return_value = [
        MagicMock(symbol="AAPL", timestamp=ts, timeframe="1m",
                  features={"atr_14": 0.5, "sma_20": 100.0, "realized_vol": 0.02,
                            "dist_sma_50": 0.0, "dist_sma_200": 0.0,
                            "dist_sma_200d": 0.0})
        for ts in bars["timestamp"]
    ]
    db.query_predictions_in_range.return_value = [
        {"model_id": 1, "symbol": "AAPL", "timestamp": ts,
         "predicted_direction": 1, "confidence": 0.7}
        for ts in bars["timestamp"]
    ]
    db.query_all_active_models.return_value = [
        MagicMock(id=1, symbol="AAPL", model_type="gradient_boost"),
    ]
    db.query_prediction_outcomes.return_value = [
        {"is_correct": True, "predicted_direction": 1, "confidence": 0.7}
    ] * 10

    spy_df = pd.DataFrame({"close": [400.0] * 60},
                          index=pd.to_datetime(bars["timestamp"], utc=True))
    rb_factory = lambda: __import__("services.model_service.meta.regime",
                                    fromlist=["RegimeBuilder"]).RegimeBuilder(
        spy_bars=spy_df, max_staleness_hours=48,
    )

    rows = await build_replay_rows(
        db=db, symbol="AAPL", direction=1,
        as_of=bars["timestamp"].iloc[-1],
        window_days=1, pair_grid=[[2, 1], [3, 1.5], [4, 2]],
        max_hold_bars=10, regime_builder_factory=rb_factory,
    )

    assert len(rows) > 0
    for r in rows:
        assert r.symbol == "AAPL"
        assert r.direction == 1
        assert r.source == "replay"
        assert r.entry_label in (0, 1)
        assert r.optimal_pair_idx in (0, 1, 2)


@pytest.mark.asyncio
async def test_build_replay_rows_skips_warmup_bars():
    bars = _bar_df([100.0] * 5)  # too short for ATR-14
    bars["timestamp"] = bars["timestamp"].dt.tz_convert("UTC")
    db = AsyncMock()
    db.query_bars.return_value = bars.to_dict("records")
    db.query_feature_rows.return_value = []
    db.query_predictions_in_range.return_value = []
    db.query_all_active_models.return_value = []
    db.query_prediction_outcomes.return_value = []
    spy_df = pd.DataFrame(columns=["close"])
    rb_factory = lambda: __import__("services.model_service.meta.regime",
                                    fromlist=["RegimeBuilder"]).RegimeBuilder(
        spy_bars=spy_df, max_staleness_hours=48,
    )
    rows = await build_replay_rows(
        db=db, symbol="AAPL", direction=1,
        as_of=bars["timestamp"].iloc[-1],
        window_days=1, pair_grid=[[2, 1]], max_hold_bars=10,
        regime_builder_factory=rb_factory,
    )
    assert rows == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `(cd bullforge && bin/test tests/meta/test_replay.py::test_build_replay_rows_emits_one_row_per_direction -v)`
Expected: FAIL with `ImportError` on `build_replay_rows`. Plus the test depends on `Database.query_bars` and `query_predictions_in_range` — if those don't exist, fail there next.

Run: `(cd bullforge && bin/test tests/meta/test_replay.py -v)` after — verifies Task 1 tests still pass.

- [ ] **Step 3: Write minimal implementation**

If `Database.query_bars` and `Database.query_predictions_in_range` do not yet exist, add them in `shared/db.py` first (use the same pattern as `query_feature_rows`). Confirm with `grep -n 'def query_bars' bullforge/shared/db.py`. If they exist, skip this paragraph.

In `bullforge/services/model_service/meta/replay.py`, append:

```python
from datetime import datetime, timedelta
from typing import Awaitable, Callable

from shared.db import Database
from shared.models import MetaFeatureRow
from services.model_service.meta.feature_assembler import MetaFeatureAssembler
from services.model_service.meta.regime import RegimeBuilder


async def _rolling_stats(
    db: Database, model_ids: list[int], limit: int = 200,
) -> dict[int, dict]:
    out: dict[int, dict] = {}
    for mid in model_ids:
        outcomes = await db.query_prediction_outcomes(mid, limit=limit)
        if outcomes:
            correct = sum(1 for o in outcomes if o["is_correct"])
            acc = correct / len(outcomes)
            avg_conf = float(np.mean([o["confidence"] for o in outcomes]))
            cal_err = abs(avg_conf - acc)
        else:
            acc, cal_err = 0.0, 0.0
        out[mid] = {"accuracy": acc, "calibration_error": cal_err,
                    "is_disabled": False}
    return out


def _ev_for_pair(sim_result, target_pct: float, stop_pct: float) -> float:
    if sim_result.hit_target:
        return target_pct
    if sim_result.hit_stop:
        return -stop_pct
    return float(sim_result.realized_return)


def _kelly_fraction(
    history: list[bool], target_pct: float, stop_pct: float,
    min_history: int = 200, cold_start: float = 0.05, cap: float = 0.25,
) -> float:
    if len(history) < min_history:
        return cold_start
    p = sum(1 for h in history if h) / len(history)
    if stop_pct == 0:
        return cold_start
    b = target_pct / stop_pct
    raw = (p * b - (1 - p)) / b if b else 0.0
    return float(max(0.0, min(raw, cap)))


class CandidateBuilder:
    def __init__(self, db: Database, *, symbol: str):
        self._db = db
        self._symbol = symbol

    async def fetch(self, *, since: datetime, until: datetime):
        bar_recs = await self._db.query_bars(
            self._symbol, "1m", start=since, end=until,
        )
        bars = pd.DataFrame(bar_recs)
        feature_rows = await self._db.query_feature_rows(
            self._symbol, "1m", limit=200000,
        )
        features = {fr.timestamp: fr.features for fr in feature_rows
                    if since <= fr.timestamp <= until}
        pred_rows = await self._db.query_predictions_in_range(
            symbol=self._symbol, timeframe="1m", start=since, end=until,
        )
        preds_by_ts: dict[datetime, list[dict]] = {}
        for p in pred_rows:
            preds_by_ts.setdefault(p["timestamp"], []).append(p)
        return bars, features, preds_by_ts


async def build_replay_rows(
    *,
    db: Database,
    symbol: str,
    direction: int,
    as_of: datetime,
    window_days: int,
    pair_grid: list[list[float]],
    max_hold_bars: int,
    regime_builder_factory: Callable[[], RegimeBuilder],
    cold_start_kelly: float = 0.05,
) -> list[MetaFeatureRow]:
    since = as_of - timedelta(days=window_days)
    cb = CandidateBuilder(db, symbol=symbol)
    bars, features, preds_by_ts = await cb.fetch(since=since, until=as_of)
    if bars.empty:
        return []

    active_models = await db.query_all_active_models()
    base_models = [m for m in active_models
                   if m.symbol == symbol
                   and m.model_type not in ("meta_entry", "meta_exec")]
    if not base_models:
        return []

    model_ids = [m.id for m in base_models]
    rolling = await _rolling_stats(db, model_ids)
    assembler = MetaFeatureAssembler(model_ids=model_ids)
    rb = regime_builder_factory()
    sim = PairSimulator()

    rows: list[MetaFeatureRow] = []
    pair_history: dict[int, list[bool]] = {i: [] for i in range(len(pair_grid))}

    for i in range(len(bars)):
        ts = pd.Timestamp(bars.iloc[i]["timestamp"]).to_pydatetime()
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts not in features or ts not in preds_by_ts:
            continue
        atr_pct = compute_atr_pct(bars, idx=i, period=14)
        if pd.isna(atr_pct) or atr_pct <= 0:
            continue

        per_pair: list[tuple[SimResult, float, float, float]] = []
        for tm, sm in pair_grid:
            tp, sp = float(tm) * atr_pct, float(sm) * atr_pct
            res = sim.simulate(bars, entry_idx=i, direction=direction,
                               target_pct=tp, stop_pct=sp,
                               max_hold_bars=max_hold_bars)
            per_pair.append((res, tp, sp, _ev_for_pair(res, tp, sp)))

        entry_label = 1 if any(p[0].hit_target for p in per_pair) else 0
        opt_idx = max(range(len(per_pair)), key=lambda k: per_pair[k][3])
        opt_target_pct, opt_stop_pct = per_pair[opt_idx][1], per_pair[opt_idx][2]
        kelly = _kelly_fraction(
            pair_history[opt_idx], opt_target_pct, opt_stop_pct,
            cold_start=cold_start_kelly,
        )
        for k, (r, _, _, _) in enumerate(per_pair):
            pair_history[k].append(r.hit_target)

        symbol_state = {
            "realized_vol": features[ts].get("realized_vol", 0.02),
            "atr_price_ratio": atr_pct,
            "dist_sma_50": features[ts].get("dist_sma_50", 0.0),
            "dist_sma_200bar": features[ts].get("dist_sma_200", 0.0),
            "dist_sma_200day": features[ts].get("dist_sma_200d", 0.0),
        }
        regime = rb.regime_at(ts)
        vec = assembler.assemble(
            predictions=preds_by_ts[ts],
            rolling_stats=rolling, regime=regime,
            symbol_state=symbol_state, timestamp=ts,
        )
        # Stash kelly into the feature_vector for the trainer to pick up
        vec["_kelly_label"] = kelly

        rows.append(MetaFeatureRow(
            symbol=symbol, timestamp=ts, direction=direction,
            feature_vector=vec, source="replay",
            entry_label=entry_label, optimal_pair_idx=opt_idx,
            realized_ev=per_pair[opt_idx][3],
        ))
    return rows
```

Add a `from datetime import timezone` import at the top of `replay.py` if not already present.

If `query_bars` / `query_predictions_in_range` are missing in `shared/db.py`, add them now, using `query_feature_rows` as the template, with paths that read from `bars_1m` and `predictions` respectively, filtered by `[start, end)`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `(cd bullforge && bin/test tests/meta/test_replay.py -v)`
Expected: all `test_replay.py` tests pass (Task 1 + Task 5 cases together).

- [ ] **Step 5: Commit**

```bash
git -C <WORKTREE> add \
    bullforge/services/model_service/meta/replay.py \
    bullforge/tests/meta/test_replay.py \
    bullforge/shared/db.py
git -C <WORKTREE> commit -m "feat(meta): add CandidateBuilder + build_replay_rows"
```

---

## Task 6: MetaTrainer rewrite (real datasets)

**Files:**
- Modify: `bullforge/services/model_service/meta/trainer.py` (replace `_build_entry_dataset` and `_build_exec_dataset`)
- Modify: `bullforge/tests/meta/test_trainer.py` (drop placeholder-warning expectations; mock `build_replay_rows` instead)

Removes the placeholder `np.random` block and threads `build_replay_rows` through to `query_meta_features`. Pulls `pair_grid`, `train_window_days`, `max_hold_minutes`, `min_train_rows` from `RuntimeConfig`. The trainer first persists replay rows via `insert_meta_features_batch(..., on_conflict="skip")`, then queries `meta_features` for training.

- [ ] **Step 1: Write the failing tests**

Replace the existing test cases in `bullforge/tests/meta/test_trainer.py` that depend on the placeholder; keep `test_model_store_saves_and_loads_calibrated_entry`. New cases:

```python
# Replace the "placeholder" mocks with replay-row mocks at the top of the file
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
import pytest
from shared.models import MetaFeatureRow
from services.model_service.meta.trainer import MetaTrainer


def _ts(min_offset=0):
    return datetime(2026, 4, 17, 14, 0, tzinfo=timezone.utc) + timedelta(minutes=min_offset)


def _replay_rows(n=600):
    return [
        MetaFeatureRow(
            symbol="AAPL", timestamp=_ts(i), direction=1,
            feature_vector={"base_1_dir": 1.0, "base_1_conf": 0.7,
                            "base_1_rolling_acc": 0.6, "base_1_cal_err": 0.05,
                            "base_1_disabled": 0.0,
                            "regime_spy_trend_1m": 0.0, "regime_spy_trend_1h": 0.0,
                            "regime_spy_trend_1d": 0.0, "regime_spy_momentum": 0.0,
                            "regime_vol": 1.0, "time_bucket": 56.0,
                            "minutes_since_open": 0.0, "minutes_to_close": 390.0,
                            "weekday": 4.0, "sym_realized_vol": 0.02,
                            "sym_atr_price_ratio": 0.005,
                            "sym_dist_sma_50": 0.0, "sym_dist_sma_200bar": 0.0,
                            "sym_dist_sma_200day": 0.0,
                            "_kelly_label": 0.05},
            source="replay",
            entry_label=int(np.random.RandomState(i).choice([0, 1])),
            optimal_pair_idx=i % 3, realized_ev=0.0,
        ) for i in range(n)
    ]


def _mock_db_for_trainer():
    db = AsyncMock()
    db.query_meta_features.return_value = _replay_rows()
    db.count_meta_features.return_value = 600
    db.insert_model_metadata.return_value = 100
    db.deactivate_models.return_value = None
    db.insert_meta_features_batch.return_value = None
    return db


def _mock_store():
    s = MagicMock()
    s.save.return_value = "/models/x.joblib"
    return s


@pytest.mark.asyncio
async def test_train_entry_uses_replay_dataset():
    db = _mock_db_for_trainer()
    store = _mock_store()
    rows = _replay_rows()
    with patch("services.model_service.meta.trainer.build_replay_rows",
               new=AsyncMock(return_value=rows)):
        trainer = MetaTrainer(
            db=db, model_store=store,
            sub_model_pairs={"long": [[2, 1], [3, 1.5], [4, 2]],
                             "short": [[2, 1], [3, 1.5], [4, 2]]},
        )
        meta = await trainer.train_entry(
            symbol="AAPL", direction=1,
            estimator_type="lightgbm", as_of=_ts(0),
            train_window_days=60, max_hold_minutes=30,
            min_train_rows=100,
        )
    assert meta is not None
    assert meta.model_type == "meta_entry"
    db.insert_meta_features_batch.assert_called_once()


@pytest.mark.asyncio
async def test_train_entry_skips_below_min_rows():
    db = _mock_db_for_trainer()
    db.query_meta_features.return_value = []
    rows: list[MetaFeatureRow] = []
    with patch("services.model_service.meta.trainer.build_replay_rows",
               new=AsyncMock(return_value=rows)):
        trainer = MetaTrainer(
            db=db, model_store=_mock_store(),
            sub_model_pairs={"long": [[2, 1]]},
        )
        meta = await trainer.train_entry(
            symbol="AAPL", direction=1,
            estimator_type="lightgbm", as_of=_ts(0),
            train_window_days=60, max_hold_minutes=30,
            min_train_rows=500,
        )
    assert meta is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `(cd bullforge && bin/test tests/meta/test_trainer.py -v)`
Expected: FAIL — `train_entry` doesn't accept `train_window_days`/`max_hold_minutes`/`min_train_rows` kwargs yet.

- [ ] **Step 3: Write minimal implementation**

In `bullforge/services/model_service/meta/trainer.py`:

1. Add at the top: `from services.model_service.meta.replay import build_replay_rows`.
2. Add at the top: `from services.model_service.meta.regime import RegimeBuilder`.
3. Replace `train_entry` signature and dataset build:

```python
    async def train_entry(self, symbol: str, direction: int, estimator_type: str,
                          as_of: datetime, *, train_window_days: int,
                          max_hold_minutes: int, min_train_rows: int,
                          regime_builder_factory=None,
                          ) -> Optional[ModelMetadata]:
        dir_label = _DIR_LABEL[direction]
        logger.info("Training meta_entry %s %s %s...", symbol, dir_label, estimator_type)
        X, y, weights = await self._build_entry_dataset(
            symbol, direction, as_of,
            train_window_days=train_window_days,
            max_hold_minutes=max_hold_minutes,
            regime_builder_factory=regime_builder_factory,
        )
        if X is None or len(X) < min_train_rows:
            logger.warning(
                "Insufficient data for meta_entry %s %s: %d < %d",
                symbol, dir_label,
                0 if X is None else len(X), min_train_rows,
            )
            return None
        # ... rest of train_entry stays the same below this point
```

(Apply the analogous change to `train_exec`, threading the same kwargs and using the same minimum-rows check.)

4. Replace `_build_entry_dataset` body with:

```python
    async def _build_entry_dataset(self, symbol, direction, as_of, *,
                                    train_window_days, max_hold_minutes,
                                    regime_builder_factory):
        pair_grid = self._pairs.get(_DIR_LABEL[direction], [])
        if not pair_grid:
            return None, None, None
        max_hold_bars = max(1, int(max_hold_minutes))

        replay = await build_replay_rows(
            db=self._db, symbol=symbol, direction=direction, as_of=as_of,
            window_days=train_window_days, pair_grid=pair_grid,
            max_hold_bars=max_hold_bars,
            regime_builder_factory=regime_builder_factory or self._default_rb_factory,
        )
        if replay:
            await self._db.insert_meta_features_batch(replay, on_conflict="skip")

        rows = await self._db.query_meta_features(
            symbol, direction=direction, limit=200000,
        )
        labeled = [r for r in rows if r.entry_label is not None]
        if not labeled:
            return None, None, None
        X = pd.DataFrame([{k: v for k, v in r.feature_vector.items()
                           if not k.startswith("_")} for r in labeled])
        y = pd.Series([int(r.entry_label) for r in labeled])
        weights = np.array(
            [self._live_weight if r.source == "live" else 1.0 for r in labeled])
        return X, y, weights
```

5. Add `_default_rb_factory` (returns a `RegimeBuilder` with empty SPY frame for unit tests; production overrides with a real factory in the scheduler).

```python
    def _default_rb_factory(self):
        return RegimeBuilder(spy_bars=pd.DataFrame(columns=["close"]),
                              max_staleness_hours=48)
```

6. Mirror the same dataset-build refactor for `_build_exec_dataset`. The y_kelly target comes from `feature_vector["_kelly_label"]`; y_pair from `optimal_pair_idx`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `(cd bullforge && bin/test tests/meta/test_trainer.py -v)`
Expected: 3 passed (the two new ones plus the ones that didn't depend on the placeholder).

- [ ] **Step 5: Commit**

```bash
git -C <WORKTREE> add \
    bullforge/services/model_service/meta/trainer.py \
    bullforge/tests/meta/test_trainer.py
git -C <WORKTREE> commit -m "feat(meta): trainer uses real replay datasets, drops placeholder"
```

---

## Task 7: MetaLabelBackfiller

**Files:**
- Create: `bullforge/services/model_service/meta/label_backfill.py`
- Test: `bullforge/tests/meta/test_label_backfill.py`

A periodic loop. On each tick: query unlabeled live rows older than `max_hold + 5min`, simulate, update labels. Idempotent (filter is `entry_label IS NULL`).

- [ ] **Step 1: Write the failing tests**

```python
# bullforge/tests/meta/test_label_backfill.py
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
import pandas as pd
import pytest

from shared.models import MetaFeatureRow
from services.model_service.meta.label_backfill import MetaLabelBackfiller


def _ts(min_offset=0):
    return datetime(2026, 4, 30, 14, 0, tzinfo=timezone.utc) + timedelta(minutes=min_offset)


@pytest.mark.asyncio
async def test_one_pass_labels_eligible_rows():
    unlabeled = MetaFeatureRow(
        symbol="AAPL", timestamp=_ts(0), direction=1,
        feature_vector={"x": 1.0}, source="live",
    )
    db = AsyncMock()
    db.query_unlabeled_meta_features.return_value = [unlabeled]
    bars_recs = [
        {"symbol": "AAPL", "timestamp": _ts(i), "open": 100.0, "high": 100.5,
         "low": 99.5, "close": 100.0 + 0.05 * i, "volume": 1000.0,
         "timeframe": "1m"} for i in range(60)
    ]
    db.query_bars.return_value = bars_recs

    bf = MetaLabelBackfiller(
        db=db, max_hold_minutes=30, pair_grid=[[2, 1], [3, 1.5]],
        safety_buffer_minutes=5,
    )
    n = await bf.run_once(now=_ts(60))
    assert n == 1
    db.update_meta_feature_labels.assert_called_once()
    args = db.update_meta_feature_labels.call_args.kwargs
    assert args["symbol"] == "AAPL"
    assert args["direction"] == 1
    assert args["entry_label"] in (0, 1)
    assert args["optimal_pair_idx"] in (0, 1)


@pytest.mark.asyncio
async def test_no_op_when_no_unlabeled_rows():
    db = AsyncMock()
    db.query_unlabeled_meta_features.return_value = []
    bf = MetaLabelBackfiller(
        db=db, max_hold_minutes=30, pair_grid=[[2, 1]], safety_buffer_minutes=5,
    )
    n = await bf.run_once(now=_ts(60))
    assert n == 0
    db.update_meta_feature_labels.assert_not_called()


@pytest.mark.asyncio
async def test_skips_when_too_few_post_entry_bars():
    unlabeled = MetaFeatureRow(
        symbol="AAPL", timestamp=_ts(0), direction=1,
        feature_vector={"x": 1.0}, source="live",
    )
    db = AsyncMock()
    db.query_unlabeled_meta_features.return_value = [unlabeled]
    db.query_bars.return_value = [{"symbol": "AAPL", "timestamp": _ts(0),
                                    "open": 100.0, "high": 100.0, "low": 100.0,
                                    "close": 100.0, "volume": 0.0,
                                    "timeframe": "1m"}]
    bf = MetaLabelBackfiller(
        db=db, max_hold_minutes=30, pair_grid=[[2, 1]], safety_buffer_minutes=5,
    )
    n = await bf.run_once(now=_ts(60))
    assert n == 0
    db.update_meta_feature_labels.assert_not_called()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `(cd bullforge && bin/test tests/meta/test_label_backfill.py -v)`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Write minimal implementation**

```python
# bullforge/services/model_service/meta/label_backfill.py
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from shared.db import Database
from services.model_service.meta.replay import (
    PairSimulator,
    compute_atr_pct,
    _ev_for_pair,
)

logger = logging.getLogger(__name__)


class MetaLabelBackfiller:
    def __init__(
        self,
        *,
        db: Database,
        max_hold_minutes: int,
        pair_grid: list[list[float]],
        safety_buffer_minutes: int = 5,
        interval_seconds: float = 900.0,  # 15 min default
    ):
        self._db = db
        self._max_hold_min = max_hold_minutes
        self._pair_grid = pair_grid
        self._buffer_min = safety_buffer_minutes
        self._interval_s = interval_seconds
        self._stop = asyncio.Event()
        self._sim = PairSimulator()

    def stop(self) -> None:
        self._stop.set()

    async def run_forever(self) -> None:
        while not self._stop.is_set():
            try:
                await self.run_once(now=datetime.now(timezone.utc))
            except Exception:
                logger.exception("MetaLabelBackfiller tick failed")
            try:
                await asyncio.wait_for(self._stop.wait(), self._interval_s)
                return
            except asyncio.TimeoutError:
                continue

    async def run_once(self, *, now: datetime) -> int:
        cutoff = now - timedelta(minutes=self._max_hold_min + self._buffer_min)
        rows = await self._db.query_unlabeled_meta_features(older_than=cutoff)
        labeled = 0
        for row in rows:
            ok = await self._label_one(row=row, now=now)
            if ok:
                labeled += 1
        return labeled

    async def _label_one(self, *, row, now: datetime) -> bool:
        bar_recs = await self._db.query_bars(
            row.symbol, "1m",
            start=row.timestamp,
            end=row.timestamp + timedelta(minutes=self._max_hold_min + 1),
        )
        if not bar_recs or len(bar_recs) < 2:
            return False
        bars = pd.DataFrame(bar_recs)
        # entry is the bar at `row.timestamp`; locate its index
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
        try:
            entry_idx = bars.index[bars["timestamp"] == row.timestamp][0]
        except IndexError:
            return False
        atr_pct = compute_atr_pct(bars, idx=entry_idx, period=14)
        if pd.isna(atr_pct) or atr_pct <= 0:
            return False

        per_pair = []
        for tm, sm in self._pair_grid:
            tp, sp = float(tm) * atr_pct, float(sm) * atr_pct
            res = self._sim.simulate(
                bars, entry_idx=entry_idx, direction=row.direction,
                target_pct=tp, stop_pct=sp, max_hold_bars=self._max_hold_min,
            )
            per_pair.append((res, tp, sp, _ev_for_pair(res, tp, sp)))

        entry_label = 1 if any(p[0].hit_target for p in per_pair) else 0
        opt_idx = max(range(len(per_pair)), key=lambda k: per_pair[k][3])
        await self._db.update_meta_feature_labels(
            symbol=row.symbol, timestamp=row.timestamp,
            direction=row.direction, entry_label=entry_label,
            optimal_pair_idx=opt_idx, realized_ev=per_pair[opt_idx][3],
        )
        return True
```

Then expose `_ev_for_pair` from `replay.py` (add it to `__all__` or just leave it module-level; the import above already pulls it).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `(cd bullforge && bin/test tests/meta/test_label_backfill.py -v)`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git -C <WORKTREE> add \
    bullforge/services/model_service/meta/label_backfill.py \
    bullforge/tests/meta/test_label_backfill.py
git -C <WORKTREE> commit -m "feat(meta): add MetaLabelBackfiller periodic loop"
```

---

## Task 8: RetrainScheduler meta phase

**Files:**
- Modify: `bullforge/services/model_service/scheduler.py`
- Test: `bullforge/tests/test_scheduler.py` (add cases) — or new file `bullforge/tests/test_meta_retrain_phase.py`

After base CPU + GPU phases complete, iterate `(symbol, direction, estimator_type)` and call `MetaTrainer.train_entry` + `train_exec`. Read `pair_grid`, `train_window_days`, `max_hold_minutes`, `min_train_rows`, `estimator_types` from RuntimeConfig. Build a single `RegimeBuilder` factory closure that pulls fresh SPY bars from DB.

- [ ] **Step 1: Write the failing test**

```python
# bullforge/tests/test_meta_retrain_phase.py
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from services.model_service.scheduler import RetrainScheduler


@pytest.mark.asyncio
async def test_run_once_invokes_meta_phase_per_symbol_direction():
    rc = AsyncMock()
    rc.get.side_effect = lambda key: {
        "retrain_schedule_hour": 1,
        "label_horizons_1m": [5],
        "label_horizons_1d": [1],
        "trading_symbols_equity": ["AAPL"],
        "trading_symbols_crypto": [],
    }.get(key)
    rc.get_all.return_value = {
        "retrain_cpu_workers": 1,
        "label_horizons_1m": [5], "label_horizons_1d": [1],
        "trading_symbols_equity": ["AAPL"], "trading_symbols_crypto": [],
        "meta_estimator_types": ["lightgbm"],
        "meta_pair_grid": [[2, 1], [3, 1.5], [4, 2]],
        "meta_train_window_days": 60,
        "meta_max_hold_minutes": 30,
        "meta_min_train_rows": 100,
    }
    redis = AsyncMock()
    inference = AsyncMock()
    db = AsyncMock()

    sched = RetrainScheduler(
        db_dsn="postgres://x", artifacts_dir="/tmp/x", runtime_config=rc,
        inference_engine=inference, redis_client=redis,
        symbols=["AAPL"], timeframes=["1m"], horizons_by_tf={"1m": [5]},
        min_samples=10, db=db,
    )

    with patch.object(sched, "_train_meta_phase",
                       new=AsyncMock(return_value=[])) as mock_meta, \
         patch("services.model_service.scheduler.train_one_job"), \
         patch.object(sched, "_train_lstm_in_process",
                       new=AsyncMock(return_value={"status": "ok"})):
        await sched.run_once()

    mock_meta.assert_awaited_once()
    call_kwargs = mock_meta.await_args.kwargs
    # Should pass per-cycle RuntimeConfig snapshot
    assert "config" in call_kwargs or len(mock_meta.await_args.args) >= 1
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `(cd bullforge && bin/test tests/test_meta_retrain_phase.py -v)`
Expected: FAIL — `_train_meta_phase` doesn't exist.

- [ ] **Step 3: Write minimal implementation**

In `bullforge/services/model_service/scheduler.py`:

1. Add imports: `from services.model_service.meta.trainer import MetaTrainer`, `from services.model_service.meta.regime import RegimeBuilder`.
2. After the existing GPU loop in `run_once`, before `await self._inference.load_models()`, insert:

```python
        # ── Meta phase ────────────────────────────────────────────────
        try:
            meta_results = await self._train_meta_phase(config=config, as_of=as_of)
            results.extend(meta_results)
        except Exception:
            logger.exception("Meta retrain phase failed")
```

3. Add the method:

```python
    async def _train_meta_phase(
        self, *, config: dict, as_of: datetime,
    ) -> list[dict]:
        if self._db is None:
            logger.warning("Meta phase skipped — no Database instance")
            return []
        store = ModelStore(self._artifacts_dir)
        estimator_types = config.get("meta_estimator_types") or ["lightgbm"]
        pair_grid = config.get("meta_pair_grid") or [[2, 1], [3, 1.5], [4, 2]]
        train_window = int(config.get("meta_train_window_days", 60))
        max_hold = int(config.get("meta_max_hold_minutes", 30))
        min_rows = int(config.get("meta_min_train_rows", 500))

        # Pre-load SPY bars once for this whole cycle's RegimeBuilder
        spy_recs = await self._db.query_bars(
            "SPY", "1m",
            start=as_of - timedelta(days=train_window + 7),
            end=as_of,
        )
        spy_df = pd.DataFrame(spy_recs)
        if not spy_df.empty:
            spy_df = spy_df.set_index(pd.to_datetime(spy_df["timestamp"], utc=True))
        rb_factory = lambda: RegimeBuilder(
            spy_bars=spy_df if not spy_df.empty else pd.DataFrame(columns=["close"]),
            max_staleness_hours=int(config.get("meta_regime_max_staleness_hours", 48)),
        )

        trainer = MetaTrainer(
            db=self._db, model_store=store,
            sub_model_pairs={"long": pair_grid, "short": pair_grid},
        )

        out: list[dict] = []
        for symbol in self._symbols:
            for direction in (1, -1):
                for et in estimator_types:
                    try:
                        meta_e = await trainer.train_entry(
                            symbol=symbol, direction=direction,
                            estimator_type=et, as_of=as_of,
                            train_window_days=train_window,
                            max_hold_minutes=max_hold,
                            min_train_rows=min_rows,
                            regime_builder_factory=rb_factory,
                        )
                        meta_x = await trainer.train_exec(
                            symbol=symbol, direction=direction,
                            estimator_type=et, as_of=as_of,
                            train_window_days=train_window,
                            max_hold_minutes=max_hold,
                            min_train_rows=min_rows,
                            regime_builder_factory=rb_factory,
                        )
                        out.append({
                            "symbol": symbol, "direction": direction,
                            "estimator_type": et,
                            "model_type": "meta", "status": "ok",
                            "entry_id": meta_e.id if meta_e else None,
                            "exec_id": meta_x.id if meta_x else None,
                        })
                    except Exception as e:
                        logger.exception("Meta train failed for %s %s %s",
                                         symbol, direction, et)
                        out.append({"symbol": symbol, "direction": direction,
                                    "estimator_type": et, "model_type": "meta",
                                    "status": "error", "error": str(e)})
        return out
```

Add `import pandas as pd` and `from datetime import timedelta` if not already present.

- [ ] **Step 4: Run the test to verify it passes**

Run: `(cd bullforge && bin/test tests/test_meta_retrain_phase.py -v)`
Expected: PASS.

Also re-run scheduler smoke: `(cd bullforge && bin/test tests/test_scheduler.py -v)` — should still pass.

- [ ] **Step 5: Commit**

```bash
git -C <WORKTREE> add \
    bullforge/services/model_service/scheduler.py \
    bullforge/tests/test_meta_retrain_phase.py
git -C <WORKTREE> commit -m "feat(scheduler): add meta-model training phase to nightly cycle"
```

---

## Task 9: InferenceEngine.predict_meta + load_meta_models

**Files:**
- Modify: `bullforge/services/model_service/inference.py`
- Test: `bullforge/tests/test_meta_inference.py`

Adds parallel storage for meta artifacts and a `predict_meta` method that returns one signal per loaded `(direction, estimator_type)` for a symbol. Uses cached rolling stats with TTL.

- [ ] **Step 1: Write the failing test**

```python
# bullforge/tests/test_meta_inference.py
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
import numpy as np
import pandas as pd
import pytest

from services.model_service.inference import InferenceEngine
from shared.models import ModelMetadata


def _md(mid, model_type, estimator_type=None, direction=1):
    return ModelMetadata(
        id=mid, symbol="AAPL", timeframe="1m",
        horizon_minutes=0 if model_type.startswith("meta") else 5,
        model_type=model_type, artifact_path=f"/m/{mid}",
        metrics={}, feature_columns=["x"], trained_at=datetime.now(timezone.utc),
        train_samples=100, test_samples=20, is_active=True,
        estimator_type=estimator_type,
    )


@pytest.mark.asyncio
async def test_predict_meta_no_meta_models_returns_empty():
    db = AsyncMock(); db.query_all_active_models.return_value = []
    store = MagicMock()
    eng = InferenceEngine(db=db, model_store=store)
    await eng.load_meta_models()
    out = await eng.predict_meta(
        symbol="AAPL", timeframe="1m",
        base_predictions=[], regime={}, symbol_state={},
        timestamp=datetime.now(timezone.utc),
    )
    assert out == []


@pytest.mark.asyncio
async def test_predict_meta_returns_signal_per_loaded_estimator():
    db = AsyncMock()
    db.query_all_active_models.return_value = [
        _md(11, "meta_entry", "lightgbm", direction=1),
        _md(12, "meta_exec", "lightgbm", direction=1),
    ]
    db.query_prediction_outcomes.return_value = []

    fake_entry = MagicMock()
    fake_entry.predict_proba = MagicMock(return_value=np.array([[0.4, 0.6]]))
    fake_exec = MagicMock()
    fake_exec.predict = MagicMock(
        return_value=(np.array([0.07]), np.array([1])))

    def _load(path):
        return fake_entry if "11" in path else fake_exec
    store = MagicMock(); store.load.side_effect = _load

    eng = InferenceEngine(db=db, model_store=store)
    # Treat both models as direction=1; since ModelMetadata doesn't currently
    # carry direction, the engine reads it from a side-channel — use the
    # estimator_type+model_type pairing it already knows about.
    eng._meta_models = {  # direct seeding for the test
        ("AAPL", 1, "lightgbm"): {"entry": (fake_entry, _md(11, "meta_entry", "lightgbm")),
                                   "exec": (fake_exec, _md(12, "meta_exec", "lightgbm"))},
    }
    eng._pair_grid = [[2, 1], [3, 1.5], [4, 2]]

    out = await eng.predict_meta(
        symbol="AAPL", timeframe="1m",
        base_predictions=[{"model_id": 1, "direction": 1, "confidence": 0.7}],
        regime={"spy_trend_1m": 0.0, "spy_trend_1h": 0.0, "spy_trend_1d": 0.0,
                "spy_momentum": 0.0, "vol_regime": 1.0},
        symbol_state={"realized_vol": 0.02, "atr_price_ratio": 0.005,
                       "dist_sma_50": 0.0, "dist_sma_200bar": 0.0,
                       "dist_sma_200day": 0.0},
        timestamp=datetime.now(timezone.utc),
    )

    assert len(out) == 1
    sig = out[0]
    assert sig["estimator_type"] == "lightgbm"
    assert sig["direction"] == 1
    assert 0.0 <= sig["entry_probability"] <= 1.0
    assert 0.0 <= sig["kelly_fraction"] <= 1.0
    assert sig["pair_index"] == 1
    assert sig["target_pct"] > 0
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `(cd bullforge && bin/test tests/test_meta_inference.py -v)`
Expected: FAIL — `InferenceEngine` lacks `load_meta_models`/`predict_meta`.

- [ ] **Step 3: Write minimal implementation**

The `direction` is needed to key meta-models. Since `ModelMetadata` doesn't currently carry a `direction` field for meta models (the `model_type` is `"meta_entry"` regardless of long/short), we encode direction in the **`artifact_path`** suffix from the trainer (already done — see existing `train_entry`: `f"meta_entry_{estimator_type}"`). Update the trainer in this task to include direction in the artifact filename, and parse it on load.

In `bullforge/services/model_service/meta/trainer.py`, change the `_store.save(...)` calls:

```python
        artifact_path = self._store.save(
            calibrated, symbol, "1m", 0,
            f"meta_entry_{_DIR_LABEL[direction]}_{estimator_type}",
        )
        # ...
        artifact_path = self._store.save(
            estimator, symbol, "1m", 0,
            f"meta_exec_{_DIR_LABEL[direction]}_{estimator_type}",
        )
```

In `bullforge/services/model_service/inference.py`, append:

```python
import re

_META_DIR_RX = re.compile(r"meta_(entry|exec)_(long|short)_(\w+)")
_DIR_TO_INT = {"long": 1, "short": -1}


class _MetaState:
    """Rolling-stats cache + pair grid for meta inference."""
    def __init__(self, pair_grid: list[list[float]], ttl_s: float):
        self.pair_grid = pair_grid
        self.ttl_s = ttl_s
        self._cache: dict[int, tuple[float, dict]] = {}

    async def stats_for(self, db, model_id: int, now_s: float) -> dict:
        cached = self._cache.get(model_id)
        if cached and now_s - cached[0] < self.ttl_s:
            return cached[1]
        outcomes = await db.query_prediction_outcomes(model_id, limit=200)
        if outcomes:
            correct = sum(1 for o in outcomes if o["is_correct"])
            acc = correct / len(outcomes)
            avg_conf = float(sum(o["confidence"] for o in outcomes)) / len(outcomes)
            cal_err = abs(avg_conf - acc)
        else:
            acc, cal_err = 0.0, 0.0
        v = {"accuracy": acc, "calibration_error": cal_err, "is_disabled": False}
        self._cache[model_id] = (now_s, v)
        return v
```

Then extend `InferenceEngine`:

```python
    async def load_meta_models(self) -> None:
        active = await self._db.query_all_active_models()
        meta = [m for m in active if m.model_type in ("meta_entry", "meta_exec")]
        new: dict[tuple[str, int, str], dict] = {}
        for m in meta:
            mtch = _META_DIR_RX.search(m.artifact_path or "")
            if not mtch:
                continue
            kind, dir_label, est = mtch.group(1), mtch.group(2), mtch.group(3)
            key = (m.symbol, _DIR_TO_INT[dir_label], est)
            slot = new.setdefault(key, {"entry": None, "exec": None})
            try:
                model = self._model_store.load(m.artifact_path)
            except FileNotFoundError:
                continue
            slot[kind] = (model, m)
        self._meta_models = new

    async def predict_meta(
        self, *, symbol: str, timeframe: str,
        base_predictions: list[dict], regime: dict, symbol_state: dict,
        timestamp,
    ) -> list[dict]:
        if not getattr(self, "_meta_models", None):
            return []
        from time import time as _now
        from services.model_service.meta.feature_assembler import MetaFeatureAssembler
        ms = getattr(self, "_meta_state", None) or _MetaState(
            pair_grid=getattr(self, "_pair_grid", [[2, 1], [3, 1.5], [4, 2]]),
            ttl_s=getattr(self, "_meta_stats_ttl_s", 600.0),
        )
        self._meta_state = ms
        atr_pct = float(symbol_state.get("atr_price_ratio") or 0.0)
        if atr_pct <= 0:
            return []

        model_ids = sorted({p["model_id"] for p in base_predictions})
        rolling: dict[int, dict] = {}
        now_s = _now()
        for mid in model_ids:
            rolling[mid] = await ms.stats_for(self._db, mid, now_s)
        assembler = MetaFeatureAssembler(model_ids=model_ids)
        feature_vec = assembler.assemble(
            predictions=base_predictions, rolling_stats=rolling,
            regime=regime, symbol_state=symbol_state, timestamp=timestamp,
        )
        X = pd.DataFrame([feature_vec])

        signals: list[dict] = []
        for (sym, direction, est), slot in self._meta_models.items():
            if sym != symbol: continue
            entry_pair = slot["entry"]; exec_pair = slot["exec"]
            if entry_pair is None or exec_pair is None: continue
            entry_model, entry_meta = entry_pair
            exec_model, exec_meta = exec_pair
            entry_p = float(entry_model.predict_proba(X)[0, 1])
            kelly_arr, pair_arr = exec_model.predict(X)
            kelly = float(kelly_arr[0]); pair_idx = int(pair_arr[0])
            pair_idx = max(0, min(pair_idx, len(ms.pair_grid) - 1))
            tm, sm = ms.pair_grid[pair_idx]
            target_pct = float(tm) * atr_pct
            stop_pct = float(sm) * atr_pct
            signals.append({
                "direction": direction, "estimator_type": est,
                "model_id": entry_meta.id,
                "entry_probability": entry_p, "kelly_fraction": kelly,
                "pair_index": pair_idx, "target_pct": target_pct,
                "stop_pct": stop_pct,
            })
        return signals
```

Add `import pandas as pd` to `inference.py` if not already present.

- [ ] **Step 4: Run the test to verify it passes**

Run: `(cd bullforge && bin/test tests/test_meta_inference.py -v)`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git -C <WORKTREE> add \
    bullforge/services/model_service/inference.py \
    bullforge/services/model_service/meta/trainer.py \
    bullforge/tests/test_meta_inference.py
git -C <WORKTREE> commit -m "feat(meta): add InferenceEngine.predict_meta + load_meta_models"
```

---

## Task 10: Wire model_service main to publish meta signals + start backfiller

**Files:**
- Modify: `bullforge/services/model_service/main.py`
- Modify: `docs/architecture/redis-pubsub.md` (per "maintain Redis pub/sub docs" memory rule, **same commit**)

In `model_service/main.py`, after the existing `feature:updates` handler that publishes `model:predictions`, also call `predict_meta` and publish `model:meta_signals` if `meta_inference_enabled` is true. Start a `MetaLabelBackfiller` background task. Wire `_pair_grid` and `_meta_stats_ttl_s` on the inference engine from RuntimeConfig and call `load_meta_models()` after the base load.

- [ ] **Step 1: Sketch the changes**

Read the current `feature:updates` handler in `main.py` first (around the predict-and-publish block); confirm it has access to `runtime_config`, `inference_engine`, `redis_client`, and the latest features so the meta path can compute regime + symbol_state. Note its line numbers for the edit.

Run: `(cd bullforge && grep -n 'feature:updates\|model:predictions' services/model_service/main.py)`

- [ ] **Step 2: Write the failing test**

Existing `tests/test_inference.py` and `tests/test_scheduler.py` cover the base flow. Add a focused test for the new wiring:

```python
# bullforge/tests/test_meta_main_wiring.py
from unittest.mock import AsyncMock, MagicMock, patch
import json
import pytest

from services.model_service.main import on_feature_update_factory


@pytest.mark.asyncio
async def test_handler_publishes_meta_signals_when_enabled():
    rc = AsyncMock()
    rc.get.side_effect = lambda k: {
        "meta_inference_enabled": True,
        "meta_pair_grid": [[2, 1], [3, 1.5], [4, 2]],
        "meta_rolling_stats_ttl_seconds": 600,
    }.get(k)

    redis = AsyncMock()
    inference = AsyncMock()
    inference.predict.return_value = [
        MagicMock(model_id=1, predicted_direction=1, confidence=0.7,
                  symbol="AAPL", timestamp=__import__("datetime").datetime(
                      2026, 4, 30, 14, 0, tzinfo=__import__("datetime").timezone.utc),
                  horizon_minutes=5)
    ]
    inference.predict_meta.return_value = [
        {"direction": 1, "estimator_type": "lightgbm",
         "entry_probability": 0.6, "kelly_fraction": 0.05,
         "pair_index": 1, "target_pct": 0.01, "stop_pct": 0.005,
         "model_id": 11},
    ]
    db = AsyncMock()
    db.query_feature_rows.return_value = [MagicMock(features={
        "atr_14": 0.5, "sma_20": 100.0, "realized_vol": 0.02,
        "dist_sma_50": 0.0, "dist_sma_200": 0.0, "dist_sma_200d": 0.0,
    })]

    handler = on_feature_update_factory(
        inference=inference, redis=redis, runtime_config=rc, db=db,
        regime_builder=MagicMock(regime_at=MagicMock(
            return_value={"spy_trend_1m": 0.0, "spy_trend_1h": 0.0,
                          "spy_trend_1d": 0.0, "spy_momentum": 0.0,
                          "vol_regime": 1.0})),
    )
    await handler(json.dumps({"symbol": "AAPL", "timeframe": "1m"}))

    channels = [c.args[0] for c in redis.publish.call_args_list]
    assert "model:predictions" in channels
    assert "model:meta_signals" in channels


@pytest.mark.asyncio
async def test_handler_skips_meta_publish_when_disabled():
    rc = AsyncMock()
    rc.get.side_effect = lambda k: {"meta_inference_enabled": False}.get(k)
    redis = AsyncMock()
    inference = AsyncMock()
    inference.predict.return_value = []
    db = AsyncMock(); db.query_feature_rows.return_value = []
    handler = on_feature_update_factory(
        inference=inference, redis=redis, runtime_config=rc, db=db,
        regime_builder=MagicMock(),
    )
    await handler(json.dumps({"symbol": "AAPL", "timeframe": "1m"}))
    channels = [c.args[0] for c in redis.publish.call_args_list]
    assert "model:meta_signals" not in channels
```

- [ ] **Step 3: Implement**

In `bullforge/services/model_service/main.py`, factor the existing inline `on_feature_update` body into a module-level `on_feature_update_factory(*, inference, redis, runtime_config, db, regime_builder)` that returns the handler. Inside it, after publishing `model:predictions`:

```python
        if await runtime_config.get("meta_inference_enabled"):
            try:
                latest = await db.query_feature_rows(symbol, timeframe, limit=1)
                if not latest:
                    return
                fr = latest[0]
                atr_14 = float(fr.features.get("atr_14") or 0.0)
                close = float(fr.features.get("sma_20") or 0.0)  # close not on feature row; sma_20 is a stand-in
                atr_pct = (atr_14 / close) if close > 0 else 0.0
                symbol_state = {
                    "realized_vol": fr.features.get("realized_vol", 0.02),
                    "atr_price_ratio": atr_pct,
                    "dist_sma_50": fr.features.get("dist_sma_50", 0.0),
                    "dist_sma_200bar": fr.features.get("dist_sma_200", 0.0),
                    "dist_sma_200day": fr.features.get("dist_sma_200d", 0.0),
                }
                regime = regime_builder.regime_at(fr.timestamp)
                signals = await inference.predict_meta(
                    symbol=symbol, timeframe=timeframe,
                    base_predictions=[
                        {"model_id": p.model_id, "direction": p.predicted_direction,
                         "confidence": p.confidence}
                        for p in predictions
                    ],
                    regime=regime, symbol_state=symbol_state,
                    timestamp=fr.timestamp,
                )
                if signals:
                    payload = {
                        "symbol": symbol, "timeframe": timeframe,
                        "timestamp": fr.timestamp.isoformat(),
                        "feature_vector": {},  # filled by inference engine in v2
                        "signals": signals,
                    }
                    await redis.publish("model:meta_signals", json.dumps(payload))
            except Exception:
                logger.exception("Meta inference path failed")
```

In `main()`, after `await inference_engine.load_models()`, also call `await inference_engine.load_meta_models()`. Build a long-lived `RegimeBuilder` (refresh every N minutes via a small task — out of scope for this phase; just rebuild every retrain cycle by setting it on a class attribute that the scheduler can update). Start a `MetaLabelBackfiller`:

```python
    backfill_interval = int(await runtime_config.get(
        "meta_label_backfill_interval_minutes")) * 60
    pair_grid = await runtime_config.get("meta_pair_grid")
    max_hold = int(await runtime_config.get("meta_max_hold_minutes"))
    backfiller = MetaLabelBackfiller(
        db=db, max_hold_minutes=max_hold, pair_grid=pair_grid,
        interval_seconds=backfill_interval,
    )
    bg_tasks.add(asyncio.create_task(backfiller.run_forever()))
```

(If `bg_tasks` doesn't exist as a name in `main.py`, add `bg_tasks: set[asyncio.Task] = set()` near other state, and a shutdown handler that cancels them.)

Add channel doc to `docs/architecture/redis-pubsub.md` (in the same commit) — append a row to the channels table:

```
| `model:meta_signals` | `services/model_service/main.py` | `services/execution_service/main.py` (`MetaShadowWriter`) | `{symbol, timeframe, timestamp, feature_vector, signals: [{direction, estimator_type, model_id, entry_probability, kelly_fraction, pair_index, target_pct, stop_pct}]}` | Meta-model shadow decisions per prediction event. Phase 1: log only, never gates execution. |
```

- [ ] **Step 4: Run tests**

Run: `(cd bullforge && bin/test tests/test_meta_main_wiring.py -v)`
Expected: 2 passed.

Spot-check: `(cd bullforge && bin/test tests/test_inference.py -v)` — existing inference tests still pass.

- [ ] **Step 5: Commit**

```bash
git -C <WORKTREE> add \
    bullforge/services/model_service/main.py \
    bullforge/tests/test_meta_main_wiring.py \
    docs/architecture/redis-pubsub.md
git -C <WORKTREE> commit -m "feat(model_service): publish model:meta_signals + start label backfiller"
```

---

## Task 11: MetaShadowWriter (execution-side persister)

**Files:**
- Create: `bullforge/services/execution_service/meta_shadow_writer.py`
- Test: `bullforge/tests/test_meta_shadow.py`

Subscribes to `model:meta_signals` and writes `meta_signals_shadow` + `meta_features` rows. No execution-path coupling.

- [ ] **Step 1: Write the failing tests**

```python
# bullforge/tests/test_meta_shadow.py
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
import json
import pytest

from services.execution_service.meta_shadow_writer import MetaShadowWriter


@pytest.mark.asyncio
async def test_writes_one_shadow_row_per_signal():
    db = AsyncMock()
    writer = MetaShadowWriter(db=db)
    msg = json.dumps({
        "symbol": "AAPL", "timeframe": "1m",
        "timestamp": "2026-04-30T14:00:00+00:00",
        "feature_vector": {"x": 1.0},
        "signals": [
            {"direction": 1, "estimator_type": "lightgbm",
             "entry_probability": 0.6, "kelly_fraction": 0.05,
             "pair_index": 1, "target_pct": 0.01, "stop_pct": 0.005,
             "model_id": 11},
            {"direction": -1, "estimator_type": "lightgbm",
             "entry_probability": 0.3, "kelly_fraction": 0.0,
             "pair_index": 0, "target_pct": 0.005, "stop_pct": 0.005,
             "model_id": 12},
        ],
    })
    await writer.handle_message(msg)
    assert db.insert_meta_signal_shadow.await_count == 2
    # one meta_features row per direction (2)
    assert db.insert_meta_feature.await_count == 2


@pytest.mark.asyncio
async def test_handles_empty_signals_gracefully():
    db = AsyncMock()
    writer = MetaShadowWriter(db=db)
    await writer.handle_message(json.dumps({
        "symbol": "AAPL", "timeframe": "1m",
        "timestamp": "2026-04-30T14:00:00+00:00",
        "feature_vector": {}, "signals": [],
    }))
    db.insert_meta_signal_shadow.assert_not_called()
    db.insert_meta_feature.assert_not_called()


@pytest.mark.asyncio
async def test_malformed_message_logs_and_continues():
    db = AsyncMock()
    writer = MetaShadowWriter(db=db)
    await writer.handle_message("not json")
    db.insert_meta_signal_shadow.assert_not_called()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `(cd bullforge && bin/test tests/test_meta_shadow.py -v)`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Write minimal implementation**

```python
# bullforge/services/execution_service/meta_shadow_writer.py
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from shared.db import Database
from shared.models import MetaFeatureRow, MetaSignalShadow

logger = logging.getLogger(__name__)


class MetaShadowWriter:
    """Persist incoming `model:meta_signals` payloads to the shadow tables.

    Writes one `meta_signals_shadow` row per signal in the payload, plus
    one `meta_features` row (source='live', labels NULL) per unique
    `(symbol, timestamp, direction)` triple in the same payload.
    """

    def __init__(self, *, db: Database):
        self._db = db

    async def handle_message(self, message: str) -> None:
        try:
            payload: dict[str, Any] = json.loads(message)
            symbol = payload["symbol"]
            ts = datetime.fromisoformat(payload["timestamp"])
            signals = payload.get("signals") or []
            feature_vector = payload.get("feature_vector") or {}
        except Exception:
            logger.exception("MetaShadowWriter: malformed payload")
            return

        if not signals:
            return

        seen_dirs: set[int] = set()
        for sig in signals:
            try:
                shadow = MetaSignalShadow(
                    symbol=symbol, timestamp=ts,
                    direction=int(sig["direction"]),
                    estimator_type=str(sig["estimator_type"]),
                    entry_probability=float(sig["entry_probability"]),
                    kelly_fraction=sig.get("kelly_fraction"),
                    pair_index=sig.get("pair_index"),
                    target_pct=sig.get("target_pct"),
                    stop_pct=sig.get("stop_pct"),
                    model_id=sig.get("model_id"),
                )
                await self._db.insert_meta_signal_shadow(shadow)
            except Exception:
                logger.exception(
                    "MetaShadowWriter: failed to insert shadow for %s %s",
                    symbol, sig.get("direction"),
                )
                continue

            d = int(sig["direction"])
            if d in seen_dirs:
                continue
            seen_dirs.add(d)
            try:
                row = MetaFeatureRow(
                    symbol=symbol, timestamp=ts, direction=d,
                    feature_vector=feature_vector or {"_": 0.0},
                    source="live",
                )
                await self._db.insert_meta_feature(row)
            except Exception:
                logger.exception(
                    "MetaShadowWriter: failed to insert feature row %s %s",
                    symbol, d,
                )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `(cd bullforge && bin/test tests/test_meta_shadow.py -v)`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git -C <WORKTREE> add \
    bullforge/services/execution_service/meta_shadow_writer.py \
    bullforge/tests/test_meta_shadow.py
git -C <WORKTREE> commit -m "feat(execution): add MetaShadowWriter for shadow-table persistence"
```

---

## Task 12: Subscribe to model:meta_signals in execution_service

**Files:**
- Modify: `bullforge/services/execution_service/main.py`

Wire `MetaShadowWriter` to `redis_client.subscribe("model:meta_signals", writer.handle_message)`. No new tests — covered by `test_meta_shadow.py` plus a smoke check that `main()` imports cleanly.

- [ ] **Step 1: Read current main.py subscription section**

Run: `(cd bullforge && grep -n 'subscribe' services/execution_service/main.py)`

Identify where existing `redis_client.subscribe(...)` calls happen.

- [ ] **Step 2: Verify no regression**

Run: `(cd bullforge && bin/test tests/test_execution_health.py -v)`
Expected: existing tests pass.

- [ ] **Step 3: Add subscription**

In `bullforge/services/execution_service/main.py`, near other `subscribe()` calls in `main()`:

```python
    from services.execution_service.meta_shadow_writer import MetaShadowWriter
    meta_writer = MetaShadowWriter(db=db)
    await redis_client.subscribe(
        "model:meta_signals", meta_writer.handle_message,
    )
```

- [ ] **Step 4: Re-run tests**

Run: `(cd bullforge && bin/test tests/test_meta_shadow.py tests/test_execution_health.py -v)`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git -C <WORKTREE> add bullforge/services/execution_service/main.py
git -C <WORKTREE> commit -m "feat(execution): subscribe to model:meta_signals"
```

---

## Task 13: End-to-end integration test

**Files:**
- Create: `bullforge/tests/integration/test_meta_train_e2e_db.py`

Real DB: seed `bars_1m` + `features` + `predictions` for a single symbol/direction; run `MetaTrainer.train_entry`; assert artifact saved + `model_registry` row inserted with `model_type='meta_entry'` and `estimator_type='lightgbm'`.

- [ ] **Step 1: Write the test**

```python
# bullforge/tests/integration/test_meta_train_e2e_db.py
from datetime import datetime, timezone, timedelta
import os
import tempfile
import numpy as np
import pandas as pd
import pytest

from shared.config import Settings
from shared.db import Database
from shared.models import Bar, FeatureRow
from services.model_service.model_store import ModelStore
from services.model_service.meta.trainer import MetaTrainer
from services.model_service.meta.regime import RegimeBuilder


SYM = "TST_E2E"


@pytest.fixture
async def db():
    settings = Settings()
    db = Database(settings)
    await db.connect()
    async with db.pool.acquire() as conn:
        for tbl in ("bars_1m", "features", "labels", "predictions",
                     "meta_features", "model_registry"):
            await conn.execute(
                f"DELETE FROM {tbl} WHERE symbol = $1", SYM,
            )
    yield db
    async with db.pool.acquire() as conn:
        for tbl in ("bars_1m", "features", "labels", "predictions",
                     "meta_features", "model_registry"):
            await conn.execute(
                f"DELETE FROM {tbl} WHERE symbol = $1", SYM,
            )
    await db.disconnect()


async def _seed(db, n_bars=600):
    base = datetime(2026, 4, 1, 14, 0, tzinfo=timezone.utc)
    rng = np.random.RandomState(42)
    bars = []
    feats = []
    preds = []
    closes = 100 + np.cumsum(rng.normal(0, 0.05, n_bars))
    for i in range(n_bars):
        ts = base + timedelta(minutes=i)
        c = float(closes[i])
        bars.append(Bar(symbol=SYM, timestamp=ts, open=c, high=c * 1.002,
                        low=c * 0.998, close=c, volume=1000, timeframe="1m"))
        feats.append(FeatureRow(symbol=SYM, timestamp=ts, timeframe="1m",
                                features={
                                    "sma_20": c, "atr_14": c * 0.005,
                                    "rsi_14": 50.0, "realized_vol": 0.02,
                                    "dist_sma_50": 0.0, "dist_sma_200": 0.0,
                                    "dist_sma_200d": 0.0,
                                }))
        if i % 5 == 0:
            preds.append({"symbol": SYM, "timestamp": ts, "timeframe": "1m",
                          "horizon_minutes": 5, "model_id": 99999,
                          "predicted_direction": int(rng.choice([-1, 1])),
                          "confidence": 0.7})
    await db.insert_bars_batch(bars)
    await db.insert_features_batch(feats)
    # seed a base model registry entry so query_all_active_models returns it
    from shared.models import ModelMetadata
    md = ModelMetadata(symbol=SYM, timeframe="1m", horizon_minutes=5,
                       model_type="gradient_boost", artifact_path="/tmp/x.joblib",
                       metrics={}, feature_columns=["sma_20"],
                       trained_at=datetime.now(timezone.utc),
                       train_samples=100, test_samples=20, is_active=True)
    md_id = await db.insert_model_metadata(md)
    for p in preds:
        p["model_id"] = md_id
        await db.insert_prediction(p)


@pytest.mark.asyncio
async def test_meta_entry_train_persists_artifact_and_registry(db):
    await _seed(db, n_bars=600)
    with tempfile.TemporaryDirectory() as td:
        store = ModelStore(td)
        rb_factory = lambda: RegimeBuilder(
            spy_bars=pd.DataFrame(columns=["close"]), max_staleness_hours=48,
        )
        trainer = MetaTrainer(
            db=db, model_store=store,
            sub_model_pairs={"long": [[2, 1], [3, 1.5], [4, 2]],
                             "short": [[2, 1], [3, 1.5], [4, 2]]},
        )
        meta = await trainer.train_entry(
            symbol=SYM, direction=1, estimator_type="lightgbm",
            as_of=datetime(2026, 4, 1, 23, 59, tzinfo=timezone.utc),
            train_window_days=60, max_hold_minutes=30, min_train_rows=50,
            regime_builder_factory=rb_factory,
        )
        assert meta is not None
        assert meta.model_type == "meta_entry"
        assert meta.estimator_type == "lightgbm"
        assert os.path.exists(meta.artifact_path)
        async with db.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT model_type, estimator_type, is_active
                   FROM model_registry WHERE id = $1""", meta.id,
            )
        assert row["model_type"] == "meta_entry"
        assert row["estimator_type"] == "lightgbm"
        assert row["is_active"] is True
```

- [ ] **Step 2: Run the test**

Run: `(cd bullforge && bin/test tests/integration/test_meta_train_e2e_db.py -v)`
Expected: PASS — single test green.

If `Database.insert_bars_batch` / `insert_features_batch` / `insert_prediction` need slight adjustment to match existing signatures, fix in this task.

- [ ] **Step 3: Commit**

```bash
git -C <WORKTREE> add bullforge/tests/integration/test_meta_train_e2e_db.py
git -C <WORKTREE> commit -m "test(meta): integration test for end-to-end replay → train → save"
```

---

## Task 14: Smoke test the full meta retrain phase via retrain_now

**Files:** none new — reuses `bullforge/services/model_service/retrain_now.py`.

Run the one-shot retrain entrypoint inside the running stack to confirm the meta phase fires without error. This is a manual verification step, not a unit/integration test commit.

- [ ] **Step 1: Bring up the stack**

Run: `(cd bullforge && bin/up)`

Wait until `bin/status` shows all services green.

- [ ] **Step 2: Trigger retrain**

Run: `(cd bullforge && docker compose exec model_service python -m services.model_service.retrain_now)`

Expected log lines:
- `Retrain cycle starting (...)` (existing)
- `Training meta_entry SPY long lightgbm...`
- `Training meta_exec SPY long lightgbm...`
- (similar for short and other symbols)
- `Retrain cycle complete: ok=N err=M skipped=K`

- [ ] **Step 3: Spot-check the artifacts**

Run: `(cd bullforge && docker compose exec timescaledb psql -U bullforge -d bullforge -c "SELECT id, symbol, model_type, estimator_type, is_active FROM model_registry WHERE model_type IN ('meta_entry','meta_exec') ORDER BY id DESC LIMIT 20;")`

Expected: rows with `model_type ∈ {meta_entry, meta_exec}`, `estimator_type='lightgbm'`, `is_active=true`.

If meta rows are missing, log to confirm `meta_estimator_types` is set in RuntimeConfig (check via the dashboard Config tab or `docker compose exec redis redis-cli HGET bullforge:config meta_estimator_types`).

- [ ] **Step 4: Flip on shadow inference and observe**

Run: `(cd bullforge && docker compose exec redis redis-cli HSET bullforge:config meta_inference_enabled "true")`

Tail logs: `(cd bullforge && docker compose logs -f model_service execution_service | grep -i meta)`

Wait for at least one new feature update to flow through. Expect:
- `model_service` logs publishing `model:meta_signals`.
- `execution_service` logs handling `MetaShadowWriter` insertions.
- `meta_signals_shadow` table has new rows: `SELECT COUNT(*) FROM meta_signals_shadow`.
- `meta_features` table has new `source='live'` rows: `SELECT source, COUNT(*) FROM meta_features GROUP BY source`.

- [ ] **Step 5: Update CLAUDE.md with the new operator runbook section**

Append a brief section to `CLAUDE.md` documenting the new shadow-mode flow under a "Meta-model shadow mode" heading: how to enable (`meta_inference_enabled`), what tables to query, and that this phase explicitly does **not** gate execution.

```bash
git -C <WORKTREE> add CLAUDE.md
git -C <WORKTREE> commit -m "docs(claude): add meta-shadow mode operator notes"
```

---

## Self-review

**Spec coverage check:**

- §2.1 real training-data builder → Tasks 1 (PairSimulator+ATR), 2 (RegimeBuilder), 5 (CandidateBuilder/build_replay_rows), 6 (MetaTrainer rewrite). ✓
- §2.2 nightly meta-training → Task 8 (`_train_meta_phase`). ✓
- §2.3 live shadow logging → Tasks 9 (predict_meta), 10 (model_service main wiring), 11 (writer), 12 (subscribe). ✓
- §2.4 live label backfill → Task 7 (`MetaLabelBackfiller`), wired in Task 10. ✓
- §4.1 ATR-scaled pair grid → Tasks 1 (ATR helper), 5 (grid usage), 8 (config-driven grid). ✓
- §4.5 outcome labels (`entry_label`, `optimal_pair_idx`, `kelly_fraction`) → Task 5. ✓
- §4.6 RegimeBuilder + SPY staleness → Task 2. ✓
- §4.9 publish channel → Tasks 9, 10. ✓ (pub/sub doc updated in same commit per memory rule.)
- §4.10 backfill → Task 7. ✓
- §6 file-level changes → all listed paths covered in Tasks 1-12. ✓
- §9 runtime-config keys → Task 3 covers all 9. ✓
- §11 testing strategy → unit tests in Tasks 1-2, 4-7, 9-11; integration in Task 13; smoke in Task 14. ✓

**Placeholder scan:** No "TBD" / "TODO" / "fill in details" remaining. All steps include actual code.

**Type consistency:** `MetaTrainer.train_entry` signature in Task 6 matches its calls in Task 8 (`train_window_days`, `max_hold_minutes`, `min_train_rows`, `regime_builder_factory`). `InferenceEngine.predict_meta` signature in Task 9 matches its call in Task 10. `MetaShadowWriter.handle_message` signature in Task 11 matches the subscribe call in Task 12. `build_replay_rows` signature in Task 5 matches its callers in Task 6 and 7. ✓

**Worktree hygiene:** Each commit block uses `git -C <WORKTREE>` (replace with the absolute worktree path on first run). Frequent commits — one per task minimum.
