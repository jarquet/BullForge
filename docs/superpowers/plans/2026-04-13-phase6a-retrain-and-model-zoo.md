# Phase 6A — Nightly Retrain & GPU Model Zoo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a nightly retrain scheduler with parallel CPU (LightGBM/sklearn) + GPU (PyTorch LSTM) training, a Redis-backed runtime-config system, and look-ahead-bias guardrail tests — without breaking the existing startup-training path.

**Architecture:** A new `RuntimeConfig` reads/writes a Redis hash (`bullforge:config`). A new `RetrainScheduler` background task runs inside `model_service`, waking nightly at `retrain_schedule_hour` UTC, captures an `as_of` cutoff, dispatches all gradient-boost jobs to a `ProcessPoolExecutor(retrain_cpu_workers)` on CPU, then runs LSTM jobs sequentially on GPU, then hot-swaps in-memory models via `InferenceEngine.load_models()`. `query_training_data` gains a required `as_of` parameter enforced by tests. Model artifacts dispatch `.joblib` vs `.pt` based on model type.

**Tech Stack:** Python 3.12, asyncio, asyncpg, Redis hash storage, PyTorch 2.x with CUDA 12.4, scikit-learn, LightGBM (existing), ProcessPoolExecutor, pytest-asyncio.

---

## File Structure

**Create:**
- `bullforge/shared/runtime_config.py` — Redis-backed config with CONFIG_SCHEMA
- `bullforge/services/model_service/scheduler.py` — Nightly retrain orchestrator
- `bullforge/services/model_service/lstm_model.py` — PyTorch LSTMClassifier (sklearn-compatible)
- `bullforge/services/model_service/worker.py` — Top-level ProcessPool worker entrypoint (must be picklable)
- `bullforge/tests/test_runtime_config.py`
- `bullforge/tests/test_lstm_model.py`
- `bullforge/tests/test_scheduler.py`
- `bullforge/tests/test_data_integrity.py` — 5 look-ahead-bias invariant tests
- `bullforge/tests/test_model_store_lstm.py`

**Modify:**
- `bullforge/shared/db.py` — add `as_of` to `query_training_data` (required); new `query_trades_closed_before` for invariant 3 test
- `bullforge/services/model_service/model_factory.py` — add `"lstm"`, accept `runtime_config` dict
- `bullforge/services/model_service/model_store.py` — detect `.pt` vs `.joblib` on save/load
- `bullforge/services/model_service/dataset.py` — `DatasetBuilder.build` accepts `as_of`
- `bullforge/services/model_service/trainer.py` — `train()` accepts `as_of` and `runtime_config`
- `bullforge/services/model_service/main.py` — instantiate RuntimeConfig, start scheduler background task, pass `as_of` in startup train
- `bullforge/services/model_service/Dockerfile` — switch to `nvidia/cuda:12.4.1-runtime-ubuntu22.04`, install Python 3.12 + torch
- `bullforge/services/model_service/requirements.txt` — add `torch`, `lightgbm`
- `bullforge/docker-compose.yml` — add GPU reservation to model_service

---

## Task 1: RuntimeConfig with CONFIG_SCHEMA (TDD)

**Files:**
- Create: `bullforge/shared/runtime_config.py`
- Test: `bullforge/tests/test_runtime_config.py`

- [ ] **Step 1: Write the failing test**

```python
# bullforge/tests/test_runtime_config.py
import pytest
from unittest.mock import AsyncMock
from shared.runtime_config import RuntimeConfig, CONFIG_SCHEMA


@pytest.fixture
def fake_redis():
    r = AsyncMock()
    r.client = AsyncMock()
    # Simulate empty hash on first read
    r.client.hget = AsyncMock(return_value=None)
    r.client.hset = AsyncMock(return_value=1)
    r.client.hgetall = AsyncMock(return_value={})
    return r


@pytest.mark.asyncio
async def test_get_returns_default_when_missing(fake_redis):
    cfg = RuntimeConfig(fake_redis)
    val = await cfg.get("retrain_schedule_hour")
    assert val == 1
    assert isinstance(val, int)


@pytest.mark.asyncio
async def test_get_parses_type_from_schema(fake_redis):
    fake_redis.client.hget = AsyncMock(return_value="0.005")
    cfg = RuntimeConfig(fake_redis)
    val = await cfg.get("lstm_learning_rate")
    assert val == 0.005
    assert isinstance(val, float)


@pytest.mark.asyncio
async def test_set_validates_range(fake_redis):
    cfg = RuntimeConfig(fake_redis)
    with pytest.raises(ValueError, match="must be between"):
        await cfg.set("retrain_schedule_hour", 24)


@pytest.mark.asyncio
async def test_set_validates_unknown_key(fake_redis):
    cfg = RuntimeConfig(fake_redis)
    with pytest.raises(KeyError, match="unknown config key"):
        await cfg.set("not_a_real_key", 5)


@pytest.mark.asyncio
async def test_set_writes_redis_hash(fake_redis):
    cfg = RuntimeConfig(fake_redis)
    await cfg.set("retrain_schedule_hour", 3)
    fake_redis.client.hset.assert_called_once_with(
        "bullforge:config", "retrain_schedule_hour", "3"
    )


@pytest.mark.asyncio
async def test_get_all_merges_defaults_and_stored(fake_redis):
    fake_redis.client.hgetall = AsyncMock(
        return_value={"retrain_schedule_hour": "5"}
    )
    cfg = RuntimeConfig(fake_redis)
    result = await cfg.get_all()
    assert result["retrain_schedule_hour"] == 5  # stored
    assert result["retrain_cpu_workers"] == 15   # default
    assert result["lstm_epochs"] == 50           # default


def test_schema_has_required_keys():
    required = {
        "retrain_schedule_hour", "retrain_cpu_workers",
        "lstm_epochs", "lstm_hidden_size", "lstm_sequence_length",
        "lstm_learning_rate", "lstm_batch_size",
    }
    assert required.issubset(CONFIG_SCHEMA.keys())
    for key, meta in CONFIG_SCHEMA.items():
        assert "default" in meta
        assert "type" in meta
        assert meta["type"] in ("int", "float", "str", "json")
        assert "label" in meta
        assert "description" in meta
        assert "reference" in meta
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && pytest tests/test_runtime_config.py -v`
Expected: FAIL with "No module named 'shared.runtime_config'"

- [ ] **Step 3: Write minimal implementation**

```python
# bullforge/shared/runtime_config.py
from __future__ import annotations

import json
import logging
from typing import Any

from shared.redis import RedisClient

logger = logging.getLogger(__name__)

CONFIG_HASH_KEY = "bullforge:config"

CONFIG_SCHEMA: dict[str, dict[str, Any]] = {
    "retrain_schedule_hour": {
        "default": 1, "type": "int", "min": 0, "max": 23,
        "label": "Retrain Hour (UTC)",
        "description": "Hour of day (0-23 UTC) when nightly model retraining begins. Default 1 AM UTC = 9 PM EDT.",
        "reference": "https://en.wikipedia.org/wiki/Training,_validation,_and_test_data_sets",
    },
    "retrain_cpu_workers": {
        "default": 15, "type": "int", "min": 1, "max": 20,
        "label": "CPU Workers for Training",
        "description": "Number of parallel processes for gradient boosting model training. Each worker trains one model at a time. Higher values train more models simultaneously but increase memory usage.",
        "reference": "https://en.wikipedia.org/wiki/Parallel_computing",
    },
    "lstm_epochs": {
        "default": 50, "type": "int", "min": 1, "max": 500,
        "label": "LSTM Training Epochs",
        "description": "Number of complete passes through the training dataset for LSTM neural network models. More epochs can improve accuracy but risk overfitting.",
        "reference": "https://en.wikipedia.org/wiki/Epoch_(machine_learning)",
    },
    "lstm_hidden_size": {
        "default": 64, "type": "int", "min": 16, "max": 512,
        "label": "LSTM Hidden Layer Size",
        "description": "Number of hidden units in each LSTM layer. Larger values increase model capacity but require more memory.",
        "reference": "https://en.wikipedia.org/wiki/Long_short-term_memory",
    },
    "lstm_sequence_length": {
        "default": 30, "type": "int", "min": 5, "max": 200,
        "label": "LSTM Lookback Window",
        "description": "Number of consecutive time steps fed into the LSTM as input context.",
        "reference": "https://en.wikipedia.org/wiki/Sliding_window_protocol",
    },
    "lstm_learning_rate": {
        "default": 0.001, "type": "float", "min": 0.00001, "max": 0.1,
        "label": "LSTM Learning Rate",
        "description": "Step size for the Adam optimizer during LSTM training. Smaller values train more slowly but converge more reliably.",
        "reference": "https://en.wikipedia.org/wiki/Learning_rate",
    },
    "lstm_batch_size": {
        "default": 64, "type": "int", "min": 8, "max": 512,
        "label": "LSTM Batch Size",
        "description": "Number of training samples processed together in each gradient update.",
        "reference": "https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Mini-batch_gradient_descent",
    },
}


def _parse(value: str, type_: str) -> Any:
    if type_ == "int":
        return int(value)
    if type_ == "float":
        return float(value)
    if type_ == "json":
        return json.loads(value)
    return value


def _serialize(value: Any, type_: str) -> str:
    if type_ == "json":
        return json.dumps(value)
    return str(value)


class RuntimeConfig:
    def __init__(self, redis_client: RedisClient):
        self._redis = redis_client

    async def get(self, key: str) -> Any:
        if key not in CONFIG_SCHEMA:
            raise KeyError(f"unknown config key: {key}")
        meta = CONFIG_SCHEMA[key]
        raw = await self._redis.client.hget(CONFIG_HASH_KEY, key)
        if raw is None:
            return meta["default"]
        return _parse(raw, meta["type"])

    async def set(self, key: str, value: Any) -> None:
        if key not in CONFIG_SCHEMA:
            raise KeyError(f"unknown config key: {key}")
        meta = CONFIG_SCHEMA[key]
        if "min" in meta and value < meta["min"]:
            raise ValueError(
                f"{key}={value} must be between {meta['min']} and {meta['max']}"
            )
        if "max" in meta and value > meta["max"]:
            raise ValueError(
                f"{key}={value} must be between {meta['min']} and {meta['max']}"
            )
        await self._redis.client.hset(
            CONFIG_HASH_KEY, key, _serialize(value, meta["type"])
        )

    async def get_all(self) -> dict[str, Any]:
        stored = await self._redis.client.hgetall(CONFIG_HASH_KEY)
        result: dict[str, Any] = {}
        for key, meta in CONFIG_SCHEMA.items():
            if key in stored:
                result[key] = _parse(stored[key], meta["type"])
            else:
                result[key] = meta["default"]
        return result

    def get_schema(self) -> dict[str, dict[str, Any]]:
        return CONFIG_SCHEMA
```

- [ ] **Step 4: Run tests to verify PASS**

Run: `cd bullforge && pytest tests/test_runtime_config.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/runtime_config.py bullforge/tests/test_runtime_config.py
git commit -m "feat: add RuntimeConfig with CONFIG_SCHEMA for tunable params"
```

---

## Task 2: `as_of` parameter on `query_training_data` + data-integrity test harness

**Files:**
- Modify: `bullforge/shared/db.py:253-278`
- Modify: `bullforge/services/model_service/dataset.py:19-28`
- Create: `bullforge/tests/test_data_integrity.py`

- [ ] **Step 1: Write the failing test — Invariant 2 (forward-label leakage)**

```python
# bullforge/tests/test_data_integrity.py
"""
Data-integrity guardrail tests.

These tests guard against look-ahead bias (data leakage / future-data contamination),
the #1 killer of trading ML systems. They MUST never be skipped or weakened.

Invariants tested:
1. No same-bar feature contamination (test_features_exclude_current_bar)
2. No forward-label leakage into training data (test_training_data_respects_horizon_cutoff)
3. No trade-outcome features from open or future trades (test_trade_features_exclude_open_and_future_trades)
4. Training/test split is strictly chronological (test_time_based_split_is_chronological)
5. Live inference uses only closed bars (test_feature_update_published_only_after_persist)
"""
import pytest
from datetime import datetime, timedelta, timezone

import pandas as pd

from services.model_service.dataset import time_based_split


def test_time_based_split_is_chronological():
    # Invariant 4
    n = 100
    ts = [datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=i) for i in range(n)]
    X = pd.DataFrame({"feat": range(n), "ts": ts})
    y = pd.Series(range(n))
    X_train, X_test, y_train, y_test = time_based_split(X, y, test_ratio=0.2)
    assert X_train["ts"].max() < X_test["ts"].min()
    assert len(X_train) + len(X_test) == n
```

- [ ] **Step 2: Add Invariant 2 test**

Append to `bullforge/tests/test_data_integrity.py`:

```python
@pytest.mark.asyncio
async def test_training_data_respects_horizon_cutoff(real_test_db):
    """
    Invariant 2: forward_return label must come from bars at (T, T+H],
    and training cutoff `as_of` must satisfy T+H ≤ as_of for every row.
    """
    # real_test_db fixture: yields a Database connected to a fresh test TimescaleDB
    db = real_test_db
    symbol, tf, horizon = "TEST", "1m", 5

    base = datetime(2026, 1, 1, 14, 0, tzinfo=timezone.utc)
    # Insert features+labels at T, T+1m, T+2m, ...
    for i in range(20):
        ts = base + timedelta(minutes=i)
        await db.pool.execute(
            "INSERT INTO features (symbol, timestamp, timeframe, features) VALUES ($1,$2,$3,$4)",
            symbol, ts, tf, '{"x": 1.0}',
        )
        await db.pool.execute(
            "INSERT INTO labels (symbol, timestamp, timeframe, horizon_minutes, forward_return, direction) VALUES ($1,$2,$3,$4,$5,$6)",
            symbol, ts, tf, horizon, 0.001, 1,
        )

    # as_of in the middle: feature_timestamp + horizon must be <= as_of
    as_of = base + timedelta(minutes=10)
    rows = await db.query_training_data(symbol, tf, horizon, as_of=as_of, limit=1000)
    for row in rows:
        # timestamp + horizon must be <= as_of
        assert row["timestamp"] + timedelta(minutes=horizon) <= as_of, (
            f"Row at {row['timestamp']} violates as_of cutoff: "
            f"{row['timestamp']} + {horizon}m > {as_of}"
        )
    # Boundary check: row at T where T+horizon == as_of is INCLUDED
    expected_included_cutoff = as_of - timedelta(minutes=horizon)
    included_timestamps = {r["timestamp"] for r in rows}
    assert expected_included_cutoff in included_timestamps


@pytest.mark.asyncio
async def test_query_training_data_requires_as_of(real_test_db):
    """Caller MUST pass as_of — no default allowed."""
    db = real_test_db
    with pytest.raises(TypeError):
        await db.query_training_data("TEST", "1m", 5)  # no as_of
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd bullforge && pytest tests/test_data_integrity.py::test_time_based_split_is_chronological -v`
Expected: PASS (time_based_split already chronological — this is a guardrail)

Run: `cd bullforge && pytest tests/test_data_integrity.py::test_query_training_data_requires_as_of -v`
Expected: FAIL (default currently allowed)

- [ ] **Step 4: Modify `shared/db.py` to require `as_of`**

Edit `bullforge/shared/db.py` — replace the existing `query_training_data` method (around line 253):

```python
    async def query_training_data(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        *,
        as_of: datetime,
        limit: int = 10000,
    ) -> list[dict]:
        """
        Query training rows where feature_timestamp + horizon <= as_of.

        as_of is a REQUIRED keyword argument to prevent accidental omission
        that would leak future data into training. See docs data-integrity spec.
        """
        if as_of.tzinfo is None or as_of.tzinfo != timezone.utc:
            raise ValueError(f"as_of must be UTC, got tzinfo={as_of.tzinfo}")
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT f.timestamp, f.features, l.direction, l.forward_return
                FROM features f
                INNER JOIN labels l
                    ON f.symbol = l.symbol
                    AND f.timestamp = l.timestamp
                    AND f.timeframe = l.timeframe
                WHERE f.symbol = $1 AND f.timeframe = $2 AND l.horizon_minutes = $3
                  AND f.timestamp + ($3 * INTERVAL '1 minute') <= $4
                ORDER BY f.timestamp ASC
                LIMIT $5
                """,
                symbol, timeframe, horizon_minutes, as_of, limit,
            )
        return [dict(r) for r in rows]
```

Ensure `from datetime import datetime, timezone` is present at top of file.

- [ ] **Step 5: Update `DatasetBuilder.build` to require `as_of`**

Edit `bullforge/services/model_service/dataset.py`:

```python
from datetime import datetime

class DatasetBuilder:
    def __init__(self, db: Database, min_samples: int = 500):
        self._db = db
        self._min_samples = min_samples

    async def build(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        *,
        as_of: datetime,
        limit: int = 10000,
    ) -> Optional[Tuple[pd.DataFrame, pd.Series]]:
        rows = await self._db.query_training_data(
            symbol, timeframe, horizon_minutes, as_of=as_of, limit=limit,
        )
        # ... rest unchanged
```

- [ ] **Step 6: Update `Trainer.train` and `main.py` call sites**

Edit `bullforge/services/model_service/trainer.py` — `train()` signature:

```python
async def train(
    self,
    symbol: str,
    timeframe: str,
    horizon_minutes: int,
    *,
    as_of: datetime,
    model_type: str = "gradient_boost",
    runtime_config: Optional[dict] = None,
) -> Optional[ModelMetadata]:
    ...
    result = await self._dataset_builder.build(
        symbol, timeframe, horizon_minutes, as_of=as_of,
    )
    ...
    model = create_model(model_type, runtime_config=runtime_config)
    ...
```

Edit `bullforge/services/model_service/main.py` — near line 58 and line 89:

```python
from datetime import datetime, timezone
...
# startup loop captures as_of once
startup_as_of = datetime.now(timezone.utc)
rows = await db.query_training_data(
    all_symbols[0], "1m", HORIZONS[0],
    as_of=startup_as_of, limit=settings.model_min_samples,
)
...
await trainer.train(
    symbol, timeframe, horizon,
    as_of=startup_as_of,
    model_type="gradient_boost",
)
```

- [ ] **Step 7: Run all affected tests**

Run: `cd bullforge && pytest tests/test_data_integrity.py tests/test_trainer.py tests/test_dataset.py -v`
Expected: all pass (update any existing test that called `query_training_data` without `as_of`)

- [ ] **Step 8: Fix existing tests broken by signature change**

Grep for callers: `grep -rn "query_training_data\|dataset_builder.build\|trainer.train" bullforge/tests/`
Add `as_of=datetime(2026, 4, 13, tzinfo=timezone.utc)` (or a similar deterministic UTC datetime) to each call.

- [ ] **Step 9: Commit**

```bash
git add bullforge/shared/db.py bullforge/services/model_service/dataset.py bullforge/services/model_service/trainer.py bullforge/services/model_service/main.py bullforge/tests/test_data_integrity.py bullforge/tests/test_dataset.py bullforge/tests/test_trainer.py
git commit -m "feat: require as_of cutoff on query_training_data (prevents future-data leakage)"
```

---

## Task 3: Data-integrity Invariants 1, 3, 5

**Files:**
- Modify: `bullforge/tests/test_data_integrity.py`
- Modify (if needed): `bullforge/services/feature_engine/engine.py` — to pass invariant 1 test

- [ ] **Step 1: Write Invariant 1 test (features exclude current bar)**

```python
# Append to bullforge/tests/test_data_integrity.py
import pandas as pd
from datetime import datetime, timedelta, timezone
from shared.models import Bar
from services.feature_engine.engine import FeatureEngine


def test_features_exclude_current_bar():
    """
    Invariant 1: Feature row at timestamp T must use ONLY bars with timestamp < T.
    The bar AT T must not contribute to any feature AT T.
    """
    base = datetime(2026, 1, 1, 14, 0, tzinfo=timezone.utc)
    # Build a synthetic bar stream: all bars close=100.0, except T itself which is 99999.99
    bars = []
    for i in range(100):
        ts = base + timedelta(minutes=i)
        close = 99999.99 if i == 50 else 100.0
        bars.append(Bar(
            symbol="TEST", timestamp=ts, open=close, high=close, low=close,
            close=close, volume=1000, timeframe="1m",
        ))
    target_ts = base + timedelta(minutes=50)

    engine = FeatureEngine(db=None, redis_client=None)  # offline compute
    features_at_target = engine.compute_for_bar(bars, target_ts, timeframe="1m")

    for feat_name, feat_value in features_at_target.items():
        if feat_value is None or (isinstance(feat_value, float) and pd.isna(feat_value)):
            continue
        assert abs(feat_value - 99999.99) > 1e-6, (
            f"Feature {feat_name}={feat_value} leaked from current-bar close=99999.99"
        )
        # Also catch derived values proportional to the poisoned bar
        assert abs(feat_value - 99999.99 / 2) > 1e-6
```

- [ ] **Step 2: Run the test, see it fail**

Run: `cd bullforge && pytest tests/test_data_integrity.py::test_features_exclude_current_bar -v`
Expected: FAIL (likely — if feature engine includes bar at T, it will poison SMA/EMA values)

- [ ] **Step 3: Inspect `feature_engine/engine.py` for current-bar inclusion**

Read: `bullforge/services/feature_engine/engine.py` — look for `compute_for_bar` or the equivalent. If it uses `<=` for the timestamp filter, change to `<`. If a helper method `compute_for_bar(bars, target_ts, timeframe)` does not exist, add a thin wrapper that satisfies the test:

```python
def compute_for_bar(self, bars: list[Bar], target_ts: datetime, timeframe: str) -> dict[str, float]:
    """Compute features at target_ts using ONLY bars with timestamp < target_ts."""
    historical = [b for b in bars if b.timestamp < target_ts]
    df = self._bars_to_dataframe(historical)
    return self._compute_all(df, timeframe)
```

(Adjust to match actual FeatureEngine API. If engine already does this, the test just passes; the purpose is to lock in the behavior.)

- [ ] **Step 4: Run the test, verify PASS**

Run: `cd bullforge && pytest tests/test_data_integrity.py::test_features_exclude_current_bar -v`
Expected: PASS

- [ ] **Step 5: Write Invariant 3 test (trade-outcome features exclude open/future trades)**

```python
# Append to test_data_integrity.py
@pytest.mark.asyncio
async def test_trade_features_exclude_open_and_future_trades(real_test_db):
    """
    Invariant 3: Features at timestamp T that derive from trade history must
    use ONLY trades with exit_time < T. Open trades and trades closing at/after T
    MUST NOT contribute.

    This test is a guardrail: it is required even if no trade-outcome features exist today.
    """
    from shared.models import Trade
    db = real_test_db
    T = datetime(2026, 1, 1, 14, 0, tzinfo=timezone.utc)

    # Trade 1: closed before T — MUST contribute
    await db.insert_trade(Trade(
        symbol="SPY", side="long",
        entry_time=T - timedelta(hours=2), entry_price=420.0, quantity=10,
        entry_order_id=1,
    ))
    # Simulate closing it
    await db.update_trade_exit(
        trade_id=1, exit_time=T - timedelta(hours=1), exit_price=421.0,
        pnl=10.0, hold_duration_seconds=3600, exit_order_id=2,
    )

    # Trade 2: open (no exit) — MUST NOT contribute
    await db.insert_trade(Trade(
        symbol="SPY", side="long",
        entry_time=T - timedelta(minutes=30), entry_price=421.0, quantity=10,
        entry_order_id=3,
    ))

    # Trade 3: closed after T — MUST NOT contribute
    await db.insert_trade(Trade(
        symbol="SPY", side="long",
        entry_time=T - timedelta(minutes=10), entry_price=421.5, quantity=10,
        entry_order_id=5,
    ))
    await db.update_trade_exit(
        trade_id=3, exit_time=T + timedelta(minutes=5), exit_price=422.0,
        pnl=5.0, hold_duration_seconds=900, exit_order_id=6,
    )

    # query_trades_closed_before is the authoritative source for trade-derived features
    trades = await db.query_trades_closed_before("SPY", T)
    assert len(trades) == 1
    assert trades[0]["id"] == 1
```

- [ ] **Step 6: Add `query_trades_closed_before` to `shared/db.py`**

```python
async def query_trades_closed_before(self, symbol: str, as_of: datetime) -> list[dict]:
    """Trades for symbol with exit_time strictly before as_of. Excludes open trades."""
    if as_of.tzinfo is None or as_of.tzinfo != timezone.utc:
        raise ValueError(f"as_of must be UTC, got {as_of.tzinfo}")
    async with self.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, symbol, side, entry_time, entry_price, quantity,
                   exit_time, exit_price, pnl
            FROM trades
            WHERE symbol = $1 AND exit_time IS NOT NULL AND exit_time < $2
            ORDER BY exit_time ASC
            """,
            symbol, as_of,
        )
    return [dict(r) for r in rows]
```

- [ ] **Step 7: Write Invariant 5 test (feature_updates published after persist)**

```python
# Append to test_data_integrity.py
@pytest.mark.asyncio
async def test_feature_update_published_only_after_persist(monkeypatch):
    """
    Invariant 5: feature:updates is published ONLY after the feature row for
    the closed bar has been persisted to the DB.
    """
    from unittest.mock import AsyncMock, call
    from services.feature_engine.engine import FeatureEngine

    call_order: list[str] = []

    db = AsyncMock()
    async def fake_insert(*a, **kw):
        call_order.append("insert")
    db.insert_feature = fake_insert

    redis_client = AsyncMock()
    async def fake_publish(channel, msg):
        call_order.append(f"publish:{channel}")
    redis_client.publish = fake_publish

    engine = FeatureEngine(db=db, redis_client=redis_client)
    bar = Bar(
        symbol="TEST",
        timestamp=datetime(2026, 1, 1, 14, 0, tzinfo=timezone.utc),
        open=100, high=100, low=100, close=100, volume=1, timeframe="1m",
    )
    await engine.on_bar(bar)

    insert_idx = call_order.index("insert")
    publish_idx = next(
        i for i, s in enumerate(call_order) if s.startswith("publish:feature:updates")
    )
    assert insert_idx < publish_idx, (
        f"feature:updates published before insert: order was {call_order}"
    )
```

- [ ] **Step 8: Run all 5 invariant tests**

Run: `cd bullforge && pytest tests/test_data_integrity.py -v`
Expected: all 5 invariant tests PASS (plus the `test_query_training_data_requires_as_of` guardrail). Fix `FeatureEngine.on_bar` if step 7 fails — ensure `db.insert_feature(...)` is awaited before `redis_client.publish("feature:updates", ...)`.

- [ ] **Step 9: Commit**

```bash
git add bullforge/tests/test_data_integrity.py bullforge/shared/db.py bullforge/services/feature_engine/engine.py
git commit -m "test: add 5 look-ahead-bias invariant tests and supporting db query"
```

---

## Task 4: LSTM model (sklearn-compatible wrapper)

**Files:**
- Create: `bullforge/services/model_service/lstm_model.py`
- Test: `bullforge/tests/test_lstm_model.py`
- Modify: `bullforge/services/model_service/requirements.txt`

- [ ] **Step 1: Add torch to requirements**

Edit `bullforge/services/model_service/requirements.txt`, append:

```
torch>=2.2.0
lightgbm>=4.3.0
```

- [ ] **Step 2: Write failing test**

```python
# bullforge/tests/test_lstm_model.py
import numpy as np
import pandas as pd
import pytest

from services.model_service.lstm_model import LSTMClassifier


@pytest.fixture
def synthetic_data():
    rng = np.random.default_rng(42)
    n = 500
    X = pd.DataFrame(rng.normal(size=(n, 4)), columns=["a", "b", "c", "d"])
    # label: 1 if sum of last 3 rows of 'a' > 0, else 0; flat class 2 when near zero
    roll = X["a"].rolling(3).sum().fillna(0)
    y = pd.Series(np.where(roll > 0.3, 1, np.where(roll < -0.3, -1, 0)))
    return X, y


def test_lstm_fit_predict_shapes(synthetic_data):
    X, y = synthetic_data
    m = LSTMClassifier(
        hidden_size=16, sequence_length=10, epochs=3, batch_size=32, lr=0.01,
    )
    m.fit(X, y)
    preds = m.predict(X)
    assert len(preds) == len(X)
    assert set(np.unique(preds)).issubset({-1, 0, 1})


def test_lstm_predict_proba_shape(synthetic_data):
    X, y = synthetic_data
    m = LSTMClassifier(
        hidden_size=16, sequence_length=10, epochs=2, batch_size=32, lr=0.01,
    )
    m.fit(X, y)
    proba = m.predict_proba(X)
    assert proba.shape == (len(X), 3)
    row_sums = proba.sum(axis=1)
    assert np.allclose(row_sums, 1.0, atol=1e-4)


def test_lstm_uses_cpu_when_cuda_unavailable(monkeypatch):
    import torch
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    m = LSTMClassifier(hidden_size=8, sequence_length=5, epochs=1, batch_size=16, lr=0.01)
    assert m.device.type == "cpu"


def test_lstm_early_stopping_runs(synthetic_data):
    X, y = synthetic_data
    m = LSTMClassifier(
        hidden_size=16, sequence_length=10, epochs=100, batch_size=32, lr=0.01,
        early_stopping_patience=2,
    )
    m.fit(X, y)
    # With very high epoch and small patience, we expect fewer than 100 epochs actually run
    assert m.epochs_trained_ < 100
```

- [ ] **Step 3: Run test, see it fail**

Run: `cd bullforge && pytest tests/test_lstm_model.py -v`
Expected: FAIL "No module named 'services.model_service.lstm_model'"

- [ ] **Step 4: Implement LSTMClassifier**

```python
# bullforge/services/model_service/lstm_model.py
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd
import torch
from torch import nn
from torch.utils.data import DataLoader, TensorDataset

logger = logging.getLogger(__name__)

# Mapping from direction labels {-1, 0, 1} to class indices {0, 1, 2}
_LABEL_TO_CLASS = {-1: 0, 0: 1, 1: 2}
_CLASS_TO_LABEL = {v: k for k, v in _LABEL_TO_CLASS.items()}


class _LSTMNet(nn.Module):
    def __init__(self, input_size: int, hidden_size: int, num_classes: int = 3):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            batch_first=True,
        )
        self.fc = nn.Linear(hidden_size, num_classes)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        last = out[:, -1, :]
        return self.fc(last)


def _make_sequences(
    X: np.ndarray, y: Optional[np.ndarray], seq_len: int,
) -> tuple[np.ndarray, Optional[np.ndarray]]:
    """Turn flat (N, F) rows into (N-seq_len+1, seq_len, F) sequences aligned with y[seq_len-1:]."""
    n = len(X)
    if n < seq_len:
        raise ValueError(f"Need at least {seq_len} rows, got {n}")
    seqs = np.stack([X[i : i + seq_len] for i in range(n - seq_len + 1)], axis=0)
    y_aligned = None if y is None else y[seq_len - 1 :]
    return seqs, y_aligned


class LSTMClassifier:
    """sklearn-compatible PyTorch LSTM classifier for {-1, 0, 1} direction labels."""

    def __init__(
        self,
        hidden_size: int = 64,
        sequence_length: int = 30,
        num_classes: int = 3,
        lr: float = 0.001,
        epochs: int = 50,
        batch_size: int = 64,
        early_stopping_patience: int = 5,
        val_ratio: float = 0.2,
        random_state: int = 42,
    ):
        self.hidden_size = hidden_size
        self.sequence_length = sequence_length
        self.num_classes = num_classes
        self.lr = lr
        self.epochs = epochs
        self.batch_size = batch_size
        self.early_stopping_patience = early_stopping_patience
        self.val_ratio = val_ratio
        self.random_state = random_state
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self._net: Optional[_LSTMNet] = None
        self._feature_columns: Optional[list[str]] = None
        self.epochs_trained_: int = 0

    def _to_classes(self, y: pd.Series) -> np.ndarray:
        return np.array([_LABEL_TO_CLASS[int(v)] for v in y], dtype=np.int64)

    def fit(self, X: pd.DataFrame, y: pd.Series) -> "LSTMClassifier":
        torch.manual_seed(self.random_state)
        self._feature_columns = list(X.columns)
        X_arr = X.to_numpy(dtype=np.float32)
        y_arr = self._to_classes(y)

        X_seq, y_seq = _make_sequences(X_arr, y_arr, self.sequence_length)

        # Time-based train/val split
        split = int(len(X_seq) * (1 - self.val_ratio))
        X_tr, X_val = X_seq[:split], X_seq[split:]
        y_tr, y_val = y_seq[:split], y_seq[split:]

        train_ds = TensorDataset(
            torch.from_numpy(X_tr), torch.from_numpy(y_tr),
        )
        train_dl = DataLoader(train_ds, batch_size=self.batch_size, shuffle=False)
        X_val_t = torch.from_numpy(X_val).to(self.device)
        y_val_t = torch.from_numpy(y_val).to(self.device)

        input_size = X_arr.shape[1]
        self._net = _LSTMNet(input_size, self.hidden_size, self.num_classes).to(self.device)
        optim = torch.optim.Adam(self._net.parameters(), lr=self.lr)
        criterion = nn.CrossEntropyLoss()

        best_val_loss = float("inf")
        best_state = None
        patience_left = self.early_stopping_patience

        for epoch in range(self.epochs):
            self._net.train()
            for xb, yb in train_dl:
                xb, yb = xb.to(self.device), yb.to(self.device)
                optim.zero_grad()
                logits = self._net(xb)
                loss = criterion(logits, yb)
                loss.backward()
                optim.step()

            self._net.eval()
            with torch.no_grad():
                val_logits = self._net(X_val_t)
                val_loss = criterion(val_logits, y_val_t).item()

            self.epochs_trained_ = epoch + 1
            if val_loss < best_val_loss - 1e-5:
                best_val_loss = val_loss
                best_state = {k: v.clone() for k, v in self._net.state_dict().items()}
                patience_left = self.early_stopping_patience
            else:
                patience_left -= 1
                if patience_left <= 0:
                    logger.info("LSTM early stopping at epoch %d (val_loss=%.4f)",
                                epoch + 1, best_val_loss)
                    break

        if best_state is not None:
            self._net.load_state_dict(best_state)
        return self

    def _predict_logits(self, X: pd.DataFrame) -> np.ndarray:
        assert self._net is not None, "Must call fit() before predict"
        X_arr = X.to_numpy(dtype=np.float32)
        X_seq, _ = _make_sequences(X_arr, None, self.sequence_length)
        self._net.eval()
        with torch.no_grad():
            logits = self._net(torch.from_numpy(X_seq).to(self.device))
        logits_np = logits.cpu().numpy()
        # Pad the first (sequence_length - 1) rows with the first prediction so
        # output length matches input length (sklearn convention).
        pad = np.repeat(logits_np[:1], self.sequence_length - 1, axis=0)
        return np.concatenate([pad, logits_np], axis=0)

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        logits = self._predict_logits(X)
        cls = logits.argmax(axis=1)
        return np.array([_CLASS_TO_LABEL[int(c)] for c in cls])

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        logits = self._predict_logits(X)
        t = torch.from_numpy(logits)
        return torch.softmax(t, dim=1).numpy()
```

- [ ] **Step 5: Run tests, verify pass**

Run: `cd bullforge && pytest tests/test_lstm_model.py -v`
Expected: 4 pass

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/model_service/lstm_model.py bullforge/services/model_service/requirements.txt bullforge/tests/test_lstm_model.py
git commit -m "feat: add PyTorch LSTMClassifier with sklearn-compatible interface"
```

---

## Task 5: Model factory accepts lstm + runtime_config

**Files:**
- Modify: `bullforge/services/model_service/model_factory.py`
- Modify: `bullforge/tests/test_model_factory.py` (if exists; else create)

- [ ] **Step 1: Write failing test**

Create or append to `bullforge/tests/test_model_factory.py`:

```python
import pytest
from services.model_service.model_factory import create_model, SUPPORTED_MODELS
from services.model_service.lstm_model import LSTMClassifier


def test_supported_models_includes_lstm():
    assert "lstm" in SUPPORTED_MODELS


def test_create_lstm_default_config():
    m = create_model("lstm")
    assert isinstance(m, LSTMClassifier)
    assert m.hidden_size == 64
    assert m.sequence_length == 30
    assert m.epochs == 50


def test_create_lstm_runtime_config_override():
    cfg = {
        "lstm_hidden_size": 128, "lstm_sequence_length": 20,
        "lstm_learning_rate": 0.005, "lstm_epochs": 10, "lstm_batch_size": 32,
    }
    m = create_model("lstm", runtime_config=cfg)
    assert m.hidden_size == 128
    assert m.sequence_length == 20
    assert m.lr == 0.005
    assert m.epochs == 10
    assert m.batch_size == 32


def test_create_gradient_boost_still_works():
    m = create_model("gradient_boost")
    assert m is not None
```

- [ ] **Step 2: Run, verify fail**

Run: `cd bullforge && pytest tests/test_model_factory.py -v`
Expected: FAIL

- [ ] **Step 3: Update model_factory.py**

```python
# bullforge/services/model_service/model_factory.py
from __future__ import annotations
from typing import Optional

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from services.model_service.lstm_model import LSTMClassifier

SUPPORTED_MODELS = {"gradient_boost", "logistic_regression", "lstm"}


def create_model(model_type: str, runtime_config: Optional[dict] = None) -> object:
    cfg = runtime_config or {}
    if model_type == "gradient_boost":
        return Pipeline([
            ("scaler", StandardScaler()),
            ("classifier", GradientBoostingClassifier(
                n_estimators=100, max_depth=3, learning_rate=0.1, random_state=42,
            )),
        ])
    if model_type == "logistic_regression":
        return Pipeline([
            ("scaler", StandardScaler()),
            ("classifier", LogisticRegression(max_iter=1000, random_state=42)),
        ])
    if model_type == "lstm":
        return LSTMClassifier(
            hidden_size=cfg.get("lstm_hidden_size", 64),
            sequence_length=cfg.get("lstm_sequence_length", 30),
            lr=cfg.get("lstm_learning_rate", 0.001),
            epochs=cfg.get("lstm_epochs", 50),
            batch_size=cfg.get("lstm_batch_size", 64),
        )
    raise ValueError(
        f"Unknown model type: {model_type}. Supported: {SUPPORTED_MODELS}"
    )
```

- [ ] **Step 4: Run tests, PASS**

Run: `cd bullforge && pytest tests/test_model_factory.py -v`
Expected: 4 pass

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/model_service/model_factory.py bullforge/tests/test_model_factory.py
git commit -m "feat: model_factory supports lstm with runtime_config overrides"
```

---

## Task 6: ModelStore save/load dispatches .joblib vs .pt

**Files:**
- Modify: `bullforge/services/model_service/model_store.py`
- Test: `bullforge/tests/test_model_store_lstm.py`

- [ ] **Step 1: Write failing test**

```python
# bullforge/tests/test_model_store_lstm.py
import numpy as np
import pandas as pd
import pytest

from services.model_service.model_store import ModelStore
from services.model_service.lstm_model import LSTMClassifier


def test_save_lstm_uses_pt_extension(tmp_path):
    store = ModelStore(str(tmp_path))
    m = LSTMClassifier(hidden_size=8, sequence_length=5, epochs=1, batch_size=16, lr=0.01)
    rng = np.random.default_rng(0)
    X = pd.DataFrame(rng.normal(size=(100, 3)), columns=list("abc"))
    y = pd.Series(rng.choice([-1, 0, 1], size=100))
    m.fit(X, y)

    path = store.save(m, "SPY", "1m", 5, "lstm")
    assert path.endswith(".pt")
    assert "lstm_SPY_1m_5m" in path


def test_load_lstm_roundtrip(tmp_path):
    store = ModelStore(str(tmp_path))
    m = LSTMClassifier(hidden_size=8, sequence_length=5, epochs=1, batch_size=16, lr=0.01)
    rng = np.random.default_rng(1)
    X = pd.DataFrame(rng.normal(size=(100, 3)), columns=list("abc"))
    y = pd.Series(rng.choice([-1, 0, 1], size=100))
    m.fit(X, y)
    path = store.save(m, "SPY", "1m", 5, "lstm")
    loaded = store.load(path)
    assert isinstance(loaded, LSTMClassifier)
    orig = m.predict(X)
    new = loaded.predict(X)
    assert np.array_equal(orig, new)


def test_save_gradient_boost_still_uses_joblib(tmp_path):
    from services.model_service.model_factory import create_model
    store = ModelStore(str(tmp_path))
    m = create_model("gradient_boost")
    rng = np.random.default_rng(2)
    X = pd.DataFrame(rng.normal(size=(50, 3)), columns=list("abc"))
    y = pd.Series(rng.choice([-1, 0, 1], size=50))
    m.fit(X, y)
    path = store.save(m, "SPY", "1m", 5, "gradient_boost")
    assert path.endswith(".joblib")
```

- [ ] **Step 2: Run, see failures**

Run: `cd bullforge && pytest tests/test_model_store_lstm.py -v`
Expected: FAIL

- [ ] **Step 3: Update `model_store.py`**

```python
from __future__ import annotations

import logging
import pickle
from datetime import datetime, timezone
from pathlib import Path

import joblib
import torch

from services.model_service.lstm_model import LSTMClassifier

logger = logging.getLogger(__name__)


class ModelStore:
    def __init__(self, artifacts_dir: str):
        self._dir = Path(artifacts_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

    def save(self, model: object, symbol: str, timeframe: str,
             horizon_minutes: int, model_type: str) -> str:
        now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        if isinstance(model, LSTMClassifier):
            ext = "pt"
        else:
            ext = "joblib"
        filename = f"{model_type}_{symbol}_{timeframe}_{horizon_minutes}m_{now}.{ext}"
        path = self._dir / filename
        if isinstance(model, LSTMClassifier):
            # Save full object via torch.save (uses pickle under the hood, preserves device)
            torch.save(model, path)
        else:
            joblib.dump(model, path)
        logger.info("Saved model artifact to %s", path)
        return str(path)

    def load(self, artifact_path: str) -> object:
        path = Path(artifact_path)
        if not path.exists():
            raise FileNotFoundError(f"Model artifact not found: {artifact_path}")
        if path.suffix == ".pt":
            # weights_only=False because we pickled the full LSTMClassifier object
            model = torch.load(path, weights_only=False, map_location="cpu")
            # Move underlying network to best available device
            if isinstance(model, LSTMClassifier) and model._net is not None:
                model.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
                model._net = model._net.to(model.device)
        else:
            model = joblib.load(path)
        logger.info("Loaded model artifact from %s", path)
        return model
```

- [ ] **Step 4: Run, PASS**

Run: `cd bullforge && pytest tests/test_model_store_lstm.py -v`
Expected: 3 pass

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/model_service/model_store.py bullforge/tests/test_model_store_lstm.py
git commit -m "feat: ModelStore dispatches .pt for LSTM, .joblib for sklearn"
```

---

## Task 7: ProcessPool worker entrypoint

**Files:**
- Create: `bullforge/services/model_service/worker.py`
- Test: covered by scheduler test in Task 8

- [ ] **Step 1: Implement worker (must be top-level function, picklable)**

```python
# bullforge/services/model_service/worker.py
"""
Top-level worker function for ProcessPoolExecutor. Must be importable by the
child process; no closures, no local state.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def train_one_job(
    db_dsn: str,
    artifacts_dir: str,
    symbol: str,
    timeframe: str,
    horizon_minutes: int,
    model_type: str,
    as_of: datetime,
    runtime_config: dict,
    min_samples: int,
) -> dict:
    """
    Run a full training job in a worker process.

    Returns a summary dict: {symbol, timeframe, horizon, model_type, status, model_id?, error?}
    """
    # Local imports inside the worker to avoid pickling module-level state
    from shared.config import Settings
    from shared.db import Database
    from services.model_service.trainer import Trainer
    from services.model_service.model_store import ModelStore

    async def _run() -> dict:
        settings = Settings()
        # Override DSN if caller provided an explicit one
        db = Database(settings)
        await db.connect()
        try:
            store = ModelStore(artifacts_dir)
            trainer = Trainer(db=db, model_store=store, min_samples=min_samples)
            meta = await trainer.train(
                symbol, timeframe, horizon_minutes,
                as_of=as_of,
                model_type=model_type,
                runtime_config=runtime_config,
            )
            if meta is None:
                return {
                    "symbol": symbol, "timeframe": timeframe,
                    "horizon": horizon_minutes, "model_type": model_type,
                    "status": "skipped_insufficient_data",
                }
            return {
                "symbol": symbol, "timeframe": timeframe,
                "horizon": horizon_minutes, "model_type": model_type,
                "status": "ok", "model_id": meta.id,
                "metrics": meta.metrics,
            }
        finally:
            await db.disconnect()

    try:
        return asyncio.run(_run())
    except Exception as e:
        logger.exception("Training job failed: %s %s %sm %s",
                         symbol, timeframe, horizon_minutes, model_type)
        return {
            "symbol": symbol, "timeframe": timeframe,
            "horizon": horizon_minutes, "model_type": model_type,
            "status": "error", "error": str(e),
        }
```

- [ ] **Step 2: Commit**

```bash
git add bullforge/services/model_service/worker.py
git commit -m "feat: add ProcessPool worker entrypoint for parallel CPU training"
```

---

## Task 8: RetrainScheduler (TDD)

**Files:**
- Create: `bullforge/services/model_service/scheduler.py`
- Test: `bullforge/tests/test_scheduler.py`

- [ ] **Step 1: Write failing test**

```python
# bullforge/tests/test_scheduler.py
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.model_service.scheduler import RetrainScheduler, compute_seconds_until


def test_compute_seconds_until_same_day():
    now = datetime(2026, 4, 13, 10, 0, 0, tzinfo=timezone.utc)
    # Next 14:00 UTC today
    secs = compute_seconds_until(hour=14, now=now)
    assert secs == 4 * 3600


def test_compute_seconds_until_next_day():
    now = datetime(2026, 4, 13, 15, 0, 0, tzinfo=timezone.utc)
    # 14:00 already passed today -> 14:00 tomorrow
    secs = compute_seconds_until(hour=14, now=now)
    assert secs == 23 * 3600


@pytest.mark.asyncio
async def test_run_once_dispatches_cpu_then_gpu_phase():
    runtime_config = AsyncMock()
    runtime_config.get = AsyncMock(side_effect=lambda k: {
        "retrain_cpu_workers": 2,
        "lstm_epochs": 1, "lstm_hidden_size": 8, "lstm_sequence_length": 5,
        "lstm_learning_rate": 0.01, "lstm_batch_size": 16,
    }.get(k))
    runtime_config.get_all = AsyncMock(return_value={
        "retrain_cpu_workers": 2,
        "lstm_epochs": 1, "lstm_hidden_size": 8, "lstm_sequence_length": 5,
        "lstm_learning_rate": 0.01, "lstm_batch_size": 16,
    })
    runtime_config.set = AsyncMock()

    inference_engine = AsyncMock()
    redis_client = AsyncMock()

    sched = RetrainScheduler(
        db_dsn="postgres://fake",
        artifacts_dir="/tmp/artifacts",
        runtime_config=runtime_config,
        inference_engine=inference_engine,
        redis_client=redis_client,
        symbols=["SPY"],
        timeframes=["1m"],
        horizons=[5],
        min_samples=10,
    )

    # Patch the ProcessPoolExecutor path so the test runs in-process
    with patch("services.model_service.scheduler.ProcessPoolExecutor") as pool_cls:
        pool_instance = MagicMock()
        pool_cls.return_value.__enter__.return_value = pool_instance
        future = MagicMock()
        future.result.return_value = {
            "symbol": "SPY", "timeframe": "1m", "horizon": 5,
            "model_type": "gradient_boost", "status": "ok", "model_id": 42,
        }
        pool_instance.submit.return_value = future

        # Patch the in-process LSTM training call
        with patch.object(sched, "_train_lstm_in_process", new=AsyncMock(
            return_value={"symbol": "SPY", "timeframe": "1m", "horizon": 5,
                          "model_type": "lstm", "status": "ok", "model_id": 43}
        )):
            summary = await sched.run_once()

    # CPU phase: ProcessPool.submit called once for gradient_boost job
    assert pool_instance.submit.call_count == 1
    # GPU phase called
    assert sched._train_lstm_in_process.await_count == 1
    # Hot-swap invoked
    inference_engine.load_models.assert_awaited_once()
    # model:retrained published
    redis_client.publish.assert_awaited()
    # as_of stored
    runtime_config.set.assert_any_call("last_retrain_time", ANY := runtime_config.set.call_args_list[-1][0][1])


@pytest.mark.asyncio
async def test_run_once_handles_worker_error():
    runtime_config = AsyncMock()
    runtime_config.get = AsyncMock(return_value=1)
    runtime_config.get_all = AsyncMock(return_value={
        "retrain_cpu_workers": 1, "lstm_epochs": 1, "lstm_hidden_size": 8,
        "lstm_sequence_length": 5, "lstm_learning_rate": 0.01, "lstm_batch_size": 16,
    })
    runtime_config.set = AsyncMock()
    inference_engine = AsyncMock()
    redis_client = AsyncMock()

    sched = RetrainScheduler(
        db_dsn="postgres://fake", artifacts_dir="/tmp", runtime_config=runtime_config,
        inference_engine=inference_engine, redis_client=redis_client,
        symbols=["SPY"], timeframes=["1m"], horizons=[5], min_samples=10,
    )

    with patch("services.model_service.scheduler.ProcessPoolExecutor") as pool_cls:
        pool_instance = MagicMock()
        pool_cls.return_value.__enter__.return_value = pool_instance
        future = MagicMock()
        future.result.return_value = {
            "symbol": "SPY", "timeframe": "1m", "horizon": 5,
            "model_type": "gradient_boost", "status": "error", "error": "boom",
        }
        pool_instance.submit.return_value = future

        with patch.object(sched, "_train_lstm_in_process", new=AsyncMock(
            return_value={"status": "ok", "symbol": "SPY", "timeframe": "1m",
                          "horizon": 5, "model_type": "lstm"}
        )):
            summary = await sched.run_once()

    # Error surfaces in summary but doesn't raise
    assert any(r["status"] == "error" for r in summary["results"])
    # Hot-swap still called so successful models take effect
    inference_engine.load_models.assert_awaited_once()
```

- [ ] **Step 2: Run, fail**

Run: `cd bullforge && pytest tests/test_scheduler.py -v`
Expected: FAIL

- [ ] **Step 3: Implement scheduler**

```python
# bullforge/services/model_service/scheduler.py
from __future__ import annotations

import asyncio
import json
import logging
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Optional

from shared.db import Database
from shared.redis import RedisClient
from shared.runtime_config import RuntimeConfig
from services.model_service.inference import InferenceEngine
from services.model_service.model_store import ModelStore
from services.model_service.trainer import Trainer
from services.model_service.worker import train_one_job

logger = logging.getLogger(__name__)


def compute_seconds_until(hour: int, now: Optional[datetime] = None) -> float:
    now = now or datetime.now(timezone.utc)
    target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    return (target - now).total_seconds()


class RetrainScheduler:
    def __init__(
        self,
        db_dsn: str,
        artifacts_dir: str,
        runtime_config: RuntimeConfig,
        inference_engine: InferenceEngine,
        redis_client: RedisClient,
        symbols: list[str],
        timeframes: list[str],
        horizons: list[int],
        min_samples: int,
        db: Optional[Database] = None,
    ):
        self._db_dsn = db_dsn
        self._artifacts_dir = artifacts_dir
        self._rc = runtime_config
        self._inference = inference_engine
        self._redis = redis_client
        self._symbols = symbols
        self._timeframes = timeframes
        self._horizons = horizons
        self._min_samples = min_samples
        self._db = db  # for GPU phase (in-process)
        self._stop = asyncio.Event()

    async def run_forever(self) -> None:
        while not self._stop.is_set():
            hour = int(await self._rc.get("retrain_schedule_hour"))
            delay = compute_seconds_until(hour)
            logger.info("Next retrain in %.0f seconds (at %02d:00 UTC)", delay, hour)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
                return  # stop requested
            except asyncio.TimeoutError:
                pass
            try:
                await self.run_once()
            except Exception:
                logger.exception("Retrain cycle failed")

    def stop(self) -> None:
        self._stop.set()

    async def run_once(self) -> dict:
        as_of = datetime.now(timezone.utc)
        config = await self._rc.get_all()
        workers = int(config["retrain_cpu_workers"])
        logger.info("Retrain cycle starting (as_of=%s, workers=%d)", as_of, workers)

        # Build CPU jobs (gradient_boost) and GPU jobs (lstm)
        cpu_jobs = []
        gpu_jobs = []
        for symbol in self._symbols:
            for tf in self._timeframes:
                for h in self._horizons:
                    cpu_jobs.append((symbol, tf, h, "gradient_boost"))
                    gpu_jobs.append((symbol, tf, h, "lstm"))

        results: list[dict] = []

        # Phase 1: CPU parallel
        loop = asyncio.get_running_loop()
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = []
            for symbol, tf, h, mt in cpu_jobs:
                fut = pool.submit(
                    train_one_job,
                    self._db_dsn, self._artifacts_dir,
                    symbol, tf, h, mt, as_of, config, self._min_samples,
                )
                futures.append(fut)
            for fut in futures:
                try:
                    results.append(await loop.run_in_executor(None, fut.result))
                except Exception as e:
                    logger.exception("CPU worker raised")
                    results.append({"status": "error", "error": str(e)})

        # Phase 2: GPU sequential
        for symbol, tf, h, mt in gpu_jobs:
            try:
                res = await self._train_lstm_in_process(symbol, tf, h, as_of, config)
                results.append(res)
            except Exception as e:
                logger.exception("GPU worker raised")
                results.append({
                    "symbol": symbol, "timeframe": tf, "horizon": h,
                    "model_type": mt, "status": "error", "error": str(e),
                })

        # Hot-swap active models
        await self._inference.load_models()

        summary = {
            "as_of": as_of.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "ok": sum(1 for r in results if r.get("status") == "ok"),
            "errors": sum(1 for r in results if r.get("status") == "error"),
            "skipped": sum(1 for r in results if r.get("status") == "skipped_insufficient_data"),
            "results": results,
        }

        await self._redis.publish("model:retrained", json.dumps(summary))
        await self._rc.set("last_retrain_time", as_of.isoformat())
        logger.info("Retrain cycle complete: ok=%d err=%d skipped=%d",
                    summary["ok"], summary["errors"], summary["skipped"])
        return summary

    async def _train_lstm_in_process(
        self, symbol: str, timeframe: str, horizon: int,
        as_of: datetime, runtime_config: dict,
    ) -> dict:
        """LSTM trains in the main process (GPU single-tenant)."""
        if self._db is None:
            raise RuntimeError("RetrainScheduler needs a Database instance for GPU phase")
        store = ModelStore(self._artifacts_dir)
        trainer = Trainer(db=self._db, model_store=store, min_samples=self._min_samples)
        meta = await trainer.train(
            symbol, timeframe, horizon,
            as_of=as_of, model_type="lstm", runtime_config=runtime_config,
        )
        if meta is None:
            return {"symbol": symbol, "timeframe": timeframe, "horizon": horizon,
                    "model_type": "lstm", "status": "skipped_insufficient_data"}
        return {"symbol": symbol, "timeframe": timeframe, "horizon": horizon,
                "model_type": "lstm", "status": "ok", "model_id": meta.id,
                "metrics": meta.metrics}
```

- [ ] **Step 4: Run tests, PASS**

Run: `cd bullforge && pytest tests/test_scheduler.py -v`
Expected: pass (adjust any `ANY` import if pytest complains — use `unittest.mock.ANY`)

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/model_service/scheduler.py bullforge/tests/test_scheduler.py
git commit -m "feat: RetrainScheduler with parallel CPU + sequential GPU phases"
```

---

## Task 9: Wire scheduler into main.py

**Files:**
- Modify: `bullforge/services/model_service/main.py`

- [ ] **Step 1: Update main.py to start the scheduler background task**

Replace the relevant part of `main()` in `bullforge/services/model_service/main.py` — after inference_engine setup, before the subscribe:

```python
from datetime import datetime, timezone
from shared.runtime_config import RuntimeConfig
from services.model_service.scheduler import RetrainScheduler

# ... after inference_engine is created and model_store:
runtime_config = RuntimeConfig(redis_client)

# Startup train (existing behavior — only if no active models)
startup_as_of = datetime.now(timezone.utc)
for symbol in all_symbols:
    for timeframe in ["1m", "1d"]:
        for horizon in HORIZONS:
            existing = await db.query_active_model(symbol, timeframe, horizon)
            if existing:
                continue
            await trainer.train(
                symbol, timeframe, horizon,
                as_of=startup_as_of, model_type="gradient_boost",
            )

await inference_engine.load_models()

# Start nightly scheduler
scheduler = RetrainScheduler(
    db_dsn=settings.database_dsn if hasattr(settings, "database_dsn") else "",
    artifacts_dir=settings.model_artifacts_dir,
    runtime_config=runtime_config,
    inference_engine=inference_engine,
    redis_client=redis_client,
    symbols=all_symbols,
    timeframes=["1m", "1d"],
    horizons=HORIZONS,
    min_samples=settings.model_min_samples,
    db=db,
)
scheduler_task = asyncio.create_task(scheduler.run_forever())
logger.info("Retrain scheduler started")
```

And in the shutdown path:

```python
finally:
    logger.info("Shutting down...")
    scheduler.stop()
    scheduler_task.cancel()
    try:
        await scheduler_task
    except (asyncio.CancelledError, Exception):
        pass
    await runner.cleanup()
    await redis_client.disconnect()
    await db.disconnect()
```

- [ ] **Step 2: Smoke-test**

Run: `cd bullforge && pytest tests/ -v -k "model_service or scheduler or runtime_config"`
Expected: all related tests pass

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/model_service/main.py
git commit -m "feat: wire RetrainScheduler background task into model_service"
```

---

## Task 10: Dockerfile + Compose GPU setup

**Files:**
- Modify: `bullforge/services/model_service/Dockerfile`
- Modify: `bullforge/docker-compose.yml`

- [ ] **Step 1: Replace Dockerfile**

```dockerfile
FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.12 python3.12-venv python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3.12 /usr/bin/python \
    && ln -sf /usr/bin/python3.12 /usr/bin/python3

WORKDIR /app

COPY services/model_service/requirements.txt /app/services/model_service/requirements.txt
RUN pip install --no-cache-dir --break-system-packages \
    --index-url https://download.pytorch.org/whl/cu124 torch \
 && pip install --no-cache-dir --break-system-packages \
    -r services/model_service/requirements.txt

COPY shared/ /app/shared/
COPY migrations/ /app/migrations/
COPY services/__init__.py /app/services/__init__.py
COPY services/model_service/ /app/services/model_service/

RUN mkdir -p /app/models/artifacts

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "services.model_service.main"]
```

(Remove `torch` from `requirements.txt` since we install via the CUDA wheel index.)

- [ ] **Step 2: Update requirements.txt**

```
scikit-learn>=1.4.0
joblib>=1.3.0
pandas>=2.2.0
numpy>=1.26.0
asyncpg>=0.29.0
redis>=5.0.0
aiohttp>=3.9.0
lightgbm>=4.3.0
```

(`torch` no longer listed — installed from CUDA index in Dockerfile.)

- [ ] **Step 3: Update docker-compose.yml — model_service section**

Add under the `model_service` service:

```yaml
  model_service:
    # ... existing keys preserved
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
```

- [ ] **Step 4: Rebuild and verify**

```bash
cd bullforge && docker compose build model_service
docker compose up -d model_service
docker compose logs -f model_service | head -50
```

Expected: container starts, logs show torch CUDA detection line on GPU-enabled host, or falls back to CPU on non-GPU hosts.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/model_service/Dockerfile bullforge/services/model_service/requirements.txt bullforge/docker-compose.yml
git commit -m "build: model_service uses CUDA base image with GPU reservation"
```

---

## Final Self-Review Checklist

- [ ] All 5 data-integrity invariants have a named test in `test_data_integrity.py`
- [ ] `query_training_data` refuses to run without `as_of` (Task 2 step 2 test)
- [ ] RuntimeConfig CONFIG_SCHEMA has every key called out in the spec (retrain_schedule_hour, retrain_cpu_workers, lstm_{epochs, hidden_size, sequence_length, learning_rate, batch_size})
- [ ] LSTM model is sklearn-compatible (`fit`/`predict`/`predict_proba`) and trained via the same `Trainer`
- [ ] ModelStore dispatches `.pt` for LSTM and `.joblib` for sklearn models
- [ ] Scheduler dispatches CPU via ProcessPoolExecutor(retrain_cpu_workers), GPU sequentially in main process
- [ ] `InferenceEngine.load_models()` called after every successful retrain cycle
- [ ] `model:retrained` event published to Redis
- [ ] `last_retrain_time` stored back to RuntimeConfig
- [ ] Dockerfile uses CUDA base image; compose reserves 1 GPU
- [ ] Startup training path still works (skips if active model exists) and uses `as_of=now`

---

## Execution Handoff

Plan complete. Execute via:

1. **Subagent-Driven** (recommended) — `superpowers:subagent-driven-development`
2. **Inline** — `superpowers:executing-plans`
