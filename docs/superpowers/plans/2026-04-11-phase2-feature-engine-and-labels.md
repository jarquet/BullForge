# Phase 2: Feature Engine & Labels — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a feature computation engine that reads raw bar data from TimescaleDB, computes technical indicators and price features, generates forward-looking training labels, and stores everything back in TimescaleDB — ready for Phase 3 model training.

**Architecture:** A new `feature_engine` service reads bars from TimescaleDB (written by the Phase 1 data pipeline), computes features through a pluggable registry of feature calculators, and writes feature rows + labels back to new TimescaleDB tables. The engine runs on-demand (backfill mode) and reactively (triggered via Redis pub/sub when new bars arrive). Each feature calculator is a pure function that takes a pandas DataFrame of bars and returns a DataFrame of computed columns — no DB or I/O inside calculators. The engine orchestrator handles all DB reads/writes and calls calculators in sequence.

**Tech Stack:** Python 3.12+, pandas, numpy, asyncio, asyncpg (from Phase 1 shared/db.py), Redis pub/sub (from Phase 1 shared/redis.py), Docker Compose, pytest, pytest-asyncio

---

## File Structure

```
bullforge/
  shared/
    config.py                             — (modify) Add feature engine settings
    db.py                                 — (modify) Add feature/label query and insert methods
    models.py                             — (modify) Add FeatureRow and Label dataclasses

  services/
    feature_engine/
      __init__.py
      Dockerfile                          — Container image for feature engine
      requirements.txt                    — Python dependencies (adds pandas, numpy)
      main.py                             — Entrypoint: runs backfill and reactive modes
      registry.py                         — Feature registry: registers and runs calculators
      calculators/
        __init__.py
        price.py                          — Returns, log returns, volatility, spread
        technical.py                      — SMA, EMA, RSI, MACD, Bollinger Bands, ATR
        volume.py                         — VWAP, OBV, volume ratio
        calendar.py                       — Day-of-week, time-of-day, is_fed_day, is_near_earnings
      labels.py                           — Forward return labels, triple-barrier labels
      engine.py                           — Orchestrator: reads bars, runs calculators, writes results

  migrations/
    003_create_features.sql               — TimescaleDB schema for features table
    004_create_labels.sql                 — TimescaleDB schema for labels table

  tests/
    test_feature_registry.py              — Tests for registry
    test_calc_price.py                    — Tests for price calculators
    test_calc_technical.py                — Tests for technical indicator calculators
    test_calc_volume.py                   — Tests for volume calculators
    test_calc_calendar.py                 — Tests for calendar feature calculators
    test_labels.py                        — Tests for label generation
    test_engine.py                        — Tests for orchestrator
```

---

## Task 1: Feature and Label Data Models

**Files:**
- Modify: `bullforge/shared/models.py`
- Create: `bullforge/tests/test_feature_models.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_feature_models.py`:

```python
from datetime import datetime, timezone
import pytest
from shared.models import FeatureRow, Label


def test_feature_row_creation():
    row = FeatureRow(
        symbol="SPY",
        timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
        timeframe="1m",
        features={"sma_20": 420.5, "rsi_14": 55.3, "log_return": 0.001},
    )
    assert row.symbol == "SPY"
    assert row.features["sma_20"] == 420.5
    assert row.timeframe == "1m"


def test_feature_row_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="UTC"):
        FeatureRow(
            symbol="SPY",
            timestamp=datetime(2026, 4, 10, 15, 30, 0),
            timeframe="1m",
            features={"sma_20": 420.5},
        )


def test_feature_row_rejects_empty_features():
    with pytest.raises(ValueError, match="empty"):
        FeatureRow(
            symbol="SPY",
            timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
            timeframe="1m",
            features={},
        )


def test_label_creation():
    label = Label(
        symbol="SPY",
        timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
        timeframe="1m",
        horizon_minutes=30,
        forward_return=0.0025,
        direction=1,
    )
    assert label.symbol == "SPY"
    assert label.forward_return == 0.0025
    assert label.direction == 1
    assert label.horizon_minutes == 30


def test_label_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="UTC"):
        Label(
            symbol="SPY",
            timestamp=datetime(2026, 4, 10, 15, 30, 0),
            timeframe="1m",
            horizon_minutes=30,
            forward_return=0.0025,
            direction=1,
        )


def test_label_direction_values():
    """Direction must be -1, 0, or 1."""
    with pytest.raises(ValueError, match="direction"):
        Label(
            symbol="SPY",
            timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
            timeframe="1m",
            horizon_minutes=30,
            forward_return=0.0025,
            direction=2,
        )
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_feature_models.py -v
```

Expected: FAIL with `ImportError: cannot import name 'FeatureRow' from 'shared.models'`

- [ ] **Step 3: Implement FeatureRow and Label models**

Add to the end of `bullforge/shared/models.py`:

```python
@dataclass(frozen=True)
class FeatureRow:
    symbol: str
    timestamp: datetime
    timeframe: str  # "1m", "1d"
    features: dict  # {"sma_20": 420.5, "rsi_14": 55.3, ...}

    def __post_init__(self):
        if self.timestamp.tzinfo is None or self.timestamp.tzinfo != timezone.utc:
            raise ValueError(
                f"FeatureRow timestamp must be UTC, got tzinfo={self.timestamp.tzinfo}"
            )
        if not self.features:
            raise ValueError("FeatureRow features must not be empty")


@dataclass(frozen=True)
class Label:
    symbol: str
    timestamp: datetime
    timeframe: str  # "1m", "1d"
    horizon_minutes: int  # how far forward the label looks
    forward_return: float  # actual return over the horizon
    direction: int  # -1 = down, 0 = flat, 1 = up

    def __post_init__(self):
        if self.timestamp.tzinfo is None or self.timestamp.tzinfo != timezone.utc:
            raise ValueError(
                f"Label timestamp must be UTC, got tzinfo={self.timestamp.tzinfo}"
            )
        if self.direction not in (-1, 0, 1):
            raise ValueError(
                f"Label direction must be -1, 0, or 1, got {self.direction}"
            )
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_feature_models.py -v
```

Expected: All 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
cd <repo>
git add bullforge/shared/models.py bullforge/tests/test_feature_models.py
git commit -m "feat: add FeatureRow and Label data models"
```

---

## Task 2: Feature and Label Database Schema

**Files:**
- Create: `bullforge/migrations/003_create_features.sql`
- Create: `bullforge/migrations/004_create_labels.sql`
- Modify: `bullforge/shared/db.py`
- Create: `bullforge/tests/test_feature_db.py`

- [ ] **Step 1: Write migration SQL for features**

Write `bullforge/migrations/003_create_features.sql`:

```sql
CREATE TABLE IF NOT EXISTS features (
    symbol      TEXT            NOT NULL,
    timestamp   TIMESTAMPTZ     NOT NULL,
    timeframe   TEXT            NOT NULL,
    features    JSONB           NOT NULL,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, timestamp, timeframe)
);

SELECT create_hypertable('features', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_features_symbol_timeframe
    ON features (symbol, timeframe, timestamp DESC);
```

- [ ] **Step 2: Write migration SQL for labels**

Write `bullforge/migrations/004_create_labels.sql`:

```sql
CREATE TABLE IF NOT EXISTS labels (
    symbol          TEXT            NOT NULL,
    timestamp       TIMESTAMPTZ     NOT NULL,
    timeframe       TEXT            NOT NULL,
    horizon_minutes INT             NOT NULL,
    forward_return  DOUBLE PRECISION NOT NULL,
    direction       SMALLINT        NOT NULL,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, timestamp, timeframe, horizon_minutes)
);

SELECT create_hypertable('labels', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_labels_symbol_timeframe
    ON labels (symbol, timeframe, timestamp DESC);
```

- [ ] **Step 3: Write the failing test for feature/label DB methods**

Write `bullforge/tests/test_feature_db.py`:

```python
import pytest
import pytest_asyncio
from datetime import datetime, timezone
from shared.config import Settings
from shared.db import Database
from shared.models import FeatureRow, Label


@pytest_asyncio.fixture
async def db():
    settings = Settings()
    database = Database(settings)
    await database.connect()
    yield database
    async with database.pool.acquire() as conn:
        await conn.execute("DELETE FROM features")
        await conn.execute("DELETE FROM labels")
    await database.disconnect()


@pytest.mark.asyncio
async def test_insert_and_query_feature_row(db):
    row = FeatureRow(
        symbol="SPY",
        timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
        timeframe="1m",
        features={"sma_20": 420.5, "rsi_14": 55.3},
    )
    await db.insert_feature_row(row)

    rows = await db.query_feature_rows("SPY", "1m", limit=1)
    assert len(rows) == 1
    assert rows[0].symbol == "SPY"
    assert rows[0].features["sma_20"] == 420.5
    assert rows[0].features["rsi_14"] == 55.3


@pytest.mark.asyncio
async def test_insert_feature_row_upsert(db):
    ts = datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc)
    row1 = FeatureRow(symbol="SPY", timestamp=ts, timeframe="1m",
                      features={"sma_20": 420.5})
    row2 = FeatureRow(symbol="SPY", timestamp=ts, timeframe="1m",
                      features={"sma_20": 421.0, "rsi_14": 60.0})

    await db.insert_feature_row(row1)
    await db.insert_feature_row(row2)

    rows = await db.query_feature_rows("SPY", "1m", limit=10)
    assert len(rows) == 1
    assert rows[0].features["sma_20"] == 421.0
    assert rows[0].features["rsi_14"] == 60.0


@pytest.mark.asyncio
async def test_insert_feature_rows_batch(db):
    rows = [
        FeatureRow(
            symbol="SPY",
            timestamp=datetime(2026, 4, 10, 15, i, 0, tzinfo=timezone.utc),
            timeframe="1m",
            features={"sma_20": 420.0 + i},
        )
        for i in range(5)
    ]
    await db.insert_feature_rows(rows)

    result = await db.query_feature_rows("SPY", "1m", limit=10)
    assert len(result) == 5


@pytest.mark.asyncio
async def test_insert_and_query_label(db):
    label = Label(
        symbol="SPY",
        timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
        timeframe="1m",
        horizon_minutes=30,
        forward_return=0.0025,
        direction=1,
    )
    await db.insert_label(label)

    labels = await db.query_labels("SPY", "1m", horizon_minutes=30, limit=1)
    assert len(labels) == 1
    assert labels[0].forward_return == 0.0025
    assert labels[0].direction == 1


@pytest.mark.asyncio
async def test_insert_labels_batch(db):
    labels = [
        Label(
            symbol="SPY",
            timestamp=datetime(2026, 4, 10, 15, i, 0, tzinfo=timezone.utc),
            timeframe="1m",
            horizon_minutes=30,
            forward_return=0.001 * i,
            direction=1 if i % 2 == 0 else -1,
        )
        for i in range(5)
    ]
    await db.insert_labels(labels)

    result = await db.query_labels("SPY", "1m", horizon_minutes=30, limit=10)
    assert len(result) == 5
```

- [ ] **Step 4: Run test to verify it fails**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_feature_db.py -v
```

Expected: FAIL with `AttributeError: 'Database' object has no attribute 'insert_feature_row'`

**Note:** Requires a running TimescaleDB instance (same as Phase 1 tests).

- [ ] **Step 5: Implement feature/label DB methods**

Add the following imports to the top of `bullforge/shared/db.py`, alongside the existing ones:

```python
import json
```

Add `FeatureRow, Label` to the existing import from `shared.models`:

```python
from shared.models import Bar, FeatureRow, Label
```

Add the following methods to the `Database` class in `bullforge/shared/db.py`, after the existing `query_bars` method:

```python
    async def insert_feature_row(self, row: FeatureRow) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO features (symbol, timestamp, timeframe, features)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (symbol, timestamp, timeframe)
                DO UPDATE SET features = $4
                """,
                row.symbol,
                row.timestamp,
                row.timeframe,
                json.dumps(row.features),
            )

    async def insert_feature_rows(self, rows: List[FeatureRow]) -> None:
        if not rows:
            return
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO features (symbol, timestamp, timeframe, features)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (symbol, timestamp, timeframe)
                DO UPDATE SET features = $4
                """,
                [
                    (r.symbol, r.timestamp, r.timeframe, json.dumps(r.features))
                    for r in rows
                ],
            )

    async def query_feature_rows(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 100,
    ) -> List[FeatureRow]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, timestamp, timeframe, features
                FROM features
                WHERE symbol = $1 AND timeframe = $2
                ORDER BY timestamp DESC
                LIMIT $3
                """,
                symbol,
                timeframe,
                limit,
            )
        return [
            FeatureRow(
                symbol=r["symbol"],
                timestamp=r["timestamp"],
                timeframe=r["timeframe"],
                features=json.loads(r["features"]),
            )
            for r in rows
        ]

    async def insert_label(self, label: Label) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO labels (symbol, timestamp, timeframe, horizon_minutes,
                                    forward_return, direction)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (symbol, timestamp, timeframe, horizon_minutes)
                DO UPDATE SET forward_return = $5, direction = $6
                """,
                label.symbol,
                label.timestamp,
                label.timeframe,
                label.horizon_minutes,
                label.forward_return,
                label.direction,
            )

    async def insert_labels(self, labels: List[Label]) -> None:
        if not labels:
            return
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO labels (symbol, timestamp, timeframe, horizon_minutes,
                                    forward_return, direction)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (symbol, timestamp, timeframe, horizon_minutes)
                DO UPDATE SET forward_return = $5, direction = $6
                """,
                [
                    (
                        l.symbol, l.timestamp, l.timeframe,
                        l.horizon_minutes, l.forward_return, l.direction,
                    )
                    for l in labels
                ],
            )

    async def query_labels(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        limit: int = 100,
    ) -> List[Label]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, timestamp, timeframe, horizon_minutes,
                       forward_return, direction
                FROM labels
                WHERE symbol = $1 AND timeframe = $2 AND horizon_minutes = $3
                ORDER BY timestamp DESC
                LIMIT $4
                """,
                symbol,
                timeframe,
                horizon_minutes,
                limit,
            )
        return [
            Label(
                symbol=r["symbol"],
                timestamp=r["timestamp"],
                timeframe=r["timeframe"],
                horizon_minutes=r["horizon_minutes"],
                forward_return=r["forward_return"],
                direction=r["direction"],
            )
            for r in rows
        ]
```

- [ ] **Step 6: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_feature_db.py -v
```

Expected: All 5 tests PASS.

- [ ] **Step 7: Commit**

```bash
cd <repo>
git add bullforge/migrations/003_create_features.sql bullforge/migrations/004_create_labels.sql bullforge/shared/db.py bullforge/tests/test_feature_db.py
git commit -m "feat: feature and label DB schema with insert/query methods"
```

---

## Task 3: Feature Registry

**Files:**
- Create: `bullforge/services/feature_engine/__init__.py`
- Create: `bullforge/services/feature_engine/calculators/__init__.py`
- Create: `bullforge/services/feature_engine/registry.py`
- Create: `bullforge/tests/test_feature_registry.py`

- [ ] **Step 1: Create directory structure**

```bash
cd <repo>
mkdir -p bullforge/services/feature_engine/calculators
touch bullforge/services/feature_engine/__init__.py
touch bullforge/services/feature_engine/calculators/__init__.py
```

- [ ] **Step 2: Write the failing test**

Write `bullforge/tests/test_feature_registry.py`:

```python
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from services.feature_engine.registry import FeatureRegistry


def _make_bars_df(n: int = 50) -> pd.DataFrame:
    """Create a realistic bars DataFrame for testing."""
    timestamps = pd.date_range(
        start="2026-04-10 14:00:00", periods=n, freq="1min", tz="UTC"
    )
    np.random.seed(42)
    close = 420.0 + np.cumsum(np.random.randn(n) * 0.1)
    return pd.DataFrame({
        "symbol": "SPY",
        "timestamp": timestamps,
        "open": close - np.random.rand(n) * 0.2,
        "high": close + np.random.rand(n) * 0.3,
        "low": close - np.random.rand(n) * 0.3,
        "close": close,
        "volume": np.random.randint(1000, 50000, size=n),
    })


def simple_calculator(df: pd.DataFrame) -> pd.DataFrame:
    """A trivial calculator for testing the registry."""
    return pd.DataFrame({
        "close_doubled": df["close"] * 2,
    }, index=df.index)


def another_calculator(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "volume_log": np.log1p(df["volume"]),
    }, index=df.index)


def test_register_and_list():
    registry = FeatureRegistry()
    registry.register("simple", simple_calculator)
    assert "simple" in registry.list_calculators()


def test_register_duplicate_raises():
    registry = FeatureRegistry()
    registry.register("simple", simple_calculator)
    with pytest.raises(ValueError, match="already registered"):
        registry.register("simple", simple_calculator)


def test_compute_all_runs_registered_calculators():
    registry = FeatureRegistry()
    registry.register("simple", simple_calculator)
    registry.register("another", another_calculator)

    df = _make_bars_df(30)
    result = registry.compute_all(df)

    assert "close_doubled" in result.columns
    assert "volume_log" in result.columns
    assert len(result) == 30


def test_compute_all_preserves_index():
    registry = FeatureRegistry()
    registry.register("simple", simple_calculator)

    df = _make_bars_df(20)
    result = registry.compute_all(df)

    assert result.index.equals(df.index)


def test_compute_all_empty_registry():
    registry = FeatureRegistry()
    df = _make_bars_df(10)
    result = registry.compute_all(df)
    assert result.empty or len(result.columns) == 0
```

- [ ] **Step 3: Run test to verify it fails**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_feature_registry.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'services.feature_engine'`

- [ ] **Step 4: Implement feature registry**

Write `bullforge/services/feature_engine/registry.py`:

```python
from __future__ import annotations

import logging
from typing import Callable, Dict, List

import pandas as pd

logger = logging.getLogger(__name__)

CalculatorFn = Callable[[pd.DataFrame], pd.DataFrame]


class FeatureRegistry:
    def __init__(self):
        self._calculators: Dict[str, CalculatorFn] = {}

    def register(self, name: str, fn: CalculatorFn) -> None:
        if name in self._calculators:
            raise ValueError(f"Calculator '{name}' already registered")
        self._calculators[name] = fn
        logger.debug("Registered feature calculator: %s", name)

    def list_calculators(self) -> List[str]:
        return list(self._calculators.keys())

    def compute_all(self, bars_df: pd.DataFrame) -> pd.DataFrame:
        parts = []
        for name, fn in self._calculators.items():
            logger.debug("Running calculator: %s", name)
            result = fn(bars_df)
            parts.append(result)
        if not parts:
            return pd.DataFrame(index=bars_df.index)
        return pd.concat(parts, axis=1)
```

- [ ] **Step 5: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_feature_registry.py -v
```

Expected: All 5 tests PASS.

- [ ] **Step 6: Commit**

```bash
cd <repo>
git add bullforge/services/feature_engine/ bullforge/tests/test_feature_registry.py
git commit -m "feat: feature registry with pluggable calculator pattern"
```

---

## Task 4: Price Feature Calculators

**Files:**
- Create: `bullforge/services/feature_engine/calculators/price.py`
- Create: `bullforge/tests/test_calc_price.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_calc_price.py`:

```python
import pytest
import pandas as pd
import numpy as np
from services.feature_engine.calculators.price import compute_price_features


def _make_bars_df(n: int = 60) -> pd.DataFrame:
    timestamps = pd.date_range(
        start="2026-04-10 14:00:00", periods=n, freq="1min", tz="UTC"
    )
    np.random.seed(42)
    close = 420.0 + np.cumsum(np.random.randn(n) * 0.1)
    return pd.DataFrame({
        "symbol": "SPY",
        "timestamp": timestamps,
        "open": close - np.random.rand(n) * 0.2,
        "high": close + np.random.rand(n) * 0.3,
        "low": close - np.random.rand(n) * 0.3,
        "close": close,
        "volume": np.random.randint(1000, 50000, size=n),
    })


def test_price_features_returns_expected_columns():
    df = _make_bars_df()
    result = compute_price_features(df)
    expected_cols = [
        "return_1", "return_5", "return_10",
        "log_return_1",
        "volatility_10", "volatility_20",
        "high_low_spread", "close_open_spread",
    ]
    for col in expected_cols:
        assert col in result.columns, f"Missing column: {col}"


def test_price_features_return_1_is_pct_change():
    df = _make_bars_df(30)
    result = compute_price_features(df)
    expected = df["close"].pct_change(1)
    pd.testing.assert_series_equal(
        result["return_1"], expected, check_names=False
    )


def test_price_features_log_return():
    df = _make_bars_df(30)
    result = compute_price_features(df)
    expected = np.log(df["close"] / df["close"].shift(1))
    pd.testing.assert_series_equal(
        result["log_return_1"], expected, check_names=False
    )


def test_price_features_volatility_is_rolling_std():
    df = _make_bars_df(30)
    result = compute_price_features(df)
    returns = df["close"].pct_change(1)
    expected = returns.rolling(window=10).std()
    pd.testing.assert_series_equal(
        result["volatility_10"], expected, check_names=False
    )


def test_price_features_spread():
    df = _make_bars_df(30)
    result = compute_price_features(df)
    expected_hl = (df["high"] - df["low"]) / df["close"]
    pd.testing.assert_series_equal(
        result["high_low_spread"], expected_hl, check_names=False
    )


def test_price_features_preserves_length():
    df = _make_bars_df(50)
    result = compute_price_features(df)
    assert len(result) == len(df)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_calc_price.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'services.feature_engine.calculators.price'`

- [ ] **Step 3: Implement price feature calculators**

Write `bullforge/services/feature_engine/calculators/price.py`:

```python
from __future__ import annotations

import numpy as np
import pandas as pd


def compute_price_features(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]
    high = df["high"]
    low = df["low"]
    open_ = df["open"]

    return pd.DataFrame({
        # Simple returns at different lookbacks
        "return_1": close.pct_change(1),
        "return_5": close.pct_change(5),
        "return_10": close.pct_change(10),
        # Log return
        "log_return_1": np.log(close / close.shift(1)),
        # Rolling volatility (std of returns)
        "volatility_10": close.pct_change(1).rolling(window=10).std(),
        "volatility_20": close.pct_change(1).rolling(window=20).std(),
        # Spread features
        "high_low_spread": (high - low) / close,
        "close_open_spread": (close - open_) / open_,
    }, index=df.index)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_calc_price.py -v
```

Expected: All 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
cd <repo>
git add bullforge/services/feature_engine/calculators/price.py bullforge/tests/test_calc_price.py
git commit -m "feat: price feature calculators (returns, volatility, spreads)"
```

---

## Task 5: Technical Indicator Calculators

**Files:**
- Create: `bullforge/services/feature_engine/calculators/technical.py`
- Create: `bullforge/tests/test_calc_technical.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_calc_technical.py`:

```python
import pytest
import pandas as pd
import numpy as np
from services.feature_engine.calculators.technical import compute_technical_features


def _make_bars_df(n: int = 100) -> pd.DataFrame:
    timestamps = pd.date_range(
        start="2026-04-10 12:00:00", periods=n, freq="1min", tz="UTC"
    )
    np.random.seed(42)
    close = 420.0 + np.cumsum(np.random.randn(n) * 0.1)
    return pd.DataFrame({
        "symbol": "SPY",
        "timestamp": timestamps,
        "open": close - np.random.rand(n) * 0.2,
        "high": close + np.random.rand(n) * 0.3,
        "low": close - np.random.rand(n) * 0.3,
        "close": close,
        "volume": np.random.randint(1000, 50000, size=n),
    })


def test_technical_features_returns_expected_columns():
    df = _make_bars_df()
    result = compute_technical_features(df)
    expected_cols = [
        "sma_10", "sma_20", "sma_50",
        "ema_10", "ema_20",
        "rsi_14",
        "macd", "macd_signal", "macd_histogram",
        "bb_upper", "bb_middle", "bb_lower", "bb_width",
        "atr_14",
    ]
    for col in expected_cols:
        assert col in result.columns, f"Missing column: {col}"


def test_sma_is_rolling_mean():
    df = _make_bars_df(50)
    result = compute_technical_features(df)
    expected = df["close"].rolling(window=10).mean()
    pd.testing.assert_series_equal(
        result["sma_10"], expected, check_names=False
    )


def test_ema_uses_ewm():
    df = _make_bars_df(50)
    result = compute_technical_features(df)
    expected = df["close"].ewm(span=10, adjust=False).mean()
    pd.testing.assert_series_equal(
        result["ema_10"], expected, check_names=False
    )


def test_rsi_bounded_0_100():
    df = _make_bars_df()
    result = compute_technical_features(df)
    rsi = result["rsi_14"].dropna()
    assert (rsi >= 0).all()
    assert (rsi <= 100).all()


def test_bollinger_bands_relation():
    """Upper > middle > lower for all non-NaN rows."""
    df = _make_bars_df()
    result = compute_technical_features(df)
    valid = result.dropna(subset=["bb_upper", "bb_middle", "bb_lower"])
    assert (valid["bb_upper"] >= valid["bb_middle"]).all()
    assert (valid["bb_middle"] >= valid["bb_lower"]).all()


def test_bollinger_width_positive():
    df = _make_bars_df()
    result = compute_technical_features(df)
    valid = result["bb_width"].dropna()
    assert (valid >= 0).all()


def test_atr_positive():
    df = _make_bars_df()
    result = compute_technical_features(df)
    valid = result["atr_14"].dropna()
    assert (valid >= 0).all()


def test_macd_histogram_is_macd_minus_signal():
    df = _make_bars_df()
    result = compute_technical_features(df)
    valid = result.dropna(subset=["macd", "macd_signal", "macd_histogram"])
    expected = valid["macd"] - valid["macd_signal"]
    pd.testing.assert_series_equal(
        valid["macd_histogram"], expected, check_names=False
    )


def test_technical_features_preserves_length():
    df = _make_bars_df(80)
    result = compute_technical_features(df)
    assert len(result) == len(df)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_calc_technical.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'services.feature_engine.calculators.technical'`

- [ ] **Step 3: Implement technical indicator calculators**

Write `bullforge/services/feature_engine/calculators/technical.py`:

```python
from __future__ import annotations

import numpy as np
import pandas as pd


def _compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi


def _compute_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


def compute_technical_features(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]
    high = df["high"]
    low = df["low"]

    # Simple moving averages
    sma_10 = close.rolling(window=10).mean()
    sma_20 = close.rolling(window=20).mean()
    sma_50 = close.rolling(window=50).mean()

    # Exponential moving averages
    ema_10 = close.ewm(span=10, adjust=False).mean()
    ema_20 = close.ewm(span=20, adjust=False).mean()

    # RSI
    rsi_14 = _compute_rsi(close, 14)

    # MACD (12, 26, 9)
    ema_12 = close.ewm(span=12, adjust=False).mean()
    ema_26 = close.ewm(span=26, adjust=False).mean()
    macd = ema_12 - ema_26
    macd_signal = macd.ewm(span=9, adjust=False).mean()
    macd_histogram = macd - macd_signal

    # Bollinger Bands (20, 2)
    bb_middle = sma_20
    bb_std = close.rolling(window=20).std()
    bb_upper = bb_middle + 2 * bb_std
    bb_lower = bb_middle - 2 * bb_std
    bb_width = (bb_upper - bb_lower) / bb_middle

    # ATR
    atr_14 = _compute_atr(high, low, close, 14)

    return pd.DataFrame({
        "sma_10": sma_10,
        "sma_20": sma_20,
        "sma_50": sma_50,
        "ema_10": ema_10,
        "ema_20": ema_20,
        "rsi_14": rsi_14,
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_histogram": macd_histogram,
        "bb_upper": bb_upper,
        "bb_middle": bb_middle,
        "bb_lower": bb_lower,
        "bb_width": bb_width,
        "atr_14": atr_14,
    }, index=df.index)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_calc_technical.py -v
```

Expected: All 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
cd <repo>
git add bullforge/services/feature_engine/calculators/technical.py bullforge/tests/test_calc_technical.py
git commit -m "feat: technical indicator calculators (SMA, EMA, RSI, MACD, Bollinger, ATR)"
```

---

## Task 6: Volume Feature Calculators

**Files:**
- Create: `bullforge/services/feature_engine/calculators/volume.py`
- Create: `bullforge/tests/test_calc_volume.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_calc_volume.py`:

```python
import pytest
import pandas as pd
import numpy as np
from services.feature_engine.calculators.volume import compute_volume_features


def _make_bars_df(n: int = 60) -> pd.DataFrame:
    timestamps = pd.date_range(
        start="2026-04-10 14:00:00", periods=n, freq="1min", tz="UTC"
    )
    np.random.seed(42)
    close = 420.0 + np.cumsum(np.random.randn(n) * 0.1)
    volume = np.random.randint(5000, 50000, size=n)
    return pd.DataFrame({
        "symbol": "SPY",
        "timestamp": timestamps,
        "open": close - np.random.rand(n) * 0.2,
        "high": close + np.random.rand(n) * 0.3,
        "low": close - np.random.rand(n) * 0.3,
        "close": close,
        "volume": volume,
    })


def test_volume_features_returns_expected_columns():
    df = _make_bars_df()
    result = compute_volume_features(df)
    expected_cols = ["vwap", "obv", "volume_ratio_10", "volume_ratio_20"]
    for col in expected_cols:
        assert col in result.columns, f"Missing column: {col}"


def test_vwap_calculation():
    df = _make_bars_df(30)
    result = compute_volume_features(df)
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    cumulative_tp_vol = (typical_price * df["volume"]).cumsum()
    cumulative_vol = df["volume"].cumsum()
    expected = cumulative_tp_vol / cumulative_vol
    pd.testing.assert_series_equal(
        result["vwap"], expected, check_names=False
    )


def test_obv_direction():
    """OBV should increase when close goes up and decrease when close goes down."""
    df = _make_bars_df(30)
    result = compute_volume_features(df)
    obv = result["obv"]
    # OBV should not be all NaN
    assert not obv.isna().all()


def test_volume_ratio_is_relative_to_rolling_mean():
    df = _make_bars_df(30)
    result = compute_volume_features(df)
    vol = df["volume"].astype(float)
    expected = vol / vol.rolling(window=10).mean()
    pd.testing.assert_series_equal(
        result["volume_ratio_10"], expected, check_names=False
    )


def test_volume_features_preserves_length():
    df = _make_bars_df(50)
    result = compute_volume_features(df)
    assert len(result) == len(df)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_calc_volume.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'services.feature_engine.calculators.volume'`

- [ ] **Step 3: Implement volume feature calculators**

Write `bullforge/services/feature_engine/calculators/volume.py`:

```python
from __future__ import annotations

import numpy as np
import pandas as pd


def compute_volume_features(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"].astype(float)

    # VWAP (cumulative for the session)
    typical_price = (high + low + close) / 3
    cumulative_tp_vol = (typical_price * volume).cumsum()
    cumulative_vol = volume.cumsum()
    vwap = cumulative_tp_vol / cumulative_vol

    # On-Balance Volume
    direction = np.sign(close.diff())
    direction.iloc[0] = 0
    obv = (volume * direction).cumsum()

    # Volume ratio (current volume / rolling average)
    volume_ratio_10 = volume / volume.rolling(window=10).mean()
    volume_ratio_20 = volume / volume.rolling(window=20).mean()

    return pd.DataFrame({
        "vwap": vwap,
        "obv": obv,
        "volume_ratio_10": volume_ratio_10,
        "volume_ratio_20": volume_ratio_20,
    }, index=df.index)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_calc_volume.py -v
```

Expected: All 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
cd <repo>
git add bullforge/services/feature_engine/calculators/volume.py bullforge/tests/test_calc_volume.py
git commit -m "feat: volume feature calculators (VWAP, OBV, volume ratio)"
```

---

## Task 7: Calendar Feature Calculators

**Files:**
- Create: `bullforge/services/feature_engine/calculators/calendar.py`
- Create: `bullforge/tests/test_calc_calendar.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_calc_calendar.py`:

```python
import pytest
import pandas as pd
import numpy as np
from services.feature_engine.calculators.calendar import compute_calendar_features


def _make_bars_df_multiday() -> pd.DataFrame:
    """Create bars spanning multiple days and times for calendar feature testing."""
    # Mix of weekdays and hours
    timestamps = pd.to_datetime([
        "2026-04-06 14:00:00+00:00",  # Monday
        "2026-04-06 15:30:00+00:00",  # Monday
        "2026-04-07 12:00:00+00:00",  # Tuesday
        "2026-04-08 16:00:00+00:00",  # Wednesday
        "2026-04-09 11:30:00+00:00",  # Thursday
        "2026-04-10 13:45:00+00:00",  # Friday
    ])
    n = len(timestamps)
    np.random.seed(42)
    close = 420.0 + np.cumsum(np.random.randn(n) * 0.1)
    return pd.DataFrame({
        "symbol": "SPY",
        "timestamp": timestamps,
        "open": close - 0.1,
        "high": close + 0.2,
        "low": close - 0.2,
        "close": close,
        "volume": np.random.randint(1000, 50000, size=n),
    })


def test_calendar_features_returns_expected_columns():
    df = _make_bars_df_multiday()
    result = compute_calendar_features(df)
    expected_cols = [
        "day_of_week",
        "hour_of_day",
        "minute_of_hour",
        "is_market_open_first_30",
        "is_market_close_last_30",
    ]
    for col in expected_cols:
        assert col in result.columns, f"Missing column: {col}"


def test_day_of_week_encoding():
    df = _make_bars_df_multiday()
    result = compute_calendar_features(df)
    # Monday=0, Tuesday=1, ..., Friday=4
    assert result["day_of_week"].iloc[0] == 0  # Monday
    assert result["day_of_week"].iloc[2] == 1  # Tuesday
    assert result["day_of_week"].iloc[5] == 4  # Friday


def test_hour_of_day():
    df = _make_bars_df_multiday()
    result = compute_calendar_features(df)
    assert result["hour_of_day"].iloc[0] == 14
    assert result["hour_of_day"].iloc[1] == 15
    assert result["hour_of_day"].iloc[2] == 12


def test_market_open_first_30():
    """Bars between 14:30-15:00 UTC (9:30-10:00 ET) should be flagged."""
    timestamps = pd.to_datetime([
        "2026-04-10 14:30:00+00:00",  # market open
        "2026-04-10 14:45:00+00:00",  # first 30 min
        "2026-04-10 15:00:00+00:00",  # just outside
        "2026-04-10 16:00:00+00:00",  # mid-day
    ])
    df = pd.DataFrame({
        "symbol": "SPY",
        "timestamp": timestamps,
        "open": [420.0] * 4,
        "high": [421.0] * 4,
        "low": [419.0] * 4,
        "close": [420.5] * 4,
        "volume": [10000] * 4,
    })
    result = compute_calendar_features(df)
    assert result["is_market_open_first_30"].iloc[0] == 1
    assert result["is_market_open_first_30"].iloc[1] == 1
    assert result["is_market_open_first_30"].iloc[2] == 0
    assert result["is_market_open_first_30"].iloc[3] == 0


def test_market_close_last_30():
    """Bars between 20:30-21:00 UTC (3:30-4:00 PM ET) should be flagged."""
    timestamps = pd.to_datetime([
        "2026-04-10 19:00:00+00:00",  # mid-day
        "2026-04-10 20:30:00+00:00",  # last 30 min
        "2026-04-10 20:45:00+00:00",  # last 30 min
        "2026-04-10 20:59:00+00:00",  # last 30 min
    ])
    df = pd.DataFrame({
        "symbol": "SPY",
        "timestamp": timestamps,
        "open": [420.0] * 4,
        "high": [421.0] * 4,
        "low": [419.0] * 4,
        "close": [420.5] * 4,
        "volume": [10000] * 4,
    })
    result = compute_calendar_features(df)
    assert result["is_market_close_last_30"].iloc[0] == 0
    assert result["is_market_close_last_30"].iloc[1] == 1
    assert result["is_market_close_last_30"].iloc[2] == 1
    assert result["is_market_close_last_30"].iloc[3] == 1


def test_calendar_features_preserves_length():
    df = _make_bars_df_multiday()
    result = compute_calendar_features(df)
    assert len(result) == len(df)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_calc_calendar.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'services.feature_engine.calculators.calendar'`

- [ ] **Step 3: Implement calendar feature calculators**

Write `bullforge/services/feature_engine/calculators/calendar.py`:

```python
from __future__ import annotations

import pandas as pd


def compute_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    ts = pd.to_datetime(df["timestamp"], utc=True)

    day_of_week = ts.dt.dayofweek
    hour_of_day = ts.dt.hour
    minute_of_hour = ts.dt.minute

    # US equity market open: 9:30 AM ET = 14:30 UTC (during EDT)
    # First 30 minutes: 14:30 - 15:00 UTC
    time_minutes = hour_of_day * 60 + minute_of_hour
    market_open_start = 14 * 60 + 30  # 14:30 UTC
    market_open_end = 15 * 60          # 15:00 UTC
    is_market_open_first_30 = (
        (time_minutes >= market_open_start) & (time_minutes < market_open_end)
    ).astype(int)

    # US equity market close: 4:00 PM ET = 21:00 UTC (during EDT)
    # Last 30 minutes: 20:30 - 21:00 UTC
    market_close_start = 20 * 60 + 30  # 20:30 UTC
    market_close_end = 21 * 60          # 21:00 UTC
    is_market_close_last_30 = (
        (time_minutes >= market_close_start) & (time_minutes < market_close_end)
    ).astype(int)

    return pd.DataFrame({
        "day_of_week": day_of_week.values,
        "hour_of_day": hour_of_day.values,
        "minute_of_hour": minute_of_hour.values,
        "is_market_open_first_30": is_market_open_first_30.values,
        "is_market_close_last_30": is_market_close_last_30.values,
    }, index=df.index)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_calc_calendar.py -v
```

Expected: All 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
cd <repo>
git add bullforge/services/feature_engine/calculators/calendar.py bullforge/tests/test_calc_calendar.py
git commit -m "feat: calendar feature calculators (day/hour, market open/close flags)"
```

---

## Task 8: Label Generator

**Files:**
- Create: `bullforge/services/feature_engine/labels.py`
- Create: `bullforge/tests/test_labels.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_labels.py`:

```python
import pytest
import pandas as pd
import numpy as np
from services.feature_engine.labels import compute_forward_return_labels


def _make_bars_df(n: int = 100) -> pd.DataFrame:
    timestamps = pd.date_range(
        start="2026-04-10 14:00:00", periods=n, freq="1min", tz="UTC"
    )
    np.random.seed(42)
    close = 420.0 + np.cumsum(np.random.randn(n) * 0.1)
    return pd.DataFrame({
        "symbol": "SPY",
        "timestamp": timestamps,
        "open": close - np.random.rand(n) * 0.2,
        "high": close + np.random.rand(n) * 0.3,
        "low": close - np.random.rand(n) * 0.3,
        "close": close,
        "volume": np.random.randint(1000, 50000, size=n),
    })


def test_forward_return_labels_expected_columns():
    df = _make_bars_df()
    result = compute_forward_return_labels(df, horizons=[5, 10, 30])
    expected_cols = [
        "fwd_return_5", "fwd_direction_5",
        "fwd_return_10", "fwd_direction_10",
        "fwd_return_30", "fwd_direction_30",
    ]
    for col in expected_cols:
        assert col in result.columns, f"Missing column: {col}"


def test_forward_return_is_future_pct_change():
    df = _make_bars_df(50)
    result = compute_forward_return_labels(df, horizons=[5])
    close = df["close"]
    expected = close.shift(-5) / close - 1.0
    pd.testing.assert_series_equal(
        result["fwd_return_5"], expected, check_names=False
    )


def test_forward_direction_sign():
    df = _make_bars_df(50)
    result = compute_forward_return_labels(df, horizons=[5], threshold=0.0)
    # direction should match sign of return (with 0 threshold, no flat zone)
    valid = result.dropna(subset=["fwd_return_5"])
    for idx in valid.index:
        ret = valid.loc[idx, "fwd_return_5"]
        direction = valid.loc[idx, "fwd_direction_5"]
        if ret > 0:
            assert direction == 1
        elif ret < 0:
            assert direction == -1
        else:
            assert direction == 0


def test_forward_direction_threshold():
    """Returns within threshold should be labeled as 0 (flat)."""
    df = _make_bars_df(50)
    result = compute_forward_return_labels(df, horizons=[5], threshold=0.01)
    valid = result.dropna(subset=["fwd_return_5"])
    for idx in valid.index:
        ret = valid.loc[idx, "fwd_return_5"]
        direction = valid.loc[idx, "fwd_direction_5"]
        if abs(ret) <= 0.01:
            assert direction == 0


def test_forward_return_nans_at_tail():
    """Last `horizon` rows should be NaN (no future data available)."""
    df = _make_bars_df(50)
    result = compute_forward_return_labels(df, horizons=[10])
    tail = result["fwd_return_10"].iloc[-10:]
    assert tail.isna().all()


def test_multiple_horizons():
    df = _make_bars_df()
    result = compute_forward_return_labels(df, horizons=[5, 10, 30])
    # Different horizons should produce different NaN tails
    assert result["fwd_return_5"].iloc[-5:].isna().all()
    assert result["fwd_return_10"].iloc[-10:].isna().all()
    assert result["fwd_return_30"].iloc[-30:].isna().all()
    # But earlier rows should have values for all
    assert result["fwd_return_5"].iloc[0:50].notna().all()


def test_preserves_length():
    df = _make_bars_df(80)
    result = compute_forward_return_labels(df, horizons=[5, 10])
    assert len(result) == len(df)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_labels.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'services.feature_engine.labels'`

- [ ] **Step 3: Implement label generator**

Write `bullforge/services/feature_engine/labels.py`:

```python
from __future__ import annotations

import numpy as np
import pandas as pd


def compute_forward_return_labels(
    df: pd.DataFrame,
    horizons: list[int],
    threshold: float = 0.0,
) -> pd.DataFrame:
    close = df["close"]
    result = {}

    for h in horizons:
        fwd_return = close.shift(-h) / close - 1.0
        if threshold > 0:
            direction = pd.Series(0, index=df.index, dtype=int)
            direction[fwd_return > threshold] = 1
            direction[fwd_return < -threshold] = -1
            direction[fwd_return.isna()] = np.nan
        else:
            direction = np.sign(fwd_return)

        result[f"fwd_return_{h}"] = fwd_return
        result[f"fwd_direction_{h}"] = direction

    return pd.DataFrame(result, index=df.index)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_labels.py -v
```

Expected: All 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
cd <repo>
git add bullforge/services/feature_engine/labels.py bullforge/tests/test_labels.py
git commit -m "feat: forward return label generator with configurable horizons and threshold"
```

---

## Task 9: Feature Engine Orchestrator

**Files:**
- Create: `bullforge/services/feature_engine/engine.py`
- Create: `bullforge/tests/test_engine.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_engine.py`:

```python
import pytest
import pytest_asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from shared.config import Settings
from shared.models import Bar, FeatureRow, Label
from services.feature_engine.engine import FeatureEngine
from services.feature_engine.registry import FeatureRegistry


def _make_bars(symbol: str, n: int = 60) -> list[Bar]:
    base = datetime(2026, 4, 10, 14, 0, 0, tzinfo=timezone.utc)
    np.random.seed(42)
    close = 420.0 + np.cumsum(np.random.randn(n) * 0.1)
    return [
        Bar(
            symbol=symbol,
            timestamp=base + timedelta(minutes=i),
            open=float(close[i] - np.random.rand() * 0.2),
            high=float(close[i] + np.random.rand() * 0.3),
            low=float(close[i] - np.random.rand() * 0.3),
            close=float(close[i]),
            volume=int(np.random.randint(1000, 50000)),
            timeframe="1m",
        )
        for i in range(n)
    ]


@pytest.fixture
def mock_settings(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test_key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test_secret")
    monkeypatch.setenv("TRADING_SYMBOLS_EQUITY", "SPY")
    monkeypatch.setenv("TRADING_SYMBOLS_CRYPTO", "")
    return Settings()


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.query_bars = AsyncMock(return_value=_make_bars("SPY", 60))
    db.insert_feature_rows = AsyncMock()
    db.insert_labels = AsyncMock()
    return db


@pytest.fixture
def simple_registry():
    registry = FeatureRegistry()

    def double_close(df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"close_x2": df["close"] * 2}, index=df.index)

    registry.register("double", double_close)
    return registry


def test_engine_creation(mock_settings, mock_db, simple_registry):
    engine = FeatureEngine(
        settings=mock_settings,
        db=mock_db,
        registry=simple_registry,
        label_horizons=[5, 10],
    )
    assert engine is not None


@pytest.mark.asyncio
async def test_compute_features_for_symbol(mock_settings, mock_db, simple_registry):
    engine = FeatureEngine(
        settings=mock_settings,
        db=mock_db,
        registry=simple_registry,
        label_horizons=[5],
    )

    await engine.compute_features_for_symbol("SPY", "1m")

    mock_db.query_bars.assert_called_once_with("SPY", "1m", limit=500)
    mock_db.insert_feature_rows.assert_called_once()
    inserted_rows = mock_db.insert_feature_rows.call_args[0][0]
    assert len(inserted_rows) > 0
    assert all(isinstance(r, FeatureRow) for r in inserted_rows)
    assert all(r.symbol == "SPY" for r in inserted_rows)
    # The simple calculator adds close_x2
    assert "close_x2" in inserted_rows[0].features


@pytest.mark.asyncio
async def test_compute_labels_for_symbol(mock_settings, mock_db, simple_registry):
    engine = FeatureEngine(
        settings=mock_settings,
        db=mock_db,
        registry=simple_registry,
        label_horizons=[5, 10],
    )

    await engine.compute_labels_for_symbol("SPY", "1m")

    mock_db.query_bars.assert_called_once()
    mock_db.insert_labels.assert_called_once()
    inserted_labels = mock_db.insert_labels.call_args[0][0]
    assert len(inserted_labels) > 0
    assert all(isinstance(l, Label) for l in inserted_labels)
    # Should have labels for both horizons
    horizons = {l.horizon_minutes for l in inserted_labels}
    assert 5 in horizons
    assert 10 in horizons


@pytest.mark.asyncio
async def test_compute_all_symbols(mock_settings, mock_db, simple_registry):
    engine = FeatureEngine(
        settings=mock_settings,
        db=mock_db,
        registry=simple_registry,
        label_horizons=[5],
    )

    await engine.compute_all()

    # Should have been called for SPY (only equity, no crypto since empty string)
    assert mock_db.query_bars.call_count >= 1


@pytest.mark.asyncio
async def test_compute_skips_insufficient_bars(mock_settings, simple_registry):
    mock_db = AsyncMock()
    mock_db.query_bars = AsyncMock(return_value=_make_bars("SPY", 3))
    mock_db.insert_feature_rows = AsyncMock()
    mock_db.insert_labels = AsyncMock()

    engine = FeatureEngine(
        settings=mock_settings,
        db=mock_db,
        registry=simple_registry,
        label_horizons=[5],
        min_bars=10,
    )

    await engine.compute_features_for_symbol("SPY", "1m")

    # Should not insert anything with insufficient bars
    mock_db.insert_feature_rows.assert_not_called()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_engine.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'services.feature_engine.engine'`

- [ ] **Step 3: Implement the feature engine orchestrator**

Write `bullforge/services/feature_engine/engine.py`:

```python
from __future__ import annotations

import logging
from datetime import timezone
from typing import List

import numpy as np
import pandas as pd

from shared.config import Settings
from shared.db import Database
from shared.models import Bar, FeatureRow, Label
from services.feature_engine.registry import FeatureRegistry
from services.feature_engine.labels import compute_forward_return_labels

logger = logging.getLogger(__name__)


class FeatureEngine:
    def __init__(
        self,
        settings: Settings,
        db: Database,
        registry: FeatureRegistry,
        label_horizons: List[int],
        label_threshold: float = 0.0,
        min_bars: int = 50,
    ):
        self._settings = settings
        self._db = db
        self._registry = registry
        self._label_horizons = label_horizons
        self._label_threshold = label_threshold
        self._min_bars = min_bars

    def _bars_to_dataframe(self, bars: List[Bar]) -> pd.DataFrame:
        if not bars:
            return pd.DataFrame()
        # Bars come from DB ordered DESC, reverse for chronological order
        bars_sorted = sorted(bars, key=lambda b: b.timestamp)
        return pd.DataFrame({
            "symbol": [b.symbol for b in bars_sorted],
            "timestamp": [b.timestamp for b in bars_sorted],
            "open": [b.open for b in bars_sorted],
            "high": [b.high for b in bars_sorted],
            "low": [b.low for b in bars_sorted],
            "close": [b.close for b in bars_sorted],
            "volume": [b.volume for b in bars_sorted],
        })

    async def compute_features_for_symbol(
        self, symbol: str, timeframe: str
    ) -> None:
        bars = await self._db.query_bars(symbol, timeframe, limit=500)
        if len(bars) < self._min_bars:
            logger.warning(
                "Insufficient bars for %s %s: %d < %d",
                symbol, timeframe, len(bars), self._min_bars,
            )
            return

        df = self._bars_to_dataframe(bars)
        features_df = self._registry.compute_all(df)
        if features_df.empty:
            logger.warning("No features computed for %s %s", symbol, timeframe)
            return

        # Drop rows where all features are NaN (warmup period)
        valid_mask = features_df.notna().any(axis=1)
        valid_df = features_df[valid_mask]
        valid_timestamps = df.loc[valid_mask, "timestamp"]

        feature_rows = []
        for idx in valid_df.index:
            row_features = valid_df.loc[idx].dropna().to_dict()
            if row_features:
                feature_rows.append(
                    FeatureRow(
                        symbol=symbol,
                        timestamp=valid_timestamps.loc[idx],
                        timeframe=timeframe,
                        features=row_features,
                    )
                )

        await self._db.insert_feature_rows(feature_rows)
        logger.info(
            "Computed %d feature rows for %s %s",
            len(feature_rows), symbol, timeframe,
        )

    async def compute_labels_for_symbol(
        self, symbol: str, timeframe: str
    ) -> None:
        bars = await self._db.query_bars(symbol, timeframe, limit=500)
        if len(bars) < self._min_bars:
            logger.warning(
                "Insufficient bars for labels %s %s: %d < %d",
                symbol, timeframe, len(bars), self._min_bars,
            )
            return

        df = self._bars_to_dataframe(bars)
        labels_df = compute_forward_return_labels(
            df, horizons=self._label_horizons, threshold=self._label_threshold
        )

        labels = []
        for h in self._label_horizons:
            ret_col = f"fwd_return_{h}"
            dir_col = f"fwd_direction_{h}"
            valid = labels_df.dropna(subset=[ret_col, dir_col])
            for idx in valid.index:
                labels.append(
                    Label(
                        symbol=symbol,
                        timestamp=df.loc[idx, "timestamp"],
                        timeframe=timeframe,
                        horizon_minutes=h,
                        forward_return=float(valid.loc[idx, ret_col]),
                        direction=int(valid.loc[idx, dir_col]),
                    )
                )

        await self._db.insert_labels(labels)
        logger.info(
            "Computed %d labels for %s %s",
            len(labels), symbol, timeframe,
        )

    async def compute_all(self) -> None:
        all_symbols = (
            self._settings.trading_symbols_equity
            + self._settings.trading_symbols_crypto
        )

        for symbol in all_symbols:
            for timeframe in ["1m", "1d"]:
                await self.compute_features_for_symbol(symbol, timeframe)
                await self.compute_labels_for_symbol(symbol, timeframe)

        logger.info("Feature engine compute_all complete")
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_engine.py -v
```

Expected: All 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
cd <repo>
git add bullforge/services/feature_engine/engine.py bullforge/tests/test_engine.py
git commit -m "feat: feature engine orchestrator with feature computation and label generation"
```

---

## Task 10: Feature Engine Service Entrypoint

**Files:**
- Create: `bullforge/services/feature_engine/main.py`
- Create: `bullforge/services/feature_engine/requirements.txt`
- Create: `bullforge/services/feature_engine/Dockerfile`
- Modify: `bullforge/docker-compose.yml`

- [ ] **Step 1: Create requirements.txt**

Write `bullforge/services/feature_engine/requirements.txt`:

```
pandas>=2.2.0
numpy>=1.26.0
asyncpg>=0.29.0
redis>=5.0.0
aiohttp>=3.9.0
```

- [ ] **Step 2: Implement the entrypoint**

Write `bullforge/services/feature_engine/main.py`:

```python
from __future__ import annotations

import asyncio
import json
import logging
import signal

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient
from services.feature_engine.registry import FeatureRegistry
from services.feature_engine.calculators.price import compute_price_features
from services.feature_engine.calculators.technical import compute_technical_features
from services.feature_engine.calculators.volume import compute_volume_features
from services.feature_engine.calculators.calendar import compute_calendar_features
from services.feature_engine.engine import FeatureEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("feature_engine")


def build_registry() -> FeatureRegistry:
    registry = FeatureRegistry()
    registry.register("price", compute_price_features)
    registry.register("technical", compute_technical_features)
    registry.register("volume", compute_volume_features)
    registry.register("calendar", compute_calendar_features)
    return registry


async def main() -> None:
    settings = Settings()
    db = Database(settings)
    redis_client = RedisClient(settings)

    logger.info("Connecting to TimescaleDB...")
    await db.connect()
    logger.info("Connecting to Redis...")
    await redis_client.connect()

    registry = build_registry()
    engine = FeatureEngine(
        settings=settings,
        db=db,
        registry=registry,
        label_horizons=[5, 10, 30],
        label_threshold=0.001,
        min_bars=50,
    )

    # Run initial full computation
    logger.info("Running initial feature computation for all symbols...")
    await engine.compute_all()

    # Subscribe to market ticks for reactive computation
    async def on_tick(message: str) -> None:
        data = json.loads(message)
        symbol = data["symbol"]
        logger.debug("Tick received for %s, scheduling feature update", symbol)
        await engine.compute_features_for_symbol(symbol, "1m")

    await redis_client.subscribe("market:ticks", on_tick)
    logger.info("Subscribed to market:ticks for reactive feature updates")

    # Set up graceful shutdown
    shutdown_event = asyncio.Event()

    def handle_signal():
        logger.info("Shutdown signal received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    logger.info("Feature engine running. Press Ctrl+C to stop.")

    try:
        await shutdown_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down...")
        await redis_client.disconnect()
        await db.disconnect()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 3: Create Dockerfile**

Write `bullforge/services/feature_engine/Dockerfile`:

```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY services/feature_engine/requirements.txt /app/services/feature_engine/requirements.txt
RUN pip install --no-cache-dir -r services/feature_engine/requirements.txt
RUN pip install --no-cache-dir asyncpg redis

# Copy shared code and service code
COPY shared/ /app/shared/
COPY migrations/ /app/migrations/
COPY services/__init__.py /app/services/__init__.py
COPY services/feature_engine/ /app/services/feature_engine/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "services.feature_engine.main"]
```

- [ ] **Step 4: Add feature_engine service to docker-compose.yml**

Add the following service block to `bullforge/docker-compose.yml`, after the `data_pipeline` service and before the `volumes:` section:

```yaml
  feature_engine:
    build:
      context: .
      dockerfile: services/feature_engine/Dockerfile
    env_file:
      - .env
    environment:
      TIMESCALEDB_HOST: timescaledb
      REDIS_HOST: redis
    depends_on:
      timescaledb:
        condition: service_healthy
      redis:
        condition: service_healthy
      data_pipeline:
        condition: service_started
    restart: unless-stopped
```

The full `docker-compose.yml` should now be:

```yaml
services:
  timescaledb:
    image: timescale/timescaledb:latest-pg16
    environment:
      POSTGRES_USER: bullforge
      POSTGRES_PASSWORD: bullforge_dev
      POSTGRES_DB: bullforge
    ports:
      - "15432:5432"
    volumes:
      - timescaledb_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U bullforge"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    ports:
      - "16379:6379"
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

  data_pipeline:
    build:
      context: .
      dockerfile: services/data_pipeline/Dockerfile
    env_file:
      - .env
    environment:
      TIMESCALEDB_HOST: timescaledb
      REDIS_HOST: redis
    ports:
      - "18080:8080"
    depends_on:
      timescaledb:
        condition: service_healthy
      redis:
        condition: service_healthy
    restart: unless-stopped

  feature_engine:
    build:
      context: .
      dockerfile: services/feature_engine/Dockerfile
    env_file:
      - .env
    environment:
      TIMESCALEDB_HOST: timescaledb
      REDIS_HOST: redis
    depends_on:
      timescaledb:
        condition: service_healthy
      redis:
        condition: service_healthy
      data_pipeline:
        condition: service_started
    restart: unless-stopped

volumes:
  timescaledb_data:
  redis_data:
```

- [ ] **Step 5: Test Docker Compose builds**

```bash
cd <repo>/bullforge
docker compose build feature_engine
```

Expected: Image builds successfully with no errors.

- [ ] **Step 6: Commit**

```bash
cd <repo>
git add bullforge/services/feature_engine/main.py bullforge/services/feature_engine/requirements.txt bullforge/services/feature_engine/Dockerfile bullforge/docker-compose.yml
git commit -m "feat: feature engine service entrypoint with Docker Compose integration"
```

---

## Task 11: Run Full Test Suite

- [ ] **Step 1: Run all unit tests**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_feature_models.py tests/test_feature_registry.py tests/test_calc_price.py tests/test_calc_technical.py tests/test_calc_volume.py tests/test_calc_calendar.py tests/test_labels.py tests/test_engine.py -v
```

Expected: All tests pass. Fix any failures before proceeding.

- [ ] **Step 2: Run Phase 1 tests to verify no regressions**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/ -v
```

Expected: All tests pass, including Phase 1 tests.

- [ ] **Step 3: Verify Docker Compose end-to-end**

```bash
cd <repo>/bullforge
docker compose down -v
docker compose up -d
# Wait 60 seconds for data pipeline backfill + feature engine computation
docker compose logs feature_engine --tail 20
```

Expected: Feature engine logs show "Running initial feature computation for all symbols..." followed by "Computed N feature rows for SPY 1m" and "Computed N labels for SPY 1m" messages for each symbol.

- [ ] **Step 4: Verify features and labels in database**

```bash
docker compose exec timescaledb psql -U bullforge -c "SELECT symbol, timeframe, count(*) FROM features GROUP BY symbol, timeframe;"
docker compose exec timescaledb psql -U bullforge -c "SELECT symbol, timeframe, horizon_minutes, count(*) FROM labels GROUP BY symbol, timeframe, horizon_minutes;"
docker compose exec timescaledb psql -U bullforge -c "SELECT symbol, timestamp, features FROM features WHERE symbol = 'ETH/USD' AND timeframe = '1m' ORDER BY timestamp DESC LIMIT 3;"
```

Expected: Row counts for configured symbols across both timeframes. Feature JSONB column should contain keys like `sma_20`, `rsi_14`, `vwap`, etc.

- [ ] **Step 5: Final commit if any fixes were needed**

```bash
cd <repo>
git add -A
git status
# If there are changes:
git commit -m "fix: address issues found during Phase 2 full test suite run"
```
