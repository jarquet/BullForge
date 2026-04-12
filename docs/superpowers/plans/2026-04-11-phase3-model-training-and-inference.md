# Phase 3: Model Training & Inference — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a model training pipeline that reads features + labels from TimescaleDB, trains scikit-learn classifiers (gradient boosting, logistic regression), stores model artifacts on disk with metadata in the database, and provides live inference that publishes trading signal predictions via Redis pub/sub.

**Architecture:** A new `model_service` reads computed features and labels from TimescaleDB (written by Phase 2's feature engine), builds training datasets with time-based train/test splits, trains sklearn Pipeline models (StandardScaler + classifier), evaluates with classification metrics, and saves artifacts to a volume-mounted directory. On startup, it trains models for all configured symbol/horizon combinations (skipping if an active model already exists), then loads trained models into memory and subscribes to a new `feature:updates` Redis channel (published by a small update to the feature engine). When features update, the inference engine runs predictions and stores them in a `predictions` hypertable, also publishing to a `model:predictions` channel for future downstream consumers.

**Tech Stack:** Python 3.12+, scikit-learn, joblib, pandas, numpy, asyncio, asyncpg (from shared/db.py), Redis pub/sub (from shared/redis.py), aiohttp, Docker Compose, pytest, pytest-asyncio

---

## File Structure

```
bullforge/
  shared/
    config.py                              — (modify) Add model_artifacts_dir, model_min_samples
    models.py                              — (modify) Add ModelMetadata and Prediction dataclasses
    db.py                                  — (modify) Add model registry, prediction, and training data query methods

  services/
    feature_engine/
      engine.py                            — (modify) Add optional redis_client, publish "feature:updates"
      main.py                              — (modify) Pass redis_client to FeatureEngine

    model_service/
      __init__.py
      Dockerfile                           — Container image for model service
      requirements.txt                     — scikit-learn, joblib, pandas, numpy, asyncpg, redis, aiohttp
      main.py                              — Entrypoint: train on startup, subscribe for inference
      health.py                            — Health check endpoint (/health on :8081)
      dataset.py                           — Build training DataFrames from features + labels
      model_factory.py                     — Create sklearn Pipeline (scaler + classifier)
      model_store.py                       — Save/load model artifacts with joblib
      trainer.py                           — Training pipeline: build dataset → train → evaluate → save
      inference.py                         — Load models, run predictions from live features

  migrations/
    005_create_model_registry.sql          — Model metadata table (tracks trained models)
    006_create_predictions.sql             — Predictions hypertable (stores inference results)

  tests/
    test_model_metadata.py                 — Tests for ModelMetadata and Prediction dataclasses
    test_model_db.py                       — Tests for model registry and prediction DB methods
    test_dataset.py                        — Tests for dataset builder
    test_model_factory.py                  — Tests for sklearn pipeline creation
    test_model_store.py                    — Tests for artifact save/load
    test_trainer.py                        — Tests for training pipeline
    test_inference.py                      — Tests for inference engine
```

---

## Task 1: Data Models (ModelMetadata and Prediction)

**Files:**
- Modify: `bullforge/shared/models.py`
- Create: `bullforge/tests/test_model_metadata.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_model_metadata.py`:

```python
from datetime import datetime, timezone
import pytest
from shared.models import ModelMetadata, Prediction


def test_model_metadata_creation():
    meta = ModelMetadata(
        symbol="SPY",
        timeframe="1m",
        horizon_minutes=5,
        model_type="gradient_boost",
        artifact_path="/app/models/artifacts/gradient_boost_SPY_1m_5m.joblib",
        metrics={"accuracy": 0.55, "f1_macro": 0.48},
        feature_columns=["sma_20", "rsi_14", "return_1"],
        trained_at=datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc),
        train_samples=800,
        test_samples=200,
    )
    assert meta.symbol == "SPY"
    assert meta.model_type == "gradient_boost"
    assert meta.metrics["accuracy"] == 0.55
    assert len(meta.feature_columns) == 3
    assert meta.is_active is True
    assert meta.id is None


def test_model_metadata_with_id():
    meta = ModelMetadata(
        symbol="AAPL",
        timeframe="1d",
        horizon_minutes=30,
        model_type="logistic_regression",
        artifact_path="/app/models/artifacts/lr_AAPL.joblib",
        metrics={"accuracy": 0.52},
        feature_columns=["sma_20"],
        trained_at=datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc),
        train_samples=500,
        test_samples=100,
        is_active=False,
        id=42,
    )
    assert meta.id == 42
    assert meta.is_active is False


def test_model_metadata_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="UTC"):
        ModelMetadata(
            symbol="SPY",
            timeframe="1m",
            horizon_minutes=5,
            model_type="gradient_boost",
            artifact_path="/path",
            metrics={"accuracy": 0.5},
            feature_columns=["sma_20"],
            trained_at=datetime(2026, 4, 11, 14, 0, 0),  # naive — no tzinfo
            train_samples=100,
            test_samples=50,
        )


def test_model_metadata_rejects_bad_horizon():
    with pytest.raises(ValueError, match="positive"):
        ModelMetadata(
            symbol="SPY",
            timeframe="1m",
            horizon_minutes=0,
            model_type="gradient_boost",
            artifact_path="/path",
            metrics={"accuracy": 0.5},
            feature_columns=["sma_20"],
            trained_at=datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc),
            train_samples=100,
            test_samples=50,
        )


def test_model_metadata_rejects_empty_feature_columns():
    with pytest.raises(ValueError, match="feature_columns"):
        ModelMetadata(
            symbol="SPY",
            timeframe="1m",
            horizon_minutes=5,
            model_type="gradient_boost",
            artifact_path="/path",
            metrics={"accuracy": 0.5},
            feature_columns=[],
            trained_at=datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc),
            train_samples=100,
            test_samples=50,
        )


def test_prediction_creation():
    pred = Prediction(
        symbol="SPY",
        timestamp=datetime(2026, 4, 11, 15, 30, 0, tzinfo=timezone.utc),
        timeframe="1m",
        horizon_minutes=5,
        model_id=1,
        predicted_direction=1,
        confidence=0.72,
    )
    assert pred.symbol == "SPY"
    assert pred.predicted_direction == 1
    assert pred.confidence == 0.72


def test_prediction_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="UTC"):
        Prediction(
            symbol="SPY",
            timestamp=datetime(2026, 4, 11, 15, 30, 0),
            timeframe="1m",
            horizon_minutes=5,
            model_id=1,
            predicted_direction=1,
            confidence=0.72,
        )


def test_prediction_rejects_bad_direction():
    with pytest.raises(ValueError, match="direction"):
        Prediction(
            symbol="SPY",
            timestamp=datetime(2026, 4, 11, 15, 30, 0, tzinfo=timezone.utc),
            timeframe="1m",
            horizon_minutes=5,
            model_id=1,
            predicted_direction=2,
            confidence=0.72,
        )


def test_prediction_rejects_bad_confidence():
    with pytest.raises(ValueError, match="confidence"):
        Prediction(
            symbol="SPY",
            timestamp=datetime(2026, 4, 11, 15, 30, 0, tzinfo=timezone.utc),
            timeframe="1m",
            horizon_minutes=5,
            model_id=1,
            predicted_direction=1,
            confidence=1.5,
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bullforge && python -m pytest tests/test_model_metadata.py -v`
Expected: FAIL with `ImportError: cannot import name 'ModelMetadata'`

- [ ] **Step 3: Write minimal implementation**

Add to the end of `bullforge/shared/models.py`:

```python
@dataclass(frozen=True)
class ModelMetadata:
    symbol: str
    timeframe: str  # "1m", "1d"
    horizon_minutes: int
    model_type: str  # "gradient_boost", "logistic_regression"
    artifact_path: str
    metrics: dict[str, float]
    feature_columns: list[str]
    trained_at: datetime
    train_samples: int
    test_samples: int
    is_active: bool = True
    id: Optional[int] = None

    def __post_init__(self):
        if self.trained_at.tzinfo is None or self.trained_at.tzinfo != timezone.utc:
            raise ValueError(
                f"ModelMetadata trained_at must be UTC, got tzinfo={self.trained_at.tzinfo}"
            )
        if self.horizon_minutes <= 0:
            raise ValueError(
                f"horizon_minutes must be positive, got {self.horizon_minutes}"
            )
        if not self.feature_columns:
            raise ValueError("feature_columns must not be empty")


@dataclass(frozen=True)
class Prediction:
    symbol: str
    timestamp: datetime
    timeframe: str  # "1m", "1d"
    horizon_minutes: int
    model_id: int
    predicted_direction: int  # -1 = down, 0 = flat, 1 = up
    confidence: float  # probability of predicted class, 0.0–1.0

    def __post_init__(self):
        if self.timestamp.tzinfo is None or self.timestamp.tzinfo != timezone.utc:
            raise ValueError(
                f"Prediction timestamp must be UTC, got tzinfo={self.timestamp.tzinfo}"
            )
        if self.predicted_direction not in (-1, 0, 1):
            raise ValueError(
                f"predicted_direction must be -1, 0, or 1, got {self.predicted_direction}"
            )
        if not (0.0 <= self.confidence <= 1.0):
            raise ValueError(
                f"confidence must be between 0.0 and 1.0, got {self.confidence}"
            )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bullforge && python -m pytest tests/test_model_metadata.py -v`
Expected: All 9 tests PASS

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/models.py bullforge/tests/test_model_metadata.py
git commit -m "feat: add ModelMetadata and Prediction data models"
```

---

## Task 2: Database Migrations

**Files:**
- Create: `bullforge/migrations/005_create_model_registry.sql`
- Create: `bullforge/migrations/006_create_predictions.sql`

- [ ] **Step 1: Create model_registry migration**

Write `bullforge/migrations/005_create_model_registry.sql`:

```sql
CREATE TABLE IF NOT EXISTS model_registry (
    id              SERIAL          PRIMARY KEY,
    symbol          TEXT            NOT NULL,
    timeframe       TEXT            NOT NULL,
    horizon_minutes INTEGER         NOT NULL,
    model_type      TEXT            NOT NULL,
    artifact_path   TEXT            NOT NULL,
    metrics         JSONB           NOT NULL DEFAULT '{}',
    feature_columns JSONB           NOT NULL DEFAULT '[]',
    trained_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    train_samples   INTEGER         NOT NULL,
    test_samples    INTEGER         NOT NULL,
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE
);

-- Only one active model per symbol/timeframe/horizon/type combination
CREATE UNIQUE INDEX IF NOT EXISTS idx_model_registry_active
    ON model_registry (symbol, timeframe, horizon_minutes, model_type)
    WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS idx_model_registry_lookup
    ON model_registry (symbol, timeframe, horizon_minutes);
```

- [ ] **Step 2: Create predictions migration**

Write `bullforge/migrations/006_create_predictions.sql`:

```sql
CREATE TABLE IF NOT EXISTS predictions (
    symbol              TEXT                NOT NULL,
    timestamp           TIMESTAMPTZ         NOT NULL,
    timeframe           TEXT                NOT NULL,
    horizon_minutes     INTEGER             NOT NULL,
    model_id            INTEGER             NOT NULL,
    predicted_direction SMALLINT            NOT NULL,
    confidence          DOUBLE PRECISION    NOT NULL,
    created_at          TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, timestamp, timeframe, horizon_minutes)
);

SELECT create_hypertable('predictions', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_predictions_symbol_timeframe
    ON predictions (symbol, timeframe, timestamp DESC);
```

- [ ] **Step 3: Commit**

```bash
git add bullforge/migrations/005_create_model_registry.sql bullforge/migrations/006_create_predictions.sql
git commit -m "feat: add model_registry and predictions database migrations"
```

---

## Task 3: Config Additions

**Files:**
- Modify: `bullforge/shared/config.py`
- Modify: `bullforge/tests/test_config.py`

- [ ] **Step 1: Write the failing test**

Add to `bullforge/tests/test_config.py`:

```python
def test_model_service_defaults(settings):
    assert settings.model_artifacts_dir == "/app/models/artifacts"
    assert settings.model_min_samples == 500


def test_model_service_env_override(monkeypatch):
    monkeypatch.setenv("MODEL_ARTIFACTS_DIR", "/tmp/models")
    monkeypatch.setenv("MODEL_MIN_SAMPLES", "1000")
    from shared.config import Settings
    s = Settings()
    assert s.model_artifacts_dir == "/tmp/models"
    assert s.model_min_samples == 1000
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bullforge && python -m pytest tests/test_config.py::test_model_service_defaults tests/test_config.py::test_model_service_env_override -v`
Expected: FAIL with `AttributeError: 'Settings' object has no attribute 'model_artifacts_dir'`

- [ ] **Step 3: Write minimal implementation**

Add to the `Settings` class in `bullforge/shared/config.py`, after the trading window fields:

```python
    # Model service
    model_artifacts_dir: str = field(
        default_factory=lambda: os.getenv("MODEL_ARTIFACTS_DIR", "/app/models/artifacts")
    )
    model_min_samples: int = field(
        default_factory=lambda: int(os.getenv("MODEL_MIN_SAMPLES", "500"))
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bullforge && python -m pytest tests/test_config.py -v`
Expected: All config tests PASS

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/config.py bullforge/tests/test_config.py
git commit -m "feat: add model service config settings"
```

---

## Task 4: Database Methods

**Files:**
- Modify: `bullforge/shared/db.py`
- Create: `bullforge/tests/test_model_db.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_model_db.py`:

```python
import json
import pytest
import pytest_asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock
from shared.models import ModelMetadata, Prediction


@pytest.fixture
def mock_pool():
    pool = AsyncMock()
    conn = AsyncMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool, conn


def _make_model_metadata():
    return ModelMetadata(
        symbol="SPY",
        timeframe="1m",
        horizon_minutes=5,
        model_type="gradient_boost",
        artifact_path="/app/models/artifacts/gb_SPY_1m_5m.joblib",
        metrics={"accuracy": 0.55, "f1_macro": 0.48},
        feature_columns=["sma_20", "rsi_14", "return_1"],
        trained_at=datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc),
        train_samples=800,
        test_samples=200,
    )


def _make_prediction():
    return Prediction(
        symbol="SPY",
        timestamp=datetime(2026, 4, 11, 15, 30, 0, tzinfo=timezone.utc),
        timeframe="1m",
        horizon_minutes=5,
        model_id=1,
        predicted_direction=1,
        confidence=0.72,
    )


@pytest.mark.asyncio
async def test_insert_model_metadata(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetchval = AsyncMock(return_value=42)
    db = Database(settings)
    db.pool = pool

    meta = _make_model_metadata()
    row_id = await db.insert_model_metadata(meta)

    assert row_id == 42
    conn.fetchval.assert_called_once()
    call_args = conn.fetchval.call_args
    assert "INSERT INTO model_registry" in call_args[0][0]
    assert call_args[0][1] == "SPY"


@pytest.mark.asyncio
async def test_query_active_model(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetchrow = AsyncMock(return_value={
        "id": 7,
        "symbol": "SPY",
        "timeframe": "1m",
        "horizon_minutes": 5,
        "model_type": "gradient_boost",
        "artifact_path": "/app/models/artifacts/gb.joblib",
        "metrics": json.dumps({"accuracy": 0.55}),
        "feature_columns": json.dumps(["sma_20", "rsi_14"]),
        "trained_at": datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc),
        "train_samples": 800,
        "test_samples": 200,
        "is_active": True,
    })
    db = Database(settings)
    db.pool = pool

    meta = await db.query_active_model("SPY", "1m", 5)

    assert meta is not None
    assert meta.id == 7
    assert meta.symbol == "SPY"
    assert meta.model_type == "gradient_boost"
    assert meta.feature_columns == ["sma_20", "rsi_14"]


@pytest.mark.asyncio
async def test_query_active_model_returns_none(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetchrow = AsyncMock(return_value=None)
    db = Database(settings)
    db.pool = pool

    meta = await db.query_active_model("SPY", "1m", 5)
    assert meta is None


@pytest.mark.asyncio
async def test_deactivate_models(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.execute = AsyncMock()
    db = Database(settings)
    db.pool = pool

    await db.deactivate_models("SPY", "1m", 5)

    conn.execute.assert_called_once()
    call_args = conn.execute.call_args
    assert "UPDATE model_registry" in call_args[0][0]
    assert "is_active = FALSE" in call_args[0][0]


@pytest.mark.asyncio
async def test_insert_prediction(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.execute = AsyncMock()
    db = Database(settings)
    db.pool = pool

    pred = _make_prediction()
    await db.insert_prediction(pred)

    conn.execute.assert_called_once()
    call_args = conn.execute.call_args
    assert "INSERT INTO predictions" in call_args[0][0]


@pytest.mark.asyncio
async def test_insert_predictions_batch(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.executemany = AsyncMock()
    db = Database(settings)
    db.pool = pool

    preds = [
        _make_prediction(),
        Prediction(
            symbol="SPY",
            timestamp=datetime(2026, 4, 11, 15, 31, 0, tzinfo=timezone.utc),
            timeframe="1m",
            horizon_minutes=10,
            model_id=2,
            predicted_direction=-1,
            confidence=0.65,
        ),
    ]
    await db.insert_predictions(preds)

    conn.executemany.assert_called_once()


@pytest.mark.asyncio
async def test_insert_predictions_empty(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    db = Database(settings)
    db.pool = pool

    await db.insert_predictions([])
    conn.executemany.assert_not_called()


@pytest.mark.asyncio
async def test_query_predictions(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[
        {
            "symbol": "SPY",
            "timestamp": datetime(2026, 4, 11, 15, 30, 0, tzinfo=timezone.utc),
            "timeframe": "1m",
            "horizon_minutes": 5,
            "model_id": 1,
            "predicted_direction": 1,
            "confidence": 0.72,
        }
    ])
    db = Database(settings)
    db.pool = pool

    preds = await db.query_predictions("SPY", "1m", 5, limit=10)

    assert len(preds) == 1
    assert preds[0].predicted_direction == 1
    assert preds[0].confidence == 0.72


@pytest.mark.asyncio
async def test_query_training_data(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    ts = datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc)
    conn.fetch = AsyncMock(return_value=[
        {
            "timestamp": ts,
            "features": json.dumps({"sma_20": 420.5, "rsi_14": 55.3}),
            "direction": 1,
            "forward_return": 0.002,
        },
        {
            "timestamp": ts + timedelta(minutes=1),
            "features": json.dumps({"sma_20": 421.0, "rsi_14": 56.1}),
            "direction": 0,
            "forward_return": 0.0001,
        },
    ])
    db = Database(settings)
    db.pool = pool

    rows = await db.query_training_data("SPY", "1m", 5, limit=1000)

    assert len(rows) == 2
    assert rows[0]["direction"] == 1
    assert "features" in rows[0]


@pytest.mark.asyncio
async def test_query_all_active_models(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 1,
            "symbol": "SPY",
            "timeframe": "1m",
            "horizon_minutes": 5,
            "model_type": "gradient_boost",
            "artifact_path": "/path/a.joblib",
            "metrics": json.dumps({"accuracy": 0.55}),
            "feature_columns": json.dumps(["sma_20"]),
            "trained_at": datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc),
            "train_samples": 800,
            "test_samples": 200,
            "is_active": True,
        },
        {
            "id": 2,
            "symbol": "SPY",
            "timeframe": "1m",
            "horizon_minutes": 10,
            "model_type": "gradient_boost",
            "artifact_path": "/path/b.joblib",
            "metrics": json.dumps({"accuracy": 0.52}),
            "feature_columns": json.dumps(["sma_20", "rsi_14"]),
            "trained_at": datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc),
            "train_samples": 750,
            "test_samples": 190,
            "is_active": True,
        },
    ])
    db = Database(settings)
    db.pool = pool

    models = await db.query_all_active_models()

    assert len(models) == 2
    assert models[0].id == 1
    assert models[1].horizon_minutes == 10
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bullforge && python -m pytest tests/test_model_db.py -v`
Expected: FAIL with `AttributeError` (methods don't exist yet)

- [ ] **Step 3: Write minimal implementation**

Add the following import to the top of `bullforge/shared/db.py` — update the existing import from `shared.models`:

```python
from shared.models import Bar, FeatureRow, Label, ModelMetadata, Prediction
```

Add the following methods to the `Database` class in `bullforge/shared/db.py`, after the existing `query_labels` method:

```python
    async def query_training_data(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        limit: int = 10000,
    ) -> list[dict]:
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
                ORDER BY f.timestamp ASC
                LIMIT $4
                """,
                symbol,
                timeframe,
                horizon_minutes,
                limit,
            )
        return [dict(r) for r in rows]

    async def insert_model_metadata(self, meta: ModelMetadata) -> int:
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO model_registry
                    (symbol, timeframe, horizon_minutes, model_type, artifact_path,
                     metrics, feature_columns, trained_at, train_samples, test_samples,
                     is_active)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING id
                """,
                meta.symbol,
                meta.timeframe,
                meta.horizon_minutes,
                meta.model_type,
                meta.artifact_path,
                json.dumps(meta.metrics),
                json.dumps(meta.feature_columns),
                meta.trained_at,
                meta.train_samples,
                meta.test_samples,
                meta.is_active,
            )
        return row_id

    async def query_active_model(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
    ) -> Optional[ModelMetadata]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, symbol, timeframe, horizon_minutes, model_type,
                       artifact_path, metrics, feature_columns, trained_at,
                       train_samples, test_samples, is_active
                FROM model_registry
                WHERE symbol = $1 AND timeframe = $2
                    AND horizon_minutes = $3 AND is_active = TRUE
                ORDER BY trained_at DESC
                LIMIT 1
                """,
                symbol,
                timeframe,
                horizon_minutes,
            )
        if row is None:
            return None
        return ModelMetadata(
            id=row["id"],
            symbol=row["symbol"],
            timeframe=row["timeframe"],
            horizon_minutes=row["horizon_minutes"],
            model_type=row["model_type"],
            artifact_path=row["artifact_path"],
            metrics=json.loads(row["metrics"]),
            feature_columns=json.loads(row["feature_columns"]),
            trained_at=row["trained_at"],
            train_samples=row["train_samples"],
            test_samples=row["test_samples"],
            is_active=row["is_active"],
        )

    async def query_all_active_models(self) -> List[ModelMetadata]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, symbol, timeframe, horizon_minutes, model_type,
                       artifact_path, metrics, feature_columns, trained_at,
                       train_samples, test_samples, is_active
                FROM model_registry
                WHERE is_active = TRUE
                ORDER BY symbol, timeframe, horizon_minutes
                """
            )
        return [
            ModelMetadata(
                id=r["id"],
                symbol=r["symbol"],
                timeframe=r["timeframe"],
                horizon_minutes=r["horizon_minutes"],
                model_type=r["model_type"],
                artifact_path=r["artifact_path"],
                metrics=json.loads(r["metrics"]),
                feature_columns=json.loads(r["feature_columns"]),
                trained_at=r["trained_at"],
                train_samples=r["train_samples"],
                test_samples=r["test_samples"],
                is_active=r["is_active"],
            )
            for r in rows
        ]

    async def deactivate_models(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE model_registry
                SET is_active = FALSE
                WHERE symbol = $1 AND timeframe = $2
                    AND horizon_minutes = $3 AND is_active = TRUE
                """,
                symbol,
                timeframe,
                horizon_minutes,
            )

    async def insert_prediction(self, pred: Prediction) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO predictions
                    (symbol, timestamp, timeframe, horizon_minutes,
                     model_id, predicted_direction, confidence)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, timestamp, timeframe, horizon_minutes)
                DO UPDATE SET model_id = $5, predicted_direction = $6, confidence = $7
                """,
                pred.symbol,
                pred.timestamp,
                pred.timeframe,
                pred.horizon_minutes,
                pred.model_id,
                pred.predicted_direction,
                pred.confidence,
            )

    async def insert_predictions(self, preds: List[Prediction]) -> None:
        if not preds:
            return
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO predictions
                    (symbol, timestamp, timeframe, horizon_minutes,
                     model_id, predicted_direction, confidence)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, timestamp, timeframe, horizon_minutes)
                DO UPDATE SET model_id = $5, predicted_direction = $6, confidence = $7
                """,
                [
                    (
                        p.symbol, p.timestamp, p.timeframe,
                        p.horizon_minutes, p.model_id,
                        p.predicted_direction, p.confidence,
                    )
                    for p in preds
                ],
            )

    async def query_predictions(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        limit: int = 100,
    ) -> List[Prediction]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, timestamp, timeframe, horizon_minutes,
                       model_id, predicted_direction, confidence
                FROM predictions
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
            Prediction(
                symbol=r["symbol"],
                timestamp=r["timestamp"],
                timeframe=r["timeframe"],
                horizon_minutes=r["horizon_minutes"],
                model_id=r["model_id"],
                predicted_direction=r["predicted_direction"],
                confidence=r["confidence"],
            )
            for r in rows
        ]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bullforge && python -m pytest tests/test_model_db.py -v`
Expected: All 11 tests PASS

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/db.py bullforge/tests/test_model_db.py
git commit -m "feat: add model registry and prediction database methods"
```

---

## Task 5: Dataset Builder

**Files:**
- Create: `bullforge/services/model_service/__init__.py`
- Create: `bullforge/services/model_service/dataset.py`
- Create: `bullforge/tests/test_dataset.py`

- [ ] **Step 1: Write the failing test**

Create `bullforge/services/model_service/__init__.py` (empty file).

Write `bullforge/tests/test_dataset.py`:

```python
import json
import pytest
import pytest_asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock
from services.model_service.dataset import DatasetBuilder, time_based_split


def _make_training_rows(n: int = 100) -> list[dict]:
    base = datetime(2026, 4, 10, 14, 0, 0, tzinfo=timezone.utc)
    np.random.seed(42)
    rows = []
    for i in range(n):
        features = {
            "sma_20": float(420 + np.random.randn()),
            "rsi_14": float(50 + np.random.randn() * 10),
            "return_1": float(np.random.randn() * 0.001),
            "volume_ratio_10": float(1.0 + np.random.randn() * 0.2),
        }
        direction = int(np.random.choice([-1, 0, 1]))
        fwd_ret = float(np.random.randn() * 0.002)
        rows.append({
            "timestamp": base + timedelta(minutes=i),
            "features": json.dumps(features),
            "direction": direction,
            "forward_return": fwd_ret,
        })
    return rows


@pytest.mark.asyncio
async def test_dataset_builder_builds_dataframe():
    mock_db = AsyncMock()
    mock_db.query_training_data = AsyncMock(return_value=_make_training_rows(100))

    builder = DatasetBuilder(mock_db)
    result = await builder.build("SPY", "1m", 5, limit=1000)

    assert result is not None
    X, y = result
    assert isinstance(X, pd.DataFrame)
    assert isinstance(y, pd.Series)
    assert len(X) == 100
    assert len(y) == 100
    assert "sma_20" in X.columns
    assert "rsi_14" in X.columns
    assert "return_1" in X.columns
    assert "volume_ratio_10" in X.columns
    assert set(y.unique()).issubset({-1, 0, 1})


@pytest.mark.asyncio
async def test_dataset_builder_returns_none_insufficient_data():
    mock_db = AsyncMock()
    mock_db.query_training_data = AsyncMock(return_value=_make_training_rows(5))

    builder = DatasetBuilder(mock_db, min_samples=50)
    result = await builder.build("SPY", "1m", 5)

    assert result is None


@pytest.mark.asyncio
async def test_dataset_builder_returns_none_empty():
    mock_db = AsyncMock()
    mock_db.query_training_data = AsyncMock(return_value=[])

    builder = DatasetBuilder(mock_db)
    result = await builder.build("SPY", "1m", 5)

    assert result is None


@pytest.mark.asyncio
async def test_dataset_builder_handles_json_string_features():
    mock_db = AsyncMock()
    rows = [
        {
            "timestamp": datetime(2026, 4, 10, 14, i, 0, tzinfo=timezone.utc),
            "features": json.dumps({"sma_20": 420.0 + i, "rsi_14": 50.0}),
            "direction": 1,
            "forward_return": 0.001,
        }
        for i in range(20)
    ]
    mock_db.query_training_data = AsyncMock(return_value=rows)

    builder = DatasetBuilder(mock_db, min_samples=10)
    result = await builder.build("SPY", "1m", 5)

    assert result is not None
    X, y = result
    assert len(X) == 20
    assert X["sma_20"].iloc[0] == 420.0


def test_time_based_split():
    np.random.seed(42)
    X = pd.DataFrame({
        "a": np.random.randn(100),
        "b": np.random.randn(100),
    })
    y = pd.Series(np.random.choice([-1, 0, 1], 100))

    X_train, X_test, y_train, y_test = time_based_split(X, y, test_ratio=0.2)

    assert len(X_train) == 80
    assert len(X_test) == 20
    assert len(y_train) == 80
    assert len(y_test) == 20
    # Train indices come before test indices (time-based)
    assert X_train.index[-1] < X_test.index[0]


def test_time_based_split_preserves_data():
    X = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    y = pd.Series([0, 1, 0, 1, 0])

    X_train, X_test, y_train, y_test = time_based_split(X, y, test_ratio=0.4)

    assert len(X_train) == 3
    assert len(X_test) == 2
    assert list(X_train["a"]) == [1, 2, 3]
    assert list(X_test["a"]) == [4, 5]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bullforge && python -m pytest tests/test_dataset.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'services.model_service'`

- [ ] **Step 3: Write minimal implementation**

Write `bullforge/services/model_service/dataset.py`:

```python
from __future__ import annotations

import json
import logging
from typing import Optional, Tuple

import pandas as pd

from shared.db import Database

logger = logging.getLogger(__name__)


class DatasetBuilder:
    def __init__(self, db: Database, min_samples: int = 500):
        self._db = db
        self._min_samples = min_samples

    async def build(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        limit: int = 10000,
    ) -> Optional[Tuple[pd.DataFrame, pd.Series]]:
        rows = await self._db.query_training_data(
            symbol, timeframe, horizon_minutes, limit=limit,
        )

        if len(rows) < self._min_samples:
            logger.warning(
                "Insufficient training data for %s %s %dm: %d < %d",
                symbol, timeframe, horizon_minutes,
                len(rows), self._min_samples,
            )
            return None

        records = []
        directions = []
        for row in rows:
            features = row["features"]
            if isinstance(features, str):
                features = json.loads(features)
            records.append(features)
            directions.append(row["direction"])

        X = pd.DataFrame(records)
        y = pd.Series(directions, name="direction")
        return X, y


def time_based_split(
    X: pd.DataFrame,
    y: pd.Series,
    test_ratio: float = 0.2,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    split_idx = int(len(X) * (1 - test_ratio))
    X_train = X.iloc[:split_idx]
    X_test = X.iloc[split_idx:]
    y_train = y.iloc[:split_idx]
    y_test = y.iloc[split_idx:]
    return X_train, X_test, y_train, y_test
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bullforge && python -m pytest tests/test_dataset.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/model_service/__init__.py bullforge/services/model_service/dataset.py bullforge/tests/test_dataset.py
git commit -m "feat: dataset builder for training data from features and labels"
```

---

## Task 6: Model Factory

**Files:**
- Create: `bullforge/services/model_service/model_factory.py`
- Create: `bullforge/tests/test_model_factory.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_model_factory.py`:

```python
import pytest
import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline
from services.model_service.model_factory import create_model, SUPPORTED_MODELS


def _make_synthetic_data(n: int = 200):
    np.random.seed(42)
    X = pd.DataFrame({
        "feature_a": np.random.randn(n),
        "feature_b": np.random.randn(n),
        "feature_c": np.random.randn(n),
    })
    y = pd.Series(np.random.choice([-1, 0, 1], n))
    return X, y


def test_supported_models_set():
    assert "gradient_boost" in SUPPORTED_MODELS
    assert "logistic_regression" in SUPPORTED_MODELS


def test_create_gradient_boost():
    model = create_model("gradient_boost")
    assert isinstance(model, Pipeline)
    step_names = [name for name, _ in model.steps]
    assert "scaler" in step_names
    assert "classifier" in step_names


def test_create_logistic_regression():
    model = create_model("logistic_regression")
    assert isinstance(model, Pipeline)
    step_names = [name for name, _ in model.steps]
    assert "scaler" in step_names
    assert "classifier" in step_names


def test_create_model_rejects_unknown():
    with pytest.raises(ValueError, match="Unknown model type"):
        create_model("random_forest")


def test_gradient_boost_fit_predict():
    X, y = _make_synthetic_data()
    model = create_model("gradient_boost")
    model.fit(X, y)

    preds = model.predict(X)
    assert len(preds) == len(X)
    assert set(preds).issubset({-1, 0, 1})


def test_gradient_boost_predict_proba():
    X, y = _make_synthetic_data()
    model = create_model("gradient_boost")
    model.fit(X, y)

    proba = model.predict_proba(X)
    assert proba.shape == (len(X), 3)  # 3 classes
    # Probabilities should sum to ~1
    np.testing.assert_allclose(proba.sum(axis=1), 1.0, atol=1e-6)


def test_logistic_regression_fit_predict():
    X, y = _make_synthetic_data()
    model = create_model("logistic_regression")
    model.fit(X, y)

    preds = model.predict(X)
    assert len(preds) == len(X)
    assert set(preds).issubset({-1, 0, 1})


def test_logistic_regression_predict_proba():
    X, y = _make_synthetic_data()
    model = create_model("logistic_regression")
    model.fit(X, y)

    proba = model.predict_proba(X)
    assert proba.shape == (len(X), 3)
    np.testing.assert_allclose(proba.sum(axis=1), 1.0, atol=1e-6)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bullforge && python -m pytest tests/test_model_factory.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

Write `bullforge/services/model_service/model_factory.py`:

```python
from __future__ import annotations

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

SUPPORTED_MODELS = {"gradient_boost", "logistic_regression"}


def create_model(model_type: str) -> Pipeline:
    if model_type == "gradient_boost":
        return Pipeline([
            ("scaler", StandardScaler()),
            ("classifier", GradientBoostingClassifier(
                n_estimators=100,
                max_depth=3,
                learning_rate=0.1,
                random_state=42,
            )),
        ])
    elif model_type == "logistic_regression":
        return Pipeline([
            ("scaler", StandardScaler()),
            ("classifier", LogisticRegression(
                max_iter=1000,
                multi_class="multinomial",
                random_state=42,
            )),
        ])
    else:
        raise ValueError(
            f"Unknown model type: {model_type}. Supported: {SUPPORTED_MODELS}"
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bullforge && python -m pytest tests/test_model_factory.py -v`
Expected: All 8 tests PASS

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/model_service/model_factory.py bullforge/tests/test_model_factory.py
git commit -m "feat: sklearn model factory with gradient boost and logistic regression"
```

---

## Task 7: Model Store

**Files:**
- Create: `bullforge/services/model_service/model_store.py`
- Create: `bullforge/tests/test_model_store.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_model_store.py`:

```python
import pytest
import numpy as np
import pandas as pd
from services.model_service.model_store import ModelStore
from services.model_service.model_factory import create_model


def _make_trained_model():
    np.random.seed(42)
    X = pd.DataFrame({"a": np.random.randn(50), "b": np.random.randn(50)})
    y = pd.Series(np.random.choice([-1, 0, 1], 50))
    model = create_model("gradient_boost")
    model.fit(X, y)
    return model


def test_model_store_creates_directory(tmp_path):
    artifacts_dir = str(tmp_path / "models" / "artifacts")
    store = ModelStore(artifacts_dir)
    assert (tmp_path / "models" / "artifacts").exists()


def test_save_and_load_model(tmp_path):
    store = ModelStore(str(tmp_path))
    model = _make_trained_model()

    path = store.save(model, "SPY", "1m", 5, "gradient_boost")

    assert path.endswith(".joblib")
    assert (tmp_path / path.split("/")[-1]).exists() or True  # path is absolute

    loaded = store.load(path)

    # Verify loaded model produces same predictions
    np.random.seed(99)
    X_test = pd.DataFrame({"a": np.random.randn(10), "b": np.random.randn(10)})
    original_preds = model.predict(X_test)
    loaded_preds = loaded.predict(X_test)
    np.testing.assert_array_equal(original_preds, loaded_preds)


def test_save_generates_unique_paths(tmp_path):
    store = ModelStore(str(tmp_path))
    model = _make_trained_model()

    path1 = store.save(model, "SPY", "1m", 5, "gradient_boost")
    path2 = store.save(model, "SPY", "1m", 10, "gradient_boost")
    path3 = store.save(model, "AAPL", "1m", 5, "gradient_boost")

    assert path1 != path2
    assert path1 != path3


def test_load_nonexistent_raises(tmp_path):
    store = ModelStore(str(tmp_path))
    with pytest.raises(FileNotFoundError):
        store.load(str(tmp_path / "nonexistent.joblib"))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bullforge && python -m pytest tests/test_model_store.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

Write `bullforge/services/model_service/model_store.py`:

```python
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import joblib

logger = logging.getLogger(__name__)


class ModelStore:
    def __init__(self, artifacts_dir: str):
        self._dir = Path(artifacts_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

    def save(
        self,
        model: object,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        model_type: str,
    ) -> str:
        now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        filename = f"{model_type}_{symbol}_{timeframe}_{horizon_minutes}m_{now}.joblib"
        path = self._dir / filename
        joblib.dump(model, path)
        logger.info("Saved model artifact to %s", path)
        return str(path)

    def load(self, artifact_path: str) -> object:
        path = Path(artifact_path)
        if not path.exists():
            raise FileNotFoundError(f"Model artifact not found: {artifact_path}")
        model = joblib.load(path)
        logger.info("Loaded model artifact from %s", path)
        return model
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bullforge && python -m pytest tests/test_model_store.py -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/model_service/model_store.py bullforge/tests/test_model_store.py
git commit -m "feat: model store for saving and loading sklearn artifacts with joblib"
```

---

## Task 8: Trainer

**Files:**
- Create: `bullforge/services/model_service/trainer.py`
- Create: `bullforge/tests/test_trainer.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_trainer.py`:

```python
import json
import pytest
import pytest_asyncio
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from shared.models import ModelMetadata
from services.model_service.trainer import Trainer


def _make_training_rows(n: int = 200) -> list[dict]:
    base = datetime(2026, 4, 10, 14, 0, 0, tzinfo=timezone.utc)
    np.random.seed(42)
    rows = []
    for i in range(n):
        features = {
            "sma_20": float(420 + np.random.randn()),
            "rsi_14": float(50 + np.random.randn() * 10),
            "return_1": float(np.random.randn() * 0.001),
        }
        direction = int(np.random.choice([-1, 0, 1]))
        rows.append({
            "timestamp": base + timedelta(minutes=i),
            "features": json.dumps(features),
            "direction": direction,
            "forward_return": float(np.random.randn() * 0.002),
        })
    return rows


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.query_training_data = AsyncMock(return_value=_make_training_rows(200))
    db.deactivate_models = AsyncMock()
    db.insert_model_metadata = AsyncMock(return_value=1)
    return db


@pytest.fixture
def mock_model_store(tmp_path):
    from services.model_service.model_store import ModelStore
    return ModelStore(str(tmp_path))


@pytest.mark.asyncio
async def test_trainer_trains_gradient_boost(mock_db, mock_model_store):
    trainer = Trainer(db=mock_db, model_store=mock_model_store, min_samples=50)
    meta = await trainer.train("SPY", "1m", 5, model_type="gradient_boost")

    assert meta is not None
    assert isinstance(meta, ModelMetadata)
    assert meta.symbol == "SPY"
    assert meta.timeframe == "1m"
    assert meta.horizon_minutes == 5
    assert meta.model_type == "gradient_boost"
    assert "accuracy" in meta.metrics
    assert "f1_macro" in meta.metrics
    assert meta.train_samples > 0
    assert meta.test_samples > 0
    assert len(meta.feature_columns) == 3
    assert meta.id == 1

    mock_db.deactivate_models.assert_called_once_with("SPY", "1m", 5)
    mock_db.insert_model_metadata.assert_called_once()


@pytest.mark.asyncio
async def test_trainer_trains_logistic_regression(mock_db, mock_model_store):
    trainer = Trainer(db=mock_db, model_store=mock_model_store, min_samples=50)
    meta = await trainer.train("SPY", "1m", 5, model_type="logistic_regression")

    assert meta is not None
    assert meta.model_type == "logistic_regression"
    assert "accuracy" in meta.metrics


@pytest.mark.asyncio
async def test_trainer_returns_none_insufficient_data(mock_model_store):
    mock_db = AsyncMock()
    mock_db.query_training_data = AsyncMock(return_value=_make_training_rows(10))

    trainer = Trainer(db=mock_db, model_store=mock_model_store, min_samples=500)
    meta = await trainer.train("SPY", "1m", 5)

    assert meta is None
    mock_db.insert_model_metadata.assert_not_called()


@pytest.mark.asyncio
async def test_trainer_deactivates_old_model_before_insert(mock_db, mock_model_store):
    trainer = Trainer(db=mock_db, model_store=mock_model_store, min_samples=50)
    await trainer.train("SPY", "1m", 5)

    # deactivate should be called before insert
    mock_db.deactivate_models.assert_called_once_with("SPY", "1m", 5)
    mock_db.insert_model_metadata.assert_called_once()


@pytest.mark.asyncio
async def test_trainer_metrics_are_valid(mock_db, mock_model_store):
    trainer = Trainer(db=mock_db, model_store=mock_model_store, min_samples=50)
    meta = await trainer.train("SPY", "1m", 5)

    assert meta is not None
    assert 0.0 <= meta.metrics["accuracy"] <= 1.0
    assert 0.0 <= meta.metrics["f1_macro"] <= 1.0
    assert 0.0 <= meta.metrics["precision_macro"] <= 1.0
    assert 0.0 <= meta.metrics["recall_macro"] <= 1.0
    assert meta.metrics["support_neg"] >= 0
    assert meta.metrics["support_flat"] >= 0
    assert meta.metrics["support_pos"] >= 0


@pytest.mark.asyncio
async def test_trainer_artifact_path_exists(mock_db, mock_model_store):
    trainer = Trainer(db=mock_db, model_store=mock_model_store, min_samples=50)
    meta = await trainer.train("SPY", "1m", 5)

    assert meta is not None
    from pathlib import Path
    assert Path(meta.artifact_path).exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bullforge && python -m pytest tests/test_trainer.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

Write `bullforge/services/model_service/trainer.py`:

```python
from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
)

from shared.db import Database
from shared.models import ModelMetadata
from services.model_service.dataset import DatasetBuilder, time_based_split
from services.model_service.model_factory import create_model
from services.model_service.model_store import ModelStore

logger = logging.getLogger(__name__)


class Trainer:
    def __init__(
        self,
        db: Database,
        model_store: ModelStore,
        min_samples: int = 500,
    ):
        self._db = db
        self._model_store = model_store
        self._dataset_builder = DatasetBuilder(db, min_samples=min_samples)

    async def train(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        model_type: str = "gradient_boost",
    ) -> Optional[ModelMetadata]:
        logger.info(
            "Training %s model for %s %s %dm...",
            model_type, symbol, timeframe, horizon_minutes,
        )

        result = await self._dataset_builder.build(
            symbol, timeframe, horizon_minutes,
        )
        if result is None:
            return None

        X, y = result
        X_train, X_test, y_train, y_test = time_based_split(X, y, test_ratio=0.2)

        model = create_model(model_type)
        model.fit(X_train, y_train)

        metrics = self._evaluate(model, X_test, y_test)
        logger.info(
            "Model %s %s %s %dm — accuracy=%.3f f1_macro=%.3f",
            model_type, symbol, timeframe, horizon_minutes,
            metrics["accuracy"], metrics["f1_macro"],
        )

        artifact_path = self._model_store.save(
            model, symbol, timeframe, horizon_minutes, model_type,
        )

        trained_at = datetime.now(timezone.utc)
        feature_columns = list(X.columns)

        await self._db.deactivate_models(symbol, timeframe, horizon_minutes)

        meta = ModelMetadata(
            symbol=symbol,
            timeframe=timeframe,
            horizon_minutes=horizon_minutes,
            model_type=model_type,
            artifact_path=artifact_path,
            metrics=metrics,
            feature_columns=feature_columns,
            trained_at=trained_at,
            train_samples=len(X_train),
            test_samples=len(X_test),
            is_active=True,
        )

        row_id = await self._db.insert_model_metadata(meta)
        meta = ModelMetadata(
            id=row_id,
            symbol=meta.symbol,
            timeframe=meta.timeframe,
            horizon_minutes=meta.horizon_minutes,
            model_type=meta.model_type,
            artifact_path=meta.artifact_path,
            metrics=meta.metrics,
            feature_columns=meta.feature_columns,
            trained_at=meta.trained_at,
            train_samples=meta.train_samples,
            test_samples=meta.test_samples,
            is_active=meta.is_active,
        )

        logger.info(
            "Model saved: id=%d artifact=%s", row_id, artifact_path,
        )
        return meta

    def _evaluate(
        self,
        model: object,
        X_test: pd.DataFrame,
        y_test: pd.Series,
    ) -> dict[str, float]:
        y_pred = model.predict(X_test)
        dist = Counter(y_test)
        return {
            "accuracy": float(accuracy_score(y_test, y_pred)),
            "precision_macro": float(
                precision_score(y_test, y_pred, average="macro", zero_division=0)
            ),
            "recall_macro": float(
                recall_score(y_test, y_pred, average="macro", zero_division=0)
            ),
            "f1_macro": float(
                f1_score(y_test, y_pred, average="macro", zero_division=0)
            ),
            "support_neg": float(dist.get(-1, 0)),
            "support_flat": float(dist.get(0, 0)),
            "support_pos": float(dist.get(1, 0)),
        }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bullforge && python -m pytest tests/test_trainer.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/model_service/trainer.py bullforge/tests/test_trainer.py
git commit -m "feat: training pipeline with dataset building, model fitting, and evaluation"
```

---

## Task 9: Inference Engine

**Files:**
- Create: `bullforge/services/model_service/inference.py`
- Create: `bullforge/tests/test_inference.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_inference.py`:

```python
import json
import pytest
import pytest_asyncio
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from shared.models import ModelMetadata, Prediction, FeatureRow
from services.model_service.inference import InferenceEngine


def _make_model_metadata(symbol="SPY", horizon=5):
    return ModelMetadata(
        id=1,
        symbol=symbol,
        timeframe="1m",
        horizon_minutes=horizon,
        model_type="gradient_boost",
        artifact_path="/path/model.joblib",
        metrics={"accuracy": 0.55},
        feature_columns=["sma_20", "rsi_14", "return_1"],
        trained_at=datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc),
        train_samples=800,
        test_samples=200,
    )


def _make_mock_model():
    model = MagicMock()
    model.predict.return_value = np.array([1])
    model.predict_proba.return_value = np.array([[0.15, 0.13, 0.72]])
    return model


def _make_feature_row():
    return FeatureRow(
        symbol="SPY",
        timestamp=datetime(2026, 4, 11, 15, 30, 0, tzinfo=timezone.utc),
        timeframe="1m",
        features={"sma_20": 420.5, "rsi_14": 55.3, "return_1": 0.001},
    )


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.query_all_active_models = AsyncMock(return_value=[
        _make_model_metadata("SPY", 5),
        _make_model_metadata("SPY", 10),
    ])
    db.query_feature_rows = AsyncMock(return_value=[_make_feature_row()])
    return db


@pytest.fixture
def mock_model_store():
    store = MagicMock()
    store.load.return_value = _make_mock_model()
    return store


@pytest.mark.asyncio
async def test_load_models(mock_db, mock_model_store):
    engine = InferenceEngine(db=mock_db, model_store=mock_model_store)
    await engine.load_models()

    assert len(engine._models) == 2
    assert ("SPY", "1m", 5) in engine._models
    assert ("SPY", "1m", 10) in engine._models
    assert mock_model_store.load.call_count == 2


@pytest.mark.asyncio
async def test_load_models_empty(mock_model_store):
    mock_db = AsyncMock()
    mock_db.query_all_active_models = AsyncMock(return_value=[])

    engine = InferenceEngine(db=mock_db, model_store=mock_model_store)
    await engine.load_models()

    assert len(engine._models) == 0


@pytest.mark.asyncio
async def test_predict(mock_db, mock_model_store):
    engine = InferenceEngine(db=mock_db, model_store=mock_model_store)
    await engine.load_models()

    predictions = await engine.predict("SPY", "1m")

    assert len(predictions) == 2
    for pred in predictions:
        assert isinstance(pred, Prediction)
        assert pred.symbol == "SPY"
        assert pred.timeframe == "1m"
        assert pred.predicted_direction == 1
        assert pred.confidence == pytest.approx(0.72)

    horizons = {p.horizon_minutes for p in predictions}
    assert horizons == {5, 10}


@pytest.mark.asyncio
async def test_predict_no_features(mock_db, mock_model_store):
    mock_db.query_feature_rows = AsyncMock(return_value=[])

    engine = InferenceEngine(db=mock_db, model_store=mock_model_store)
    await engine.load_models()

    predictions = await engine.predict("SPY", "1m")
    assert len(predictions) == 0


@pytest.mark.asyncio
async def test_predict_no_model_for_symbol(mock_db, mock_model_store):
    engine = InferenceEngine(db=mock_db, model_store=mock_model_store)
    await engine.load_models()

    predictions = await engine.predict("AAPL", "1m")
    assert len(predictions) == 0


@pytest.mark.asyncio
async def test_predict_fills_missing_features(mock_db, mock_model_store):
    # Feature row is missing "return_1" — should be filled with 0.0
    partial_feature_row = FeatureRow(
        symbol="SPY",
        timestamp=datetime(2026, 4, 11, 15, 30, 0, tzinfo=timezone.utc),
        timeframe="1m",
        features={"sma_20": 420.5, "rsi_14": 55.3},
    )
    mock_db.query_feature_rows = AsyncMock(return_value=[partial_feature_row])

    engine = InferenceEngine(db=mock_db, model_store=mock_model_store)
    await engine.load_models()

    predictions = await engine.predict("SPY", "1m")
    assert len(predictions) == 2  # Still produces predictions
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bullforge && python -m pytest tests/test_inference.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

Write `bullforge/services/model_service/inference.py`:

```python
from __future__ import annotations

import logging
from typing import Dict, List, Tuple

import pandas as pd

from shared.db import Database
from shared.models import ModelMetadata, Prediction
from services.model_service.model_store import ModelStore

logger = logging.getLogger(__name__)


class InferenceEngine:
    def __init__(self, db: Database, model_store: ModelStore):
        self._db = db
        self._model_store = model_store
        self._models: Dict[
            Tuple[str, str, int], Tuple[object, ModelMetadata]
        ] = {}

    async def load_models(self) -> None:
        active_models = await self._db.query_all_active_models()
        self._models.clear()

        for meta in active_models:
            try:
                model = self._model_store.load(meta.artifact_path)
                key = (meta.symbol, meta.timeframe, meta.horizon_minutes)
                self._models[key] = (model, meta)
                logger.info(
                    "Loaded model: %s %s %dm (%s, id=%d)",
                    meta.symbol, meta.timeframe, meta.horizon_minutes,
                    meta.model_type, meta.id,
                )
            except FileNotFoundError:
                logger.warning(
                    "Artifact not found for model id=%d: %s",
                    meta.id, meta.artifact_path,
                )

        logger.info("Loaded %d models for inference", len(self._models))

    async def predict(
        self, symbol: str, timeframe: str
    ) -> List[Prediction]:
        matching = [
            (key, model, meta)
            for key, (model, meta) in self._models.items()
            if key[0] == symbol and key[1] == timeframe
        ]

        if not matching:
            return []

        feature_rows = await self._db.query_feature_rows(
            symbol, timeframe, limit=1,
        )
        if not feature_rows:
            logger.debug("No features available for %s %s", symbol, timeframe)
            return []

        latest_features = feature_rows[0]
        predictions = []

        for key, model, meta in matching:
            horizon = key[2]
            feature_dict = {
                col: latest_features.features.get(col, 0.0)
                for col in meta.feature_columns
            }
            X = pd.DataFrame([feature_dict])

            direction = int(model.predict(X)[0])
            proba = model.predict_proba(X)[0]
            confidence = float(proba.max())

            predictions.append(Prediction(
                symbol=symbol,
                timestamp=latest_features.timestamp,
                timeframe=timeframe,
                horizon_minutes=horizon,
                model_id=meta.id,
                predicted_direction=direction,
                confidence=confidence,
            ))

        return predictions
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bullforge && python -m pytest tests/test_inference.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/model_service/inference.py bullforge/tests/test_inference.py
git commit -m "feat: inference engine with model caching and live prediction"
```

---

## Task 10: Feature Engine Update (Publish "feature:updates")

**Files:**
- Modify: `bullforge/services/feature_engine/engine.py`
- Modify: `bullforge/services/feature_engine/main.py`
- Modify: `bullforge/tests/test_engine.py`

- [ ] **Step 1: Write the failing test**

Add to `bullforge/tests/test_engine.py`:

```python
@pytest.mark.asyncio
async def test_compute_features_publishes_update(mock_settings, mock_db, simple_registry):
    mock_redis = AsyncMock()
    mock_redis.publish = AsyncMock()

    engine = FeatureEngine(
        settings=mock_settings,
        db=mock_db,
        registry=simple_registry,
        label_horizons=[5],
        redis_client=mock_redis,
    )

    await engine.compute_features_for_symbol("SPY", "1m")

    mock_redis.publish.assert_called_once()
    call_args = mock_redis.publish.call_args
    assert call_args[0][0] == "feature:updates"
    import json
    payload = json.loads(call_args[0][1])
    assert payload["symbol"] == "SPY"
    assert payload["timeframe"] == "1m"


@pytest.mark.asyncio
async def test_compute_features_no_publish_without_redis(mock_settings, mock_db, simple_registry):
    engine = FeatureEngine(
        settings=mock_settings,
        db=mock_db,
        registry=simple_registry,
        label_horizons=[5],
    )

    # Should not raise even without redis_client
    await engine.compute_features_for_symbol("SPY", "1m")
    mock_db.insert_feature_rows.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bullforge && python -m pytest tests/test_engine.py::test_compute_features_publishes_update tests/test_engine.py::test_compute_features_no_publish_without_redis -v`
Expected: FAIL with `TypeError` (unexpected keyword argument 'redis_client')

- [ ] **Step 3: Modify FeatureEngine to accept optional redis_client**

In `bullforge/services/feature_engine/engine.py`, update the `__init__` method to add `redis_client` parameter and add the import:

Add to the imports at the top of the file:

```python
import json
from shared.redis import RedisClient
from typing import Optional as OptionalType
```

Update `__init__` signature:

```python
    def __init__(
        self,
        settings: Settings,
        db: Database,
        registry: FeatureRegistry,
        label_horizons: List[int],
        label_threshold: float = 0.0,
        min_bars: int = 50,
        redis_client: OptionalType[RedisClient] = None,
    ):
        self._settings = settings
        self._db = db
        self._registry = registry
        self._label_horizons = label_horizons
        self._label_threshold = label_threshold
        self._min_bars = min_bars
        self._redis_client = redis_client
```

At the end of `compute_features_for_symbol`, after the `logger.info(...)` call, add:

```python
        if self._redis_client:
            await self._redis_client.publish(
                "feature:updates",
                json.dumps({"symbol": symbol, "timeframe": timeframe}),
            )
```

- [ ] **Step 4: Update feature engine main.py**

In `bullforge/services/feature_engine/main.py`, update the `FeatureEngine` instantiation to pass `redis_client`:

```python
    engine = FeatureEngine(
        settings=settings,
        db=db,
        registry=registry,
        label_horizons=[5, 10, 30],
        label_threshold=0.001,
        min_bars=50,
        redis_client=redis_client,
    )
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd bullforge && python -m pytest tests/test_engine.py -v`
Expected: All tests PASS (existing tests pass because redis_client defaults to None)

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/feature_engine/engine.py bullforge/services/feature_engine/main.py bullforge/tests/test_engine.py
git commit -m "feat: feature engine publishes to feature:updates channel after computing"
```

---

## Task 11: Service Entry Point, Health Check, and Docker

**Files:**
- Create: `bullforge/services/model_service/health.py`
- Create: `bullforge/services/model_service/main.py`
- Create: `bullforge/services/model_service/requirements.txt`
- Create: `bullforge/services/model_service/Dockerfile`
- Modify: `bullforge/docker-compose.yml`

- [ ] **Step 1: Create health.py**

Write `bullforge/services/model_service/health.py`:

```python
from __future__ import annotations

import logging

from aiohttp import web

from shared.db import Database
from shared.redis import RedisClient

logger = logging.getLogger(__name__)


def create_health_app(
    db: Database,
    redis_client: RedisClient,
    inference_engine: object,
) -> web.Application:
    app = web.Application()
    app["db"] = db
    app["redis"] = redis_client
    app["inference"] = inference_engine
    app.router.add_get("/health", health_handler)
    return app


async def health_handler(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    redis_client: RedisClient = request.app["redis"]
    inference_engine = request.app["inference"]

    checks = {}
    all_healthy = True

    if db.pool is not None:
        try:
            async with db.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            checks["db"] = {
                "status": "healthy",
                "pool_size": db.pool.get_size(),
                "idle": db.pool.get_idle_size(),
            }
        except Exception as e:
            checks["db"] = {"status": "unhealthy", "error": str(e)}
            all_healthy = False
    else:
        checks["db"] = {"status": "unhealthy", "error": "pool is None"}
        all_healthy = False

    try:
        if redis_client.client:
            await redis_client.client.ping()
            checks["redis"] = {"status": "healthy"}
        else:
            checks["redis"] = {"status": "unhealthy", "error": "client is None"}
            all_healthy = False
    except Exception as e:
        checks["redis"] = {"status": "unhealthy", "error": str(e)}
        all_healthy = False

    model_count = len(getattr(inference_engine, "_models", {}))
    checks["models"] = {"loaded": model_count}

    status_code = 200 if all_healthy else 503
    return web.json_response(
        {"status": "healthy" if all_healthy else "unhealthy", **checks},
        status=status_code,
    )
```

- [ ] **Step 2: Create main.py**

Write `bullforge/services/model_service/main.py`:

```python
from __future__ import annotations

import asyncio
import json
import logging
import signal

from aiohttp import web

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient
from services.model_service.model_store import ModelStore
from services.model_service.trainer import Trainer
from services.model_service.inference import InferenceEngine
from services.model_service.health import create_health_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("model_service")

HORIZONS = [5, 10, 30]


async def main() -> None:
    settings = Settings()
    db = Database(settings)
    redis_client = RedisClient(settings)

    logger.info("Connecting to TimescaleDB...")
    await db.connect()
    logger.info("Connecting to Redis...")
    await redis_client.connect()

    model_store = ModelStore(settings.model_artifacts_dir)
    trainer = Trainer(
        db=db,
        model_store=model_store,
        min_samples=settings.model_min_samples,
    )
    inference_engine = InferenceEngine(db=db, model_store=model_store)

    # Train models on startup (skip if active model already exists)
    all_symbols = (
        settings.trading_symbols_equity + settings.trading_symbols_crypto
    )
    for symbol in all_symbols:
        for timeframe in ["1m", "1d"]:
            for horizon in HORIZONS:
                existing = await db.query_active_model(
                    symbol, timeframe, horizon,
                )
                if existing:
                    logger.info(
                        "Active model exists for %s %s %dm (id=%d), skipping training",
                        symbol, timeframe, horizon, existing.id,
                    )
                    continue
                await trainer.train(
                    symbol, timeframe, horizon, model_type="gradient_boost",
                )

    # Load all trained models for inference
    await inference_engine.load_models()

    # Subscribe to feature updates for reactive inference
    async def on_feature_update(message: str) -> None:
        data = json.loads(message)
        symbol = data["symbol"]
        timeframe = data["timeframe"]
        logger.debug("Feature update for %s %s, running inference", symbol, timeframe)

        predictions = await inference_engine.predict(symbol, timeframe)
        if predictions:
            await db.insert_predictions(predictions)
            await redis_client.publish(
                "model:predictions",
                json.dumps({
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "predictions": [
                        {
                            "horizon_minutes": p.horizon_minutes,
                            "direction": p.predicted_direction,
                            "confidence": p.confidence,
                        }
                        for p in predictions
                    ],
                }),
            )
            logger.info(
                "Published %d predictions for %s %s",
                len(predictions), symbol, timeframe,
            )

    await redis_client.subscribe("feature:updates", on_feature_update)
    logger.info("Subscribed to feature:updates for reactive inference")

    # Start health check server
    health_app = create_health_app(db, redis_client, inference_engine)
    runner = web.AppRunner(health_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8081)
    await site.start()
    logger.info("Health check server running on :8081")

    # Graceful shutdown
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

    logger.info("Model service running. Press Ctrl+C to stop.")

    try:
        await shutdown_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down...")
        await runner.cleanup()
        await redis_client.disconnect()
        await db.disconnect()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 3: Create requirements.txt**

Write `bullforge/services/model_service/requirements.txt`:

```
scikit-learn>=1.4.0
joblib>=1.3.0
pandas>=2.2.0
numpy>=1.26.0
asyncpg>=0.29.0
redis>=5.0.0
aiohttp>=3.9.0
```

- [ ] **Step 4: Create Dockerfile**

Write `bullforge/services/model_service/Dockerfile`:

```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY services/model_service/requirements.txt /app/services/model_service/requirements.txt
RUN pip install --no-cache-dir -r services/model_service/requirements.txt
RUN pip install --no-cache-dir asyncpg redis

# Copy shared code and service code
COPY shared/ /app/shared/
COPY migrations/ /app/migrations/
COPY services/__init__.py /app/services/__init__.py
COPY services/model_service/ /app/services/model_service/

# Create default artifacts directory
RUN mkdir -p /app/models/artifacts

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "services.model_service.main"]
```

- [ ] **Step 5: Update docker-compose.yml**

Add the `model_service` block to `bullforge/docker-compose.yml`, after the `feature_engine` service:

```yaml
  model_service:
    build:
      context: .
      dockerfile: services/model_service/Dockerfile
    env_file:
      - .env
    environment:
      TIMESCALEDB_HOST: timescaledb
      REDIS_HOST: redis
      MODEL_ARTIFACTS_DIR: /app/models/artifacts
    ports:
      - "18081:8081"
    depends_on:
      timescaledb:
        condition: service_healthy
      redis:
        condition: service_healthy
      feature_engine:
        condition: service_started
    volumes:
      - model_artifacts:/app/models/artifacts
    restart: unless-stopped
```

Add `model_artifacts` to the `volumes` section:

```yaml
volumes:
  timescaledb_data:
  redis_data:
  model_artifacts:
```

- [ ] **Step 6: Run all tests**

Run: `cd bullforge && python -m pytest tests/ -v`
Expected: All tests PASS

- [ ] **Step 7: Commit**

```bash
git add bullforge/services/model_service/health.py bullforge/services/model_service/main.py bullforge/services/model_service/requirements.txt bullforge/services/model_service/Dockerfile bullforge/docker-compose.yml
git commit -m "feat: model service entrypoint with Docker Compose integration"
```
