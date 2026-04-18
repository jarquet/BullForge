import json
import pytest
import pytest_asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock
from shared.models import ModelMetadata, Prediction


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    conn = AsyncMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = ctx
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
        "estimator_type": None,
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

    rows = await db.query_training_data(
        "SPY", "1m", 5,
        as_of=datetime(2026, 4, 13, tzinfo=timezone.utc),
        limit=1000,
    )

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
            "estimator_type": None,
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
            "estimator_type": None,
        },
    ])
    db = Database(settings)
    db.pool = pool

    models = await db.query_all_active_models()

    assert len(models) == 2
    assert models[0].id == 1
    assert models[1].horizon_minutes == 10
