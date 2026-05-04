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
    # Key is (symbol, timeframe, horizon_minutes, model_type) so two
    # model_types at the same (symbol, timeframe, horizon) coexist.
    assert ("SPY", "1m", 5, "gradient_boost") in engine._models
    assert ("SPY", "1m", 10, "gradient_boost") in engine._models
    assert mock_model_store.load.call_count == 2


@pytest.mark.asyncio
async def test_load_models_empty(mock_model_store):
    mock_db = AsyncMock()
    mock_db.query_all_active_models = AsyncMock(return_value=[])

    engine = InferenceEngine(db=mock_db, model_store=mock_model_store)
    await engine.load_models()

    assert len(engine._models) == 0


@pytest.mark.asyncio
async def test_load_models_keeps_both_model_types_for_same_slot(mock_model_store):
    """Regression: two active models that share (symbol, timeframe, horizon)
    but differ in model_type (e.g., gradient_boost AND lstm both active for
    SPY 1m 5m after a retrain) must both load. The earlier 3-tuple key
    silently dropped one of them — operator-visible symptom was 'Loaded N
    models for inference' showing half the registered active count."""
    gb = _make_model_metadata("SPY", 5)  # default model_type=gradient_boost
    lstm = ModelMetadata(
        id=2, symbol="SPY", timeframe="1m", horizon_minutes=5,
        model_type="lstm", artifact_path="/path/lstm.pt",
        metrics={"accuracy": 0.6},
        feature_columns=["sma_20", "rsi_14", "return_1"],
        trained_at=datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc),
        train_samples=800, test_samples=200,
    )

    mock_db = AsyncMock()
    mock_db.query_all_active_models = AsyncMock(return_value=[gb, lstm])

    engine = InferenceEngine(db=mock_db, model_store=mock_model_store)
    await engine.load_models()

    assert len(engine._models) == 2, (
        "Both model_types must coexist for the same (symbol, tf, horizon); "
        "if this is 1, the key collision regression has returned."
    )
    assert ("SPY", "1m", 5, "gradient_boost") in engine._models
    assert ("SPY", "1m", 5, "lstm") in engine._models


@pytest.mark.asyncio
async def test_load_models_skips_meta_artifacts(mock_model_store):
    """Regression for batch-3: meta_entry / meta_exec artifacts must NOT
    end up in self._models. If they do, predict() iterates them and calls
    .predict() on a CalibratedEntryEstimator (which only exposes
    predict_proba), crashing the redis pubsub listener for every 1m
    feature:updates event."""
    base_gb = _make_model_metadata("SPY", 5)
    meta_entry = ModelMetadata(
        id=99, symbol="SPY", timeframe="1m", horizon_minutes=0,
        model_type="meta_entry",
        artifact_path="/path/meta_entry_long_lightgbm_SPY_1m_0m_x.joblib",
        metrics={}, feature_columns=["base_1_dir"],
        trained_at=datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc),
        train_samples=800, test_samples=200, estimator_type="lightgbm",
    )
    meta_exec = ModelMetadata(
        id=100, symbol="SPY", timeframe="1m", horizon_minutes=0,
        model_type="meta_exec",
        artifact_path="/path/meta_exec_long_lightgbm_SPY_1m_0m_x.joblib",
        metrics={}, feature_columns=["base_1_dir"],
        trained_at=datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc),
        train_samples=800, test_samples=200, estimator_type="lightgbm",
    )

    mock_db = AsyncMock()
    mock_db.query_all_active_models = AsyncMock(
        return_value=[base_gb, meta_entry, meta_exec],
    )

    engine = InferenceEngine(db=mock_db, model_store=mock_model_store)
    await engine.load_models()

    assert len(engine._models) == 1
    assert ("SPY", "1m", 5, "gradient_boost") in engine._models
    for key in engine._models:
        assert key[3] not in ("meta_entry", "meta_exec"), (
            f"Meta artifact leaked into base _models: {key}"
        )


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


from unittest.mock import AsyncMock, MagicMock

from shared.models import BacktestScope
from services.model_service.inference import InferenceEngine


@pytest.mark.asyncio
async def test_inference_engine_scoped_load_queries_with_scope():
    db = MagicMock()
    db.query_all_active_models = AsyncMock(return_value=[])
    store = MagicMock()
    scope = BacktestScope(backtest_run_id=11, fold_index=4)

    engine = InferenceEngine(db, store, scope=scope)
    await engine.load_models()

    db.query_all_active_models.assert_awaited_once_with(scope=scope)


@pytest.mark.asyncio
async def test_inference_engine_live_load_queries_without_scope():
    db = MagicMock()
    db.query_all_active_models = AsyncMock(return_value=[])
    store = MagicMock()

    engine = InferenceEngine(db, store)
    await engine.load_models()

    db.query_all_active_models.assert_awaited_once_with(scope=None)


@pytest.mark.asyncio
async def test_load_models_skips_corrupt_artifact_keeps_good_models(
    tmp_path, caplog,
):
    """Regression for 2026-05-04 incident: a corrupt pickle on disk
    raises EOFError / UnpicklingError, not FileNotFoundError. The old
    load_models cleared self._models BEFORE the load loop and only
    caught FileNotFoundError, so a single corrupt artifact wiped the
    entire inference dict. The fix: build a new dict locally, broaden
    the except to log+skip the bad row, swap atomically at the end."""
    import dataclasses
    import joblib

    from services.model_service.model_store import ModelStore

    good_path = tmp_path / "gradient_boost_SPY_1m_5m_20260504T010000Z.joblib"
    # The model object only needs to be pickleable; this test never
    # calls .predict() on it. A plain dict round-trips cleanly through
    # joblib whereas MagicMock does not.
    joblib.dump({"dummy": "good model"}, good_path)

    bad_path = tmp_path / "gradient_boost_SPY_1m_10m_20260504T010000Z.joblib"
    bad_path.write_bytes(b"\x00\x01not a valid pickle stream\x00")

    base = _make_model_metadata("SPY", 5)
    good_meta = dataclasses.replace(base, artifact_path=str(good_path))
    bad_meta = dataclasses.replace(
        base, id=2, horizon_minutes=10, artifact_path=str(bad_path),
    )

    mock_db = AsyncMock()
    mock_db.query_all_active_models = AsyncMock(return_value=[good_meta, bad_meta])

    store = ModelStore(str(tmp_path))
    engine = InferenceEngine(db=mock_db, model_store=store)

    with caplog.at_level("ERROR"):
        await engine.load_models()

    assert len(engine._models) == 1, (
        "Corrupt artifact must not poison the load — only the bad row "
        "is skipped, the good model remains in the inference dict."
    )
    assert ("SPY", "1m", 5, "gradient_boost") in engine._models
    assert any(
        "Failed to load artifact" in rec.message
        for rec in caplog.records
    ), "Bad artifact should produce an ERROR-level log"


@pytest.mark.asyncio
async def test_load_models_preserves_state_when_db_query_fails(
    mock_db, mock_model_store,
):
    """If load_models raises midway through (e.g., DB outage between
    reading active_models and assigning the new dict), the previous
    in-memory model set must remain intact. The old code cleared
    self._models BEFORE awaiting the DB, so any propagated exception
    left the engine modelless. The atomic-swap rewrite removes that
    foot-gun."""
    engine = InferenceEngine(db=mock_db, model_store=mock_model_store)
    await engine.load_models()
    assert len(engine._models) == 2, "Precondition: initial load succeeded"

    mock_db.query_all_active_models = AsyncMock(
        side_effect=ConnectionError("simulated DB outage"),
    )

    with pytest.raises(ConnectionError):
        await engine.load_models()

    assert len(engine._models) == 2, (
        "Previous models must survive a failed reload — last-known-good"
    )


@pytest.mark.asyncio
async def test_load_meta_models_skips_corrupt_artifact_keeps_good_models(
    tmp_path, caplog,
):
    """Mirror the load_models corrupt-artifact fix on the meta-load path.
    load_meta_models already builds a fresh dict locally and assigns at
    the end (atomic-swap), but the inner except still only catches
    FileNotFoundError — so a corrupt meta pickle would propagate, abort
    the loop, and leave _meta_models with whatever partial state had
    been built so far. Same defect, smaller blast radius (meta is
    shadow-only), still worth closing."""
    import dataclasses
    import joblib

    from services.model_service.model_store import ModelStore

    good_path = (
        tmp_path / "meta_entry_long_lightgbm_SPY_1m_0m_20260504T010000Z.joblib"
    )
    joblib.dump({"dummy": "good entry"}, good_path)
    bad_path = (
        tmp_path / "meta_exec_long_lightgbm_SPY_1m_0m_20260504T010000Z.joblib"
    )
    bad_path.write_bytes(b"\x00\x01not a valid meta pickle stream\x00")

    base_entry = ModelMetadata(
        id=1, symbol="SPY", timeframe="1m", horizon_minutes=0,
        model_type="meta_entry", artifact_path=str(good_path),
        metrics={}, feature_columns=["base_1_dir"],
        trained_at=datetime(2026, 5, 4, 1, 0, 0, tzinfo=timezone.utc),
        train_samples=800, test_samples=200, estimator_type="lightgbm",
    )
    bad_exec = dataclasses.replace(
        base_entry, id=2, model_type="meta_exec",
        artifact_path=str(bad_path),
    )

    mock_db = AsyncMock()
    mock_db.query_all_active_models = AsyncMock(return_value=[base_entry, bad_exec])

    store = ModelStore(str(tmp_path))
    engine = InferenceEngine(db=mock_db, model_store=store)

    with caplog.at_level("ERROR"):
        await engine.load_meta_models()

    assert ("SPY", 1, "lightgbm") in engine._meta_models, (
        "Good meta_entry must populate the slot keyed by "
        "(symbol, direction, estimator)"
    )
    slot = engine._meta_models[("SPY", 1, "lightgbm")]
    assert slot["entry"] is not None, "Good meta_entry must have loaded"
    assert slot["exec"] is None, (
        "Corrupt meta_exec must have been skipped, not propagated"
    )
    assert any(
        "Failed to load meta artifact" in rec.message
        for rec in caplog.records
    ), "Bad meta artifact should produce an ERROR-level log"
