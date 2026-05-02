import logging
import math
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
import numpy as np
import pandas as pd
import pytest

from services.model_service.inference import InferenceEngine, _parse_meta_artifact
from shared.models import ModelMetadata


def test_parse_meta_artifact_simple_estimator():
    parsed = _parse_meta_artifact(
        "/m/meta_entry_long_lightgbm_AAPL_1m_0m_20260430T140000Z.joblib"
    )
    assert parsed == ("entry", "long", "lightgbm")


def test_parse_meta_artifact_estimator_with_underscore_and_digits():
    """Tightening of the old [a-z]+ regex: estimator names like xgboost_v2
    must parse as a single estimator, not be truncated to `xgboost`."""
    parsed = _parse_meta_artifact(
        "/m/meta_exec_short_xgboost_v2_BTC-USD_1m_0m_20260430T140000Z.joblib"
    )
    assert parsed == ("exec", "short", "xgboost_v2")


def test_parse_meta_artifact_estimator_with_trailing_digit_segment():
    """catboost_3 has a digit segment in the middle of the model_type — the
    parser stops at the symbol boundary (uppercase) so `_3` stays in the
    estimator portion."""
    parsed = _parse_meta_artifact(
        "/m/meta_entry_long_catboost_3_AAPL_1m_0m_20260430T140000Z.joblib"
    )
    assert parsed == ("entry", "long", "catboost_3")


def test_parse_meta_artifact_test_fixture_format():
    """Test fixtures use a simpler `meta_entry_long_lightgbm_<id>.joblib`
    format with no symbol — the trailing pure-digit segment is stripped."""
    parsed = _parse_meta_artifact("/m/meta_entry_long_lightgbm_11.joblib")
    assert parsed == ("entry", "long", "lightgbm")


def test_parse_meta_artifact_rejects_non_meta_paths():
    assert _parse_meta_artifact("") is None
    assert _parse_meta_artifact("/m/lstm_AAPL_1m_5m.joblib") is None
    assert _parse_meta_artifact("/m/meta_entry_garbage_lightgbm_AAPL_1m.joblib") is None


def test_parse_meta_artifact_handles_windows_path_separators():
    parsed = _parse_meta_artifact(
        "C:\\models\\meta_entry_long_lightgbm_AAPL_1m_0m_20260430T140000Z.joblib"
    )
    assert parsed == ("entry", "long", "lightgbm")


def _md(mid, model_type, estimator_type=None, dir_label=None):
    suffix = f"{model_type}"
    if dir_label:
        suffix = f"{model_type}_{dir_label}_{estimator_type}"
    elif estimator_type:
        suffix = f"{model_type}_{estimator_type}"
    return ModelMetadata(
        id=mid, symbol="AAPL", timeframe="1m",
        horizon_minutes=0 if model_type.startswith("meta") else 5,
        model_type=model_type,
        artifact_path=f"/m/{suffix}_{mid}.joblib",
        metrics={}, feature_columns=["x"], trained_at=datetime.now(timezone.utc),
        train_samples=100, test_samples=20, is_active=True,
        estimator_type=estimator_type,
    )


@pytest.mark.asyncio
async def test_meta_state_keys_rolling_stats_by_slot_not_model_id():
    """Regression: _MetaState must aggregate prediction outcomes per
    (symbol, timeframe, horizon, model_type) slot, not per model_id.
    Otherwise every base retrain mints a new model_id whose outcome query
    returns empty, and the meta features see a sudden drop to 0.0
    accuracy on the first inference after retrain."""
    from services.model_service.inference import _MetaState

    db = AsyncMock()
    db.query_prediction_outcomes_for_slot.return_value = [
        {"is_correct": True, "predicted_direction": 1, "confidence": 0.7}
    ] * 8 + [
        {"is_correct": False, "predicted_direction": 1, "confidence": 0.7}
    ] * 2  # 8/10 correct → 0.8 accuracy

    ms = _MetaState(pair_grid=[[2, 1]], ttl_s=600.0)
    slot = ("AAPL", "1m", 5, "lstm")
    stats = await ms.stats_for(db, slot, now_s=1000.0)
    assert stats["accuracy"] == pytest.approx(0.8)
    db.query_prediction_outcomes_for_slot.assert_awaited_once()
    kwargs = db.query_prediction_outcomes_for_slot.await_args.kwargs
    assert kwargs.get("symbol") == "AAPL"
    assert kwargs.get("timeframe") == "1m"
    assert kwargs.get("horizon_minutes") == 5
    assert kwargs.get("model_type") == "lstm"

    # Cached: second call within TTL must not hit DB again.
    db.query_prediction_outcomes_for_slot.reset_mock()
    stats2 = await ms.stats_for(db, slot, now_s=1000.5)
    assert stats2 == stats
    db.query_prediction_outcomes_for_slot.assert_not_awaited()


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
    assert out == ([], {})


@pytest.mark.asyncio
async def test_predict_meta_returns_signal_per_loaded_estimator():
    db = AsyncMock()
    db.query_all_active_models.return_value = [
        _md(11, "meta_entry", "lightgbm", dir_label="long"),
        _md(12, "meta_exec", "lightgbm", dir_label="long"),
    ]
    db.query_prediction_outcomes_for_slot.return_value = []

    fake_entry = MagicMock()
    fake_entry.predict_proba = MagicMock(return_value=np.array([[0.4, 0.6]]))
    fake_exec = MagicMock()
    fake_exec.predict = MagicMock(
        return_value=(np.array([0.07]), np.array([1])))

    def _load(path):
        return fake_entry if "11" in path else fake_exec
    store = MagicMock(); store.load.side_effect = _load

    eng = InferenceEngine(db=db, model_store=store)
    await eng.load_meta_models()

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

    signals, feature_vec = out
    assert len(signals) == 1
    sig = signals[0]
    assert sig["estimator_type"] == "lightgbm"
    assert sig["direction"] == 1
    assert 0.0 <= sig["entry_probability"] <= 1.0
    assert 0.0 <= sig["kelly_fraction"] <= 1.0
    assert sig["pair_index"] == 1
    assert sig["target_pct"] > 0
    # The assembled L2 vector is surfaced alongside the signals so the
    # publisher can forward it on model:meta_signals. JSON-friendly
    # str→float dict (no numpy scalars).
    assert feature_vec
    for k, v in feature_vec.items():
        assert isinstance(k, str)
        assert isinstance(v, float)


@pytest.mark.asyncio
async def test_predict_meta_drops_non_finite_features(caplog):
    """Important review finding: NaN/Inf must not survive into the
    published feature_vector. The whole point of Task 3 was to stop
    putting garbage in meta_features.feature_vector — non-finite floats
    are the same class of garbage. ``json.dumps`` would emit them as the
    non-standard ``NaN``/``Infinity`` tokens and Postgres ``JSONB``
    rejects those, either crashing the writer or corrupting the row.

    We expect predict_meta to filter every non-finite key out of
    feature_vec and log a single warning naming the dropped keys (with
    the symbol, timestamp, direction context); finite values must be
    preserved unchanged.
    """
    db = AsyncMock()
    db.query_all_active_models.return_value = [
        _md(11, "meta_entry", "lightgbm", dir_label="long"),
        _md(12, "meta_exec", "lightgbm", dir_label="long"),
    ]
    db.query_prediction_outcomes_for_slot.return_value = []

    fake_entry = MagicMock()
    fake_entry.predict_proba = MagicMock(return_value=np.array([[0.4, 0.6]]))
    fake_exec = MagicMock()
    fake_exec.predict = MagicMock(
        return_value=(np.array([0.07]), np.array([1])))

    def _load(path):
        return fake_entry if "11" in path else fake_exec
    store = MagicMock(); store.load.side_effect = _load

    eng = InferenceEngine(db=db, model_store=store)
    await eng.load_meta_models()

    # Inject NaN via regime (RegimeBuilder zeroes after staleness, but
    # other paths can plausibly NaN out under degenerate inputs) and Inf
    # via symbol_state (atr_price_ratio is finite/positive — required
    # for predict_meta to proceed at all — but realized_vol can blow up
    # on a near-flat denominator).
    caplog.set_level(logging.WARNING, logger="services.model_service.inference")
    out = await eng.predict_meta(
        symbol="AAPL", timeframe="1m",
        base_predictions=[{"model_id": 1, "direction": 1, "confidence": 0.7}],
        regime={"spy_trend_1m": float("nan"), "spy_trend_1h": 0.0,
                "spy_trend_1d": 0.0, "spy_momentum": 0.0, "vol_regime": 1.0},
        symbol_state={"realized_vol": float("inf"),
                       "atr_price_ratio": 0.005,
                       "dist_sma_50": float("-inf"),
                       "dist_sma_200bar": 0.0,
                       "dist_sma_200day": 0.0},
        timestamp=datetime.now(timezone.utc),
    )

    signals, feature_vec = out
    # The signal still flows — one bad feature must not silence the
    # entire shadow signal.
    assert len(signals) == 1
    # Every surviving value is finite; no NaN/Inf leaks into the
    # JSON-bound payload.
    assert feature_vec
    for k, v in feature_vec.items():
        assert isinstance(k, str)
        assert isinstance(v, float)
        assert math.isfinite(v), f"non-finite value survived for key {k}: {v!r}"

    # The three injected non-finite inputs map to known assembler keys.
    expected_dropped = {
        "regime_spy_trend_1m", "sym_realized_vol", "sym_dist_sma_50",
    }
    for dropped in expected_dropped:
        assert dropped not in feature_vec

    # Single warning per inference call, naming the dropped keys.
    drop_warnings = [
        r for r in caplog.records
        if r.levelno == logging.WARNING
        and "non-finite" in r.getMessage().lower()
    ]
    assert len(drop_warnings) == 1, drop_warnings
    msg = drop_warnings[0].getMessage()
    for dropped in expected_dropped:
        assert dropped in msg
    # Context: symbol + direction(s) appear in the log.
    assert "AAPL" in msg


@pytest.mark.asyncio
async def test_predict_meta_no_warning_when_all_features_finite(caplog):
    """Companion to the drop test: if every value is finite, predict_meta
    must not emit the non-finite-features warning."""
    db = AsyncMock()
    db.query_all_active_models.return_value = [
        _md(11, "meta_entry", "lightgbm", dir_label="long"),
        _md(12, "meta_exec", "lightgbm", dir_label="long"),
    ]
    db.query_prediction_outcomes_for_slot.return_value = []

    fake_entry = MagicMock()
    fake_entry.predict_proba = MagicMock(return_value=np.array([[0.4, 0.6]]))
    fake_exec = MagicMock()
    fake_exec.predict = MagicMock(
        return_value=(np.array([0.07]), np.array([1])))

    def _load(path):
        return fake_entry if "11" in path else fake_exec
    store = MagicMock(); store.load.side_effect = _load

    eng = InferenceEngine(db=db, model_store=store)
    await eng.load_meta_models()

    caplog.set_level(logging.WARNING, logger="services.model_service.inference")
    await eng.predict_meta(
        symbol="AAPL", timeframe="1m",
        base_predictions=[{"model_id": 1, "direction": 1, "confidence": 0.7}],
        regime={"spy_trend_1m": 0.0, "spy_trend_1h": 0.0, "spy_trend_1d": 0.0,
                "spy_momentum": 0.0, "vol_regime": 1.0},
        symbol_state={"realized_vol": 0.02, "atr_price_ratio": 0.005,
                       "dist_sma_50": 0.0, "dist_sma_200bar": 0.0,
                       "dist_sma_200day": 0.0},
        timestamp=datetime.now(timezone.utc),
    )
    drop_warnings = [
        r for r in caplog.records
        if r.levelno == logging.WARNING
        and "non-finite" in r.getMessage().lower()
    ]
    assert drop_warnings == []


@pytest.mark.asyncio
async def test_predict_meta_skips_when_atr_zero():
    db = AsyncMock()
    db.query_all_active_models.return_value = [
        _md(11, "meta_entry", "lightgbm", dir_label="long"),
        _md(12, "meta_exec", "lightgbm", dir_label="long"),
    ]
    db.query_prediction_outcomes_for_slot.return_value = []

    store = MagicMock(); store.load.return_value = MagicMock()

    eng = InferenceEngine(db=db, model_store=store)
    await eng.load_meta_models()
    out = await eng.predict_meta(
        symbol="AAPL", timeframe="1m",
        base_predictions=[{"model_id": 1, "direction": 1, "confidence": 0.7}],
        regime={"spy_trend_1m": 0.0, "spy_trend_1h": 0.0, "spy_trend_1d": 0.0,
                "spy_momentum": 0.0, "vol_regime": 1.0},
        symbol_state={"realized_vol": 0.02, "atr_price_ratio": 0.0,  # zero ATR
                       "dist_sma_50": 0.0, "dist_sma_200bar": 0.0,
                       "dist_sma_200day": 0.0},
        timestamp=datetime.now(timezone.utc),
    )
    assert out == ([], {})
