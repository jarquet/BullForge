from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
import numpy as np
import pandas as pd
import pytest

from shared.models import MetaFeatureRow
from services.model_service.meta.trainer import MetaTrainer


def _ts(min_offset=0):
    return datetime(2026, 4, 17, 14, 0, tzinfo=timezone.utc) + timedelta(minutes=min_offset)


def _replay_rows(n=600):
    rng = np.random.RandomState(0)
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
            entry_label=int(rng.choice([0, 1])),
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
    assert meta.estimator_type == "lightgbm"
    db.insert_meta_features_batch.assert_called_once()
    # The trainer must call insert with on_conflict="skip" so replay rows
    # don't overwrite live rows that may share keys.
    kw = db.insert_meta_features_batch.call_args.kwargs
    assert kw.get("on_conflict") == "skip"


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


@pytest.mark.asyncio
async def test_train_exec_uses_replay_dataset():
    db = _mock_db_for_trainer()
    store = _mock_store()
    store.save.return_value = "/models/meta_exec_AAPL_long_lightgbm.joblib"
    rows = _replay_rows()
    with patch("services.model_service.meta.trainer.build_replay_rows",
               new=AsyncMock(return_value=rows)):
        trainer = MetaTrainer(
            db=db, model_store=store,
            sub_model_pairs={"long": [[2, 1], [3, 1.5], [4, 2]],
                             "short": [[2, 1], [3, 1.5], [4, 2]]},
        )
        meta = await trainer.train_exec(
            symbol="AAPL", direction=1,
            estimator_type="lightgbm", as_of=_ts(0),
            train_window_days=60, max_hold_minutes=30,
            min_train_rows=100,
        )
    assert meta is not None
    assert meta.model_type == "meta_exec"


@pytest.mark.asyncio
async def test_artifact_filename_includes_direction_and_estimator():
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
        await trainer.train_entry(
            symbol="AAPL", direction=-1,
            estimator_type="lightgbm", as_of=_ts(0),
            train_window_days=60, max_hold_minutes=30,
            min_train_rows=100,
        )
    # store.save was called with artifact-name suffix containing direction + estimator
    saved_name = store.save.call_args.args[-1]  # last positional arg of `save`
    assert "short" in saved_name
    assert "lightgbm" in saved_name
    assert "meta_entry" in saved_name


def test_evaluate_entry_clamps_nan_to_safe_defaults():
    """When y_test is single-class, sklearn's roc_auc_score raises ValueError
    or returns NaN. Either way, the trainer must emit JSON-safe metrics
    (Postgres JSONB rejects NaN), clamping to chance/zero defaults."""
    trainer = MetaTrainer(
        db=_mock_db_for_trainer(), model_store=_mock_store(),
        sub_model_pairs={"long": [[2, 1]]},
    )
    fake_model = MagicMock()
    fake_model.predict_proba = MagicMock(
        return_value=np.array([[0.3, 0.7], [0.4, 0.6], [0.2, 0.8]])
    )
    X = pd.DataFrame({"f": [0.1, 0.2, 0.3]})
    y_single_class = pd.Series([1, 1, 1])

    metrics = trainer._evaluate_entry(fake_model, X, y_single_class)

    assert metrics["auc"] == 0.5
    for k, v in metrics.items():
        assert not np.isnan(v), f"metric {k} is NaN: {metrics}"


def test_evaluate_exec_clamps_nan_to_safe_defaults():
    """Single-class or empty exec test sets must not produce NaN metrics."""
    trainer = MetaTrainer(
        db=_mock_db_for_trainer(), model_store=_mock_store(),
        sub_model_pairs={"long": [[2, 1]]},
    )
    fake_model = MagicMock()
    fake_model.predict = MagicMock(
        return_value=(np.array([]), np.array([], dtype=int))
    )
    X = pd.DataFrame({"f": []})
    y_kelly = pd.Series([], dtype=float)
    y_pair = pd.Series([], dtype=int)

    metrics = trainer._evaluate_exec(fake_model, X, y_kelly, y_pair)

    for k, v in metrics.items():
        assert not np.isnan(v), f"metric {k} is NaN: {metrics}"


def test_model_store_saves_and_loads_calibrated_entry(tmp_path):
    from services.model_service.meta.calibrator import CalibratedEntryEstimator
    from services.model_service.meta.estimators.lightgbm import LightGBMEntryEstimator
    from services.model_service.model_store import ModelStore
    X, y = pd.DataFrame({"f1": np.random.rand(100)}), pd.Series(np.random.choice([0, 1], 100))
    base = LightGBMEntryEstimator()
    cal = CalibratedEntryEstimator(base)
    cal.fit(X, y, X_cal=X[:20], y_cal=y[:20])
    store = ModelStore(str(tmp_path))
    path = store.save(cal, "AAPL", "1m", 0, "meta_entry_lightgbm")
    loaded = store.load(path)
    proba = loaded.predict_proba(X[:5])
    assert proba.shape == (5, 2)
