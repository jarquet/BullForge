from datetime import datetime, timezone, timedelta
import os
import tempfile
import numpy as np
import pandas as pd
import pytest

from shared.config import Settings
from shared.db import Database
from shared.models import Bar, FeatureRow, ModelMetadata, Prediction
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
    # Volatility large enough that target/stop (≈0.8% of close at 2x ATR)
    # gets hit on a meaningful fraction of bars within max_hold_bars=30,
    # so the labeled set spans both classes.
    closes = 100 + np.cumsum(rng.normal(0, 0.5, n_bars))

    bars = []
    feats = []
    for i in range(n_bars):
        ts = base + timedelta(minutes=i)
        c = float(closes[i])
        bars.append(Bar(
            symbol=SYM, timestamp=ts, open=c, high=c * 1.005,
            low=c * 0.995, close=c, volume=1000, timeframe="1m",
        ))
        feats.append(FeatureRow(
            symbol=SYM, timestamp=ts, timeframe="1m",
            features={
                "sma_20": c, "atr_14": c * 0.005,
                "rsi_14": 50.0, "realized_vol": 0.02,
                "dist_sma_50": 0.0, "dist_sma_200": 0.0,
                "dist_sma_200d": 0.0,
            },
        ))
    await db.insert_bars(bars)
    await db.insert_feature_rows(feats)

    # Seed one base-model registry row so query_all_active_models returns it.
    md = ModelMetadata(
        symbol=SYM, timeframe="1m", horizon_minutes=5,
        model_type="gradient_boost",
        artifact_path="/tmp/x_base.joblib",
        metrics={}, feature_columns=["sma_20"],
        trained_at=datetime.now(timezone.utc),
        train_samples=100, test_samples=20, is_active=True,
    )
    md_id = await db.insert_model_metadata(md)

    # Seed predictions on every 5th bar — enough for training but cheap.
    preds = []
    for i in range(0, n_bars, 5):
        ts = base + timedelta(minutes=i)
        preds.append(Prediction(
            symbol=SYM, timestamp=ts, timeframe="1m", horizon_minutes=5,
            model_id=md_id,
            predicted_direction=int(rng.choice([-1, 1])),
            confidence=0.7,
        ))
    await db.insert_predictions(preds)


@pytest.mark.asyncio
async def test_meta_entry_train_persists_artifact_and_registry(db):
    await _seed(db, n_bars=600)
    with tempfile.TemporaryDirectory() as td:
        store = ModelStore(td)

        def rb_factory():
            return RegimeBuilder(
                spy_bars=pd.DataFrame(columns=["close"]),
                max_staleness_hours=48,
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
        assert meta is not None, "Trainer returned None — likely insufficient labeled rows"
        assert meta.model_type == "meta_entry"
        assert meta.estimator_type == "lightgbm"
        assert os.path.exists(meta.artifact_path), \
            f"artifact file not on disk: {meta.artifact_path}"

        async with db.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT model_type, estimator_type, is_active
                   FROM model_registry WHERE id = $1""", meta.id,
            )
        assert row["model_type"] == "meta_entry"
        assert row["estimator_type"] == "lightgbm"
        assert row["is_active"] is True
