from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import Optional
import numpy as np
import pandas as pd
from shared.db import Database
from shared.models import ModelMetadata
from services.model_service.model_store import ModelStore
from services.model_service.meta.factory import create_meta_entry, create_meta_exec, get_cold_start_min_rows
from services.model_service.meta.calibrator import CalibratedEntryEstimator
from services.model_service.meta.feature_assembler import MetaFeatureAssembler
from services.model_service.meta.replay import build_replay_rows
from services.model_service.meta.regime import RegimeBuilder

logger = logging.getLogger(__name__)
_DIR_LABEL = {1: "long", -1: "short"}

class MetaTrainer:
    def __init__(self, db: Database, model_store: ModelStore,
                 sub_model_pairs: dict[str, list[tuple[float, float]]],
                 live_weight_multiplier: float = 3.0):
        self._db = db
        self._store = model_store
        self._pairs = sub_model_pairs
        self._live_weight = live_weight_multiplier

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

        n = len(X)
        split_train, split_cal = int(n * 0.6), int(n * 0.8)
        X_train, y_train = X.iloc[:split_train], y.iloc[:split_train]
        w_train = weights[:split_train] if weights is not None else None
        X_cal, y_cal = X.iloc[split_train:split_cal], y.iloc[split_train:split_cal]
        X_test, y_test = X.iloc[split_cal:], y.iloc[split_cal:]

        input_dim = X.shape[1]
        base_estimator = create_meta_entry(estimator_type, input_dim=input_dim) \
            if estimator_type == "mlp" else create_meta_entry(estimator_type)
        calibrated = CalibratedEntryEstimator(base_estimator)
        calibrated.fit(X_train, y_train, sample_weight=w_train, X_cal=X_cal, y_cal=y_cal)

        metrics = self._evaluate_entry(calibrated, X_test, y_test)
        logger.info("meta_entry %s %s %s — auc=%.3f cal_err=%.4f",
                     symbol, dir_label, estimator_type,
                     metrics.get("auc", 0), metrics.get("calibration_error", 0))

        artifact_path = self._store.save(
            calibrated, symbol, "1m", 0,
            f"meta_entry_{dir_label}_{estimator_type}",
        )
        trained_at = datetime.now(timezone.utc)
        meta = ModelMetadata(symbol=symbol, timeframe="1m", horizon_minutes=0,
                             model_type="meta_entry", artifact_path=artifact_path,
                             metrics=metrics, feature_columns=list(X.columns),
                             trained_at=trained_at, train_samples=len(X_train),
                             test_samples=len(X_test), is_active=True,
                             estimator_type=estimator_type)
        row_id = await self._db.insert_model_metadata(meta)
        return ModelMetadata(id=row_id, symbol=meta.symbol, timeframe=meta.timeframe,
                             horizon_minutes=meta.horizon_minutes, model_type=meta.model_type,
                             artifact_path=meta.artifact_path, metrics=meta.metrics,
                             feature_columns=meta.feature_columns, trained_at=meta.trained_at,
                             train_samples=meta.train_samples, test_samples=meta.test_samples,
                             is_active=meta.is_active, estimator_type=meta.estimator_type)

    async def train_exec(self, symbol: str, direction: int, estimator_type: str,
                         as_of: datetime, *, train_window_days: int,
                         max_hold_minutes: int, min_train_rows: int,
                         regime_builder_factory=None,
                         ) -> Optional[ModelMetadata]:
        dir_label = _DIR_LABEL[direction]
        logger.info("Training meta_exec %s %s %s...", symbol, dir_label, estimator_type)
        X, y_kelly, y_pair, weights = await self._build_exec_dataset(
            symbol, direction, as_of,
            train_window_days=train_window_days,
            max_hold_minutes=max_hold_minutes,
            regime_builder_factory=regime_builder_factory,
        )
        if X is None or len(X) < min_train_rows:
            logger.warning(
                "Insufficient data for meta_exec %s %s: %d < %d",
                symbol, dir_label,
                0 if X is None else len(X), min_train_rows,
            )
            return None

        n = len(X)
        split = int(n * 0.8)
        X_train, X_test = X.iloc[:split], X.iloc[split:]
        y_kelly_train, y_kelly_test = y_kelly.iloc[:split], y_kelly.iloc[split:]
        y_pair_train, y_pair_test = y_pair.iloc[:split], y_pair.iloc[split:]
        w_train = weights[:split] if weights is not None else None

        num_pairs = len(self._pairs.get(dir_label, []))
        estimator = create_meta_exec(estimator_type, num_pairs=num_pairs, input_dim=X.shape[1]) \
            if estimator_type == "mlp" else create_meta_exec(estimator_type, num_pairs=num_pairs)
        estimator.fit(X_train, y_kelly_train, y_pair_train, sample_weight=w_train)

        metrics = self._evaluate_exec(estimator, X_test, y_kelly_test, y_pair_test)
        logger.info("meta_exec %s %s %s — kelly_mae=%.4f pair_acc=%.3f",
                     symbol, dir_label, estimator_type,
                     metrics.get("kelly_mae", 0), metrics.get("pair_accuracy", 0))

        artifact_path = self._store.save(
            estimator, symbol, "1m", 0,
            f"meta_exec_{dir_label}_{estimator_type}",
        )
        trained_at = datetime.now(timezone.utc)
        meta = ModelMetadata(symbol=symbol, timeframe="1m", horizon_minutes=0,
                             model_type="meta_exec", artifact_path=artifact_path,
                             metrics=metrics, feature_columns=list(X.columns),
                             trained_at=trained_at, train_samples=len(X_train),
                             test_samples=len(X_test), is_active=True,
                             estimator_type=estimator_type)
        row_id = await self._db.insert_model_metadata(meta)
        return ModelMetadata(id=row_id, symbol=meta.symbol, timeframe=meta.timeframe,
                             horizon_minutes=meta.horizon_minutes, model_type=meta.model_type,
                             artifact_path=meta.artifact_path, metrics=meta.metrics,
                             feature_columns=meta.feature_columns, trained_at=meta.trained_at,
                             train_samples=meta.train_samples, test_samples=meta.test_samples,
                             is_active=meta.is_active, estimator_type=meta.estimator_type)

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

    def _default_rb_factory(self):
        return RegimeBuilder(
            spy_bars=pd.DataFrame(columns=["close"]),
            max_staleness_hours=48,
        )

    async def _build_exec_dataset(self, symbol, direction, as_of, *,
                                   train_window_days, max_hold_minutes,
                                   regime_builder_factory):
        pair_grid = self._pairs.get(_DIR_LABEL[direction], [])
        if not pair_grid:
            return None, None, None, None
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
        labeled = [r for r in rows
                   if r.entry_label is not None
                   and r.optimal_pair_idx is not None]
        if not labeled:
            return None, None, None, None

        X = pd.DataFrame([{k: v for k, v in r.feature_vector.items()
                           if not k.startswith("_")} for r in labeled])
        y_kelly = pd.Series(
            [float(r.feature_vector.get("_kelly_label", 0.0)) for r in labeled]
        )
        y_pair = pd.Series([int(r.optimal_pair_idx) for r in labeled])
        weights = np.array(
            [self._live_weight if r.source == "live" else 1.0 for r in labeled])
        return X, y_kelly, y_pair, weights

    def _evaluate_entry(self, model, X_test, y_test) -> dict[str, float]:
        from sklearn.metrics import roc_auc_score, accuracy_score
        proba = model.predict_proba(X_test)[:, 1]
        y_pred = (proba >= 0.5).astype(int)
        try:
            auc = float(roc_auc_score(y_test, proba))
        except ValueError:
            auc = 0.5
        bins = np.linspace(0, 1, 11)
        cal_err = total = 0.0
        for lo, hi in zip(bins[:-1], bins[1:]):
            mask = (proba >= lo) & (proba < hi)
            if mask.sum() == 0: continue
            cal_err += mask.sum() * abs(proba[mask].mean() - y_test.values[mask].mean())
            total += mask.sum()
        return {"auc": auc, "accuracy": float(accuracy_score(y_test, y_pred)),
                "calibration_error": float(cal_err / max(total, 1))}

    def _evaluate_exec(self, model, X_test, y_kelly_test, y_pair_test) -> dict[str, float]:
        from sklearn.metrics import mean_absolute_error, accuracy_score
        kelly_pred, pair_pred = model.predict(X_test)
        return {"kelly_mae": float(mean_absolute_error(y_kelly_test, kelly_pred)),
                "pair_accuracy": float(accuracy_score(y_pair_test, pair_pred))}
