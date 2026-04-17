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
                          as_of: datetime) -> Optional[ModelMetadata]:
        dir_label = _DIR_LABEL[direction]
        logger.info("Training meta_entry %s %s %s...", symbol, dir_label, estimator_type)
        X, y, weights = await self._build_entry_dataset(symbol, direction, as_of)
        if X is None or len(X) < max(100, get_cold_start_min_rows(estimator_type) // 10):
            logger.warning("Insufficient data for meta_entry %s %s: %d rows",
                           symbol, dir_label, 0 if X is None else len(X))
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
                     symbol, dir_label, estimator_type, metrics.get("auc", 0), metrics.get("calibration_error", 0))

        artifact_path = self._store.save(calibrated, symbol, "1m", 0, f"meta_entry_{estimator_type}")
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
                         as_of: datetime) -> Optional[ModelMetadata]:
        dir_label = _DIR_LABEL[direction]
        logger.info("Training meta_exec %s %s %s...", symbol, dir_label, estimator_type)
        X, y_kelly, y_pair, weights = await self._build_exec_dataset(symbol, direction, as_of)
        if X is None or len(X) < max(100, get_cold_start_min_rows(estimator_type) // 10):
            logger.warning("Insufficient data for meta_exec %s %s: %d rows",
                           symbol, dir_label, 0 if X is None else len(X))
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
                     symbol, dir_label, estimator_type, metrics.get("kelly_mae", 0), metrics.get("pair_accuracy", 0))

        artifact_path = self._store.save(estimator, symbol, "1m", 0, f"meta_exec_{estimator_type}")
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

    async def _build_entry_dataset(self, symbol: str, direction: int, as_of: datetime):
        logger.warning(
            "MetaTrainer._build_entry_dataset is using PLACEHOLDER random base-model "
            "predictions — NOT suitable for production training. Real base-model replay "
            "will be integrated in Plan B."
        )
        rng = np.random.RandomState(42)
        active_models = await self._db.query_all_active_models()
        symbol_models = [m for m in active_models
                         if m.symbol == symbol and m.model_type not in ("meta_entry", "meta_exec")]
        if not symbol_models:
            return None, None, None

        model_ids = [m.id for m in symbol_models]
        assembler = MetaFeatureAssembler(model_ids=model_ids)

        feature_rows = await self._db.query_feature_rows(symbol, "1m", limit=50000)
        if not feature_rows:
            return None, None, None
        feature_rows = list(reversed(feature_rows))

        labels = await self._db.query_labels(symbol, "1m", horizon_minutes=5)
        label_by_ts = {lb.timestamp: lb for lb in labels}

        rolling_stats = {}
        for m in symbol_models:
            outcomes = await self._db.query_prediction_outcomes(m.id, limit=200)
            if outcomes:
                correct = sum(1 for o in outcomes if o["is_correct"])
                acc = correct / len(outcomes)
                avg_conf = np.mean([o["confidence"] for o in outcomes])
                cal_err = abs(avg_conf - acc)
            else:
                acc, cal_err = 0.0, 0.0
            rolling_stats[m.id] = {"accuracy": acc, "calibration_error": cal_err, "is_disabled": False}

        regime = {"spy_trend_1m": 0.0, "spy_trend_1h": 0.0, "spy_trend_1d": 0.0,
                  "spy_momentum": 0.0, "vol_regime": 1.0}

        rows_X, rows_y, sources = [], [], []
        for fr in feature_rows:
            if fr.timestamp > as_of:
                continue
            label = label_by_ts.get(fr.timestamp)
            if label is None:
                continue
            if direction == 1:
                y_val = 1 if label.forward_return > 0 else 0
            else:
                y_val = 1 if label.forward_return < 0 else 0

            predictions = [{"model_id": m.id, "direction": rng.choice([-1, 0, 1]),
                            "confidence": rng.uniform(0.5, 0.9)} for m in symbol_models]
            symbol_state = {
                "realized_vol": fr.features.get("realized_vol", 0.02),
                "atr_price_ratio": fr.features.get("atr_14", 2.0) / max(fr.features.get("sma_20", 150.0), 1.0),
                "dist_sma_50": fr.features.get("dist_sma_50", 0.0),
                "dist_sma_200bar": fr.features.get("dist_sma_200", 0.0),
                "dist_sma_200day": fr.features.get("dist_sma_200d", 0.0),
            }
            vec = assembler.assemble(predictions=predictions, rolling_stats=rolling_stats,
                                     regime=regime, symbol_state=symbol_state, timestamp=fr.timestamp)
            rows_X.append(vec)
            rows_y.append(y_val)
            sources.append("replay")

        if not rows_X:
            return None, None, None
        X = pd.DataFrame(rows_X)
        y = pd.Series(rows_y)
        weights = np.array([self._live_weight if s == "live" else 1.0 for s in sources])
        return X, y, weights

    async def _build_exec_dataset(self, symbol: str, direction: int, as_of: datetime):
        X, y_entry, weights = await self._build_entry_dataset(symbol, direction, as_of)
        if X is None:
            return None, None, None, None
        dir_label = _DIR_LABEL[direction]
        pairs = self._pairs.get(dir_label, [])
        num_pairs = len(pairs)
        rng = np.random.RandomState(42)
        y_kelly = pd.Series(np.clip((2 * y_entry.values.astype(float) - 1) * rng.uniform(0.1, 0.4, len(y_entry)), 0.0, 1.0))
        y_pair = pd.Series(rng.randint(0, max(num_pairs, 1), len(y_entry)))
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
