from __future__ import annotations

import logging
import math
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd

from shared.db import Database
from shared.models import BacktestScope, ModelMetadata, Prediction
from shared.redis import RedisClient
from services.model_service.meta.feature_assembler import MetaFeatureAssembler
from services.model_service.model_store import ModelStore

logger = logging.getLogger(__name__)


_DIR_TO_INT = {"long": 1, "short": -1}

# Sliding-window counter prefixes for the dashboard "Meta shadow health" tile.
# One bucket per minute (UTC epoch minute = int(time.time() // 60)), each with
# a 30-minute TTL. The dashboard sums the most recent 15 buckets — see
# services/dashboard_service/models_api.py `_handle_meta_shadow_health`.
#
# We use epoch-minutes (int) rather than a strftime("%Y%m%d%H%M") string for
# two reasons: cheaper key arithmetic in the dashboard's window math, and
# monotonic ordering without timezone parsing.
META_INFERENCES_KEY_PREFIX = "bullforge:metrics:meta_inferences"
META_PUBLISHER_FINITE_DROPS_KEY_PREFIX = "bullforge:metrics:meta_publisher_finite_drops"
_BUCKET_TTL_SECONDS = 1800  # 30 minutes


def _parse_meta_artifact(path: str) -> Optional[Tuple[str, str, str]]:
    """Extract (kind, direction_label, estimator_type) from a meta artifact path.

    Production paths follow the ModelStore format
    ``{model_type}_{symbol}_{timeframe}_{horizon}m_{timestamp}.{ext}`` where
    ``model_type`` is ``meta_{entry|exec}_{long|short}_{estimator}``. The
    estimator boundary is detected as the first segment that starts with an
    uppercase letter (the symbol). For non-production paths that have no
    uppercase boundary (e.g., test fixtures ending in ``_<id>.{ext}``) a
    trailing pure-digit segment is treated as the boundary instead.

    This replaces the older ``meta_(entry|exec)_(long|short)_([a-z]+)`` regex
    so estimator names containing digits or underscores (``xgboost_v2``,
    ``catboost_3``) parse correctly.
    """
    if not path:
        return None
    name = path.replace("\\", "/").rsplit("/", 1)[-1]
    name = name.rsplit(".", 1)[0]
    parts = name.split("_")
    if len(parts) < 4 or parts[0] != "meta":
        return None
    kind, dir_label = parts[1], parts[2]
    if kind not in ("entry", "exec") or dir_label not in ("long", "short"):
        return None
    for i in range(3, len(parts)):
        if parts[i] and parts[i][0].isupper():
            est = "_".join(parts[3:i])
            return (kind, dir_label, est) if est else None
    end = len(parts)
    if parts[-1].isdigit():
        end -= 1
    est = "_".join(parts[3:end])
    return (kind, dir_label, est) if est else None


_SlotKey = tuple[str, str, int, str]
# (symbol, timeframe, horizon_minutes, model_type)


class _MetaState:
    """Rolling-stats cache + pair grid for meta inference.

    Caches outcomes per logical slot (symbol, timeframe, horizon,
    model_type) rather than per model_id. Without slot keying, every
    base retrain mints a fresh model_id whose query_prediction_outcomes
    returns zero rows on first inference — meta features would see a
    sudden drop to 0.0 accuracy / 0.0 calibration_error every retrain
    night until enough live predictions accumulated under the new id.
    """
    def __init__(self, pair_grid: list[list[float]], ttl_s: float):
        self.pair_grid = pair_grid
        self.ttl_s = ttl_s
        self._cache: dict[_SlotKey, tuple[float, dict]] = {}

    async def stats_for(self, db, slot: _SlotKey, now_s: float) -> dict:
        cached = self._cache.get(slot)
        if cached and now_s - cached[0] < self.ttl_s:
            return cached[1]
        symbol, timeframe, horizon, model_type = slot
        outcomes = await db.query_prediction_outcomes_for_slot(
            symbol=symbol, timeframe=timeframe,
            horizon_minutes=horizon, model_type=model_type, limit=200,
        )
        if outcomes:
            correct = sum(1 for o in outcomes if o["is_correct"])
            acc = correct / len(outcomes)
            avg_conf = float(sum(o["confidence"] for o in outcomes)) / len(outcomes)
            cal_err = abs(avg_conf - acc)
        else:
            acc, cal_err = 0.0, 0.0
        v = {"accuracy": acc, "calibration_error": cal_err, "is_disabled": False}
        self._cache[slot] = (now_s, v)
        return v


class InferenceEngine:
    def __init__(
        self,
        db: Database,
        model_store: ModelStore,
        *,
        scope: Optional[BacktestScope] = None,
        redis_client: Optional[RedisClient] = None,
    ):
        self._db = db
        self._model_store = model_store
        self._scope = scope
        # Optional: only the live model_service path passes a redis_client,
        # so meta-shadow counters reflect production traffic. The backtest
        # engine constructs InferenceEngine without one to avoid
        # contaminating live metrics with per-fold inferences.
        self._redis = redis_client
        # Key is (symbol, timeframe, horizon_minutes, model_type). The
        # model_type element is required because multiple model_types can
        # share the same (symbol, timeframe, horizon) — e.g., a freshly
        # retrained gradient_boost AND lstm both active for SPY 1m 5m. An
        # earlier version of this key omitted model_type and silently
        # collapsed both into one slot, dropping half the active models.
        self._models: Dict[
            Tuple[str, str, int, str], Tuple[object, ModelMetadata]
        ] = {}

    async def load_models(self) -> None:
        active_models = await self._db.query_all_active_models(scope=self._scope)
        # Atomic swap: build a fresh dict, assign to self._models only
        # after the loop. Mirrors load_meta_models below. Without this,
        # any exception propagating out of the loop (corrupt pickle,
        # torch.load on bad .pt, etc.) would leave self._models empty —
        # the 2026-05-04 incident.
        new: Dict[
            Tuple[str, str, int, str], Tuple[object, ModelMetadata]
        ] = {}

        for meta in active_models:
            # Meta artifacts (meta_entry / meta_exec) belong to _meta_models,
            # not the base inference dict. Without this skip, predict() would
            # iterate them too and crash with AttributeError because meta
            # estimators expose predict_proba but no predict — symptom was a
            # CalibratedEntryEstimator.predict() lookup blowing up the redis
            # pubsub listener for every 1m feature:updates event.
            if meta.model_type in ("meta_entry", "meta_exec"):
                continue
            try:
                model = self._model_store.load(meta.artifact_path)
            except FileNotFoundError:
                logger.warning(
                    "Artifact not found for model id=%d: %s",
                    meta.id, meta.artifact_path,
                )
                continue
            except Exception:
                # Corrupt / truncated / version-skew artifact. Skipping
                # one is always correct — the alternative (propagating)
                # clears every other model with it, which is the
                # 2026-05-04 incident. logger.exception captures the
                # traceback so operators can identify the bad file.
                logger.exception(
                    "Failed to load artifact for model id=%d (%s); skipping",
                    meta.id, meta.artifact_path,
                )
                continue

            key = (
                meta.symbol, meta.timeframe, meta.horizon_minutes,
                meta.model_type,
            )
            new[key] = (model, meta)
            logger.info(
                "Loaded model: %s %s %dm (%s, id=%d)",
                meta.symbol, meta.timeframe, meta.horizon_minutes,
                meta.model_type, meta.id,
            )

        self._models = new
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

        # Fetch a window large enough for sequence models (LSTM seq_len=30).
        # Gradient-boost ignores history; LSTM needs the window.
        feature_rows = await self._db.query_feature_rows(
            symbol, timeframe, limit=60,
        )
        if not feature_rows:
            logger.debug("No features available for %s %s", symbol, timeframe)
            return []

        # query_feature_rows returns DESC; flip to ascending so the last row is latest.
        feature_rows = list(reversed(feature_rows))
        latest_features = feature_rows[-1]
        predictions = []

        for key, model, meta in matching:
            horizon = key[2]
            rows = [
                {col: fr.features.get(col, 0.0) for col in meta.feature_columns}
                for fr in feature_rows
            ]
            X = pd.DataFrame(rows)

            try:
                direction = int(model.predict(X)[-1])
                proba = model.predict_proba(X)[-1]
            except ValueError as e:
                logger.warning(
                    "Skipping %s %s %dm (id=%d): %s",
                    symbol, timeframe, horizon, meta.id, e,
                )
                continue
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

    async def load_meta_models(self) -> None:
        active = await self._db.query_all_active_models()
        meta = [m for m in active if m.model_type in ("meta_entry", "meta_exec")]
        new: dict[tuple[str, int, str], dict] = {}
        loaded_count = 0
        for m in meta:
            parsed = _parse_meta_artifact(m.artifact_path or "")
            if not parsed:
                continue
            kind, dir_label, est = parsed
            key = (m.symbol, _DIR_TO_INT[dir_label], est)
            slot = new.setdefault(key, {"entry": None, "exec": None})
            try:
                model = self._model_store.load(m.artifact_path)
            except FileNotFoundError:
                continue
            slot[kind] = (model, m)
            loaded_count += 1
        self._meta_models = new
        complete_slots = sum(
            1 for s in new.values() if s["entry"] is not None and s["exec"] is not None
        )
        logger.info(
            "Loaded %d meta artifacts across %d slots (%d complete entry+exec pairs)",
            loaded_count, len(new), complete_slots,
        )

    async def predict_meta(
        self, *, symbol: str, timeframe: str,
        base_predictions: list[dict], regime: dict, symbol_state: dict,
        timestamp,
    ) -> tuple[list[dict], dict[str, float]]:
        """Run meta-model inference for a single (symbol, timeframe) event.

        Returns ``(signals, feature_vector)``: the per-(direction, estimator)
        decisions plus the assembled L2 feature dict that those decisions
        were scored against. The publisher in ``model_service/main.py``
        forwards both onto ``model:meta_signals`` so the shadow-mode writer
        can persist the real vector into ``meta_features.feature_vector``
        verbatim — previously the payload carried ``{}`` and the writer
        substituted a useless ``{"_": 0.0}`` placeholder.
        """
        if not getattr(self, "_meta_models", None):
            return [], {}
        # Bump the inference counter once per real attempt (past the no-meta
        # short-circuit). This is the denominator for the dashboard "Meta
        # shadow health" tile — drops/skips alone are meaningless without
        # knowing how many inferences ran in the same window.
        await self._increment_counter(META_INFERENCES_KEY_PREFIX)
        from time import time as _now
        ms = getattr(self, "_meta_state", None) or _MetaState(
            pair_grid=getattr(self, "_pair_grid", [[2, 1], [3, 1.5], [4, 2]]),
            ttl_s=getattr(self, "_meta_stats_ttl_s", 600.0),
        )
        self._meta_state = ms
        atr_pct = float(symbol_state.get("atr_price_ratio") or 0.0)
        if atr_pct <= 0:
            return [], {}

        # Normalize base predictions: replay/DB rows use "predicted_direction",
        # in-memory pubsub rows use "direction". Accept either.
        norm_preds = [
            {"model_id": p["model_id"],
             "direction": p.get("direction", p.get("predicted_direction", 0)),
             "confidence": p["confidence"]}
            for p in base_predictions
        ]

        model_ids = sorted({p["model_id"] for p in norm_preds})
        # Resolve each base prediction's model_id back to its logical slot
        # (timeframe, horizon, model_type) so rolling stats aggregate across
        # every prior version of the same slot. self._models is loaded by
        # load_models() and keyed by (symbol, timeframe, horizon); we keep
        # an inverse id-keyed map here.
        mid_to_slot: dict[int, _SlotKey] = {
            meta.id: (meta.symbol, meta.timeframe, meta.horizon_minutes, meta.model_type)
            for _model, meta in self._models.values()
        }
        rolling: dict[int, dict] = {}
        now_s = _now()
        for mid in model_ids:
            slot = mid_to_slot.get(mid)
            if slot is None:
                # Prediction came from a model_id that is no longer active.
                # Fall back to zero stats; the assembler will still produce
                # a vector and the meta model can decide what to do.
                rolling[mid] = {"accuracy": 0.0, "calibration_error": 0.0,
                                 "is_disabled": False}
                continue
            rolling[mid] = await ms.stats_for(self._db, slot, now_s)
        assembler = MetaFeatureAssembler(model_ids=model_ids)
        feature_vec = assembler.assemble(
            predictions=norm_preds, rolling_stats=rolling,
            regime=regime, symbol_state=symbol_state, timestamp=timestamp,
        )
        # Coerce every value to a builtin Python float and drop any
        # non-finite (NaN/Inf) entries — the assembler inherits int/numpy
        # types from upstream dicts (e.g., regime builder returns numpy
        # scalars on the SPY frame), and those do not survive json.dumps
        # cleanly. Worse, NaN/Inf are valid Python floats so float() does
        # not catch them, but json.dumps emits the non-standard
        # NaN/Infinity/-Infinity tokens that Postgres JSONB and strict
        # JSON consumers reject — corrupting meta_features.feature_vector
        # or crashing the writer. Drop here so a single degenerate
        # feature can't silence the whole shadow signal; the empty-vector
        # case still falls into the writer's existing warn-and-skip.
        coerced: dict[str, float] = {}
        dropped_keys: list[str] = []
        for k, v in feature_vec.items():
            fv = float(v)
            if math.isfinite(fv):
                coerced[str(k)] = fv
            else:
                dropped_keys.append(str(k))
        if dropped_keys:
            directions = sorted({
                int(d) for (sym, d, _est) in self._meta_models
                if sym == symbol
            })
            logger.warning(
                "predict_meta dropped non-finite feature(s) for %s ts=%s "
                "directions=%s: %s",
                symbol, timestamp, directions, sorted(dropped_keys),
            )
            # One increment per call-with-drops — pairs with the per-call
            # warn log. Counting per-key would skew the rate denominator,
            # since a single bad regime input can poison many derived keys.
            await self._increment_counter(META_PUBLISHER_FINITE_DROPS_KEY_PREFIX)
        feature_vec = coerced
        X = pd.DataFrame([feature_vec])

        signals: list[dict] = []
        for (sym, direction, est), slot in self._meta_models.items():
            if sym != symbol:
                continue
            entry_pair = slot["entry"]
            exec_pair = slot["exec"]
            if entry_pair is None or exec_pair is None:
                continue
            entry_model, entry_meta = entry_pair
            exec_model, _exec_meta = exec_pair
            entry_p = float(entry_model.predict_proba(X)[0, 1])
            kelly_arr, pair_arr = exec_model.predict(X)
            kelly = float(kelly_arr[0])
            pair_idx = int(pair_arr[0])
            pair_idx = max(0, min(pair_idx, len(ms.pair_grid) - 1))
            tm, sm = ms.pair_grid[pair_idx]
            target_pct = float(tm) * atr_pct
            stop_pct = float(sm) * atr_pct
            signals.append({
                "direction": direction, "estimator_type": est,
                "model_id": entry_meta.id,
                "entry_probability": entry_p,
                "kelly_fraction": kelly,
                "pair_index": pair_idx,
                "target_pct": target_pct,
                "stop_pct": stop_pct,
            })
        return signals, feature_vec

    async def _increment_counter(self, prefix: str) -> None:
        """Bump a sliding-window counter for the current minute bucket.

        Observability-only — failures are logged but never propagate to
        callers (a transient Redis hiccup must not silence inference).
        Backtest engines pass redis_client=None on purpose; this helper
        becomes a no-op for them.
        """
        if self._redis is None or self._redis.client is None:
            return
        try:
            bucket = int(time.time() // 60)
            key = f"{prefix}:{bucket}"
            await self._redis.client.incr(key)
            await self._redis.client.expire(key, _BUCKET_TTL_SECONDS)
        except Exception:
            logger.exception(
                "InferenceEngine: failed to increment counter %s", prefix,
            )
