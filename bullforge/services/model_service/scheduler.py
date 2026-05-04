from __future__ import annotations

import asyncio
import json
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from shared.db import Database
from shared.redis import RedisClient
from shared.runtime_config import RuntimeConfig
from services.model_service.inference import InferenceEngine
from services.model_service.meta.regime import RegimeBuilder
from services.model_service.meta.trainer import MetaTrainer
from services.model_service.model_store import ModelStore
from services.model_service.trainer import Trainer
from services.model_service.worker import train_one_job

logger = logging.getLogger(__name__)


def compute_seconds_until(hour: int, now: Optional[datetime] = None) -> float:
    now = now or datetime.now(timezone.utc)
    target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    return (target - now).total_seconds()


class RetrainScheduler:
    def __init__(
        self,
        db_dsn: str,
        artifacts_dir: str,
        runtime_config: RuntimeConfig,
        inference_engine: InferenceEngine,
        redis_client: RedisClient,
        symbols: list[str],
        timeframes: list[str],
        horizons_by_tf: dict[str, list[int]],
        min_samples: int,
        min_samples_1d: int = 84,
        db: Optional[Database] = None,
    ):
        self._db_dsn = db_dsn
        self._artifacts_dir = artifacts_dir
        self._rc = runtime_config
        self._inference = inference_engine
        self._redis = redis_client
        self._symbols = symbols
        self._timeframes = timeframes
        self._horizons_by_tf = horizons_by_tf
        self._min_samples = min_samples
        self._min_samples_1d = min_samples_1d
        self._db = db
        self._stop = asyncio.Event()

    async def run_forever(self) -> None:
        while not self._stop.is_set():
            hour = int(await self._rc.get("retrain_schedule_hour"))
            delay = compute_seconds_until(hour)
            logger.info("Next retrain in %.0f seconds (at %02d:00 UTC)", delay, hour)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self.run_once()
            except Exception:
                logger.exception("Retrain cycle failed")

    def stop(self) -> None:
        self._stop.set()

    async def run_once(self) -> dict:
        as_of = datetime.now(timezone.utc)
        config = await self._rc.get_all()
        workers = int(config["retrain_cpu_workers"])
        # Re-read symbols from RuntimeConfig each cycle so adds/removes via the
        # dashboard are picked up without a service restart.
        rc_symbols = (
            list(config.get("trading_symbols_equity") or [])
            + list(config.get("trading_symbols_crypto") or [])
        )
        if rc_symbols:
            self._symbols = rc_symbols
        logger.info(
            "Retrain cycle starting (as_of=%s, workers=%d, symbols=%s)",
            as_of, workers, self._symbols,
        )

        # Re-read horizons from runtime config each cycle
        horizons_by_tf = {
            "1m": config.get("label_horizons_1m", self._horizons_by_tf.get("1m", [])),
            "1d": config.get("label_horizons_1d", self._horizons_by_tf.get("1d", [])),
        }

        cpu_jobs = []
        gpu_jobs = []
        for symbol in self._symbols:
            for tf in self._timeframes:
                for h in horizons_by_tf.get(tf, []):
                    cpu_jobs.append((symbol, tf, h, "gradient_boost"))
                    gpu_jobs.append((symbol, tf, h, "lstm"))

        results: list[dict] = []

        loop = asyncio.get_running_loop()
        # Spawn (not fork) so workers don't inherit the parent's torch /
        # lightgbm / sklearn / UCX state — fork from a multi-threaded
        # interpreter is the documented OpenMP-segfault setup, and was
        # the root cause of the 2026-05-04 retrain incident. Manual
        # retrain_now survived because that path runs in a fresh
        # interpreter; the nightly cycle does not have that luxury.
        with ProcessPoolExecutor(
            max_workers=workers,
            mp_context=multiprocessing.get_context("spawn"),
        ) as pool:
            futures = []
            for symbol, tf, h, mt in cpu_jobs:
                fut = pool.submit(
                    train_one_job,
                    self._db_dsn, self._artifacts_dir,
                    symbol, tf, h, mt, as_of, config,
                    self._min_samples, self._min_samples_1d,
                )
                futures.append(fut)
            for fut in futures:
                try:
                    results.append(await loop.run_in_executor(None, fut.result))
                except Exception as e:
                    logger.exception("CPU worker raised")
                    results.append({"status": "error", "error": str(e)})

        for symbol, tf, h, mt in gpu_jobs:
            try:
                res = await self._train_lstm_in_process(symbol, tf, h, as_of, config)
                results.append(res)
            except Exception as e:
                logger.exception("GPU worker raised")
                results.append({
                    "symbol": symbol, "timeframe": tf, "horizon": h,
                    "model_type": mt, "status": "error", "error": str(e),
                })

        # ── Meta phase ────────────────────────────────────────────────
        try:
            meta_results = await self._train_meta_phase(
                config=config, as_of=as_of,
            )
            results.extend(meta_results)
        except Exception:
            logger.exception("Meta retrain phase failed")

        await self._inference.load_models()
        # Also reload meta models — meta artifacts created in this cycle's
        # _train_meta_phase live in model_registry but are not in the
        # InferenceEngine's _meta_models dict until load_meta_models() runs.
        # Without this, shadow inference sees an empty _meta_models dict
        # until the model_service process is restarted.
        await self._inference.load_meta_models()

        summary = {
            "as_of": as_of.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "ok": sum(1 for r in results if r.get("status") == "ok"),
            "errors": sum(1 for r in results if r.get("status") == "error"),
            "skipped": sum(1 for r in results if r.get("status") == "skipped_insufficient_data"),
            "results": results,
        }

        await self._redis.publish("model:retrained", json.dumps(summary))
        await self._rc.set("last_retrain_time", as_of.isoformat())
        logger.info("Retrain cycle complete: ok=%d err=%d skipped=%d",
                    summary["ok"], summary["errors"], summary["skipped"])
        return summary

    async def _train_lstm_in_process(
        self, symbol: str, timeframe: str, horizon: int,
        as_of: datetime, runtime_config: dict,
    ) -> dict:
        """LSTM trains in the main process (GPU single-tenant)."""
        if self._db is None:
            raise RuntimeError("RetrainScheduler needs a Database instance for GPU phase")
        store = ModelStore(self._artifacts_dir)
        trainer = Trainer(
            db=self._db, model_store=store,
            min_samples=self._min_samples, min_samples_1d=self._min_samples_1d,
        )
        meta = await trainer.train(
            symbol, timeframe, horizon,
            as_of=as_of, model_type="lstm", runtime_config=runtime_config,
        )
        if meta is None:
            return {"symbol": symbol, "timeframe": timeframe, "horizon": horizon,
                    "model_type": "lstm", "status": "skipped_insufficient_data"}
        return {"symbol": symbol, "timeframe": timeframe, "horizon": horizon,
                "model_type": "lstm", "status": "ok", "model_id": meta.id,
                "metrics": meta.metrics}

    async def _train_meta_phase(
        self, *, config: dict, as_of: datetime,
    ) -> list[dict]:
        if self._db is None:
            logger.warning("Meta phase skipped — no Database instance")
            return []
        store = ModelStore(self._artifacts_dir)
        estimator_types = config.get("meta_estimator_types") or ["lightgbm"]
        pair_grid = config.get("meta_pair_grid") or [[2, 1], [3, 1.5], [4, 2]]
        train_window = int(config.get("meta_train_window_days", 60))
        max_hold = int(config.get("meta_max_hold_minutes", 30))
        min_rows = int(config.get("meta_min_train_rows", 500))

        # Pre-flight: meta training replays over historical predictions.
        # If the predictions table is empty in the training window, every
        # (symbol, direction, estimator_type) iteration would hit
        # "Insufficient data" silently. Surface that once at WARNING level
        # instead, and skip the loop entirely.
        since_window = as_of - timedelta(days=train_window)
        pred_count = await self._db.count_predictions_in_window(
            start=since_window, end=as_of,
        )
        if pred_count == 0:
            logger.warning(
                "Meta phase skipped — predictions table has no live rows in "
                "[%s, %s]. Expected on a fresh deploy; live inference must "
                "populate predictions before meta training has anything to "
                "replay over.",
                since_window.isoformat(), as_of.isoformat(),
            )
            return []

        # Pre-load SPY bars once for this whole cycle's RegimeBuilder.
        # Buffer is +35 days (not +7): RegimeBuilder.vol_regime needs a 30-day
        # rolling median, so anything less leaves most of the warmup window
        # producing NaN regime values.
        spy_recs = await self._db.query_bars(
            "SPY", "1m",
            since=as_of - timedelta(days=train_window + 35),
            limit=400000,
        )
        # query_bars returns list[Bar]; build a lightweight DataFrame for RegimeBuilder
        if spy_recs:
            spy_df = pd.DataFrame([
                {"timestamp": b.timestamp, "close": b.close} for b in spy_recs
            ])
            spy_df = spy_df.set_index("timestamp")
        else:
            spy_df = pd.DataFrame(columns=["close"])

        rb_factory = lambda: RegimeBuilder(
            spy_bars=spy_df,
            max_staleness_hours=int(config.get("meta_regime_max_staleness_hours", 48)),
        )

        trainer = MetaTrainer(
            db=self._db, model_store=store,
            sub_model_pairs={"long": pair_grid, "short": pair_grid},
        )

        out: list[dict] = []
        for symbol in self._symbols:
            for direction in (1, -1):
                for et in estimator_types:
                    try:
                        meta_e = await trainer.train_entry(
                            symbol=symbol, direction=direction,
                            estimator_type=et, as_of=as_of,
                            train_window_days=train_window,
                            max_hold_minutes=max_hold,
                            min_train_rows=min_rows,
                            regime_builder_factory=rb_factory,
                        )
                        meta_x = await trainer.train_exec(
                            symbol=symbol, direction=direction,
                            estimator_type=et, as_of=as_of,
                            train_window_days=train_window,
                            max_hold_minutes=max_hold,
                            min_train_rows=min_rows,
                            regime_builder_factory=rb_factory,
                        )
                        # If both train_entry and train_exec returned None
                        # (insufficient data — logged inside the trainer),
                        # report skipped so the run_once summary line
                        # accurately distinguishes ok vs. skipped vs. error.
                        status = (
                            "ok" if (meta_e is not None or meta_x is not None)
                            else "skipped_insufficient_data"
                        )
                        out.append({
                            "symbol": symbol, "direction": direction,
                            "estimator_type": et,
                            "model_type": "meta", "status": status,
                            "entry_id": meta_e.id if meta_e else None,
                            "exec_id": meta_x.id if meta_x else None,
                        })
                    except Exception as e:
                        logger.exception("Meta train failed for %s %s %s",
                                         symbol, direction, et)
                        out.append({"symbol": symbol, "direction": direction,
                                    "estimator_type": et, "model_type": "meta",
                                    "status": "error", "error": str(e)})
        return out
