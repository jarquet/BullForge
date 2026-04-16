from __future__ import annotations

import asyncio
import json
import logging
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Optional

from shared.db import Database
from shared.redis import RedisClient
from shared.runtime_config import RuntimeConfig
from services.model_service.inference import InferenceEngine
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
        with ProcessPoolExecutor(max_workers=workers) as pool:
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

        await self._inference.load_models()

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
