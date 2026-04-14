"""
Top-level worker function for ProcessPoolExecutor. Must be importable by the
child process; no closures, no local state.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def train_one_job(
    db_dsn: str,
    artifacts_dir: str,
    symbol: str,
    timeframe: str,
    horizon_minutes: int,
    model_type: str,
    as_of: datetime,
    runtime_config: dict,
    min_samples: int,
    min_samples_1d: int,
) -> dict:
    """
    Run a full training job in a worker process.

    `db_dsn` is accepted for forward compatibility but the worker uses Settings()
    (env vars) to resolve DB connection info within the child process.
    Returns a summary dict: {symbol, timeframe, horizon, model_type, status, model_id?, error?}
    """
    from shared.config import Settings
    from shared.db import Database
    from services.model_service.trainer import Trainer
    from services.model_service.model_store import ModelStore

    async def _run() -> dict:
        settings = Settings()
        db = Database(settings)
        await db.connect()
        try:
            store = ModelStore(artifacts_dir)
            trainer = Trainer(
                db=db, model_store=store,
                min_samples=min_samples, min_samples_1d=min_samples_1d,
            )
            meta = await trainer.train(
                symbol, timeframe, horizon_minutes,
                as_of=as_of,
                model_type=model_type,
                runtime_config=runtime_config,
            )
            if meta is None:
                return {
                    "symbol": symbol, "timeframe": timeframe,
                    "horizon": horizon_minutes, "model_type": model_type,
                    "status": "skipped_insufficient_data",
                }
            return {
                "symbol": symbol, "timeframe": timeframe,
                "horizon": horizon_minutes, "model_type": model_type,
                "status": "ok", "model_id": meta.id,
                "metrics": meta.metrics,
            }
        finally:
            await db.disconnect()

    try:
        return asyncio.run(_run())
    except Exception as e:
        logger.exception("Training job failed: %s %s %sm %s",
                         symbol, timeframe, horizon_minutes, model_type)
        return {
            "symbol": symbol, "timeframe": timeframe,
            "horizon": horizon_minutes, "model_type": model_type,
            "status": "error", "error": str(e),
        }
