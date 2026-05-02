from __future__ import annotations

import asyncio
import json
import logging
import signal
from datetime import datetime, timezone

from aiohttp import web
import pandas as pd

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient
from shared.runtime_config import RuntimeConfig
from services.model_service.model_store import ModelStore
from services.model_service.trainer import Trainer
from services.model_service.inference import InferenceEngine
from services.model_service.health import create_health_app
from services.model_service.scheduler import RetrainScheduler
from services.model_service.meta.label_backfill import MetaLabelBackfiller
from services.model_service.meta.regime import RegimeBuilder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("model_service")


def on_feature_update_factory(
    *, inference, redis, runtime_config, db, regime_builder,
):
    """Build the feature-update handler with meta-shadow path bolted on."""
    async def on_feature_update(message: str) -> None:
        data = json.loads(message)
        symbol = data["symbol"]
        timeframe = data["timeframe"]
        logger.debug("Feature update for %s %s, running inference", symbol, timeframe)

        predictions = await inference.predict(symbol, timeframe)
        if not predictions:
            return

        await db.insert_predictions(predictions)
        await redis.publish(
            "model:predictions",
            json.dumps({
                "symbol": symbol,
                "timeframe": timeframe,
                "predictions": [
                    {
                        "horizon_minutes": p.horizon_minutes,
                        "direction": p.predicted_direction,
                        "confidence": p.confidence,
                        "model_id": p.model_id,
                    }
                    for p in predictions
                ],
            }),
        )
        logger.info(
            "Published %d predictions for %s %s",
            len(predictions), symbol, timeframe,
        )

        # Meta-shadow path
        if not await runtime_config.get("meta_inference_enabled"):
            return
        # Meta is per-bar 1m only — MetaTrainer hardcodes timeframe="1m"
        # and build_replay_rows walks 1m bars. Running predict_meta for
        # 1d events would feed it predictions and a model_id list with a
        # different feature count than what the artifact was trained on,
        # producing a LightGBM shape error and zero useful signals.
        if timeframe != "1m":
            return
        try:
            latest = await db.query_feature_rows(symbol, timeframe, limit=1)
            if not latest:
                return
            fr = latest[0]
            atr_14 = float(fr.features.get("atr_14") or 0.0)
            # close price isn't carried on the feature row; sma_20 is a
            # close-tracking stand-in for the ATR% scaling. Acceptable for
            # Phase 1 shadow mode.
            close = float(fr.features.get("sma_20") or 0.0)
            atr_pct = (atr_14 / close) if close > 0 else 0.0
            symbol_state = {
                "realized_vol": fr.features.get("realized_vol", 0.02),
                "atr_price_ratio": atr_pct,
                "dist_sma_50": fr.features.get("dist_sma_50", 0.0),
                "dist_sma_200bar": fr.features.get("dist_sma_200", 0.0),
                "dist_sma_200day": fr.features.get("dist_sma_200d", 0.0),
            }
            regime = regime_builder.regime_at(fr.timestamp)
            signals, feature_vec = await inference.predict_meta(
                symbol=symbol, timeframe=timeframe,
                base_predictions=[
                    {"model_id": p.model_id, "direction": p.predicted_direction,
                     "confidence": p.confidence}
                    for p in predictions
                ],
                regime=regime, symbol_state=symbol_state,
                timestamp=fr.timestamp,
            )
            if signals:
                # feature_vec is the L2 dict that the meta estimator just
                # scored. Forward it verbatim so MetaShadowWriter persists
                # the real keys/values into meta_features.feature_vector
                # — without this, every live row was a placeholder stub.
                payload = {
                    "symbol": symbol, "timeframe": timeframe,
                    "timestamp": fr.timestamp.isoformat(),
                    "feature_vector": feature_vec,
                    "signals": signals,
                }
                await redis.publish("model:meta_signals", json.dumps(payload))
        except Exception:
            logger.exception("Meta inference path failed")
    return on_feature_update


def on_model_retrained_factory(*, inference):
    """Build the model:retrained handler that refreshes the live
    InferenceEngine's in-memory _models and _meta_models.

    scheduler.run_once publishes "model:retrained" at the end of every
    cycle — both for in-process scheduled retrains via run_forever AND
    for out-of-process manual retrains via retrain_now.py. The latter
    runs in its own subprocess with a throwaway InferenceEngine, so the
    live model_service has no other way to learn that the active set in
    model_registry just changed. Without this subscription, in-memory
    state drifts from the DB after every manual retrain, surfacing as
    LightGBM feature-shape errors when meta inference fires against a
    stale 194-feature in-memory model whose DB row is now 134-feature.
    """
    async def on_model_retrained(message: str) -> None:
        try:
            payload = json.loads(message) if message else {}
        except json.JSONDecodeError:
            payload = {}
        logger.info(
            "model:retrained received (ok=%s errors=%s skipped=%s) — "
            "reloading in-memory model state",
            payload.get("ok"), payload.get("errors"), payload.get("skipped"),
        )
        try:
            await inference.load_models()
        except Exception:
            logger.exception("load_models failed during model:retrained refresh")
        try:
            await inference.load_meta_models()
        except Exception:
            logger.exception("load_meta_models failed during model:retrained refresh")
    return on_model_retrained


async def main() -> None:
    settings = Settings()
    db = Database(settings)
    redis_client = RedisClient(settings)

    logger.info("Connecting to TimescaleDB...")
    await db.connect()
    logger.info("Connecting to Redis...")
    await redis_client.connect()

    model_store = ModelStore(settings.model_artifacts_dir)
    trainer = Trainer(
        db=db,
        model_store=model_store,
        min_samples=settings.model_min_samples,
        min_samples_1d=settings.model_min_samples_1d,
    )
    inference_engine = InferenceEngine(db=db, model_store=model_store)
    runtime_config = RuntimeConfig(redis_client)

    horizons_by_tf = {
        "1m": await runtime_config.get("label_horizons_1m"),
        "1d": await runtime_config.get("label_horizons_1d"),
    }
    logger.info("Using label horizons from runtime config: %s", horizons_by_tf)

    # Wait for feature engine to produce enough data before training
    equity_symbols = await runtime_config.get("trading_symbols_equity")
    crypto_symbols = await runtime_config.get("trading_symbols_crypto")
    all_symbols = list(equity_symbols or []) + list(crypto_symbols or [])
    logger.info(
        "Runtime symbol config — equity=%s crypto=%s",
        equity_symbols, crypto_symbols,
    )
    startup_as_of = datetime.now(timezone.utc)
    logger.info(
        "Waiting for feature engine to produce >= %d samples...",
        settings.model_min_samples,
    )
    max_wait = 600  # 10 minutes
    poll_interval = 15
    waited = 0
    while waited < max_wait:
        rows = await db.query_training_data(
            all_symbols[0], "1m", horizons_by_tf["1m"][0],
            as_of=startup_as_of, limit=settings.model_min_samples,
        )
        if len(rows) >= settings.model_min_samples:
            logger.info("Feature data ready (%d rows), proceeding with training", len(rows))
            break
        logger.info(
            "Feature data not ready yet (%d/%d rows), retrying in %ds...",
            len(rows), settings.model_min_samples, poll_interval,
        )
        await asyncio.sleep(poll_interval)
        waited += poll_interval
    else:
        logger.warning(
            "Timed out waiting for feature data after %ds, training with available data",
            max_wait,
        )

    # Train models on startup (skip if active model already exists)
    for symbol in all_symbols:
        for timeframe in ["1m", "1d"]:
            for horizon in horizons_by_tf[timeframe]:
                existing = await db.query_active_model(
                    symbol, timeframe, horizon,
                )
                if existing:
                    logger.info(
                        "Active model exists for %s %s %d (id=%d), skipping training",
                        symbol, timeframe, horizon, existing.id,
                    )
                    continue
                await trainer.train(
                    symbol, timeframe, horizon, as_of=startup_as_of, model_type="gradient_boost",
                )

    # Load all trained models for inference
    await inference_engine.load_models()

    scheduler = RetrainScheduler(
        db_dsn="",  # Settings() env vars used inside worker processes
        artifacts_dir=settings.model_artifacts_dir,
        runtime_config=runtime_config,
        inference_engine=inference_engine,
        redis_client=redis_client,
        symbols=all_symbols,
        timeframes=["1m", "1d"],
        horizons_by_tf=horizons_by_tf,
        min_samples=settings.model_min_samples,
        min_samples_1d=settings.model_min_samples_1d,
        db=db,
    )
    scheduler_task = asyncio.create_task(scheduler.run_forever())
    logger.info("Retrain scheduler started")

    # Pre-load SPY bars once at startup for the live regime builder.
    spy_recs = await db.query_bars("SPY", "1m", limit=200000)
    if spy_recs:
        spy_df = pd.DataFrame([
            {"timestamp": b.timestamp, "close": b.close} for b in spy_recs
        ]).set_index("timestamp")
    else:
        spy_df = pd.DataFrame(columns=["close"])
    regime_builder = RegimeBuilder(
        spy_bars=spy_df,
        max_staleness_hours=int(
            await runtime_config.get("meta_regime_max_staleness_hours")
        ),
    )

    # Load any active meta-models registered by previous nightly cycles.
    await inference_engine.load_meta_models()
    # Wire the inference engine's pair grid + stats TTL from RuntimeConfig.
    inference_engine._pair_grid = await runtime_config.get("meta_pair_grid")
    inference_engine._meta_stats_ttl_s = float(
        await runtime_config.get("meta_rolling_stats_ttl_seconds")
    )

    on_feature_update = on_feature_update_factory(
        inference=inference_engine, redis=redis_client,
        runtime_config=runtime_config, db=db,
        regime_builder=regime_builder,
    )
    await redis_client.subscribe("feature:updates", on_feature_update)
    logger.info("Subscribed to feature:updates for reactive inference")

    on_model_retrained = on_model_retrained_factory(inference=inference_engine)
    await redis_client.subscribe("model:retrained", on_model_retrained)
    logger.info(
        "Subscribed to model:retrained for in-memory model state refresh"
    )

    # Start the meta label-backfiller as a background task.
    backfill_interval_s = int(
        await runtime_config.get("meta_label_backfill_interval_minutes")
    ) * 60
    pair_grid = await runtime_config.get("meta_pair_grid")
    max_hold_min = int(await runtime_config.get("meta_max_hold_minutes"))
    backfiller = MetaLabelBackfiller(
        db=db, max_hold_minutes=max_hold_min, pair_grid=pair_grid,
        interval_seconds=float(backfill_interval_s),
    )
    backfiller_task = asyncio.create_task(backfiller.run_forever())
    logger.info("MetaLabelBackfiller started (interval=%ds)", backfill_interval_s)

    # Start health check server
    health_app = create_health_app(db, redis_client, inference_engine)
    runner = web.AppRunner(health_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8081)
    await site.start()
    logger.info("Health check server running on :8081")

    # Graceful shutdown
    shutdown_event = asyncio.Event()

    def handle_signal():
        logger.info("Shutdown signal received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    logger.info("Model service running. Press Ctrl+C to stop.")

    try:
        await shutdown_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down...")
        scheduler.stop()
        scheduler_task.cancel()
        try:
            await scheduler_task
        except (asyncio.CancelledError, Exception):
            pass
        backfiller.stop()
        backfiller_task.cancel()
        try:
            await backfiller_task
        except (asyncio.CancelledError, Exception):
            pass
        await runner.cleanup()
        await redis_client.disconnect()
        await db.disconnect()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
