"""One-shot historical feature + label backfill for every symbol with bars.

Discovers symbols from the bars tables (not runtime_config) so it covers
symbols that are no longer actively traded but still have historical data.
For each (symbol, timeframe) it calls the same FeatureEngine path used at
service startup, without the 300k cap — so every bar on disk produces a
feature row (modulo warmup) and a label row per configured horizon. Inserts
are upserts, so running this repeatedly is safe.

Invoke:
    docker compose run --rm feature_engine \\
        python -m services.feature_engine.backfill_features
"""
from __future__ import annotations

import asyncio
import logging

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient
from shared.runtime_config import RuntimeConfig
from services.feature_engine.engine import FeatureEngine
from services.feature_engine.main import build_registry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("feature_backfill")

# Larger than any plausible single-symbol bar count on this platform.
_UNCAPPED_LIMIT = 50_000_000


async def discover_symbols(db: Database) -> dict[str, list[str]]:
    """Return distinct symbols per timeframe sourced from the bars tables."""
    async with db.pool.acquire() as conn:
        minute_rows = await conn.fetch(
            "SELECT DISTINCT symbol FROM bars_1m ORDER BY symbol"
        )
        daily_rows = await conn.fetch(
            "SELECT DISTINCT symbol FROM bars_daily ORDER BY symbol"
        )
    return {
        "1m": [r["symbol"] for r in minute_rows],
        "1d": [r["symbol"] for r in daily_rows],
    }


async def main() -> None:
    settings = Settings()
    db = Database(settings)
    redis_client = RedisClient(settings)

    logger.info("Connecting to TimescaleDB...")
    await db.connect()
    logger.info("Connecting to Redis...")
    await redis_client.connect()

    runtime_config = RuntimeConfig(redis_client)
    label_horizons = {
        "1m": await runtime_config.get("label_horizons_1m"),
        "1d": await runtime_config.get("label_horizons_1d"),
    }
    logger.info("Label horizons from runtime config: %s", label_horizons)

    engine = FeatureEngine(
        settings=settings,
        db=db,
        registry=build_registry(),
        label_horizons=label_horizons,
        label_threshold=0.001,
        min_bars=50,
        redis_client=redis_client,
    )

    symbols_by_tf = await discover_symbols(db)
    logger.info("Discovered symbols: %s", symbols_by_tf)

    total_pairs = sum(len(s) for s in symbols_by_tf.values())
    done = 0
    for timeframe, symbols in symbols_by_tf.items():
        for symbol in symbols:
            done += 1
            logger.info(
                "[%d/%d] Backfilling %s %s...",
                done, total_pairs, symbol, timeframe,
            )
            try:
                await engine.compute_features_for_symbol(
                    symbol, timeframe, limit=_UNCAPPED_LIMIT,
                )
                await engine.compute_labels_for_symbol(
                    symbol, timeframe, limit=_UNCAPPED_LIMIT,
                )
            except Exception:
                logger.exception(
                    "Backfill failed for %s %s (continuing)",
                    symbol, timeframe,
                )

    await redis_client.disconnect()
    await db.disconnect()
    logger.info("Historical backfill complete.")


if __name__ == "__main__":
    asyncio.run(main())
