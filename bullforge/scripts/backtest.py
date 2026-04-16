# bullforge/scripts/backtest.py
from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timezone

from shared.config import Settings
from shared.db import Database
from shared.runtime_config import RuntimeConfig
from shared.redis import RedisClient

from services.backtest_service.engine import BacktestConfig, BacktestEngine
from services.backtest_service.results_db import BacktestResultsDB
from services.model_service.model_store import ModelStore


def _parse_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="BullForge walk-forward backtest")
    p.add_argument("--name", required=True, help="run label stored in DB")
    p.add_argument("--symbols", required=True,
                   help="comma-separated, e.g. SPY,QQQ")
    p.add_argument("--timeframe", default="1m", choices=["1m", "1d"])
    p.add_argument("--horizons", default=None,
                   help="comma-separated bar counts (default: from runtime config)")
    p.add_argument("--start", required=True, type=_parse_iso,
                   help="ISO datetime (UTC) inclusive")
    p.add_argument("--end", required=True, type=_parse_iso,
                   help="ISO datetime (UTC) exclusive")
    p.add_argument("--train-days", type=int, default=60)
    p.add_argument("--test-days", type=int, default=5)
    p.add_argument("--embargo-min", type=int, default=30)
    p.add_argument("--mode", default="rolling", choices=["rolling", "anchored"])
    p.add_argument("--starting-cash", type=float, default=100_000.0)
    p.add_argument("--slippage-bps", type=float, default=5.0)
    p.add_argument("--max-volume-pct", type=float, default=0.10)
    p.add_argument("--periods-per-year", type=int, default=390 * 252,
                   help="annualization factor; default = 1m equities")
    return p.parse_args()


async def _main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")

    settings = Settings()
    live_db = Database(settings)
    await live_db.connect()  # runs migrations, including 013 that creates `backtest` schema
    sandbox_db = Database(settings, sandbox=True)
    await sandbox_db.connect()  # search_path=backtest,public; no migrations
    redis = RedisClient(settings)
    await redis.connect()
    rc = RuntimeConfig(redis)
    model_store = ModelStore(settings.model_artifacts_dir)
    results = BacktestResultsDB(live_db)  # backtest_runs/folds/trades/equity live in public

    engine = BacktestEngine(
        db=sandbox_db, runtime_config=rc, model_store=model_store,
        results_db=results,
    )

    if args.horizons:
        horizons = [int(h) for h in args.horizons.split(",") if h.strip()]
    else:
        horizons = await rc.get(f"label_horizons_{args.timeframe}")

    cfg = BacktestConfig(
        name=args.name,
        symbols=[s.strip() for s in args.symbols.split(",") if s.strip()],
        timeframe=args.timeframe,
        horizons=horizons,
        range_start=args.start,
        range_end=args.end,
        train_days=args.train_days,
        test_days=args.test_days,
        embargo_minutes=args.embargo_min,
        mode=args.mode,
        starting_cash=args.starting_cash,
        slippage_bps=args.slippage_bps,
        max_volume_pct=args.max_volume_pct,
        periods_per_year=args.periods_per_year,
    )
    try:
        run_id = await engine.run(cfg)
        print(f"backtest run_id={run_id} completed")
    finally:
        await redis.disconnect()
        await sandbox_db.disconnect()
        await live_db.disconnect()


if __name__ == "__main__":
    asyncio.run(_main())
