"""One-shot Alpaca → DB reconciliation for closed orders.

Use when the trade-updates websocket missed events (e.g., a manual broker-UI
position close) and the polling sweeper has already moved past the relevant
window. Idempotent: known alpaca_order_ids are no-op'd by the upsert path.

Example:
    PYTHONPATH=. python scripts/reconcile_from_alpaca.py \\
        --symbol BTC/USD \\
        --since 2026-05-05T00:00:00+00:00 \\
        --dry-run
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient

from services.execution_service.alpaca_broker import AlpacaBroker
from services.execution_service.reconciler_core import ReconcilerCore

logger = logging.getLogger(__name__)


def _parse_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _resolve_since(
    arg_since: datetime | None,
    now: datetime | None = None,
) -> datetime:
    """Return arg_since if provided, else now - 24h. `now` is injectable for tests."""
    if arg_since is not None:
        return arg_since
    return (now or datetime.now(timezone.utc)) - timedelta(hours=24)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Replay Alpaca closed orders into BullForge.",
    )
    p.add_argument(
        "--symbol", required=True,
        help="comma-separated symbols, e.g. BTC/USD or BTC/USD,ETH/USD",
    )
    p.add_argument(
        "--since", type=_parse_iso, default=None,
        help="ISO datetime (UTC); default = now - 24h",
    )
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


async def reconcile(
    *,
    core: ReconcilerCore,
    broker: AlpacaBroker,
    symbols: list[str],
    since: datetime,
    dry_run: bool,
) -> dict:
    summary = {
        "since": since.isoformat(),
        "symbols": symbols,
        "orders_fetched": 0,
        "orders_dispatched": 0,
        "dry_run": dry_run,
    }
    snaps = await broker.list_closed_orders(since)
    for snap in snaps:
        if snap["symbol"] not in symbols:
            continue
        summary["orders_fetched"] += 1
        if dry_run:
            logger.info(
                "[dry-run] would replay alpaca_id=%s %s qty=%s status=%s filled_at=%s",
                snap["id"], snap["side"], snap["qty"],
                snap["status"], snap.get("filled_at"),
            )
            continue
        await core.apply_alpaca_order(snap, source="manual_replay")
        summary["orders_dispatched"] += 1
    return summary


async def _main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    since = _resolve_since(args.since)
    symbols = [s.strip() for s in args.symbol.split(",") if s.strip()]

    settings = Settings()
    db = Database(settings)
    await db.connect()
    redis = RedisClient(settings)
    await redis.connect()
    broker = AlpacaBroker(settings)
    core = ReconcilerCore(db=db, redis_client=redis, broker=broker)

    summary = await reconcile(
        core=core, broker=broker,
        symbols=symbols, since=since, dry_run=args.dry_run,
    )

    print("\n=== Reconciliation summary ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    asyncio.run(_main())
