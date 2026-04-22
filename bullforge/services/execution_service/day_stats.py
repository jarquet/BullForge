from __future__ import annotations

from datetime import datetime, timezone

from shared.db import Database
from services.execution_service.alpaca_broker import AlpacaBroker


async def compute_day_stats(
    db: Database, broker: AlpacaBroker, now: datetime,
) -> dict:
    """Return a snapshot of account and trading activity for the current UTC day.

    The 'day' boundary is UTC midnight, matching how closed-trade exit_time is
    stored. This differs from a regular-hours equities session but keeps the
    query aligned with the TIMESTAMPTZ column.
    """
    account = await broker.get_account()
    equity = float(account.get("equity", 0.0))
    last_equity = float(account.get("last_equity", 0.0))
    day_pnl = equity - last_equity
    day_pnl_pct = (day_pnl / last_equity) if last_equity else 0.0

    day_start = datetime(
        now.year, now.month, now.day, tzinfo=timezone.utc,
    )
    trades = await db.query_closed_trades(since=day_start, limit=1000)

    wins = sum(1 for t in trades if float(t.pnl or 0.0) > 0)
    losses = sum(1 for t in trades if float(t.pnl or 0.0) < 0)
    denom = wins + losses
    win_rate = (wins / denom) if denom else 0.0
    realized = sum(float(t.pnl or 0.0) for t in trades)

    return {
        "equity": equity,
        "last_equity": last_equity,
        "day_pnl": day_pnl,
        "day_pnl_pct": day_pnl_pct,
        "day_trades": len(trades),
        "day_wins": wins,
        "day_losses": losses,
        "day_win_rate": win_rate,
        "day_realized_pnl": realized,
    }
