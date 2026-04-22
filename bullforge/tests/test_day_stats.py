import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from services.execution_service.day_stats import compute_day_stats


def _trade(pnl):
    t = MagicMock()
    t.pnl = pnl
    return t


@pytest.mark.asyncio
async def test_compute_day_stats_mixed_wins_losses_breakeven():
    db = AsyncMock()
    db.query_closed_trades = AsyncMock(return_value=[
        _trade(150.0), _trade(-50.0), _trade(0.0), _trade(25.0),
    ])
    broker = AsyncMock()
    broker.get_account = AsyncMock(return_value={
        "equity": 101_025.0, "last_equity": 100_900.0,
    })
    now = datetime(2026, 4, 22, 15, 0, tzinfo=timezone.utc)

    stats = await compute_day_stats(db, broker, now)

    assert stats["equity"] == 101_025.0
    assert stats["last_equity"] == 100_900.0
    assert stats["day_pnl"] == pytest.approx(125.0)
    assert stats["day_pnl_pct"] == pytest.approx(125.0 / 100_900.0)
    assert stats["day_trades"] == 4
    assert stats["day_wins"] == 2
    assert stats["day_losses"] == 1
    assert stats["day_win_rate"] == pytest.approx(2 / 3)
    assert stats["day_realized_pnl"] == pytest.approx(125.0)

    call = db.query_closed_trades.await_args
    assert call.kwargs["since"] == datetime(2026, 4, 22, 0, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_compute_day_stats_handles_zero_last_equity():
    db = AsyncMock()
    db.query_closed_trades = AsyncMock(return_value=[])
    broker = AsyncMock()
    broker.get_account = AsyncMock(return_value={
        "equity": 100.0, "last_equity": 0.0,
    })
    now = datetime(2026, 4, 22, 0, 30, tzinfo=timezone.utc)

    stats = await compute_day_stats(db, broker, now)
    assert stats["day_pnl"] == pytest.approx(100.0)
    assert stats["day_pnl_pct"] == 0.0
    assert stats["day_trades"] == 0
    assert stats["day_wins"] == 0
    assert stats["day_losses"] == 0
    assert stats["day_win_rate"] == 0.0


@pytest.mark.asyncio
async def test_compute_day_stats_no_losses_gives_full_win_rate():
    db = AsyncMock()
    db.query_closed_trades = AsyncMock(return_value=[_trade(10.0), _trade(20.0)])
    broker = AsyncMock()
    broker.get_account = AsyncMock(return_value={
        "equity": 1_000.0, "last_equity": 970.0,
    })
    now = datetime(2026, 4, 22, 15, 0, tzinfo=timezone.utc)

    stats = await compute_day_stats(db, broker, now)
    assert stats["day_win_rate"] == pytest.approx(1.0)
    assert stats["day_wins"] == 2
    assert stats["day_losses"] == 0
