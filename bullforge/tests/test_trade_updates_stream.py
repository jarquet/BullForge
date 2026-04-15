import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from services.execution_service.trade_updates_stream import (
    TradeUpdatesStream, normalize_trade_update,
)


def test_normalize_trade_update_matches_broker_snapshot_shape():
    event = MagicMock()
    event.order = MagicMock(
        id="alp-42",
        symbol="SPY",
        side=MagicMock(value="buy"),
        qty="2",
        order_type=MagicMock(value="market"),
        status=MagicMock(value="filled"),
        submitted_at=datetime(2026, 4, 14, tzinfo=timezone.utc),
        filled_qty="2",
        filled_avg_price="420.5",
        filled_at=datetime(2026, 4, 14, 14, 0, 1, tzinfo=timezone.utc),
    )
    snap = normalize_trade_update(event)
    assert snap["id"] == "alp-42"
    assert snap["status"] == "filled"
    assert snap["filled_qty"] == 2.0
    assert snap["filled_avg_price"] == 420.5
    assert snap["filled_at"].tzinfo == timezone.utc


@pytest.mark.asyncio
async def test_handler_forwards_to_core():
    core = AsyncMock()
    stream = TradeUpdatesStream(
        settings=MagicMock(
            alpaca_api_key="k", alpaca_secret_key="s",
            alpaca_base_url="https://paper-api.alpaca.markets",
        ),
        core=core,
        on_reconnect=AsyncMock(),
    )
    event = MagicMock()
    event.order = MagicMock(
        id="a", symbol="SPY", side=MagicMock(value="buy"), qty="1",
        order_type=MagicMock(value="market"),
        status=MagicMock(value="filled"),
        submitted_at=datetime.now(timezone.utc),
        filled_qty="1", filled_avg_price="100",
        filled_at=datetime.now(timezone.utc),
    )
    await stream._handle_event(event)
    core.apply_alpaca_order.assert_awaited_once()
    args, kwargs = core.apply_alpaca_order.await_args
    assert (kwargs.get("source") or args[1]) == "stream"


@pytest.mark.asyncio
async def test_handler_swallows_malformed_event():
    core = AsyncMock()
    stream = TradeUpdatesStream(
        settings=MagicMock(
            alpaca_api_key="k", alpaca_secret_key="s",
            alpaca_base_url="https://paper-api.alpaca.markets",
        ),
        core=core,
        on_reconnect=AsyncMock(),
    )
    bad = MagicMock()
    bad.order = None
    await stream._handle_event(bad)
    core.apply_alpaca_order.assert_not_called()


@pytest.mark.asyncio
async def test_reconnect_backoff_schedule():
    stream = TradeUpdatesStream(
        settings=MagicMock(
            alpaca_api_key="k", alpaca_secret_key="s",
            alpaca_base_url="https://paper-api.alpaca.markets",
        ),
        core=AsyncMock(),
        on_reconnect=AsyncMock(),
    )
    delays = [stream._backoff_delay(n) for n in range(7)]
    assert delays[0] == 1
    assert delays[1] == 2
    assert delays[2] == 4
    assert delays[-1] <= 30
    assert delays == sorted(delays)
