import asyncio
import json
import pytest
from unittest.mock import AsyncMock

from services.data_pipeline.main import handle_symbol_change_event


@pytest.mark.asyncio
async def test_add_equity_triggers_backfill_then_subscribe():
    stream = AsyncMock()
    backfill = AsyncMock()
    background_tasks: set[asyncio.Task] = set()
    event = json.dumps({"action": "add", "kind": "equity", "symbol": "AAPL"})
    await handle_symbol_change_event(
        event, stream=stream, backfill=backfill,
        background_tasks=background_tasks,
    )
    backfill.backfill_symbol.assert_awaited_once_with("AAPL", kind="equity")
    stream.subscribe_symbol.assert_awaited_once_with("AAPL", kind="equity")
    assert len(background_tasks) == 2
    await asyncio.gather(*background_tasks)
    backfill.reach_back.assert_any_await("AAPL", "1m")
    backfill.reach_back.assert_any_await("AAPL", "1d")
    assert backfill.reach_back.await_count == 2


@pytest.mark.asyncio
async def test_add_crypto_triggers_backfill_then_subscribe():
    stream = AsyncMock()
    backfill = AsyncMock()
    background_tasks: set[asyncio.Task] = set()
    event = json.dumps(
        {"action": "add", "kind": "crypto", "symbol": "BTC/USD"},
    )
    await handle_symbol_change_event(
        event, stream=stream, backfill=backfill,
        background_tasks=background_tasks,
    )
    backfill.backfill_symbol.assert_awaited_once_with("BTC/USD", kind="crypto")
    stream.subscribe_symbol.assert_awaited_once_with("BTC/USD", kind="crypto")
    assert len(background_tasks) == 2
    await asyncio.gather(*background_tasks)
    backfill.reach_back.assert_any_await("BTC/USD", "1m")
    backfill.reach_back.assert_any_await("BTC/USD", "1d")
    assert backfill.reach_back.await_count == 2


@pytest.mark.asyncio
async def test_remove_unsubscribes_only():
    stream = AsyncMock()
    backfill = AsyncMock()
    event = json.dumps(
        {"action": "remove", "kind": "crypto", "symbol": "BTC/USD"},
    )
    await handle_symbol_change_event(
        event, stream=stream, backfill=backfill,
        background_tasks=set(),
    )
    stream.unsubscribe_symbol.assert_awaited_once_with(
        "BTC/USD", kind="crypto",
    )
    backfill.backfill_symbol.assert_not_called()
    stream.subscribe_symbol.assert_not_called()
