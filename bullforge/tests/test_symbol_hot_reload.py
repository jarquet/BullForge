import asyncio
import json
import pytest
from unittest.mock import AsyncMock, Mock

from services.data_pipeline.main import handle_symbol_change_event


@pytest.mark.asyncio
async def test_add_equity_triggers_subscribe_then_backfill():
    parent = Mock()
    parent.stream = AsyncMock()
    parent.backfill = AsyncMock()
    background_tasks: set[asyncio.Task] = set()
    event = json.dumps({"action": "add", "kind": "equity", "symbol": "AAPL"})
    await handle_symbol_change_event(
        event, stream=parent.stream, backfill=parent.backfill,
        background_tasks=background_tasks,
    )
    parent.backfill.backfill_symbol.assert_awaited_once_with(
        "AAPL", kind="equity",
    )
    parent.stream.subscribe_symbol.assert_awaited_once_with(
        "AAPL", kind="equity",
    )
    # Subscribe must happen before backfill so live bars during forward-fill
    # are captured via the stream (upsert dedups overlap).
    call_names = [c[0] for c in parent.mock_calls]
    sub_idx = call_names.index("stream.subscribe_symbol")
    fill_idx = call_names.index("backfill.backfill_symbol")
    assert sub_idx < fill_idx
    assert len(background_tasks) == 2
    await asyncio.gather(*background_tasks)
    parent.backfill.reach_back.assert_any_await("AAPL", "1m")
    parent.backfill.reach_back.assert_any_await("AAPL", "1d")
    assert parent.backfill.reach_back.await_count == 2


@pytest.mark.asyncio
async def test_add_crypto_triggers_subscribe_then_backfill():
    parent = Mock()
    parent.stream = AsyncMock()
    parent.backfill = AsyncMock()
    background_tasks: set[asyncio.Task] = set()
    event = json.dumps(
        {"action": "add", "kind": "crypto", "symbol": "BTC/USD"},
    )
    await handle_symbol_change_event(
        event, stream=parent.stream, backfill=parent.backfill,
        background_tasks=background_tasks,
    )
    parent.backfill.backfill_symbol.assert_awaited_once_with(
        "BTC/USD", kind="crypto",
    )
    parent.stream.subscribe_symbol.assert_awaited_once_with(
        "BTC/USD", kind="crypto",
    )
    call_names = [c[0] for c in parent.mock_calls]
    sub_idx = call_names.index("stream.subscribe_symbol")
    fill_idx = call_names.index("backfill.backfill_symbol")
    assert sub_idx < fill_idx
    assert len(background_tasks) == 2
    await asyncio.gather(*background_tasks)
    parent.backfill.reach_back.assert_any_await("BTC/USD", "1m")
    parent.backfill.reach_back.assert_any_await("BTC/USD", "1d")
    assert parent.backfill.reach_back.await_count == 2


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
