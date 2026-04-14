import json
import pytest
from unittest.mock import AsyncMock

from services.data_pipeline.main import handle_symbol_change_event


@pytest.mark.asyncio
async def test_add_equity_triggers_backfill_then_subscribe():
    stream = AsyncMock()
    backfill = AsyncMock()
    event = json.dumps({"action": "add", "kind": "equity", "symbol": "AAPL"})
    await handle_symbol_change_event(event, stream=stream, backfill=backfill)
    backfill.backfill_symbol.assert_awaited_once_with("AAPL", kind="equity")
    stream.subscribe_symbol.assert_awaited_once_with("AAPL", kind="equity")


@pytest.mark.asyncio
async def test_add_crypto_triggers_backfill_then_subscribe():
    stream = AsyncMock()
    backfill = AsyncMock()
    event = json.dumps(
        {"action": "add", "kind": "crypto", "symbol": "BTC/USD"},
    )
    await handle_symbol_change_event(event, stream=stream, backfill=backfill)
    backfill.backfill_symbol.assert_awaited_once_with("BTC/USD", kind="crypto")
    stream.subscribe_symbol.assert_awaited_once_with("BTC/USD", kind="crypto")


@pytest.mark.asyncio
async def test_remove_unsubscribes_only():
    stream = AsyncMock()
    backfill = AsyncMock()
    event = json.dumps(
        {"action": "remove", "kind": "crypto", "symbol": "BTC/USD"},
    )
    await handle_symbol_change_event(event, stream=stream, backfill=backfill)
    stream.unsubscribe_symbol.assert_awaited_once_with(
        "BTC/USD", kind="crypto",
    )
    backfill.backfill_symbol.assert_not_called()
    stream.subscribe_symbol.assert_not_called()
