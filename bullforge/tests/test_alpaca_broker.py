import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
from services.execution_service.alpaca_broker import AlpacaBroker


@pytest.fixture
def mock_settings():
    s = MagicMock()
    s.alpaca_api_key = "test_key"
    s.alpaca_secret_key = "test_secret"
    return s


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def broker(mock_settings, mock_client):
    with patch(
        "services.execution_service.alpaca_broker.TradingClient",
        return_value=mock_client,
    ):
        return AlpacaBroker(mock_settings)


@pytest.mark.asyncio
async def test_get_account(broker, mock_client):
    mock_account = MagicMock()
    mock_account.equity = "100000.00"
    mock_account.last_equity = "99500.00"
    mock_account.buying_power = "200000.00"
    mock_account.cash = "100000.00"
    mock_account.portfolio_value = "100000.00"
    mock_client.get_account.return_value = mock_account

    result = await broker.get_account()

    assert result["equity"] == 100000.0
    assert result["last_equity"] == 99500.0
    assert result["buying_power"] == 200000.0
    assert result["cash"] == 100000.0
    assert result["portfolio_value"] == 100000.0


@pytest.mark.asyncio
async def test_get_positions(broker, mock_client):
    mock_pos = MagicMock()
    mock_pos.symbol = "SPY"
    mock_pos.qty = "10"
    mock_pos.side = "long"
    mock_pos.avg_entry_price = "420.00"
    mock_pos.current_price = "425.00"
    mock_pos.unrealized_pl = "50.00"
    mock_pos.market_value = "4250.00"
    mock_client.get_all_positions.return_value = [mock_pos]

    result = await broker.get_positions()

    assert len(result) == 1
    assert result[0]["symbol"] == "SPY"
    assert result[0]["qty"] == 10.0
    assert result[0]["avg_entry_price"] == 420.0
    assert result[0]["unrealized_pl"] == 50.0


@pytest.mark.asyncio
async def test_get_position_found(broker, mock_client):
    mock_pos = MagicMock()
    mock_pos.symbol = "SPY"
    mock_pos.qty = "10"
    mock_pos.side = "long"
    mock_pos.avg_entry_price = "420.00"
    mock_pos.current_price = "425.00"
    mock_pos.unrealized_pl = "50.00"
    mock_pos.market_value = "4250.00"
    mock_client.get_open_position.return_value = mock_pos

    result = await broker.get_position("SPY")

    assert result is not None
    assert result["symbol"] == "SPY"


@pytest.mark.asyncio
async def test_get_position_not_found(broker, mock_client):
    mock_client.get_open_position.side_effect = Exception("position does not exist")

    result = await broker.get_position("AAPL")

    assert result is None


@pytest.mark.asyncio
async def test_submit_market_order(broker, mock_client):
    mock_order = MagicMock()
    mock_order.id = "order-abc-123"
    mock_client.submit_order.return_value = mock_order

    result = await broker.submit_market_order("SPY", 10, "buy")

    assert result == "order-abc-123"
    mock_client.submit_order.assert_called_once()


@pytest.mark.asyncio
async def test_submit_market_order_equity_uses_day_tif(broker, mock_client):
    """Equities must keep `time_in_force=day` — Alpaca's equity default."""
    from alpaca.trading.enums import TimeInForce

    mock_order = MagicMock()
    mock_order.id = "order-eq-1"
    mock_client.submit_order.return_value = mock_order

    await broker.submit_market_order("SPY", 10, "buy")

    args, _ = mock_client.submit_order.call_args
    request = args[0]
    assert request.symbol == "SPY"
    assert request.time_in_force == TimeInForce.DAY


@pytest.mark.asyncio
async def test_submit_market_order_crypto_uses_gtc_tif(broker, mock_client):
    """Crypto symbols (BASE/QUOTE) must use `time_in_force=gtc`.

    Alpaca's crypto endpoint rejects `day` with code 42210000; only `gtc`
    and `ioc` are accepted. Regression test for crypto execution path.
    """
    from alpaca.trading.enums import TimeInForce

    mock_order = MagicMock()
    mock_order.id = "order-btc-1"
    mock_client.submit_order.return_value = mock_order

    await broker.submit_market_order("BTC/USD", 0.01, "buy")

    args, _ = mock_client.submit_order.call_args
    request = args[0]
    assert request.symbol == "BTC/USD"
    assert request.time_in_force == TimeInForce.GTC


@pytest.mark.asyncio
async def test_get_order(broker, mock_client):
    mock_order = MagicMock()
    mock_order.id = "order-abc-123"
    mock_order.status = "filled"
    mock_order.filled_qty = "10"
    mock_order.filled_avg_price = "420.50"
    mock_order.filled_at = datetime(2026, 4, 12, 14, 0, 1, tzinfo=timezone.utc)
    mock_client.get_order_by_id.return_value = mock_order

    result = await broker.get_order("order-abc-123")

    assert result["id"] == "order-abc-123"
    assert result["status"] == "filled"
    assert result["filled_qty"] == 10.0
    assert result["filled_avg_price"] == 420.50
    assert result["filled_at"] == datetime(2026, 4, 12, 14, 0, 1, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_close_position(broker, mock_client):
    mock_order = MagicMock()
    mock_order.id = "close-order-456"
    mock_client.close_position.return_value = mock_order

    result = await broker.close_position("SPY")

    assert result == "close-order-456"


@pytest.mark.asyncio
async def test_close_position_fails(broker, mock_client):
    mock_client.close_position.side_effect = Exception("no position")

    result = await broker.close_position("SPY")

    assert result is None


@pytest.mark.asyncio
async def test_get_order_handles_enum_status(broker, mock_client):
    mock_order = MagicMock()
    mock_order.id = "order-abc-123"
    mock_status = MagicMock()
    mock_status.value = "partially_filled"
    mock_order.status = mock_status
    mock_order.filled_qty = "5"
    mock_order.filled_avg_price = "421.00"
    mock_order.filled_at = datetime(2026, 4, 12, 14, 0, 2, tzinfo=timezone.utc)
    mock_client.get_order_by_id.return_value = mock_order

    result = await broker.get_order("order-abc-123")

    assert result["status"] == "partially_filled"
    assert result["filled_qty"] == 5.0


@pytest.mark.asyncio
async def test_list_open_orders_calls_alpaca_with_bounded_window():
    from services.execution_service.alpaca_broker import AlpacaBroker
    from shared.config import Settings

    fake_client = MagicMock()
    fake_client.get_orders.return_value = [
        MagicMock(
            id="alp-1", symbol="SPY", side=MagicMock(value="buy"),
            qty="1", status=MagicMock(value="new"),
            filled_qty="0", filled_avg_price=None, filled_at=None,
            submitted_at=datetime.now(timezone.utc),
            order_type=MagicMock(value="market"),
        ),
    ]
    with patch(
        "services.execution_service.alpaca_broker.TradingClient",
        return_value=fake_client,
    ):
        broker = AlpacaBroker(Settings())
        after = datetime.now(timezone.utc) - timedelta(hours=48)
        results = await broker.list_open_orders(after=after)

    assert len(results) == 1
    assert results[0]["id"] == "alp-1"
    assert results[0]["status"] == "new"
    args, kwargs = fake_client.get_orders.call_args
    req = args[0] if args else kwargs.get("filter")
    assert req is not None
