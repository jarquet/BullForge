import pytest
from datetime import datetime, timezone, timedelta
from shared.models import TradingSignal, Order


# --- TradingSignal tests ---

def test_valid_trading_signal():
    ts = TradingSignal(
        symbol="SPY",
        timestamp=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        direction=1,
        strength=0.75,
        contributing_predictions=3,
    )
    assert ts.symbol == "SPY"
    assert ts.direction == 1
    assert ts.strength == 0.75
    assert ts.contributing_predictions == 3


def test_trading_signal_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="UTC"):
        TradingSignal(
            symbol="SPY",
            timestamp=datetime(2026, 4, 12, 14, 0, 0),
            direction=1,
            strength=0.75,
            contributing_predictions=3,
        )


def test_trading_signal_rejects_invalid_direction():
    with pytest.raises(ValueError, match="direction"):
        TradingSignal(
            symbol="SPY",
            timestamp=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            direction=2,
            strength=0.75,
            contributing_predictions=3,
        )


def test_trading_signal_rejects_strength_out_of_range():
    with pytest.raises(ValueError, match="strength"):
        TradingSignal(
            symbol="SPY",
            timestamp=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            direction=1,
            strength=1.5,
            contributing_predictions=3,
        )


def test_trading_signal_rejects_negative_predictions():
    with pytest.raises(ValueError, match="contributing_predictions"):
        TradingSignal(
            symbol="SPY",
            timestamp=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            direction=1,
            strength=0.75,
            contributing_predictions=-1,
        )


# --- Order tests ---

def test_valid_order():
    order = Order(
        symbol="SPY",
        side="buy",
        quantity=10.0,
        order_type="market",
        submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        signal_strength=0.8,
    )
    assert order.symbol == "SPY"
    assert order.side == "buy"
    assert order.quantity == 10.0
    assert order.status == "pending"
    assert order.alpaca_order_id is None
    assert order.id is None


def test_order_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="submitted_at must be UTC"):
        Order(
            symbol="SPY",
            side="buy",
            quantity=10.0,
            order_type="market",
            submitted_at=datetime(2026, 4, 12, 14, 0, 0),
            signal_strength=0.8,
        )


def test_order_rejects_invalid_side():
    with pytest.raises(ValueError, match="side"):
        Order(
            symbol="SPY",
            side="hold",
            quantity=10.0,
            order_type="market",
            submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            signal_strength=0.8,
        )


def test_order_rejects_zero_quantity():
    with pytest.raises(ValueError, match="quantity"):
        Order(
            symbol="SPY",
            side="buy",
            quantity=0.0,
            order_type="market",
            submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            signal_strength=0.8,
        )


def test_order_rejects_invalid_order_type():
    with pytest.raises(ValueError, match="order_type"):
        Order(
            symbol="SPY",
            side="buy",
            quantity=10.0,
            order_type="stop",
            submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            signal_strength=0.8,
        )


def test_order_rejects_limit_without_price():
    with pytest.raises(ValueError, match="limit_price"):
        Order(
            symbol="SPY",
            side="buy",
            quantity=10.0,
            order_type="limit",
            submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            signal_strength=0.8,
        )


def test_order_rejects_invalid_status():
    with pytest.raises(ValueError, match="status"):
        Order(
            symbol="SPY",
            side="buy",
            quantity=10.0,
            order_type="market",
            submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            signal_strength=0.8,
            status="unknown",
        )


def test_order_rejects_signal_strength_out_of_range():
    with pytest.raises(ValueError, match="signal_strength"):
        Order(
            symbol="SPY",
            side="buy",
            quantity=10.0,
            order_type="market",
            submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            signal_strength=1.5,
        )


def test_order_accepts_filled_state():
    order = Order(
        symbol="SPY",
        side="buy",
        quantity=10.0,
        order_type="market",
        submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        signal_strength=0.8,
        alpaca_order_id="abc-123",
        status="filled",
        filled_at=datetime(2026, 4, 12, 14, 0, 1, tzinfo=timezone.utc),
        filled_price=420.50,
        filled_quantity=10.0,
        id=42,
    )
    assert order.status == "filled"
    assert order.filled_price == 420.50
    assert order.id == 42


def test_order_rejects_naive_filled_at():
    with pytest.raises(ValueError, match="filled_at must be UTC"):
        Order(
            symbol="SPY",
            side="buy",
            quantity=10.0,
            order_type="market",
            submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            signal_strength=0.8,
            filled_at=datetime(2026, 4, 12, 14, 0, 1),
        )


def test_order_has_reconciliation_fields():
    from shared.models import Order
    from datetime import datetime, timezone
    o = Order(
        symbol="SPY", side="buy", quantity=1.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.5,
    )
    assert o.origin == "internal"
    assert o.last_reconciled_at is None
    assert o.reconcile_source is None


def test_order_origin_must_be_valid():
    import pytest
    from shared.models import Order
    from datetime import datetime, timezone
    with pytest.raises(ValueError, match="origin"):
        Order(
            symbol="SPY", side="buy", quantity=1.0, order_type="market",
            submitted_at=datetime.now(timezone.utc), signal_strength=0.5,
            origin="bogus",
        )


def test_order_status_expired_is_valid():
    from shared.models import Order
    from datetime import datetime, timezone
    o = Order(
        symbol="SPY", side="buy", quantity=1.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.5,
        status="expired",
    )
    assert o.status == "expired"
