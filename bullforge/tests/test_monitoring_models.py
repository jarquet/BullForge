import pytest
from datetime import datetime, timezone


def test_trade_valid():
    from shared.models import Trade

    t = Trade(
        symbol="SPY",
        side="long",
        entry_time=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        entry_price=420.0,
        quantity=10.0,
        entry_order_id=1,
    )
    assert t.symbol == "SPY"
    assert t.side == "long"
    assert t.exit_time is None
    assert t.pnl is None
    assert t.id is None


def test_trade_with_exit():
    from shared.models import Trade

    t = Trade(
        symbol="SPY",
        side="long",
        entry_time=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        entry_price=420.0,
        quantity=10.0,
        entry_order_id=1,
        exit_time=datetime(2026, 4, 12, 15, 0, 0, tzinfo=timezone.utc),
        exit_price=425.0,
        pnl=50.0,
        hold_duration_seconds=3600,
        exit_order_id=2,
        id=42,
    )
    assert t.exit_price == 425.0
    assert t.pnl == 50.0
    assert t.hold_duration_seconds == 3600
    assert t.id == 42


def test_trade_rejects_non_utc_entry_time():
    from shared.models import Trade

    with pytest.raises(ValueError, match="entry_time must be UTC"):
        Trade(
            symbol="SPY",
            side="long",
            entry_time=datetime(2026, 4, 12, 14, 0, 0),
            entry_price=420.0,
            quantity=10.0,
            entry_order_id=1,
        )


def test_trade_rejects_invalid_side():
    from shared.models import Trade

    with pytest.raises(ValueError, match="side must be one of"):
        Trade(
            symbol="SPY",
            side="up",
            entry_time=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            entry_price=420.0,
            quantity=10.0,
            entry_order_id=1,
        )


def test_trade_rejects_non_positive_quantity():
    from shared.models import Trade

    with pytest.raises(ValueError, match="quantity must be positive"):
        Trade(
            symbol="SPY",
            side="long",
            entry_time=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            entry_price=420.0,
            quantity=0,
            entry_order_id=1,
        )


def test_model_status_valid():
    from shared.models import ModelStatus

    ms = ModelStatus(
        model_id=5,
        status="active",
        reason="Within expected range",
        updated_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
    )
    assert ms.model_id == 5
    assert ms.status == "active"
    assert ms.reason == "Within expected range"


def test_model_status_rejects_invalid_status():
    from shared.models import ModelStatus

    with pytest.raises(ValueError, match="status must be one of"):
        ModelStatus(
            model_id=5,
            status="paused",
            reason="test",
            updated_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        )


def test_model_status_rejects_non_utc():
    from shared.models import ModelStatus

    with pytest.raises(ValueError, match="updated_at must be UTC"):
        ModelStatus(
            model_id=5,
            status="active",
            reason="test",
            updated_at=datetime(2026, 4, 12, 14, 0, 0),
        )
