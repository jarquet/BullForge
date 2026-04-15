from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from shared.models import Order


pytestmark = pytest.mark.asyncio(loop_scope="session")


def _snapshot(**overrides):
    base = {
        "id": "alp-1",
        "symbol": "AAPL",
        "side": "buy",
        "qty": 10.0,
        "order_type": "market",
        "status": "new",
        "submitted_at": datetime.now(timezone.utc),
        "filled_at": None,
        "filled_avg_price": None,
        "filled_qty": None,
    }
    base.update(overrides)
    return base


async def _insert_submitted(db, alpaca_id="alp-1"):
    order = Order(
        symbol="AAPL", side="buy", quantity=10.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.7,
        alpaca_order_id=alpaca_id, status="submitted",
    )
    return await db.insert_order(order)


async def test_upsert_transitions_submitted_to_cancelled(real_db):
    await _insert_submitted(real_db)
    result = await real_db.upsert_order_from_alpaca(
        _snapshot(status="canceled"), source="poll",
    )
    assert result.old_status == "submitted"
    assert result.new_status == "cancelled"
    assert result.order.reconcile_source == "poll"
    assert result.order.last_reconciled_at is not None


async def test_upsert_is_idempotent_under_duplicate_terminal(real_db):
    await _insert_submitted(real_db)
    await real_db.upsert_order_from_alpaca(_snapshot(status="filled",
        filled_avg_price=150.0, filled_qty=10.0), source="stream")
    second = await real_db.upsert_order_from_alpaca(_snapshot(status="filled",
        filled_avg_price=150.0, filled_qty=10.0), source="poll")
    assert second.old_status == "filled"
    assert second.new_status == "filled"


async def test_orphan_order_ingested_as_external(real_db):
    result = await real_db.upsert_order_from_alpaca(
        _snapshot(id="alp-orphan", status="new"), source="poll",
    )
    assert result.inserted is True
    assert result.order.origin == "external"


async def test_unique_partial_index_rejects_duplicate_alpaca_id(real_db):
    await _insert_submitted(real_db, alpaca_id="alp-unique")
    with pytest.raises(Exception):
        await _insert_submitted(real_db, alpaca_id="alp-unique")


async def test_non_terminal_query_excludes_terminal(real_db):
    await _insert_submitted(real_db, alpaca_id="alp-open")
    await real_db.upsert_order_from_alpaca(
        _snapshot(id="alp-open", status="filled",
                  filled_avg_price=150.0, filled_qty=10.0),
        source="stream",
    )
    rows = await real_db.query_non_terminal_orders(within=timedelta(hours=48))
    assert all(r.status not in ("filled", "cancelled", "rejected", "expired")
               for r in rows)


async def test_submit_path_is_idempotent_with_stream_winner(real_db):
    await real_db.upsert_order_from_alpaca(
        _snapshot(id="alp-race", status="new"), source="stream",
    )
    order = Order(
        symbol="AAPL", side="buy", quantity=10.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.7,
        alpaca_order_id="alp-race", status="submitted",
    )
    result = await real_db.insert_order_if_new(order)
    assert result is None
    existing = await real_db.get_order_by_alpaca_id("alp-race")
    assert existing.origin == "external"


async def test_status_check_rejects_garbage(real_db):
    await _insert_submitted(real_db, alpaca_id="alp-check")
    async with real_db.pool.acquire() as conn:
        with pytest.raises(Exception):
            await conn.execute(
                "UPDATE orders SET status='not_a_status' "
                "WHERE alpaca_order_id=$1",
                "alp-check",
            )
