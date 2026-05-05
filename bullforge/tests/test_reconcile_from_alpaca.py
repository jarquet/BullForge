import json
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# The script lives at scripts/reconcile_from_alpaca.py; tests import it as a
# module. PYTHONPATH=/app already covers `scripts.` because main packaging
# treats `/app` as the root.
def _import_module():
    import importlib
    return importlib.import_module("scripts.reconcile_from_alpaca")


def test_parse_args_requires_symbol(monkeypatch):
    mod = _import_module()
    monkeypatch.setattr(sys, "argv", ["reconcile_from_alpaca.py"])
    with pytest.raises(SystemExit):
        mod._parse_args()


def test_parse_args_accepts_single_symbol(monkeypatch):
    mod = _import_module()
    monkeypatch.setattr(
        sys, "argv",
        ["reconcile_from_alpaca.py", "--symbol", "BTC/USD"],
    )
    args = mod._parse_args()
    assert args.symbol == "BTC/USD"
    assert args.since is None
    assert args.dry_run is False


def test_parse_args_accepts_comma_separated_symbols(monkeypatch):
    mod = _import_module()
    monkeypatch.setattr(
        sys, "argv",
        ["reconcile_from_alpaca.py", "--symbol", "BTC/USD,ETH/USD"],
    )
    args = mod._parse_args()
    assert args.symbol == "BTC/USD,ETH/USD"


def test_parse_args_parses_since_iso(monkeypatch):
    mod = _import_module()
    monkeypatch.setattr(
        sys, "argv",
        [
            "reconcile_from_alpaca.py",
            "--symbol", "BTC/USD",
            "--since", "2026-05-05T00:00:00+00:00",
        ],
    )
    args = mod._parse_args()
    assert args.since == datetime(2026, 5, 5, 0, 0, 0, tzinfo=timezone.utc)


def test_parse_args_dry_run_flag(monkeypatch):
    mod = _import_module()
    monkeypatch.setattr(
        sys, "argv",
        ["reconcile_from_alpaca.py", "--symbol", "BTC/USD", "--dry-run"],
    )
    args = mod._parse_args()
    assert args.dry_run is True


def _snap(alpaca_id: str, symbol: str, side: str = "sell",
          qty: float = 0.001, status: str = "filled") -> dict:
    return {
        "id": alpaca_id, "symbol": symbol, "side": side, "qty": qty,
        "order_type": "market",
        "submitted_at": datetime(2026, 5, 5, 1, 0, tzinfo=timezone.utc),
        "status": status,
        "filled_qty": qty,
        "filled_avg_price": 60_000.0,
        "filled_at": datetime(2026, 5, 5, 1, 5, tzinfo=timezone.utc),
    }


@pytest.mark.asyncio
async def test_dispatches_each_closed_snapshot_to_reconciler():
    mod = _import_module()
    broker = MagicMock()
    broker.list_closed_orders = AsyncMock(return_value=[
        _snap("alp-1", "BTC/USD"),
        _snap("alp-2", "BTC/USD"),
        _snap("alp-3", "BTC/USD"),
    ])
    core = MagicMock()
    core.apply_alpaca_order = AsyncMock()

    summary = await mod.reconcile(
        core=core, broker=broker,
        symbols=["BTC/USD"],
        since=datetime(2026, 5, 5, 0, 0, tzinfo=timezone.utc),
        dry_run=False,
    )

    assert summary["orders_fetched"] == 3
    assert summary["orders_dispatched"] == 3
    assert core.apply_alpaca_order.await_count == 3
    sources = {c.kwargs["source"] for c in core.apply_alpaca_order.await_args_list}
    assert sources == {"manual_replay"}


@pytest.mark.asyncio
async def test_dry_run_does_not_dispatch():
    mod = _import_module()
    broker = MagicMock()
    broker.list_closed_orders = AsyncMock(return_value=[
        _snap("alp-1", "BTC/USD"),
        _snap("alp-2", "BTC/USD"),
    ])
    core = MagicMock()
    core.apply_alpaca_order = AsyncMock()

    summary = await mod.reconcile(
        core=core, broker=broker,
        symbols=["BTC/USD"],
        since=datetime(2026, 5, 5, 0, 0, tzinfo=timezone.utc),
        dry_run=True,
    )

    assert summary["orders_fetched"] == 2
    assert summary["orders_dispatched"] == 0
    assert summary["dry_run"] is True
    core.apply_alpaca_order.assert_not_called()


@pytest.mark.asyncio
async def test_filters_to_target_symbols_only():
    mod = _import_module()
    broker = MagicMock()
    broker.list_closed_orders = AsyncMock(return_value=[
        _snap("alp-1", "BTC/USD"),
        _snap("alp-2", "ETH/USD"),
        _snap("alp-3", "SPY"),
        _snap("alp-4", "BTC/USD"),
    ])
    core = MagicMock()
    core.apply_alpaca_order = AsyncMock()

    summary = await mod.reconcile(
        core=core, broker=broker,
        symbols=["BTC/USD"],
        since=datetime(2026, 5, 5, 0, 0, tzinfo=timezone.utc),
        dry_run=False,
    )

    assert summary["orders_fetched"] == 2  # only BTC/USD ones
    assert summary["orders_dispatched"] == 2
    dispatched_ids = [
        c.args[0]["id"] for c in core.apply_alpaca_order.await_args_list
    ]
    assert dispatched_ids == ["alp-1", "alp-4"]


def test_resolve_since_default_is_24h_lookback():
    mod = _import_module()
    fake_now = datetime(2026, 5, 5, 12, 0, tzinfo=timezone.utc)
    since = mod._resolve_since(None, now=fake_now)
    assert since == fake_now - timedelta(hours=24)


def test_resolve_since_returns_explicit_arg_when_provided():
    mod = _import_module()
    explicit = datetime(2026, 5, 4, 23, 0, tzinfo=timezone.utc)
    fake_now = datetime(2026, 5, 5, 12, 0, tzinfo=timezone.utc)
    since = mod._resolve_since(explicit, now=fake_now)
    assert since == explicit  # explicit arg wins, fake_now is ignored


@pytest.mark.asyncio
async def test_summary_counts_match_dispatch():
    mod = _import_module()
    broker = MagicMock()
    broker.list_closed_orders = AsyncMock(return_value=[
        _snap("alp-1", "BTC/USD"),
        _snap("alp-2", "ETH/USD"),  # filtered out
        _snap("alp-3", "BTC/USD"),
    ])
    core = MagicMock()
    core.apply_alpaca_order = AsyncMock()

    summary = await mod.reconcile(
        core=core, broker=broker,
        symbols=["BTC/USD"],
        since=datetime(2026, 5, 5, 0, 0, tzinfo=timezone.utc),
        dry_run=False,
    )

    assert summary["orders_fetched"] == 2
    assert summary["orders_dispatched"] == 2
    assert summary["since"] == "2026-05-05T00:00:00+00:00"
    assert summary["symbols"] == ["BTC/USD"]
    assert summary["dry_run"] is False
