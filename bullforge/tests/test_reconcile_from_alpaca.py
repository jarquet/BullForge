import json
import sys
from datetime import datetime, timezone
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
