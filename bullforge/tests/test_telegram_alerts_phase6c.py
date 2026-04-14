from datetime import datetime, timedelta, timezone
from services.monitoring_service.telegram_alerts import (
    format_status_expanded, format_positions, format_trades,
    format_models_summary,
)


def test_format_positions_empty():
    assert "No open positions" in format_positions([])


def test_format_positions_lists_each():
    positions = [
        {"symbol": "SPY", "qty": 10, "avg_entry_price": 420.0,
         "current_price": 421.0, "market_value": 4210.0, "unrealized_pl": 10.0},
    ]
    s = format_positions(positions)
    assert "SPY" in s
    assert "10" in s
    assert "421.0" in s


def test_format_trades_empty():
    assert "No recent trades" in format_trades([])


def test_format_models_summary_groups_by_status():
    models = [
        {"label": "gradient_boost · SPY · 1m · 5m", "status": "active",
         "metrics": {"accuracy": 0.62}, "trained_at": datetime.now(timezone.utc) - timedelta(hours=14)},
        {"label": "gradient_boost · QQQ · 1m · 30m", "status": "warned",
         "metrics": {"accuracy": 0.48}, "trained_at": datetime.now(timezone.utc) - timedelta(hours=38)},
    ]
    s = format_models_summary(models)
    assert "Active" in s
    assert "Warned" in s
    assert "SPY" in s
    assert "QQQ" in s


def test_format_status_expanded_includes_all_sections():
    s = format_status_expanded(
        account={"equity": 100000.0, "cash": 50000.0, "buying_power": 200000.0},
        positions_count=3, exposure_pct=0.284,
        trading_stats={"trades_today": 12, "win_rate": 0.67, "pnl_today": 312.4},
        model_counts={"active": 8, "warned": 1, "disabled": 0},
        freshness_summary="All data < 2m old | All models < 24h",
        active_halts_count=0,
        last_retrain_iso="2026-04-13T01:00:00+00:00",
        now=datetime(2026, 4, 13, 15, 0, tzinfo=timezone.utc),
    )
    assert "Account" in s
    assert "Positions" in s
    assert "Models" in s
    assert "2026-04-13" in s
