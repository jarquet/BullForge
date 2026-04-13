import pytest
from datetime import datetime, timezone
from shared.models import Trade
from services.monitoring_service.metrics import MetricsComputer


def _closed_trade(symbol="SPY", side="long", pnl=50.0, hold_secs=3600):
    return Trade(
        symbol=symbol,
        side=side,
        entry_time=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        entry_price=420.0,
        quantity=10.0,
        entry_order_id=1,
        exit_time=datetime(2026, 4, 12, 15, 0, 0, tzinfo=timezone.utc),
        exit_price=420.0 + pnl / 10.0,
        pnl=pnl,
        hold_duration_seconds=hold_secs,
        exit_order_id=2,
        id=1,
    )


@pytest.fixture
def computer():
    return MetricsComputer()


def test_trading_metrics_basic(computer):
    trades = [
        _closed_trade(pnl=100.0),
        _closed_trade(pnl=-30.0),
        _closed_trade(pnl=50.0),
    ]
    m = computer.compute_trading_metrics(trades)

    assert m["total_trades"] == 3
    assert m["win_rate"] == pytest.approx(2 / 3)
    assert m["total_pnl"] == pytest.approx(120.0)
    assert m["avg_win"] == pytest.approx(75.0)
    assert m["avg_loss"] == pytest.approx(-30.0)
    assert m["profit_factor"] == pytest.approx(150.0 / 30.0)


def test_trading_metrics_all_wins(computer):
    trades = [_closed_trade(pnl=50.0), _closed_trade(pnl=25.0)]
    m = computer.compute_trading_metrics(trades)

    assert m["win_rate"] == 1.0
    assert m["profit_factor"] == float("inf")
    assert m["avg_loss"] == 0.0


def test_trading_metrics_all_losses(computer):
    trades = [_closed_trade(pnl=-50.0), _closed_trade(pnl=-25.0)]
    m = computer.compute_trading_metrics(trades)

    assert m["win_rate"] == 0.0
    assert m["profit_factor"] == 0.0
    assert m["avg_win"] == 0.0


def test_trading_metrics_empty(computer):
    m = computer.compute_trading_metrics([])

    assert m["total_trades"] == 0
    assert m["win_rate"] == 0.0
    assert m["total_pnl"] == 0.0


def test_trading_metrics_hold_duration(computer):
    trades = [
        _closed_trade(hold_secs=3600),
        _closed_trade(hold_secs=7200),
    ]
    m = computer.compute_trading_metrics(trades)

    assert m["avg_hold_duration_seconds"] == pytest.approx(5400.0)


def test_trading_metrics_sharpe_ratio(computer):
    trades = [
        _closed_trade(pnl=100.0),
        _closed_trade(pnl=-30.0),
        _closed_trade(pnl=50.0),
        _closed_trade(pnl=80.0),
    ]
    m = computer.compute_trading_metrics(trades)

    # Sharpe = mean(pnl) / std(pnl) — verify it's a reasonable number
    assert "sharpe_ratio" in m
    assert m["sharpe_ratio"] > 0


def test_trading_metrics_single_trade(computer):
    trades = [_closed_trade(pnl=50.0)]
    m = computer.compute_trading_metrics(trades)

    assert m["total_trades"] == 1
    assert m["sharpe_ratio"] == 0.0  # can't compute std from 1 trade


def test_account_metrics(computer):
    account = {
        "equity": 105000.0,
        "buying_power": 200000.0,
        "cash": 95000.0,
        "portfolio_value": 105000.0,
    }
    equity_history = [100000.0, 102000.0, 105000.0, 103000.0, 105000.0]

    m = computer.compute_account_metrics(account, equity_history)

    assert m["equity"] == 105000.0
    assert m["buying_power"] == 200000.0
    assert m["peak_equity"] == 105000.0
    assert m["current_drawdown"] == pytest.approx(0.0)


def test_account_metrics_with_drawdown(computer):
    account = {
        "equity": 90000.0,
        "buying_power": 180000.0,
        "cash": 90000.0,
        "portfolio_value": 90000.0,
    }
    equity_history = [100000.0, 102000.0, 95000.0, 90000.0]

    m = computer.compute_account_metrics(account, equity_history)

    assert m["peak_equity"] == 102000.0
    assert m["current_drawdown"] == pytest.approx(
        (102000.0 - 90000.0) / 102000.0
    )
    assert m["max_drawdown"] == pytest.approx(
        (102000.0 - 90000.0) / 102000.0
    )


def test_direction_metrics(computer):
    trades = [
        _closed_trade(side="long", pnl=100.0),
        _closed_trade(side="long", pnl=-20.0),
        _closed_trade(side="short", pnl=50.0),
    ]
    long_m = computer.compute_direction_metrics(trades, "long")
    short_m = computer.compute_direction_metrics(trades, "short")

    assert long_m["total_trades"] == 2
    assert long_m["total_pnl"] == pytest.approx(80.0)
    assert short_m["total_trades"] == 1
    assert short_m["total_pnl"] == pytest.approx(50.0)
