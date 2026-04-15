import math
import pytest

from services.backtest_service.metrics import (
    sharpe_ratio,
    sortino_ratio,
    max_drawdown,
    summarize_trades,
)


def test_sharpe_zero_when_returns_constant():
    assert sharpe_ratio([0.0, 0.0, 0.0], periods_per_year=252) == 0.0


def test_sharpe_positive_for_uptrend():
    rets = [0.001, 0.002, 0.001, 0.0015]
    assert sharpe_ratio(rets, periods_per_year=252) > 0


def test_sortino_only_penalizes_downside():
    up = [0.01, 0.02, 0.01]
    down = [0.01, -0.02, 0.01]
    assert sortino_ratio(up, periods_per_year=252) > sortino_ratio(down, periods_per_year=252)


def test_max_drawdown_basic():
    equity = [100.0, 110.0, 105.0, 120.0, 90.0, 100.0]
    assert max_drawdown(equity) == pytest.approx(-0.25)


def test_max_drawdown_zero_when_monotonic():
    equity = [100.0, 101.0, 102.0]
    assert max_drawdown(equity) == 0.0


def test_summarize_trades_metrics():
    trades = [
        {"pnl": 100.0},
        {"pnl": -50.0},
        {"pnl": 75.0},
        {"pnl": -25.0},
    ]
    s = summarize_trades(trades)
    assert s["count"] == 4
    assert s["wins"] == 2
    assert s["losses"] == 2
    assert s["hit_rate"] == 0.5
    assert s["total_pnl"] == 100.0
    assert s["avg_win"] == pytest.approx(87.5)
    assert s["avg_loss"] == pytest.approx(-37.5)
    assert s["profit_factor"] == pytest.approx(175.0 / 75.0)


def test_summarize_trades_empty():
    s = summarize_trades([])
    assert s["count"] == 0
    assert s["hit_rate"] == 0.0
    assert s["profit_factor"] == 0.0
