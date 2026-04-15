from __future__ import annotations

import math
from statistics import mean, pstdev


def sharpe_ratio(returns: list[float], *, periods_per_year: int) -> float:
    if len(returns) < 2:
        return 0.0
    sd = pstdev(returns)
    if sd == 0:
        return 0.0
    return (mean(returns) / sd) * math.sqrt(periods_per_year)


def sortino_ratio(returns: list[float], *, periods_per_year: int) -> float:
    if len(returns) < 2:
        return 0.0
    downside = [r for r in returns if r < 0]
    if not downside:
        # No losing periods — return a large positive value if mean > 0, else 0
        m = mean(returns)
        return math.sqrt(periods_per_year) * m if m > 0 else 0.0
    dd_sd = math.sqrt(sum(r * r for r in downside) / len(downside))
    if dd_sd == 0:
        return 0.0
    return (mean(returns) / dd_sd) * math.sqrt(periods_per_year)


def max_drawdown(equity_curve: list[float]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    worst = 0.0
    for e in equity_curve:
        if e > peak:
            peak = e
        if peak > 0:
            dd = (e - peak) / peak
            if dd < worst:
                worst = dd
    return worst


def summarize_trades(trades: list[dict]) -> dict:
    if not trades:
        return {
            "count": 0, "wins": 0, "losses": 0,
            "hit_rate": 0.0, "total_pnl": 0.0,
            "avg_win": 0.0, "avg_loss": 0.0, "profit_factor": 0.0,
        }
    pnls = [t["pnl"] for t in trades if t.get("pnl") is not None]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    return {
        "count": len(pnls),
        "wins": len(wins),
        "losses": len(losses),
        "hit_rate": len(wins) / len(pnls) if pnls else 0.0,
        "total_pnl": sum(pnls),
        "avg_win": mean(wins) if wins else 0.0,
        "avg_loss": mean(losses) if losses else 0.0,
        "profit_factor": gross_win / gross_loss if gross_loss > 0 else 0.0,
    }
