from __future__ import annotations

import math
from typing import List

from shared.models import Trade


class MetricsComputer:
    def compute_trading_metrics(self, trades: List[Trade]) -> dict:
        if not trades:
            return {
                "total_trades": 0,
                "win_rate": 0.0,
                "total_pnl": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "profit_factor": 0.0,
                "sharpe_ratio": 0.0,
                "avg_hold_duration_seconds": 0.0,
                "best_trade": 0.0,
                "worst_trade": 0.0,
            }

        pnls = [t.pnl for t in trades if t.pnl is not None]
        if not pnls:
            return {
                "total_trades": len(trades),
                "win_rate": 0.0,
                "total_pnl": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "profit_factor": 0.0,
                "sharpe_ratio": 0.0,
                "avg_hold_duration_seconds": 0.0,
                "best_trade": 0.0,
                "worst_trade": 0.0,
            }

        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]

        total_pnl = sum(pnls)
        win_rate = len(wins) / len(pnls) if pnls else 0.0
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0

        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        if gross_loss > 0:
            profit_factor = gross_profit / gross_loss
        elif gross_profit > 0:
            profit_factor = float("inf")
        else:
            profit_factor = 0.0

        # Sharpe ratio: mean(pnl) / std(pnl)
        if len(pnls) >= 2:
            mean_pnl = total_pnl / len(pnls)
            variance = sum((p - mean_pnl) ** 2 for p in pnls) / (len(pnls) - 1)
            std_pnl = math.sqrt(variance)
            sharpe_ratio = mean_pnl / std_pnl if std_pnl > 0 else 0.0
        else:
            sharpe_ratio = 0.0

        durations = [
            t.hold_duration_seconds
            for t in trades
            if t.hold_duration_seconds is not None
        ]
        avg_hold = sum(durations) / len(durations) if durations else 0.0

        return {
            "total_trades": len(pnls),
            "win_rate": win_rate,
            "total_pnl": total_pnl,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "profit_factor": profit_factor,
            "sharpe_ratio": sharpe_ratio,
            "avg_hold_duration_seconds": avg_hold,
            "best_trade": max(pnls),
            "worst_trade": min(pnls),
        }

    def compute_account_metrics(
        self, account: dict, equity_history: List[float],
    ) -> dict:
        equity = account.get("equity", 0.0)
        peak = max(equity_history) if equity_history else equity
        current_drawdown = (peak - equity) / peak if peak > 0 else 0.0

        # Max drawdown from equity history
        max_dd = 0.0
        running_peak = 0.0
        for eq in equity_history:
            running_peak = max(running_peak, eq)
            if running_peak > 0:
                dd = (running_peak - eq) / running_peak
                max_dd = max(max_dd, dd)

        return {
            "equity": equity,
            "buying_power": account.get("buying_power", 0.0),
            "cash": account.get("cash", 0.0),
            "portfolio_value": account.get("portfolio_value", 0.0),
            "peak_equity": peak,
            "current_drawdown": current_drawdown,
            "max_drawdown": max_dd,
        }

    def compute_direction_metrics(
        self, trades: List[Trade], side: str,
    ) -> dict:
        filtered = [t for t in trades if t.side == side]
        return self.compute_trading_metrics(filtered)
