"""Custom exceptions for the backtest service."""
from __future__ import annotations


class BacktestCancelled(Exception):
    """Raised inside BacktestEngine.run when a cancellation has been requested.

    The engine catches this, marks the backtest_runs row as status='cancelled',
    then re-raises so the worker can skip `mark_completed` / `mark_failed`
    (the backtest_jobs row was already flipped to 'cancelled' by whoever
    requested cancellation).
    """
