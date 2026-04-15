from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from shared.db import Database


class BacktestResultsDB:
    """Thin async wrapper around the backtest_* tables."""

    def __init__(self, db: Database):
        self._db = db

    async def insert_run(
        self, *,
        name: str,
        symbols: list[str],
        timeframe: str,
        horizons: list[int],
        range_start: datetime,
        range_end: datetime,
        train_days: int,
        test_days: int,
        embargo_minutes: int,
        mode: str,
        config: dict,
    ) -> int:
        async with self._db.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO backtest_runs (
                    name, symbols, timeframe, horizons,
                    range_start, range_end,
                    train_days, test_days, embargo_min, mode, config_json
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING id
                """,
                name, symbols, timeframe, horizons,
                range_start, range_end,
                train_days, test_days, embargo_minutes, mode,
                json.dumps(config),
            )
        return row["id"]

    async def insert_fold(
        self, *, run_id: int, fold_index: int,
        train_start: datetime, train_end: datetime,
        test_start: datetime, test_end: datetime,
    ) -> int:
        async with self._db.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO backtest_folds (
                    run_id, fold_index,
                    train_start, train_end, test_start, test_end
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id
                """,
                run_id, fold_index,
                train_start, train_end, test_start, test_end,
            )
        return row["id"]

    async def insert_trade(
        self, *, run_id: int, fold_id: int,
        symbol: str, side: str,
        entry_time: datetime, entry_price: float, quantity: float,
        exit_time: Optional[datetime], exit_price: Optional[float],
        pnl: Optional[float], signal_strength: float,
    ) -> None:
        async with self._db.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO backtest_trades (
                    run_id, fold_id, symbol, side,
                    entry_time, entry_price, quantity,
                    exit_time, exit_price, pnl, signal_strength
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                """,
                run_id, fold_id, symbol, side,
                entry_time, entry_price, quantity,
                exit_time, exit_price, pnl, signal_strength,
            )

    async def insert_equity_point(
        self, *, run_id: int, fold_id: int,
        timestamp: datetime, equity: float, cash: float,
    ) -> None:
        async with self._db.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO backtest_equity (run_id, fold_id, timestamp, equity, cash)
                VALUES ($1,$2,$3,$4,$5)
                ON CONFLICT (run_id, fold_id, timestamp) DO UPDATE
                SET equity = EXCLUDED.equity, cash = EXCLUDED.cash
                """,
                run_id, fold_id, timestamp, equity, cash,
            )

    async def update_fold_metrics(
        self, *, fold_id: int, metrics: dict,
    ) -> None:
        async with self._db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE backtest_folds SET metrics_json = $1 WHERE id = $2",
                json.dumps(metrics), fold_id,
            )

    async def finish_run(self, *, run_id: int, status: str) -> None:
        async with self._db.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE backtest_runs
                SET finished_at = NOW(), status = $1
                WHERE id = $2
                """,
                status, run_id,
            )
