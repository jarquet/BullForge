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
        custom_name: Optional[str] = None,
    ) -> int:
        async with self._db.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO backtest_runs (
                    name, custom_name, symbols, timeframe, horizons,
                    range_start, range_end,
                    train_days, test_days, embargo_min, mode, config_json
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                RETURNING id
                """,
                name, custom_name, symbols, timeframe, horizons,
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

    # ------------------------------ reads ------------------------------

    _SORTABLE = {
        "finished_at", "started_at", "name",
        "total_pnl", "sharpe", "max_dd",
    }

    async def list_runs(
        self, *,
        status: str = "all",
        q: Optional[str] = None,
        sort: str = "finished_at",
        order: str = "desc",
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]:
        if sort not in self._SORTABLE:
            sort = "finished_at"
        order = "asc" if order.lower() == "asc" else "desc"

        sql = f"""
            SELECT r.id, r.name, r.custom_name, r.symbols, r.range_start, r.range_end,
                   r.status, r.started_at, r.finished_at,
                   r.tags, r.notes,
                   COALESCE(t.n_trades, 0)                AS n_trades,
                   COALESCE(t.total_pnl, 0)               AS total_pnl,
                   COALESCE(t.hit_rate, 0)                AS hit_rate,
                   COALESCE(e.max_dd, 0)                  AS max_dd,
                   COALESCE(e.sharpe, 0)                  AS sharpe
            FROM backtest_runs r
            LEFT JOIN LATERAL (
                SELECT COUNT(*)                                  AS n_trades,
                       COALESCE(SUM(pnl), 0)                     AS total_pnl,
                       AVG(CASE WHEN pnl > 0 THEN 1.0 ELSE 0 END) AS hit_rate
                FROM backtest_trades WHERE run_id = r.id
            ) t ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    MIN(equity / NULLIF(running_max, 0) - 1) AS max_dd,
                    CASE WHEN STDDEV_SAMP(ret) > 0
                         THEN AVG(ret) / STDDEV_SAMP(ret) * SQRT(252)
                         ELSE 0 END AS sharpe
                FROM (
                    SELECT
                        equity,
                        MAX(equity) OVER (ORDER BY timestamp) AS running_max,
                        (equity - LAG(equity) OVER (ORDER BY timestamp))
                            / NULLIF(LAG(equity) OVER (ORDER BY timestamp), 0)
                            AS ret
                    FROM backtest_equity WHERE run_id = r.id
                ) s
            ) e ON TRUE
            WHERE ($1 = 'all' OR r.status = $1)
              AND ($2::text IS NULL OR r.name ILIKE '%' || $2 || '%')
            ORDER BY {sort} {order} NULLS LAST
            LIMIT $3 OFFSET $4
        """
        async with self._db.pool.acquire() as conn:
            rows = await conn.fetch(sql, status, q, limit, offset)
        return [
            {
                "id": r["id"],
                "name": r["name"],
                "custom_name": r["custom_name"],
                "symbols": list(r["symbols"]),
                "range_start": r["range_start"],
                "range_end": r["range_end"],
                "status": r["status"],
                "started_at": r["started_at"],
                "finished_at": r["finished_at"],
                "tags": list(r["tags"] or []),
                "notes": r["notes"],
                "metrics": {
                    "n_trades": r["n_trades"],
                    "total_pnl": float(r["total_pnl"]),
                    "hit_rate": float(r["hit_rate"]),
                    "max_dd": float(r["max_dd"]),
                    "sharpe": float(r["sharpe"]),
                },
            }
            for r in rows
        ]

    async def get_run(self, run_id: int) -> Optional[dict]:
        async with self._db.pool.acquire() as conn:
            header = await conn.fetchrow(
                "SELECT * FROM backtest_runs WHERE id = $1", run_id,
            )
            if header is None:
                return None
            agg = await conn.fetchrow(
                """
                SELECT
                    COALESCE(SUM(pnl), 0)                AS total_pnl,
                    COUNT(*)                             AS n_trades,
                    AVG(CASE WHEN pnl > 0 THEN 1.0 ELSE 0 END) AS hit_rate,
                    COALESCE(SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END)
                             / NULLIF(-SUM(CASE WHEN pnl < 0 THEN pnl ELSE 0 END), 0),
                             0) AS profit_factor
                FROM backtest_trades WHERE run_id = $1
                """,
                run_id,
            )
            eq = await conn.fetchrow(
                """
                SELECT
                    MIN(equity / NULLIF(running_max, 0) - 1) AS max_dd,
                    CASE WHEN STDDEV_SAMP(ret) > 0
                         THEN AVG(ret) / STDDEV_SAMP(ret) * SQRT(252) ELSE 0
                    END AS sharpe,
                    CASE WHEN STDDEV_SAMP(CASE WHEN ret < 0 THEN ret END) > 0
                         THEN AVG(ret) / STDDEV_SAMP(CASE WHEN ret < 0 THEN ret END) * SQRT(252)
                         ELSE 0 END AS sortino
                FROM (
                    SELECT equity,
                           MAX(equity) OVER (ORDER BY timestamp) AS running_max,
                           (equity - LAG(equity) OVER (ORDER BY timestamp))
                             / NULLIF(LAG(equity) OVER (ORDER BY timestamp), 0)
                             AS ret
                    FROM backtest_equity WHERE run_id = $1
                ) s
                """,
                run_id,
            )
            n_folds = await conn.fetchval(
                "SELECT COUNT(*) FROM backtest_folds WHERE run_id = $1", run_id,
            )
        return {
            "id": header["id"],
            "name": header["name"],
            "custom_name": header["custom_name"],
            "symbols": list(header["symbols"]),
            "timeframe": header["timeframe"],
            "horizons": list(header["horizons"]),
            "range_start": header["range_start"],
            "range_end": header["range_end"],
            "train_days": header["train_days"],
            "test_days": header["test_days"],
            "embargo_min": header["embargo_min"],
            "mode": header["mode"],
            "status": header["status"],
            "started_at": header["started_at"],
            "finished_at": header["finished_at"],
            "tags": list(header["tags"] or []),
            "notes": header["notes"],
            "config": json.loads(header["config_json"]) if header["config_json"] else {},
            "metrics": {
                "total_pnl": float(agg["total_pnl"]),
                "n_trades": agg["n_trades"],
                "hit_rate": float(agg["hit_rate"] or 0),
                "profit_factor": float(agg["profit_factor"] or 0),
                "max_dd": float(eq["max_dd"] or 0),
                "sharpe": float(eq["sharpe"] or 0),
                "sortino": float(eq["sortino"] or 0),
                "n_folds": n_folds,
            },
        }

    async def list_folds(self, run_id: int) -> list[dict]:
        async with self._db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT f.id, f.fold_index,
                       f.train_start, f.train_end, f.test_start, f.test_end,
                       f.metrics_json,
                       COALESCE(t.n_trades, 0)  AS n_trades,
                       COALESCE(t.total_pnl, 0) AS total_pnl,
                       COALESCE(e.max_dd, 0)    AS max_dd,
                       COALESCE(e.sharpe, 0)    AS sharpe
                FROM backtest_folds f
                LEFT JOIN LATERAL (
                    SELECT COUNT(*) AS n_trades, COALESCE(SUM(pnl),0) AS total_pnl
                    FROM backtest_trades WHERE fold_id = f.id
                ) t ON TRUE
                LEFT JOIN LATERAL (
                    SELECT MIN(equity / NULLIF(running_max,0) - 1) AS max_dd,
                           CASE WHEN STDDEV_SAMP(ret) > 0
                                THEN AVG(ret)/STDDEV_SAMP(ret)*SQRT(252) ELSE 0 END AS sharpe
                    FROM (
                        SELECT equity,
                               MAX(equity) OVER (ORDER BY timestamp) AS running_max,
                               (equity - LAG(equity) OVER (ORDER BY timestamp))
                                 / NULLIF(LAG(equity) OVER (ORDER BY timestamp),0) AS ret
                        FROM backtest_equity WHERE fold_id = f.id
                    ) s
                ) e ON TRUE
                WHERE f.run_id = $1
                ORDER BY f.fold_index
                """,
                run_id,
            )
        return [
            {
                "id": r["id"],
                "fold_index": r["fold_index"],
                "train_start": r["train_start"],
                "train_end": r["train_end"],
                "test_start": r["test_start"],
                "test_end": r["test_end"],
                "metrics": {
                    "n_trades": r["n_trades"],
                    "total_pnl": float(r["total_pnl"]),
                    "max_dd": float(r["max_dd"]),
                    "sharpe": float(r["sharpe"]),
                },
                "metrics_json": json.loads(r["metrics_json"])
                                if r["metrics_json"] else None,
            }
            for r in rows
        ]

    _TRADE_SORTABLE = {
        "entry_time", "exit_time", "pnl", "symbol", "signal_strength",
    }

    async def list_trades(
        self, run_id: int, *,
        limit: int = 50, offset: int = 0,
        sort: str = "entry_time", order: str = "desc",
    ) -> list[dict]:
        if sort not in self._TRADE_SORTABLE:
            sort = "entry_time"
        order = "asc" if order.lower() == "asc" else "desc"
        async with self._db.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT id, fold_id, symbol, side,
                       entry_time, entry_price,
                       exit_time, exit_price,
                       quantity, pnl, signal_strength
                FROM backtest_trades
                WHERE run_id = $1
                ORDER BY {sort} {order} NULLS LAST
                LIMIT $2 OFFSET $3
                """,
                run_id, limit, offset,
            )
        return [dict(r) for r in rows]

    async def get_equity_curve(
        self, run_id: int, *, fold_id: Optional[int],
    ) -> list[dict]:
        async with self._db.pool.acquire() as conn:
            if fold_id is None:
                rows = await conn.fetch(
                    """
                    SELECT timestamp, equity, cash
                    FROM backtest_equity WHERE run_id = $1
                    ORDER BY timestamp
                    """,
                    run_id,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT timestamp, equity, cash
                    FROM backtest_equity WHERE run_id = $1 AND fold_id = $2
                    ORDER BY timestamp
                    """,
                    run_id, fold_id,
                )
        return [{"t": r["timestamp"], "equity": float(r["equity"]),
                 "cash": float(r["cash"])} for r in rows]
