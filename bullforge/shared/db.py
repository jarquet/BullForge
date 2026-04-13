from __future__ import annotations

import json
import os
from pathlib import Path
from typing import List, Optional

import asyncpg

from shared.config import Settings
from datetime import datetime

from shared.models import Bar, FeatureRow, Label, ModelMetadata, Prediction, Order, Trade, ModelStatus


class Database:
    def __init__(self, settings: Settings):
        self._settings = settings
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(
            host=self._settings.timescaledb_host,
            port=self._settings.timescaledb_port,
            user=self._settings.timescaledb_user,
            password=self._settings.timescaledb_password,
            database=self._settings.timescaledb_db,
            min_size=2,
            max_size=10,
        )
        await self._run_migrations()

    async def disconnect(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def _run_migrations(self) -> None:
        migrations_dir = Path(__file__).parent.parent / "migrations"
        sql_files = sorted(migrations_dir.glob("*.sql"))
        async with self.pool.acquire() as conn:
            for sql_file in sql_files:
                sql = sql_file.read_text()
                await conn.execute(sql)

    async def insert_bar(self, bar: Bar) -> None:
        table = "bars_1m" if bar.timeframe == "1m" else "bars_daily"
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {table} (symbol, timestamp, open, high, low, close, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, timestamp)
                DO UPDATE SET open = $3, high = $4, low = $5, close = $6, volume = $7
                """,
                bar.symbol,
                bar.timestamp,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
            )

    async def insert_bars(self, bars: List[Bar]) -> None:
        if not bars:
            return
        table = "bars_1m" if bars[0].timeframe == "1m" else "bars_daily"
        async with self.pool.acquire() as conn:
            await conn.executemany(
                f"""
                INSERT INTO {table} (symbol, timestamp, open, high, low, close, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, timestamp)
                DO UPDATE SET open = $3, high = $4, low = $5, close = $6, volume = $7
                """,
                [
                    (b.symbol, b.timestamp, b.open, b.high, b.low, b.close, b.volume)
                    for b in bars
                ],
            )

    async def query_bars(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 100,
        since: Optional[str] = None,
    ) -> List[Bar]:
        table = "bars_1m" if timeframe == "1m" else "bars_daily"
        query = f"""
            SELECT symbol, timestamp, open, high, low, close, volume
            FROM {table}
            WHERE symbol = $1
        """
        args = [symbol]
        if since:
            query += " AND timestamp >= $2"
            args.append(since)
        query += " ORDER BY timestamp DESC LIMIT $" + str(len(args) + 1)
        args.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [
            Bar(
                symbol=r["symbol"],
                timestamp=r["timestamp"],
                open=r["open"],
                high=r["high"],
                low=r["low"],
                close=r["close"],
                volume=r["volume"],
                timeframe=timeframe,
            )
            for r in rows
        ]

    async def insert_feature_row(self, row: FeatureRow) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO features (symbol, timestamp, timeframe, features)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (symbol, timestamp, timeframe)
                DO UPDATE SET features = $4
                """,
                row.symbol,
                row.timestamp,
                row.timeframe,
                json.dumps(row.features),
            )

    async def insert_feature_rows(self, rows: List[FeatureRow]) -> None:
        if not rows:
            return
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO features (symbol, timestamp, timeframe, features)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (symbol, timestamp, timeframe)
                DO UPDATE SET features = $4
                """,
                [
                    (r.symbol, r.timestamp, r.timeframe, json.dumps(r.features))
                    for r in rows
                ],
            )

    async def query_feature_rows(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 100,
    ) -> List[FeatureRow]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, timestamp, timeframe, features
                FROM features
                WHERE symbol = $1 AND timeframe = $2
                ORDER BY timestamp DESC
                LIMIT $3
                """,
                symbol,
                timeframe,
                limit,
            )
        return [
            FeatureRow(
                symbol=r["symbol"],
                timestamp=r["timestamp"],
                timeframe=r["timeframe"],
                features=json.loads(r["features"]),
            )
            for r in rows
        ]

    async def insert_label(self, label: Label) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO labels (symbol, timestamp, timeframe, horizon_minutes,
                                    forward_return, direction)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (symbol, timestamp, timeframe, horizon_minutes)
                DO UPDATE SET forward_return = $5, direction = $6
                """,
                label.symbol,
                label.timestamp,
                label.timeframe,
                label.horizon_minutes,
                label.forward_return,
                label.direction,
            )

    async def insert_labels(self, labels: List[Label]) -> None:
        if not labels:
            return
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO labels (symbol, timestamp, timeframe, horizon_minutes,
                                    forward_return, direction)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (symbol, timestamp, timeframe, horizon_minutes)
                DO UPDATE SET forward_return = $5, direction = $6
                """,
                [
                    (
                        l.symbol, l.timestamp, l.timeframe,
                        l.horizon_minutes, l.forward_return, l.direction,
                    )
                    for l in labels
                ],
            )

    async def query_labels(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        limit: int = 100,
    ) -> List[Label]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, timestamp, timeframe, horizon_minutes,
                       forward_return, direction
                FROM labels
                WHERE symbol = $1 AND timeframe = $2 AND horizon_minutes = $3
                ORDER BY timestamp DESC
                LIMIT $4
                """,
                symbol,
                timeframe,
                horizon_minutes,
                limit,
            )
        return [
            Label(
                symbol=r["symbol"],
                timestamp=r["timestamp"],
                timeframe=r["timeframe"],
                horizon_minutes=r["horizon_minutes"],
                forward_return=r["forward_return"],
                direction=r["direction"],
            )
            for r in rows
        ]

    async def query_training_data(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        limit: int = 10000,
    ) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT f.timestamp, f.features, l.direction, l.forward_return
                FROM features f
                INNER JOIN labels l
                    ON f.symbol = l.symbol
                    AND f.timestamp = l.timestamp
                    AND f.timeframe = l.timeframe
                WHERE f.symbol = $1 AND f.timeframe = $2 AND l.horizon_minutes = $3
                ORDER BY f.timestamp ASC
                LIMIT $4
                """,
                symbol,
                timeframe,
                horizon_minutes,
                limit,
            )
        return [dict(r) for r in rows]

    async def insert_model_metadata(self, meta: ModelMetadata) -> int:
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO model_registry
                    (symbol, timeframe, horizon_minutes, model_type, artifact_path,
                     metrics, feature_columns, trained_at, train_samples, test_samples,
                     is_active)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING id
                """,
                meta.symbol,
                meta.timeframe,
                meta.horizon_minutes,
                meta.model_type,
                meta.artifact_path,
                json.dumps(meta.metrics),
                json.dumps(meta.feature_columns),
                meta.trained_at,
                meta.train_samples,
                meta.test_samples,
                meta.is_active,
            )
        return row_id

    async def query_active_model(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
    ) -> Optional[ModelMetadata]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, symbol, timeframe, horizon_minutes, model_type,
                       artifact_path, metrics, feature_columns, trained_at,
                       train_samples, test_samples, is_active
                FROM model_registry
                WHERE symbol = $1 AND timeframe = $2
                    AND horizon_minutes = $3 AND is_active = TRUE
                ORDER BY trained_at DESC
                LIMIT 1
                """,
                symbol,
                timeframe,
                horizon_minutes,
            )
        if row is None:
            return None
        return ModelMetadata(
            id=row["id"],
            symbol=row["symbol"],
            timeframe=row["timeframe"],
            horizon_minutes=row["horizon_minutes"],
            model_type=row["model_type"],
            artifact_path=row["artifact_path"],
            metrics=json.loads(row["metrics"]),
            feature_columns=json.loads(row["feature_columns"]),
            trained_at=row["trained_at"],
            train_samples=row["train_samples"],
            test_samples=row["test_samples"],
            is_active=row["is_active"],
        )

    async def query_all_active_models(self) -> List[ModelMetadata]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, symbol, timeframe, horizon_minutes, model_type,
                       artifact_path, metrics, feature_columns, trained_at,
                       train_samples, test_samples, is_active
                FROM model_registry
                WHERE is_active = TRUE
                ORDER BY symbol, timeframe, horizon_minutes
                """
            )
        return [
            ModelMetadata(
                id=r["id"],
                symbol=r["symbol"],
                timeframe=r["timeframe"],
                horizon_minutes=r["horizon_minutes"],
                model_type=r["model_type"],
                artifact_path=r["artifact_path"],
                metrics=json.loads(r["metrics"]),
                feature_columns=json.loads(r["feature_columns"]),
                trained_at=r["trained_at"],
                train_samples=r["train_samples"],
                test_samples=r["test_samples"],
                is_active=r["is_active"],
            )
            for r in rows
        ]

    async def deactivate_models(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE model_registry
                SET is_active = FALSE
                WHERE symbol = $1 AND timeframe = $2
                    AND horizon_minutes = $3 AND is_active = TRUE
                """,
                symbol,
                timeframe,
                horizon_minutes,
            )

    async def insert_prediction(self, pred: Prediction) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO predictions
                    (symbol, timestamp, timeframe, horizon_minutes,
                     model_id, predicted_direction, confidence)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, timestamp, timeframe, horizon_minutes)
                DO UPDATE SET model_id = $5, predicted_direction = $6, confidence = $7
                """,
                pred.symbol,
                pred.timestamp,
                pred.timeframe,
                pred.horizon_minutes,
                pred.model_id,
                pred.predicted_direction,
                pred.confidence,
            )

    async def insert_predictions(self, preds: List[Prediction]) -> None:
        if not preds:
            return
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO predictions
                    (symbol, timestamp, timeframe, horizon_minutes,
                     model_id, predicted_direction, confidence)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, timestamp, timeframe, horizon_minutes)
                DO UPDATE SET model_id = $5, predicted_direction = $6, confidence = $7
                """,
                [
                    (
                        p.symbol, p.timestamp, p.timeframe,
                        p.horizon_minutes, p.model_id,
                        p.predicted_direction, p.confidence,
                    )
                    for p in preds
                ],
            )

    async def query_predictions(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        limit: int = 100,
    ) -> List[Prediction]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, timestamp, timeframe, horizon_minutes,
                       model_id, predicted_direction, confidence
                FROM predictions
                WHERE symbol = $1 AND timeframe = $2 AND horizon_minutes = $3
                ORDER BY timestamp DESC
                LIMIT $4
                """,
                symbol,
                timeframe,
                horizon_minutes,
                limit,
            )
        return [
            Prediction(
                symbol=r["symbol"],
                timestamp=r["timestamp"],
                timeframe=r["timeframe"],
                horizon_minutes=r["horizon_minutes"],
                model_id=r["model_id"],
                predicted_direction=r["predicted_direction"],
                confidence=r["confidence"],
            )
            for r in rows
        ]

    async def insert_order(self, order: Order) -> int:
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO orders
                    (symbol, side, quantity, order_type, submitted_at,
                     signal_strength, alpaca_order_id, status,
                     filled_at, filled_price, filled_quantity, limit_price)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                RETURNING id
                """,
                order.symbol,
                order.side,
                order.quantity,
                order.order_type,
                order.submitted_at,
                order.signal_strength,
                order.alpaca_order_id,
                order.status,
                order.filled_at,
                order.filled_price,
                order.filled_quantity,
                order.limit_price,
            )
        return row_id

    async def update_order_status(
        self,
        order_id: int,
        status: str,
        filled_at: Optional[datetime] = None,
        filled_price: Optional[float] = None,
        filled_quantity: Optional[float] = None,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE orders
                SET status = $2,
                    filled_at = COALESCE($3, filled_at),
                    filled_price = COALESCE($4, filled_price),
                    filled_quantity = COALESCE($5, filled_quantity)
                WHERE id = $1
                """,
                order_id,
                status,
                filled_at,
                filled_price,
                filled_quantity,
            )

    async def query_orders(
        self,
        symbol: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> list[Order]:
        query = """SELECT id, symbol, side, quantity, order_type, submitted_at,
                          signal_strength, alpaca_order_id, status,
                          filled_at, filled_price, filled_quantity, limit_price
                   FROM orders WHERE TRUE"""
        args: list = []
        idx = 1

        if symbol is not None:
            query += f" AND symbol = ${idx}"
            args.append(symbol)
            idx += 1

        if status is not None:
            query += f" AND status = ${idx}"
            args.append(status)
            idx += 1

        query += f" ORDER BY submitted_at DESC LIMIT ${idx}"
        args.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [
            Order(
                id=r["id"],
                symbol=r["symbol"],
                side=r["side"],
                quantity=r["quantity"],
                order_type=r["order_type"],
                submitted_at=r["submitted_at"],
                signal_strength=r["signal_strength"],
                alpaca_order_id=r["alpaca_order_id"],
                status=r["status"],
                filled_at=r["filled_at"],
                filled_price=r["filled_price"],
                filled_quantity=r["filled_quantity"],
                limit_price=r["limit_price"],
            )
            for r in rows
        ]

    # ── Trade methods ──────────────────────────────────────────────

    async def insert_trade(self, trade: Trade) -> int:
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO trades
                    (symbol, side, entry_time, exit_time, entry_price, exit_price,
                     quantity, pnl, hold_duration_seconds, entry_order_id, exit_order_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING id
                """,
                trade.symbol,
                trade.side,
                trade.entry_time,
                trade.exit_time,
                trade.entry_price,
                trade.exit_price,
                trade.quantity,
                trade.pnl,
                trade.hold_duration_seconds,
                trade.entry_order_id,
                trade.exit_order_id,
            )
        return row_id

    async def update_trade_exit(
        self,
        trade_id: int,
        exit_time: datetime,
        exit_price: float,
        exit_order_id: int,
        pnl: float,
        hold_duration_seconds: int,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE trades
                SET exit_time = $2, exit_price = $3, exit_order_id = $4,
                    pnl = $5, hold_duration_seconds = $6
                WHERE id = $1
                """,
                trade_id,
                exit_time,
                exit_price,
                exit_order_id,
                pnl,
                hold_duration_seconds,
            )

    async def get_open_trade(self, symbol: str) -> Optional[Trade]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, symbol, side, entry_time, exit_time, entry_price,
                       exit_price, quantity, pnl, hold_duration_seconds,
                       entry_order_id, exit_order_id
                FROM trades
                WHERE symbol = $1 AND exit_time IS NULL
                ORDER BY entry_time DESC
                LIMIT 1
                """,
                symbol,
            )
        if row is None:
            return None
        return Trade(
            id=row["id"],
            symbol=row["symbol"],
            side=row["side"],
            entry_time=row["entry_time"],
            exit_time=row["exit_time"],
            entry_price=row["entry_price"],
            exit_price=row["exit_price"],
            quantity=row["quantity"],
            pnl=row["pnl"],
            hold_duration_seconds=row["hold_duration_seconds"],
            entry_order_id=row["entry_order_id"],
            exit_order_id=row["exit_order_id"],
        )

    async def query_closed_trades(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[Trade]:
        query = """SELECT id, symbol, side, entry_time, exit_time, entry_price,
                          exit_price, quantity, pnl, hold_duration_seconds,
                          entry_order_id, exit_order_id
                   FROM trades WHERE exit_time IS NOT NULL"""
        args: list = []
        idx = 1

        if symbol is not None:
            query += f" AND symbol = ${idx}"
            args.append(symbol)
            idx += 1

        if since is not None:
            query += f" AND exit_time >= ${idx}"
            args.append(since)
            idx += 1

        query += f" ORDER BY exit_time DESC LIMIT ${idx}"
        args.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [
            Trade(
                id=r["id"],
                symbol=r["symbol"],
                side=r["side"],
                entry_time=r["entry_time"],
                exit_time=r["exit_time"],
                entry_price=r["entry_price"],
                exit_price=r["exit_price"],
                quantity=r["quantity"],
                pnl=r["pnl"],
                hold_duration_seconds=r["hold_duration_seconds"],
                entry_order_id=r["entry_order_id"],
                exit_order_id=r["exit_order_id"],
            )
            for r in rows
        ]

    # ── Model status methods ───────────────────────────────────────

    async def upsert_model_status(
        self, model_id: int, status: str, reason: str,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO model_status (model_id, status, reason, updated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (model_id)
                DO UPDATE SET status = $2, reason = $3, updated_at = NOW()
                """,
                model_id,
                status,
                reason,
            )

    async def get_all_model_statuses(self) -> list[ModelStatus]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT model_id, status, reason, updated_at
                FROM model_status
                ORDER BY model_id
                """
            )
        return [
            ModelStatus(
                model_id=r["model_id"],
                status=r["status"],
                reason=r["reason"] or "",
                updated_at=r["updated_at"],
            )
            for r in rows
        ]

    # ── Prediction outcome methods ─────────────────────────────────

    async def query_prediction_outcomes(
        self, model_id: int, limit: int = 100,
    ) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    p.predicted_direction,
                    p.confidence,
                    b_exit.close - b_entry.close AS price_move,
                    CASE
                        WHEN p.predicted_direction = 1
                             AND b_exit.close > b_entry.close THEN TRUE
                        WHEN p.predicted_direction = -1
                             AND b_exit.close < b_entry.close THEN TRUE
                        WHEN p.predicted_direction = 0
                             AND ABS(b_exit.close - b_entry.close)
                                 / NULLIF(b_entry.close, 0) < 0.001 THEN TRUE
                        ELSE FALSE
                    END AS is_correct
                FROM predictions p
                JOIN bars_1m b_entry
                    ON b_entry.symbol = p.symbol
                    AND b_entry.timestamp = p.timestamp
                JOIN bars_1m b_exit
                    ON b_exit.symbol = p.symbol
                    AND b_exit.timestamp = p.timestamp
                        + (p.horizon_minutes * interval '1 minute')
                WHERE p.model_id = $1
                ORDER BY p.timestamp DESC
                LIMIT $2
                """,
                model_id,
                limit,
            )
        return [dict(r) for r in rows]

    # ── Halt event methods ─────────────────────────────────────────

    async def insert_halt_event(
        self, tier: int, reason: str, direction: Optional[str] = None,
    ) -> int:
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO halt_events (tier, reason, direction)
                VALUES ($1, $2, $3)
                RETURNING id
                """,
                tier,
                reason,
                direction,
            )
        return row_id

    async def get_active_halts(self) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, tier, reason, direction, activated_at
                FROM halt_events
                WHERE resolved_at IS NULL
                ORDER BY activated_at DESC
                """
            )
        return [dict(r) for r in rows]

    async def resolve_halt_event(self, halt_id: int) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE halt_events
                SET resolved_at = NOW()
                WHERE id = $1
                """,
                halt_id,
            )

    # ── Metric snapshot methods ────────────────────────────────────

    async def insert_metric_snapshot(
        self,
        category: str,
        scope: str,
        window: str,
        metrics: dict,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO metric_snapshots (category, scope, "window", metrics)
                VALUES ($1, $2, $3, $4)
                """,
                category,
                scope,
                window,
                json.dumps(metrics),
            )

    async def query_latest_metric_snapshot(
        self,
        category: str,
        scope: str,
        window: str,
    ) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, timestamp, category, scope, "window", metrics
                FROM metric_snapshots
                WHERE category = $1 AND scope = $2 AND "window" = $3
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                category,
                scope,
                window,
            )
        if row is None:
            return None
        return {
            "id": row["id"],
            "timestamp": row["timestamp"],
            "category": row["category"],
            "scope": row["scope"],
            "window": row["window"],
            "metrics": json.loads(row["metrics"]),
        }

    async def query_metric_snapshots(
        self,
        category: str,
        scope: str,
        window: str,
        limit: int = 60,
    ) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, timestamp, category, scope, "window", metrics
                FROM metric_snapshots
                WHERE category = $1 AND scope = $2 AND "window" = $3
                ORDER BY timestamp DESC
                LIMIT $4
                """,
                category,
                scope,
                window,
                limit,
            )
        return [
            {
                "id": r["id"],
                "timestamp": r["timestamp"],
                "category": r["category"],
                "scope": r["scope"],
                "window": r["window"],
                "metrics": json.loads(r["metrics"]),
            }
            for r in rows
        ]

    async def query_halt_history(self, limit: int = 50) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, tier, reason, direction, activated_at, resolved_at
                FROM halt_events
                ORDER BY activated_at DESC
                LIMIT $1
                """,
                limit,
            )
        return [dict(r) for r in rows]
