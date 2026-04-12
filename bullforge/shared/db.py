from __future__ import annotations

import json
import os
from pathlib import Path
from typing import List, Optional

import asyncpg

from shared.config import Settings
from shared.models import Bar, FeatureRow, Label


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
