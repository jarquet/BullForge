from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

import asyncpg

from shared.config import Settings
from shared.models import Bar


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
