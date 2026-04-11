# Phase 1: Foundation & Data — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the project skeleton with Docker Compose, TimescaleDB, Redis, and a Data Pipeline service that ingests live and historical market data from Alpaca and stores it in TimescaleDB.

**Architecture:** Monorepo with per-service directories. Docker Compose orchestrates TimescaleDB, Redis, and the Data Pipeline service on node-1. The Data Pipeline uses async Python (asyncio + aiohttp) to stream live ticks via Alpaca websocket and backfill historical bars via REST. All timestamps are UTC.

**Tech Stack:** Python 3.12+, asyncio, alpaca-py, TimescaleDB (PostgreSQL + timescaledb extension), Redis, Docker Compose, pytest, pytest-asyncio

---

## File Structure

```
bullforge/
  docker-compose.yml              — Orchestrates all services and infra
  .env.example                    — Template for environment variables
  .gitignore                      — Git ignore rules

  shared/
    config.py                     — Shared configuration (env var loading, defaults)
    db.py                         — Async database connection pool (asyncpg)
    redis.py                      — Async Redis connection (redis.asyncio)
    models.py                     — Shared data models (dataclasses for Bar, CalendarEvent)

  services/
    data_pipeline/
      Dockerfile                  — Container image for data pipeline
      requirements.txt            — Python dependencies
      main.py                     — Entrypoint: starts all pipeline tasks
      alpaca_client.py            — Alpaca REST + websocket client wrapper
      backfill.py                 — Historical data backfill logic
      stream.py                   — Live websocket tick streaming
      calendar_service.py         — Earnings/Fed date tracking
      health.py                   — Health check endpoint

  migrations/
    001_create_bars.sql           — TimescaleDB schema for bars_1m, bars_daily
    002_create_calendar.sql       — Schema for calendar_events table

  tests/
    conftest.py                   — Shared fixtures (db connection, redis, test config)
    test_config.py                — Tests for shared config
    test_db.py                    — Tests for database connection and schema
    test_models.py                — Tests for shared data models
    test_alpaca_client.py         — Tests for Alpaca client wrapper
    test_backfill.py              — Tests for historical backfill
    test_stream.py                — Tests for live streaming
    test_calendar_service.py      — Tests for calendar event tracking
    test_health.py                — Tests for health endpoint
```

---

## Task 1: Project Scaffolding

**Files:**
- Create: `bullforge/.gitignore`
- Create: `bullforge/.env.example`
- Create: `bullforge/docker-compose.yml`
- Create: `bullforge/shared/__init__.py`
- Create: `bullforge/shared/config.py`
- Create: `bullforge/services/__init__.py`
- Create: `bullforge/services/data_pipeline/__init__.py`
- Create: `bullforge/tests/__init__.py`
- Create: `bullforge/tests/conftest.py`
- Create: `bullforge/tests/test_config.py`

- [ ] **Step 1: Create directory structure**

```bash
cd <repo>
mkdir -p bullforge/shared bullforge/services/data_pipeline bullforge/migrations bullforge/tests
touch bullforge/shared/__init__.py bullforge/services/__init__.py bullforge/services/data_pipeline/__init__.py bullforge/tests/__init__.py
```

- [ ] **Step 2: Create .gitignore**

Write `bullforge/.gitignore`:

```gitignore
__pycache__/
*.pyc
.env
.pytest_cache/
*.egg-info/
dist/
build/
.mypy_cache/
```

- [ ] **Step 3: Create .env.example**

Write `bullforge/.env.example`:

```env
# Alpaca API
ALPACA_API_KEY=your_api_key_here
ALPACA_SECRET_KEY=your_secret_key_here
ALPACA_BASE_URL=https://paper-api.alpaca.markets
ALPACA_DATA_URL=https://data.alpaca.markets
ALPACA_STREAM_URL=wss://stream.data.alpaca.markets

# Database
TIMESCALEDB_HOST=timescaledb
TIMESCALEDB_PORT=5432
TIMESCALEDB_USER=bullforge
TIMESCALEDB_PASSWORD=bullforge_dev
TIMESCALEDB_DB=bullforge

# Redis
REDIS_HOST=redis
REDIS_PORT=6379

# Trading config
TRADING_SYMBOLS_EQUITY=SPY,QQQ,AAPL,MSFT,NVDA
TRADING_SYMBOLS_CRYPTO=BTC/USD,ETH/USD
TRADING_WINDOW_START=11:30
TRADING_WINDOW_END=14:00
```

- [ ] **Step 4: Write the failing test for config**

Write `bullforge/tests/test_config.py`:

```python
import os
import pytest
from shared.config import Settings


def test_settings_loads_defaults():
    settings = Settings()
    assert settings.timescaledb_host == "timescaledb"
    assert settings.timescaledb_port == 5432
    assert settings.redis_host == "redis"
    assert settings.redis_port == 6379


def test_settings_loads_from_env(monkeypatch):
    monkeypatch.setenv("TIMESCALEDB_HOST", "localhost")
    monkeypatch.setenv("TIMESCALEDB_PORT", "5433")
    settings = Settings()
    assert settings.timescaledb_host == "localhost"
    assert settings.timescaledb_port == 5433


def test_settings_parses_symbol_lists(monkeypatch):
    monkeypatch.setenv("TRADING_SYMBOLS_EQUITY", "SPY,QQQ")
    monkeypatch.setenv("TRADING_SYMBOLS_CRYPTO", "BTC/USD,ETH/USD")
    settings = Settings()
    assert settings.trading_symbols_equity == ["SPY", "QQQ"]
    assert settings.trading_symbols_crypto == ["BTC/USD", "ETH/USD"]


def test_settings_parses_trading_window(monkeypatch):
    monkeypatch.setenv("TRADING_WINDOW_START", "11:30")
    monkeypatch.setenv("TRADING_WINDOW_END", "14:00")
    settings = Settings()
    assert settings.trading_window_start.hour == 11
    assert settings.trading_window_start.minute == 30
    assert settings.trading_window_end.hour == 14
    assert settings.trading_window_end.minute == 0
```

- [ ] **Step 5: Run test to verify it fails**

```bash
cd <repo>/bullforge
pip install pytest
PYTHONPATH=. pytest tests/test_config.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'shared.config'`

- [ ] **Step 6: Implement config module**

Write `bullforge/shared/config.py`:

```python
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import time
from typing import List


def _parse_csv(env_key: str, default: str) -> List[str]:
    raw = os.getenv(env_key, default)
    return [s.strip() for s in raw.split(",") if s.strip()]


def _parse_time(env_key: str, default: str) -> time:
    raw = os.getenv(env_key, default)
    parts = raw.split(":")
    return time(hour=int(parts[0]), minute=int(parts[1]))


@dataclass(frozen=True)
class Settings:
    # Alpaca
    alpaca_api_key: str = field(
        default_factory=lambda: os.getenv("ALPACA_API_KEY", "")
    )
    alpaca_secret_key: str = field(
        default_factory=lambda: os.getenv("ALPACA_SECRET_KEY", "")
    )
    alpaca_base_url: str = field(
        default_factory=lambda: os.getenv(
            "ALPACA_BASE_URL", "https://paper-api.alpaca.markets"
        )
    )
    alpaca_data_url: str = field(
        default_factory=lambda: os.getenv(
            "ALPACA_DATA_URL", "https://data.alpaca.markets"
        )
    )
    alpaca_stream_url: str = field(
        default_factory=lambda: os.getenv(
            "ALPACA_STREAM_URL", "wss://stream.data.alpaca.markets"
        )
    )

    # Database
    timescaledb_host: str = field(
        default_factory=lambda: os.getenv("TIMESCALEDB_HOST", "timescaledb")
    )
    timescaledb_port: int = field(
        default_factory=lambda: int(os.getenv("TIMESCALEDB_PORT", "5432"))
    )
    timescaledb_user: str = field(
        default_factory=lambda: os.getenv("TIMESCALEDB_USER", "bullforge")
    )
    timescaledb_password: str = field(
        default_factory=lambda: os.getenv("TIMESCALEDB_PASSWORD", "bullforge_dev")
    )
    timescaledb_db: str = field(
        default_factory=lambda: os.getenv("TIMESCALEDB_DB", "bullforge")
    )

    # Redis
    redis_host: str = field(
        default_factory=lambda: os.getenv("REDIS_HOST", "redis")
    )
    redis_port: int = field(
        default_factory=lambda: int(os.getenv("REDIS_PORT", "6379"))
    )

    # Trading symbols
    trading_symbols_equity: List[str] = field(
        default_factory=lambda: _parse_csv(
            "TRADING_SYMBOLS_EQUITY", "SPY,QQQ,AAPL,MSFT,NVDA"
        )
    )
    trading_symbols_crypto: List[str] = field(
        default_factory=lambda: _parse_csv(
            "TRADING_SYMBOLS_CRYPTO", "BTC/USD,ETH/USD"
        )
    )

    # Trading window (UTC times)
    trading_window_start: time = field(
        default_factory=lambda: _parse_time("TRADING_WINDOW_START", "11:30")
    )
    trading_window_end: time = field(
        default_factory=lambda: _parse_time("TRADING_WINDOW_END", "14:00")
    )
```

- [ ] **Step 7: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_config.py -v
```

Expected: All 4 tests PASS.

- [ ] **Step 8: Create conftest with shared fixtures**

Write `bullforge/tests/conftest.py`:

```python
import os
import pytest

# Override settings for tests to use localhost instead of docker service names
os.environ.setdefault("TIMESCALEDB_HOST", "localhost")
os.environ.setdefault("REDIS_HOST", "localhost")

from shared.config import Settings


@pytest.fixture
def settings():
    return Settings()
```

- [ ] **Step 9: Commit**

```bash
cd <repo>
git add bullforge/
git commit -m "feat: project scaffolding with config module and tests"
```

---

## Task 2: Shared Data Models

**Files:**
- Create: `bullforge/shared/models.py`
- Create: `bullforge/tests/test_models.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_models.py`:

```python
from datetime import datetime, timezone
from shared.models import Bar, CalendarEvent, CalendarEventType


def test_bar_creation():
    bar = Bar(
        symbol="SPY",
        timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
        open=420.10,
        high=420.50,
        low=419.80,
        close=420.30,
        volume=15000,
        timeframe="1m",
    )
    assert bar.symbol == "SPY"
    assert bar.timestamp.tzinfo == timezone.utc
    assert bar.open == 420.10
    assert bar.timeframe == "1m"


def test_bar_rejects_naive_timestamp():
    """Bars must have timezone-aware UTC timestamps."""
    import pytest

    with pytest.raises(ValueError, match="UTC"):
        Bar(
            symbol="SPY",
            timestamp=datetime(2026, 4, 10, 15, 30, 0),  # naive
            open=420.10,
            high=420.50,
            low=419.80,
            close=420.30,
            volume=15000,
            timeframe="1m",
        )


def test_calendar_event_creation():
    event = CalendarEvent(
        event_type=CalendarEventType.EARNINGS,
        symbol="AAPL",
        date=datetime(2026, 4, 24, tzinfo=timezone.utc),
        description="AAPL Q2 2026 Earnings",
    )
    assert event.event_type == CalendarEventType.EARNINGS
    assert event.symbol == "AAPL"


def test_calendar_event_fed_has_no_symbol():
    event = CalendarEvent(
        event_type=CalendarEventType.FED_ANNOUNCEMENT,
        symbol=None,
        date=datetime(2026, 5, 7, tzinfo=timezone.utc),
        description="FOMC Meeting",
    )
    assert event.symbol is None
    assert event.event_type == CalendarEventType.FED_ANNOUNCEMENT
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_models.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'shared.models'`

- [ ] **Step 3: Implement models**

Write `bullforge/shared/models.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class CalendarEventType(Enum):
    EARNINGS = "earnings"
    FED_ANNOUNCEMENT = "fed_announcement"
    MARKET_HOLIDAY = "market_holiday"


@dataclass(frozen=True)
class Bar:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    timeframe: str  # "1m", "1d", etc.

    def __post_init__(self):
        if self.timestamp.tzinfo is None or self.timestamp.tzinfo != timezone.utc:
            raise ValueError(
                f"Bar timestamp must be UTC, got tzinfo={self.timestamp.tzinfo}"
            )


@dataclass(frozen=True)
class CalendarEvent:
    event_type: CalendarEventType
    date: datetime
    description: str
    symbol: Optional[str] = None
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_models.py -v
```

Expected: All 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
cd <repo>
git add bullforge/shared/models.py bullforge/tests/test_models.py
git commit -m "feat: shared data models for Bar and CalendarEvent"
```

---

## Task 3: Database Connection and Schema

**Files:**
- Create: `bullforge/shared/db.py`
- Create: `bullforge/migrations/001_create_bars.sql`
- Create: `bullforge/migrations/002_create_calendar.sql`
- Create: `bullforge/tests/test_db.py`

- [ ] **Step 1: Write migration SQL for bars**

Write `bullforge/migrations/001_create_bars.sql`:

```sql
CREATE TABLE IF NOT EXISTS bars_1m (
    symbol      TEXT        NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    open        DOUBLE PRECISION NOT NULL,
    high        DOUBLE PRECISION NOT NULL,
    low         DOUBLE PRECISION NOT NULL,
    close       DOUBLE PRECISION NOT NULL,
    volume      BIGINT      NOT NULL,
    PRIMARY KEY (symbol, timestamp)
);

SELECT create_hypertable('bars_1m', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS bars_daily (
    symbol      TEXT        NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    open        DOUBLE PRECISION NOT NULL,
    high        DOUBLE PRECISION NOT NULL,
    low         DOUBLE PRECISION NOT NULL,
    close       DOUBLE PRECISION NOT NULL,
    volume      BIGINT      NOT NULL,
    PRIMARY KEY (symbol, timestamp)
);

SELECT create_hypertable('bars_daily', 'timestamp', if_not_exists => TRUE);
```

- [ ] **Step 2: Write migration SQL for calendar events**

Write `bullforge/migrations/002_create_calendar.sql`:

```sql
CREATE TABLE IF NOT EXISTS calendar_events (
    id          SERIAL PRIMARY KEY,
    event_type  TEXT        NOT NULL,
    symbol      TEXT,
    event_date  TIMESTAMPTZ NOT NULL,
    description TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (event_type, symbol, event_date)
);

CREATE INDEX IF NOT EXISTS idx_calendar_events_date
    ON calendar_events (event_date);

CREATE INDEX IF NOT EXISTS idx_calendar_events_symbol
    ON calendar_events (symbol, event_date);
```

- [ ] **Step 3: Write the failing test for db module**

Write `bullforge/tests/test_db.py`:

```python
import pytest
import pytest_asyncio
import asyncpg
from shared.config import Settings
from shared.db import Database


@pytest_asyncio.fixture
async def db():
    settings = Settings()
    database = Database(settings)
    await database.connect()
    yield database
    # Clean up test data
    async with database.pool.acquire() as conn:
        await conn.execute("DELETE FROM bars_1m")
        await conn.execute("DELETE FROM bars_daily")
        await conn.execute("DELETE FROM calendar_events")
    await database.disconnect()


@pytest.mark.asyncio
async def test_connect_and_disconnect():
    settings = Settings()
    database = Database(settings)
    await database.connect()
    assert database.pool is not None
    await database.disconnect()


@pytest.mark.asyncio
async def test_run_migrations(db):
    """After connect, migrations should have created tables."""
    async with db.pool.acquire() as conn:
        # Verify bars_1m exists and is a hypertable
        result = await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.hypertables "
            "WHERE hypertable_name = 'bars_1m'"
        )
        assert result == 1

        # Verify bars_daily exists and is a hypertable
        result = await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.hypertables "
            "WHERE hypertable_name = 'bars_daily'"
        )
        assert result == 1

        # Verify calendar_events exists
        result = await conn.fetchval(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_name = 'calendar_events'"
        )
        assert result == 1


@pytest.mark.asyncio
async def test_insert_and_query_bar(db):
    from datetime import datetime, timezone
    from shared.models import Bar

    bar = Bar(
        symbol="SPY",
        timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
        open=420.10,
        high=420.50,
        low=419.80,
        close=420.30,
        volume=15000,
        timeframe="1m",
    )
    await db.insert_bar(bar)

    rows = await db.query_bars("SPY", "1m", limit=1)
    assert len(rows) == 1
    assert rows[0].symbol == "SPY"
    assert rows[0].close == 420.30


@pytest.mark.asyncio
async def test_insert_bar_upsert_on_duplicate(db):
    """Inserting a bar with the same symbol+timestamp should update, not error."""
    from datetime import datetime, timezone
    from shared.models import Bar

    ts = datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc)
    bar1 = Bar(symbol="SPY", timestamp=ts, open=420.0, high=420.5,
               low=419.8, close=420.3, volume=15000, timeframe="1m")
    bar2 = Bar(symbol="SPY", timestamp=ts, open=420.0, high=421.0,
               low=419.5, close=420.8, volume=18000, timeframe="1m")

    await db.insert_bar(bar1)
    await db.insert_bar(bar2)

    rows = await db.query_bars("SPY", "1m", limit=10)
    assert len(rows) == 1
    assert rows[0].close == 420.8
    assert rows[0].volume == 18000
```

- [ ] **Step 4: Run test to verify it fails**

```bash
cd <repo>/bullforge
pip install asyncpg pytest-asyncio
PYTHONPATH=. pytest tests/test_db.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'shared.db'`

**Note:** These tests require a running TimescaleDB instance. Run it locally:

```bash
docker run -d --name bullforge-timescaledb \
  -p 5432:5432 \
  -e POSTGRES_USER=bullforge \
  -e POSTGRES_PASSWORD=bullforge_dev \
  -e POSTGRES_DB=bullforge \
  timescale/timescaledb:latest-pg16
```

- [ ] **Step 5: Implement db module**

Write `bullforge/shared/db.py`:

```python
from __future__ import annotations

import glob
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
```

- [ ] **Step 6: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_db.py -v
```

Expected: All 4 tests PASS.

- [ ] **Step 7: Commit**

```bash
cd <repo>
git add bullforge/shared/db.py bullforge/migrations/ bullforge/tests/test_db.py
git commit -m "feat: database module with TimescaleDB schema and async connection pool"
```

---

## Task 4: Redis Connection

**Files:**
- Create: `bullforge/shared/redis.py`
- Create: `bullforge/tests/test_redis.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_redis.py`:

```python
import pytest
import pytest_asyncio
from shared.config import Settings
from shared.redis import RedisClient


@pytest_asyncio.fixture
async def redis_client():
    settings = Settings()
    client = RedisClient(settings)
    await client.connect()
    yield client
    await client.disconnect()


@pytest.mark.asyncio
async def test_connect_and_disconnect():
    settings = Settings()
    client = RedisClient(settings)
    await client.connect()
    assert client.client is not None
    await client.disconnect()


@pytest.mark.asyncio
async def test_publish_and_subscribe(redis_client):
    received = []

    async def handler(message: str):
        received.append(message)

    await redis_client.subscribe("test:channel", handler)
    await redis_client.publish("test:channel", '{"test": "data"}')

    # Give the subscriber a moment to receive
    import asyncio
    await asyncio.sleep(0.1)

    await redis_client.unsubscribe("test:channel")
    assert len(received) == 1
    assert received[0] == '{"test": "data"}'
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd <repo>/bullforge
pip install redis
PYTHONPATH=. pytest tests/test_redis.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'shared.redis'`

**Note:** Requires running Redis:

```bash
docker run -d --name bullforge-redis -p 6379:6379 redis:7-alpine
```

- [ ] **Step 3: Implement Redis client**

Write `bullforge/shared/redis.py`:

```python
from __future__ import annotations

import asyncio
import json
from typing import Callable, Awaitable, Dict, Optional

import redis.asyncio as aioredis

from shared.config import Settings


class RedisClient:
    def __init__(self, settings: Settings):
        self._settings = settings
        self.client: Optional[aioredis.Redis] = None
        self._pubsub: Optional[aioredis.client.PubSub] = None
        self._handlers: Dict[str, Callable[[str], Awaitable[None]]] = {}
        self._listener_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        self.client = aioredis.Redis(
            host=self._settings.redis_host,
            port=self._settings.redis_port,
            decode_responses=True,
        )
        self._pubsub = self.client.pubsub()
        self._listener_task = asyncio.create_task(self._listen())

    async def disconnect(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        if self._pubsub:
            await self._pubsub.close()
        if self.client:
            await self.client.close()

    async def publish(self, channel: str, message: str) -> None:
        await self.client.publish(channel, message)

    async def subscribe(
        self, channel: str, handler: Callable[[str], Awaitable[None]]
    ) -> None:
        self._handlers[channel] = handler
        await self._pubsub.subscribe(channel)

    async def unsubscribe(self, channel: str) -> None:
        await self._pubsub.unsubscribe(channel)
        self._handlers.pop(channel, None)

    async def _listen(self) -> None:
        while True:
            try:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=0.1
                )
                if message and message["type"] == "message":
                    channel = message["channel"]
                    handler = self._handlers.get(channel)
                    if handler:
                        await handler(message["data"])
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.1)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_redis.py -v
```

Expected: All 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
cd <repo>
git add bullforge/shared/redis.py bullforge/tests/test_redis.py
git commit -m "feat: async Redis client with pub/sub support"
```

---

## Task 5: Alpaca Client Wrapper

**Files:**
- Create: `bullforge/services/data_pipeline/alpaca_client.py`
- Create: `bullforge/tests/test_alpaca_client.py`
- Create: `bullforge/services/data_pipeline/requirements.txt`

- [ ] **Step 1: Create requirements.txt**

Write `bullforge/services/data_pipeline/requirements.txt`:

```
alpaca-py>=0.21.0
asyncpg>=0.29.0
redis>=5.0.0
aiohttp>=3.9.0
```

- [ ] **Step 2: Write the failing test**

Write `bullforge/tests/test_alpaca_client.py`:

```python
import pytest
import pytest_asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from shared.models import Bar
from services.data_pipeline.alpaca_client import AlpacaClient
from shared.config import Settings


@pytest.fixture
def mock_settings(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test_key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test_secret")
    return Settings()


def test_alpaca_client_creation(mock_settings):
    client = AlpacaClient(mock_settings)
    assert client is not None


def test_parse_bar_to_model(mock_settings):
    """AlpacaClient should convert Alpaca bar objects to our Bar model."""
    client = AlpacaClient(mock_settings)

    mock_alpaca_bar = MagicMock()
    mock_alpaca_bar.timestamp = datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc)
    mock_alpaca_bar.open = 420.10
    mock_alpaca_bar.high = 420.50
    mock_alpaca_bar.low = 419.80
    mock_alpaca_bar.close = 420.30
    mock_alpaca_bar.volume = 15000

    bar = client.parse_bar("SPY", mock_alpaca_bar, timeframe="1m")
    assert isinstance(bar, Bar)
    assert bar.symbol == "SPY"
    assert bar.timestamp.tzinfo == timezone.utc
    assert bar.close == 420.30
    assert bar.timeframe == "1m"


def test_parse_bar_converts_naive_timestamp_to_utc(mock_settings):
    """If Alpaca returns a naive timestamp, convert it to UTC."""
    client = AlpacaClient(mock_settings)

    mock_alpaca_bar = MagicMock()
    mock_alpaca_bar.timestamp = datetime(2026, 4, 10, 15, 30, 0)  # naive
    mock_alpaca_bar.open = 420.10
    mock_alpaca_bar.high = 420.50
    mock_alpaca_bar.low = 419.80
    mock_alpaca_bar.close = 420.30
    mock_alpaca_bar.volume = 15000

    bar = client.parse_bar("SPY", mock_alpaca_bar, timeframe="1m")
    assert bar.timestamp.tzinfo == timezone.utc
```

- [ ] **Step 3: Run test to verify it fails**

```bash
cd <repo>/bullforge
pip install alpaca-py
PYTHONPATH=. pytest tests/test_alpaca_client.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'services.data_pipeline.alpaca_client'`

- [ ] **Step 4: Implement Alpaca client wrapper**

Write `bullforge/services/data_pipeline/alpaca_client.py`:

```python
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import AsyncIterator, List, Optional

from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.live import StockDataStream, CryptoDataStream

from shared.config import Settings
from shared.models import Bar


class AlpacaClient:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._stock_historical = StockHistoricalDataClient(
            api_key=settings.alpaca_api_key,
            secret_key=settings.alpaca_secret_key,
        )
        self._crypto_historical = CryptoHistoricalDataClient(
            api_key=settings.alpaca_api_key,
            secret_key=settings.alpaca_secret_key,
        )
        self._stock_stream: Optional[StockDataStream] = None
        self._crypto_stream: Optional[CryptoDataStream] = None

    def parse_bar(self, symbol: str, alpaca_bar, timeframe: str) -> Bar:
        ts = alpaca_bar.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)
        return Bar(
            symbol=symbol,
            timestamp=ts,
            open=float(alpaca_bar.open),
            high=float(alpaca_bar.high),
            low=float(alpaca_bar.low),
            close=float(alpaca_bar.close),
            volume=int(alpaca_bar.volume),
            timeframe=timeframe,
        )

    def get_historical_bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: Optional[datetime] = None,
    ) -> List[Bar]:
        if end is None:
            end = datetime.now(timezone.utc)

        tf = TimeFrame.Minute if timeframe == "1m" else TimeFrame.Day

        if "/" in symbol:
            # Crypto symbol like BTC/USD
            request = CryptoBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf,
                start=start,
                end=end,
            )
            barset = self._crypto_historical.get_crypto_bars(request)
        else:
            request = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf,
                start=start,
                end=end,
            )
            barset = self._stock_historical.get_stock_bars(request)

        bars = []
        for alpaca_bar in barset[symbol]:
            bars.append(self.parse_bar(symbol, alpaca_bar, timeframe))
        return bars

    def create_stock_stream(self) -> StockDataStream:
        self._stock_stream = StockDataStream(
            api_key=self._settings.alpaca_api_key,
            secret_key=self._settings.alpaca_secret_key,
        )
        return self._stock_stream

    def create_crypto_stream(self) -> CryptoDataStream:
        self._crypto_stream = CryptoDataStream(
            api_key=self._settings.alpaca_api_key,
            secret_key=self._settings.alpaca_secret_key,
        )
        return self._crypto_stream
```

- [ ] **Step 5: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_alpaca_client.py -v
```

Expected: All 3 tests PASS.

- [ ] **Step 6: Commit**

```bash
cd <repo>
git add bullforge/services/data_pipeline/alpaca_client.py bullforge/services/data_pipeline/requirements.txt bullforge/tests/test_alpaca_client.py
git commit -m "feat: Alpaca client wrapper with bar parsing and historical/stream support"
```

---

## Task 6: Historical Data Backfill

**Files:**
- Create: `bullforge/services/data_pipeline/backfill.py`
- Create: `bullforge/tests/test_backfill.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_backfill.py`:

```python
import pytest
import pytest_asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from shared.config import Settings
from shared.models import Bar
from services.data_pipeline.backfill import Backfiller


@pytest.fixture
def mock_settings(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test_key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test_secret")
    monkeypatch.setenv("TRADING_SYMBOLS_EQUITY", "SPY")
    monkeypatch.setenv("TRADING_SYMBOLS_CRYPTO", "BTC/USD")
    return Settings()


def _make_bars(symbol: str, count: int, timeframe: str) -> list[Bar]:
    base = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)
    return [
        Bar(
            symbol=symbol,
            timestamp=base + timedelta(minutes=i),
            open=100.0 + i,
            high=101.0 + i,
            low=99.0 + i,
            close=100.5 + i,
            volume=1000 * (i + 1),
            timeframe=timeframe,
        )
        for i in range(count)
    ]


@pytest.mark.asyncio
async def test_backfill_fetches_and_stores_bars(mock_settings):
    mock_db = AsyncMock()
    mock_alpaca = MagicMock()
    mock_alpaca.get_historical_bars.return_value = _make_bars("SPY", 5, "1m")

    backfiller = Backfiller(
        settings=mock_settings, db=mock_db, alpaca_client=mock_alpaca
    )
    await backfiller.backfill_symbol("SPY", timeframe="1m", days=1)

    mock_alpaca.get_historical_bars.assert_called_once()
    mock_db.insert_bars.assert_called_once()
    inserted_bars = mock_db.insert_bars.call_args[0][0]
    assert len(inserted_bars) == 5
    assert all(b.symbol == "SPY" for b in inserted_bars)


@pytest.mark.asyncio
async def test_backfill_all_symbols(mock_settings):
    mock_db = AsyncMock()
    mock_alpaca = MagicMock()
    mock_alpaca.get_historical_bars.side_effect = [
        _make_bars("SPY", 3, "1m"),
        _make_bars("SPY", 3, "1d"),
        _make_bars("BTC/USD", 3, "1m"),
        _make_bars("BTC/USD", 3, "1d"),
    ]

    backfiller = Backfiller(
        settings=mock_settings, db=mock_db, alpaca_client=mock_alpaca
    )
    await backfiller.backfill_all(days_1m=1, days_daily=365)

    assert mock_alpaca.get_historical_bars.call_count == 4
    assert mock_db.insert_bars.call_count == 4


@pytest.mark.asyncio
async def test_backfill_handles_empty_response(mock_settings):
    mock_db = AsyncMock()
    mock_alpaca = MagicMock()
    mock_alpaca.get_historical_bars.return_value = []

    backfiller = Backfiller(
        settings=mock_settings, db=mock_db, alpaca_client=mock_alpaca
    )
    await backfiller.backfill_symbol("SPY", timeframe="1m", days=1)

    mock_db.insert_bars.assert_not_called()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_backfill.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'services.data_pipeline.backfill'`

- [ ] **Step 3: Implement backfill module**

Write `bullforge/services/data_pipeline/backfill.py`:

```python
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta

from shared.config import Settings
from shared.db import Database
from services.data_pipeline.alpaca_client import AlpacaClient

logger = logging.getLogger(__name__)


class Backfiller:
    def __init__(
        self, settings: Settings, db: Database, alpaca_client: AlpacaClient
    ):
        self._settings = settings
        self._db = db
        self._alpaca = alpaca_client

    async def backfill_symbol(
        self, symbol: str, timeframe: str, days: int
    ) -> int:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days)

        logger.info(
            "Backfilling %s %s bars from %s to %s",
            symbol, timeframe, start.isoformat(), end.isoformat(),
        )

        bars = self._alpaca.get_historical_bars(
            symbol=symbol, timeframe=timeframe, start=start, end=end
        )

        if not bars:
            logger.info("No bars returned for %s %s", symbol, timeframe)
            return 0

        await self._db.insert_bars(bars)
        logger.info("Inserted %d %s bars for %s", len(bars), timeframe, symbol)
        return len(bars)

    async def backfill_all(self, days_1m: int = 14, days_daily: int = 365) -> None:
        all_symbols = (
            self._settings.trading_symbols_equity
            + self._settings.trading_symbols_crypto
        )

        for symbol in all_symbols:
            await self.backfill_symbol(symbol, "1m", days_1m)
            await self.backfill_symbol(symbol, "1d", days_daily)

        logger.info("Backfill complete for all symbols")
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_backfill.py -v
```

Expected: All 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
cd <repo>
git add bullforge/services/data_pipeline/backfill.py bullforge/tests/test_backfill.py
git commit -m "feat: historical data backfill from Alpaca REST API"
```

---

## Task 7: Live Tick Streaming

**Files:**
- Create: `bullforge/services/data_pipeline/stream.py`
- Create: `bullforge/tests/test_stream.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_stream.py`:

```python
import pytest
import pytest_asyncio
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch, call
from shared.config import Settings
from shared.models import Bar
from services.data_pipeline.stream import TickStreamer


@pytest.fixture
def mock_settings(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test_key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test_secret")
    monkeypatch.setenv("TRADING_SYMBOLS_EQUITY", "SPY,QQQ")
    monkeypatch.setenv("TRADING_SYMBOLS_CRYPTO", "BTC/USD")
    return Settings()


def _make_alpaca_bar(symbol: str):
    bar = MagicMock()
    bar.symbol = symbol
    bar.timestamp = datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc)
    bar.open = 420.10
    bar.high = 420.50
    bar.low = 419.80
    bar.close = 420.30
    bar.volume = 15000
    return bar


@pytest.mark.asyncio
async def test_tick_handler_stores_bar_and_publishes(mock_settings):
    mock_db = AsyncMock()
    mock_redis = AsyncMock()
    mock_alpaca = MagicMock()
    mock_alpaca.parse_bar.return_value = Bar(
        symbol="SPY",
        timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
        open=420.10, high=420.50, low=419.80, close=420.30,
        volume=15000, timeframe="1m",
    )

    streamer = TickStreamer(
        settings=mock_settings, db=mock_db,
        redis_client=mock_redis, alpaca_client=mock_alpaca,
    )

    alpaca_bar = _make_alpaca_bar("SPY")
    await streamer.handle_bar(alpaca_bar)

    mock_db.insert_bar.assert_called_once()
    mock_redis.publish.assert_called_once()
    publish_args = mock_redis.publish.call_args
    assert publish_args[0][0] == "market:ticks"
    assert '"SPY"' in publish_args[0][1]


@pytest.mark.asyncio
async def test_tick_handler_publishes_valid_json(mock_settings):
    import json

    mock_db = AsyncMock()
    mock_redis = AsyncMock()
    mock_alpaca = MagicMock()
    mock_alpaca.parse_bar.return_value = Bar(
        symbol="SPY",
        timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
        open=420.10, high=420.50, low=419.80, close=420.30,
        volume=15000, timeframe="1m",
    )

    streamer = TickStreamer(
        settings=mock_settings, db=mock_db,
        redis_client=mock_redis, alpaca_client=mock_alpaca,
    )

    alpaca_bar = _make_alpaca_bar("SPY")
    await streamer.handle_bar(alpaca_bar)

    published_json = mock_redis.publish.call_args[0][1]
    data = json.loads(published_json)
    assert data["symbol"] == "SPY"
    assert data["close"] == 420.30
    assert "timestamp" in data
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_stream.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'services.data_pipeline.stream'`

- [ ] **Step 3: Implement stream module**

Write `bullforge/services/data_pipeline/stream.py`:

```python
from __future__ import annotations

import json
import logging
from typing import Optional

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient
from shared.models import Bar
from services.data_pipeline.alpaca_client import AlpacaClient

logger = logging.getLogger(__name__)

CHANNEL_MARKET_TICKS = "market:ticks"


class TickStreamer:
    def __init__(
        self,
        settings: Settings,
        db: Database,
        redis_client: RedisClient,
        alpaca_client: AlpacaClient,
    ):
        self._settings = settings
        self._db = db
        self._redis = redis_client
        self._alpaca = alpaca_client

    async def handle_bar(self, alpaca_bar) -> None:
        symbol = alpaca_bar.symbol
        bar = self._alpaca.parse_bar(symbol, alpaca_bar, timeframe="1m")

        await self._db.insert_bar(bar)

        message = json.dumps({
            "symbol": bar.symbol,
            "timestamp": bar.timestamp.isoformat(),
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
        })
        await self._redis.publish(CHANNEL_MARKET_TICKS, message)

        logger.debug("Tick: %s @ %s close=%.2f", bar.symbol, bar.timestamp, bar.close)

    async def start(self) -> None:
        equity_symbols = self._settings.trading_symbols_equity
        crypto_symbols = self._settings.trading_symbols_crypto

        if equity_symbols:
            stock_stream = self._alpaca.create_stock_stream()
            stock_stream.subscribe_bars(self.handle_bar, *equity_symbols)
            logger.info("Subscribed to equity bars: %s", equity_symbols)

        if crypto_symbols:
            crypto_stream = self._alpaca.create_crypto_stream()
            crypto_stream.subscribe_bars(self.handle_bar, *crypto_symbols)
            logger.info("Subscribed to crypto bars: %s", crypto_symbols)

        logger.info("Tick streamer started")

        # alpaca-py streams run in their own threads, so we just keep alive
        # The actual stream.run() calls are handled in main.py
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_stream.py -v
```

Expected: All 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
cd <repo>
git add bullforge/services/data_pipeline/stream.py bullforge/tests/test_stream.py
git commit -m "feat: live tick streaming from Alpaca websocket to TimescaleDB and Redis"
```

---

## Task 8: Calendar Event Service

**Files:**
- Create: `bullforge/services/data_pipeline/calendar_service.py`
- Create: `bullforge/tests/test_calendar_service.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_calendar_service.py`:

```python
import pytest
import pytest_asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from shared.config import Settings
from shared.models import CalendarEvent, CalendarEventType
from services.data_pipeline.calendar_service import CalendarService


@pytest.fixture
def mock_settings(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test_key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test_secret")
    return Settings()


@pytest.mark.asyncio
async def test_is_fed_day_returns_true_on_fed_day(mock_settings):
    mock_db = AsyncMock()
    fed_date = datetime(2026, 5, 7, tzinfo=timezone.utc)
    mock_db.pool.acquire.return_value.__aenter__ = AsyncMock(
        return_value=MagicMock(
            fetchval=AsyncMock(return_value=1)
        )
    )
    mock_db.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)

    service = CalendarService(settings=mock_settings, db=mock_db)
    result = await service.is_fed_day(fed_date)
    assert result is True


@pytest.mark.asyncio
async def test_is_fed_day_returns_false_on_normal_day(mock_settings):
    mock_db = AsyncMock()
    mock_db.pool.acquire.return_value.__aenter__ = AsyncMock(
        return_value=MagicMock(
            fetchval=AsyncMock(return_value=0)
        )
    )
    mock_db.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)

    service = CalendarService(settings=mock_settings, db=mock_db)
    normal_date = datetime(2026, 5, 8, tzinfo=timezone.utc)
    result = await service.is_fed_day(normal_date)
    assert result is False


@pytest.mark.asyncio
async def test_is_near_earnings_returns_true_within_window(mock_settings):
    mock_db = AsyncMock()
    mock_db.pool.acquire.return_value.__aenter__ = AsyncMock(
        return_value=MagicMock(
            fetchval=AsyncMock(return_value=1)
        )
    )
    mock_db.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)

    service = CalendarService(settings=mock_settings, db=mock_db)
    result = await service.is_near_earnings(
        "AAPL", datetime(2026, 4, 23, tzinfo=timezone.utc), days_window=3
    )
    assert result is True


@pytest.mark.asyncio
async def test_store_calendar_event(mock_settings):
    mock_db = AsyncMock()
    mock_conn = AsyncMock()
    mock_db.pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_db.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)

    service = CalendarService(settings=mock_settings, db=mock_db)
    event = CalendarEvent(
        event_type=CalendarEventType.EARNINGS,
        symbol="AAPL",
        date=datetime(2026, 4, 24, tzinfo=timezone.utc),
        description="AAPL Q2 2026 Earnings",
    )
    await service.store_event(event)
    mock_conn.execute.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_calendar_service.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'services.data_pipeline.calendar_service'`

- [ ] **Step 3: Implement calendar service**

Write `bullforge/services/data_pipeline/calendar_service.py`:

```python
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta

from shared.config import Settings
from shared.db import Database
from shared.models import CalendarEvent, CalendarEventType

logger = logging.getLogger(__name__)


class CalendarService:
    def __init__(self, settings: Settings, db: Database):
        self._settings = settings
        self._db = db

    async def store_event(self, event: CalendarEvent) -> None:
        async with self._db.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO calendar_events (event_type, symbol, event_date, description)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (event_type, symbol, event_date) DO UPDATE
                SET description = $4
                """,
                event.event_type.value,
                event.symbol,
                event.date,
                event.description,
            )
        logger.info("Stored calendar event: %s %s %s", event.event_type.value, event.symbol, event.date)

    async def is_fed_day(self, date: datetime) -> bool:
        start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + timedelta(days=1)
        async with self._db.pool.acquire() as conn:
            count = await conn.fetchval(
                """
                SELECT count(*) FROM calendar_events
                WHERE event_type = $1
                AND event_date >= $2 AND event_date < $3
                """,
                CalendarEventType.FED_ANNOUNCEMENT.value,
                start_of_day,
                end_of_day,
            )
        return count > 0

    async def is_near_earnings(
        self, symbol: str, date: datetime, days_window: int = 2
    ) -> bool:
        window_start = date - timedelta(days=days_window)
        window_end = date + timedelta(days=days_window)
        async with self._db.pool.acquire() as conn:
            count = await conn.fetchval(
                """
                SELECT count(*) FROM calendar_events
                WHERE event_type = $1
                AND symbol = $2
                AND event_date >= $3 AND event_date <= $4
                """,
                CalendarEventType.EARNINGS.value,
                symbol,
                window_start,
                window_end,
            )
        return count > 0

    async def is_market_holiday(self, date: datetime) -> bool:
        start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + timedelta(days=1)
        async with self._db.pool.acquire() as conn:
            count = await conn.fetchval(
                """
                SELECT count(*) FROM calendar_events
                WHERE event_type = $1
                AND event_date >= $2 AND event_date < $3
                """,
                CalendarEventType.MARKET_HOLIDAY.value,
                start_of_day,
                end_of_day,
            )
        return count > 0
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_calendar_service.py -v
```

Expected: All 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
cd <repo>
git add bullforge/services/data_pipeline/calendar_service.py bullforge/tests/test_calendar_service.py
git commit -m "feat: calendar service for earnings, Fed days, and market holidays"
```

---

## Task 9: Health Check Endpoint

**Files:**
- Create: `bullforge/services/data_pipeline/health.py`
- Create: `bullforge/tests/test_health.py`

- [ ] **Step 1: Write the failing test**

Write `bullforge/tests/test_health.py`:

```python
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, TestClient
from services.data_pipeline.health import create_health_app


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.pool = MagicMock()
    db.pool.get_size.return_value = 5
    db.pool.get_idle_size.return_value = 3
    return db


@pytest.fixture
def mock_redis():
    redis = AsyncMock()
    redis.client = AsyncMock()
    redis.client.ping = AsyncMock(return_value=True)
    return redis


@pytest_asyncio.fixture
async def health_client(aiohttp_client, mock_db, mock_redis):
    app = create_health_app(db=mock_db, redis_client=mock_redis)
    return await aiohttp_client(app)


@pytest.mark.asyncio
async def test_health_check_healthy(health_client):
    resp = await health_client.get("/health")
    assert resp.status == 200
    data = await resp.json()
    assert data["status"] == "healthy"
    assert "db" in data
    assert "redis" in data


@pytest.mark.asyncio
async def test_health_check_db_down(aiohttp_client):
    mock_db = AsyncMock()
    mock_db.pool = None
    mock_redis = AsyncMock()
    mock_redis.client = AsyncMock()
    mock_redis.client.ping = AsyncMock(return_value=True)

    app = create_health_app(db=mock_db, redis_client=mock_redis)
    client = await aiohttp_client(app)

    resp = await client.get("/health")
    assert resp.status == 503
    data = await resp.json()
    assert data["status"] == "unhealthy"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd <repo>/bullforge
pip install aiohttp pytest-aiohttp
PYTHONPATH=. pytest tests/test_health.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'services.data_pipeline.health'`

- [ ] **Step 3: Implement health endpoint**

Write `bullforge/services/data_pipeline/health.py`:

```python
from __future__ import annotations

import logging

from aiohttp import web

from shared.db import Database
from shared.redis import RedisClient

logger = logging.getLogger(__name__)


def create_health_app(db: Database, redis_client: RedisClient) -> web.Application:
    app = web.Application()
    app["db"] = db
    app["redis"] = redis_client
    app.router.add_get("/health", health_handler)
    return app


async def health_handler(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    redis_client: RedisClient = request.app["redis"]

    checks = {}
    all_healthy = True

    # Check database
    if db.pool is not None:
        try:
            async with db.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            checks["db"] = {
                "status": "healthy",
                "pool_size": db.pool.get_size(),
                "idle": db.pool.get_idle_size(),
            }
        except Exception as e:
            checks["db"] = {"status": "unhealthy", "error": str(e)}
            all_healthy = False
    else:
        checks["db"] = {"status": "unhealthy", "error": "pool is None"}
        all_healthy = False

    # Check Redis
    try:
        if redis_client.client:
            await redis_client.client.ping()
            checks["redis"] = {"status": "healthy"}
        else:
            checks["redis"] = {"status": "unhealthy", "error": "client is None"}
            all_healthy = False
    except Exception as e:
        checks["redis"] = {"status": "unhealthy", "error": str(e)}
        all_healthy = False

    status_code = 200 if all_healthy else 503
    return web.json_response(
        {"status": "healthy" if all_healthy else "unhealthy", **checks},
        status=status_code,
    )
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/test_health.py -v
```

Expected: All 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
cd <repo>
git add bullforge/services/data_pipeline/health.py bullforge/tests/test_health.py
git commit -m "feat: health check endpoint for data pipeline service"
```

---

## Task 10: Service Entrypoint

**Files:**
- Create: `bullforge/services/data_pipeline/main.py`

- [ ] **Step 1: Implement the entrypoint**

Write `bullforge/services/data_pipeline/main.py`:

```python
from __future__ import annotations

import asyncio
import logging
import signal
import sys

from aiohttp import web

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient
from services.data_pipeline.alpaca_client import AlpacaClient
from services.data_pipeline.backfill import Backfiller
from services.data_pipeline.stream import TickStreamer
from services.data_pipeline.calendar_service import CalendarService
from services.data_pipeline.health import create_health_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("data_pipeline")


async def main() -> None:
    settings = Settings()
    db = Database(settings)
    redis_client = RedisClient(settings)
    alpaca_client = AlpacaClient(settings)

    logger.info("Connecting to TimescaleDB...")
    await db.connect()
    logger.info("Connecting to Redis...")
    await redis_client.connect()

    # Run backfill on startup
    backfiller = Backfiller(settings=settings, db=db, alpaca_client=alpaca_client)
    logger.info("Running initial backfill...")
    await backfiller.backfill_all(days_1m=14, days_daily=365)

    # Start live tick streaming
    streamer = TickStreamer(
        settings=settings, db=db,
        redis_client=redis_client, alpaca_client=alpaca_client,
    )

    # Start health check server
    health_app = create_health_app(db=db, redis_client=redis_client)
    runner = web.AppRunner(health_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    logger.info("Health check endpoint running on :8080/health")

    # Start Alpaca streams
    stock_stream = None
    crypto_stream = None

    if settings.trading_symbols_equity:
        stock_stream = alpaca_client.create_stock_stream()
        stock_stream.subscribe_bars(streamer.handle_bar, *settings.trading_symbols_equity)
        logger.info("Starting equity stream for: %s", settings.trading_symbols_equity)

    if settings.trading_symbols_crypto:
        crypto_stream = alpaca_client.create_crypto_stream()
        crypto_stream.subscribe_bars(streamer.handle_bar, *settings.trading_symbols_crypto)
        logger.info("Starting crypto stream for: %s", settings.trading_symbols_crypto)

    # Set up graceful shutdown
    shutdown_event = asyncio.Event()

    def handle_signal():
        logger.info("Shutdown signal received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

    # Run streams in background threads (alpaca-py uses threading internally)
    stream_tasks = []
    if stock_stream:
        stream_tasks.append(
            asyncio.get_event_loop().run_in_executor(None, stock_stream.run)
        )
    if crypto_stream:
        stream_tasks.append(
            asyncio.get_event_loop().run_in_executor(None, crypto_stream.run)
        )

    logger.info("Data pipeline running. Press Ctrl+C to stop.")

    try:
        await shutdown_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down...")
        await runner.cleanup()
        await redis_client.disconnect()
        await db.disconnect()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Commit**

```bash
cd <repo>
git add bullforge/services/data_pipeline/main.py
git commit -m "feat: data pipeline service entrypoint with backfill, streaming, and health check"
```

---

## Task 11: Docker Compose Setup

**Files:**
- Create: `bullforge/services/data_pipeline/Dockerfile`
- Create: `bullforge/docker-compose.yml`

- [ ] **Step 1: Create Dockerfile for data pipeline**

Write `bullforge/services/data_pipeline/Dockerfile`:

```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY services/data_pipeline/requirements.txt /app/services/data_pipeline/requirements.txt
RUN pip install --no-cache-dir -r services/data_pipeline/requirements.txt
RUN pip install --no-cache-dir aiohttp asyncpg redis

# Copy shared code and service code
COPY shared/ /app/shared/
COPY migrations/ /app/migrations/
COPY services/__init__.py /app/services/__init__.py
COPY services/data_pipeline/ /app/services/data_pipeline/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8080

CMD ["python", "-m", "services.data_pipeline.main"]
```

- [ ] **Step 2: Create docker-compose.yml**

Write `bullforge/docker-compose.yml`:

```yaml
version: "3.8"

services:
  timescaledb:
    image: timescale/timescaledb:latest-pg16
    environment:
      POSTGRES_USER: bullforge
      POSTGRES_PASSWORD: bullforge_dev
      POSTGRES_DB: bullforge
    ports:
      - "5432:5432"
    volumes:
      - timescaledb_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U bullforge"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

  data_pipeline:
    build:
      context: .
      dockerfile: services/data_pipeline/Dockerfile
    env_file:
      - .env
    environment:
      TIMESCALEDB_HOST: timescaledb
      REDIS_HOST: redis
    ports:
      - "8080:8080"
    depends_on:
      timescaledb:
        condition: service_healthy
      redis:
        condition: service_healthy
    restart: unless-stopped

volumes:
  timescaledb_data:
  redis_data:
```

- [ ] **Step 3: Create .env from .env.example**

```bash
cd <repo>/bullforge
cp .env.example .env
# Edit .env with actual Alpaca API keys before running
```

- [ ] **Step 4: Test Docker Compose builds**

```bash
cd <repo>/bullforge
docker compose build
```

Expected: Image builds successfully with no errors.

- [ ] **Step 5: Test Docker Compose starts**

```bash
cd <repo>/bullforge
docker compose up -d timescaledb redis
# Wait for healthy
docker compose up data_pipeline
```

Expected: Data pipeline connects to TimescaleDB and Redis, runs backfill, starts streaming. Health check responds at http://localhost:8080/health.

- [ ] **Step 6: Verify data is flowing**

```bash
docker compose exec timescaledb psql -U bullforge -c "SELECT symbol, count(*) FROM bars_1m GROUP BY symbol;"
docker compose exec timescaledb psql -U bullforge -c "SELECT symbol, count(*) FROM bars_daily GROUP BY symbol;"
```

Expected: Row counts for configured symbols.

- [ ] **Step 7: Commit**

```bash
cd <repo>
git add bullforge/services/data_pipeline/Dockerfile bullforge/docker-compose.yml
git commit -m "feat: Docker Compose with TimescaleDB, Redis, and data pipeline service"
```

---

## Task 12: Run Full Test Suite

- [ ] **Step 1: Run all tests**

```bash
cd <repo>/bullforge
PYTHONPATH=. pytest tests/ -v
```

Expected: All tests pass. Fix any failures before proceeding.

- [ ] **Step 2: Verify Docker Compose end-to-end**

```bash
cd <repo>/bullforge
docker compose down -v
docker compose up -d
# Wait 30 seconds for backfill
curl http://localhost:8080/health
```

Expected: Health endpoint returns `{"status": "healthy", "db": {"status": "healthy", ...}, "redis": {"status": "healthy"}}`.

- [ ] **Step 3: Final commit if any fixes were needed**

```bash
cd <repo>
git add -A
git status
# If there are changes:
git commit -m "fix: address issues found during full test suite run"
```

---

## Phase 1 Complete

At this point you have:
- Docker Compose orchestrating TimescaleDB, Redis, and the Data Pipeline service
- Historical data backfill from Alpaca (14 days 1m bars, 1 year daily bars)
- Live tick streaming from Alpaca websocket → TimescaleDB + Redis pub/sub
- Calendar event tracking (earnings, Fed days, holidays)
- Health check endpoint
- Full test suite with unit tests and mocked integration tests

**Next:** Phase 2 plan (Feature Engine & Labels) will be written as a separate document.
