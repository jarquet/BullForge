from __future__ import annotations

import asyncio
import json
import logging
import queue
from typing import Optional

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient
from shared.models import Bar
from services.data_pipeline.alpaca_client import AlpacaClient

logger = logging.getLogger(__name__)

CHANNEL_MARKET_TICKS = "market:ticks"


class TickStreamer:
    def __init__(self, settings: Settings, db: Database, redis_client: RedisClient, alpaca_client: AlpacaClient):
        self._settings = settings
        self._db = db
        self._redis = redis_client
        self._alpaca = alpaca_client
        self._queue: queue.Queue = queue.Queue()
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None
        self._stock_stream = None
        self._crypto_stream = None
        self._active_equity: set[str] = set()
        self._active_crypto: set[str] = set()

    def attach_streams(self, stock_stream, crypto_stream) -> None:
        """Let the streamer track running Alpaca stream clients so it can
        hot-(un)subscribe when symbols are added or removed at runtime."""
        self._stock_stream = stock_stream
        self._crypto_stream = crypto_stream

    async def subscribe_symbol(self, symbol: str, *, kind: str) -> None:
        if kind == "equity":
            if self._stock_stream is not None:
                self._stock_stream.subscribe_bars(self.handle_bar, symbol)
            self._active_equity.add(symbol)
        elif kind == "crypto":
            if self._crypto_stream is not None:
                self._crypto_stream.subscribe_bars(self.handle_bar, symbol)
            self._active_crypto.add(symbol)
        logger.info("Subscribed to %s bars: %s", kind, symbol)

    async def unsubscribe_symbol(self, symbol: str, *, kind: str) -> None:
        stream = (
            self._stock_stream if kind == "equity" else self._crypto_stream
        )
        if stream is not None and hasattr(stream, "unsubscribe_bars"):
            stream.unsubscribe_bars(symbol)
        if kind == "equity":
            self._active_equity.discard(symbol)
        else:
            self._active_crypto.discard(symbol)
        logger.info("Unsubscribed from %s bars: %s", kind, symbol)

    async def handle_bar(self, alpaca_bar) -> None:
        """Called from Alpaca's stream thread. Enqueues the bar for main-loop processing."""
        symbol = alpaca_bar.symbol
        bar = self._alpaca.parse_bar(symbol, alpaca_bar, timeframe="1m")
        self._queue.put(bar)
        # Wake up the main loop's consumer
        if self._main_loop:
            self._main_loop.call_soon_threadsafe(lambda: None)

    async def _process_bar(self, bar: Bar) -> None:
        """Processes a bar on the main event loop where DB/Redis connections live."""
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

    async def consume(self) -> None:
        """Main-loop task that drains the queue and processes bars."""
        self._main_loop = asyncio.get_running_loop()
        while True:
            try:
                bar = self._queue.get_nowait()
                await self._process_bar(bar)
            except queue.Empty:
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error processing bar")

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
