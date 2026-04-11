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
    def __init__(self, settings: Settings, db: Database, redis_client: RedisClient, alpaca_client: AlpacaClient):
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
