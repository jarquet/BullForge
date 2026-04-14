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

    async def backfill_timeframe(
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
            await self.backfill_timeframe(symbol, "1m", days_1m)
            await self.backfill_timeframe(symbol, "1d", days_daily)

        logger.info("Backfill complete for all symbols")

    async def backfill_symbol(
        self, symbol: str, *, kind: str, lookback_days: int = 30,
    ) -> int:
        """Backfill both intraday (1m) and daily bars for a newly added symbol."""
        total = await self.backfill_timeframe(symbol, "1m", lookback_days)
        total += await self.backfill_timeframe(symbol, "1d", 365)
        return total
