from __future__ import annotations

import logging
import time
from datetime import date, datetime, time as dtime, timedelta, timezone

from shared.config import Settings
from shared.db import Database
from shared.symbol_utils import is_crypto
from services.data_pipeline.alpaca_client import AlpacaClient

logger = logging.getLogger(__name__)


class Backfiller:
    def __init__(
        self, settings: Settings, db: Database, alpaca_client: AlpacaClient,
    ):
        self._settings = settings
        self._db = db
        self._alpaca = alpaca_client

    # ---- helpers ----------------------------------------------------------

    def _floor_dt(self, symbol: str) -> datetime:
        floor: date = (
            self._settings.backfill_floor_crypto
            if is_crypto(symbol)
            else self._settings.backfill_floor_equity
        )
        return datetime.combine(floor, dtime.min, tzinfo=timezone.utc)

    @staticmethod
    def _step(timeframe: str) -> timedelta:
        return timedelta(minutes=1) if timeframe == "1m" else timedelta(days=1)

    async def _fetch_and_insert(
        self, symbol: str, timeframe: str,
        start: datetime, end: datetime,
    ) -> int:
        if start >= end:
            return 0
        t0 = time.monotonic()
        bars = self._alpaca.get_historical_bars(
            symbol=symbol, timeframe=timeframe, start=start, end=end,
        )
        if not bars:
            logger.info(
                "No bars returned for %s %s [%s → %s]",
                symbol, timeframe, start.isoformat(), end.isoformat(),
            )
            return 0
        await self._db.insert_bars(bars)
        logger.info(
            "Inserted %d %s bars for %s [%s → %s] in %.2fs",
            len(bars), timeframe, symbol,
            start.isoformat(), end.isoformat(), time.monotonic() - t0,
        )
        return len(bars)

    # ---- public API -------------------------------------------------------

    async def forward_fill(self, symbol: str, timeframe: str) -> int:
        _, latest = await self._db.get_bar_range(symbol, timeframe)
        start = (latest + self._step(timeframe)) if latest else self._floor_dt(symbol)
        end = datetime.now(timezone.utc)
        return await self._fetch_and_insert(symbol, timeframe, start, end)

    async def reach_back(self, symbol: str, timeframe: str) -> int:
        earliest, _ = await self._db.get_bar_range(symbol, timeframe)
        if earliest is None:
            return 0
        floor = self._floor_dt(symbol)
        if earliest <= floor:
            return 0
        return await self._fetch_and_insert(symbol, timeframe, floor, earliest)

    async def backfill_all_forward(self) -> None:
        symbols = (
            self._settings.trading_symbols_equity
            + self._settings.trading_symbols_crypto
        )
        failures: list[tuple[str, str]] = []
        for symbol in symbols:
            for timeframe in ("1m", "1d"):
                try:
                    await self.forward_fill(symbol, timeframe)
                except Exception:
                    logger.exception(
                        "forward_fill failed for %s %s", symbol, timeframe,
                    )
                    failures.append((symbol, timeframe))
        if failures:
            logger.warning(
                "Forward backfill completed with %d failures: %s",
                len(failures), failures,
            )
        else:
            logger.info("Forward backfill complete for all symbols")

    async def backfill_all_reach_back(self) -> None:
        symbols = (
            self._settings.trading_symbols_equity
            + self._settings.trading_symbols_crypto
        )
        failures: list[tuple[str, str]] = []
        for symbol in symbols:
            for timeframe in ("1m", "1d"):
                try:
                    await self.reach_back(symbol, timeframe)
                except Exception:
                    logger.exception(
                        "reach_back failed for %s %s", symbol, timeframe,
                    )
                    failures.append((symbol, timeframe))
        if failures:
            logger.warning(
                "Reach-back completed with %d failures: %s",
                len(failures), failures,
            )
        else:
            logger.info("Reach-back complete for all symbols")

    async def backfill_symbol(self, symbol: str, *, kind: str) -> int:
        """Forward-fill a newly added symbol for both timeframes.

        The caller is responsible for scheduling reach_back as a background task.
        """
        total = 0
        for timeframe in ("1m", "1d"):
            try:
                total += await self.forward_fill(symbol, timeframe)
            except Exception:
                logger.exception(
                    "forward_fill failed for %s %s", symbol, timeframe,
                )
        return total
