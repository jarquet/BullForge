from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import AsyncIterator, List, Optional

from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
from alpaca.data.enums import DataFeed
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
                feed=DataFeed.IEX,
            )
            barset = self._stock_historical.get_stock_bars(request)

        bars = []
        symbol_bars = barset.data.get(symbol, [])
        for alpaca_bar in symbol_bars:
            bars.append(self.parse_bar(symbol, alpaca_bar, timeframe))
        return bars

    def create_stock_stream(self) -> StockDataStream:
        self._stock_stream = StockDataStream(
            api_key=self._settings.alpaca_api_key,
            secret_key=self._settings.alpaca_secret_key,
            feed=DataFeed.IEX,
        )
        return self._stock_stream

    def create_crypto_stream(self) -> CryptoDataStream:
        self._crypto_stream = CryptoDataStream(
            api_key=self._settings.alpaca_api_key,
            secret_key=self._settings.alpaca_secret_key,
        )
        return self._crypto_stream
