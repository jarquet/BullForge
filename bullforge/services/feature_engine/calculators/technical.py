from __future__ import annotations

import numpy as np
import pandas as pd


def _compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    # Flat-market guard: when neither gains nor losses occurred over the EWM
    # window (e.g. BTC/USD zero-volume bars where Alpaca emits OHLC == prior
    # close), avg_gain and avg_loss are both exactly 0.0, rs = 0/0 = NaN, and
    # rsi inherits the NaN. That tanks the row at engine.py's
    # notna().all(axis=1) gate. The standard convention is RSI=50 (neutral)
    # when there's no directional pressure. Don't touch warmup rows: those
    # have NaN in avg_gain/avg_loss themselves (min_periods=period not yet
    # satisfied), and engine.py is supposed to drop them.
    flat = (avg_gain == 0.0) & (avg_loss == 0.0)
    rsi = rsi.mask(flat, 50.0)
    return rsi


def _compute_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


def compute_technical_features(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]
    high = df["high"]
    low = df["low"]

    # Simple moving averages
    sma_10 = close.rolling(window=10).mean()
    sma_20 = close.rolling(window=20).mean()
    sma_50 = close.rolling(window=50).mean()

    # Exponential moving averages
    ema_10 = close.ewm(span=10, adjust=False).mean()
    ema_20 = close.ewm(span=20, adjust=False).mean()

    # RSI
    rsi_14 = _compute_rsi(close, 14)

    # MACD (12, 26, 9)
    ema_12 = close.ewm(span=12, adjust=False).mean()
    ema_26 = close.ewm(span=26, adjust=False).mean()
    macd = ema_12 - ema_26
    macd_signal = macd.ewm(span=9, adjust=False).mean()
    macd_histogram = macd - macd_signal

    # Bollinger Bands (20, 2)
    bb_middle = sma_20
    bb_std = close.rolling(window=20).std()
    bb_upper = bb_middle + 2 * bb_std
    bb_lower = bb_middle - 2 * bb_std
    bb_width = (bb_upper - bb_lower) / bb_middle

    # ATR
    atr_14 = _compute_atr(high, low, close, 14)

    return pd.DataFrame({
        "sma_10": sma_10,
        "sma_20": sma_20,
        "sma_50": sma_50,
        "ema_10": ema_10,
        "ema_20": ema_20,
        "rsi_14": rsi_14,
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_histogram": macd_histogram,
        "bb_upper": bb_upper,
        "bb_middle": bb_middle,
        "bb_lower": bb_lower,
        "bb_width": bb_width,
        "atr_14": atr_14,
    }, index=df.index)
