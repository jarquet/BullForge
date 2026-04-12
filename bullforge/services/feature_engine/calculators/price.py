from __future__ import annotations

import numpy as np
import pandas as pd


def compute_price_features(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]
    high = df["high"]
    low = df["low"]
    open_ = df["open"]

    return pd.DataFrame({
        # Simple returns at different lookbacks
        "return_1": close.pct_change(1),
        "return_5": close.pct_change(5),
        "return_10": close.pct_change(10),
        # Log return
        "log_return_1": np.log(close / close.shift(1)),
        # Rolling volatility (std of returns)
        "volatility_10": close.pct_change(1).rolling(window=10).std(),
        "volatility_20": close.pct_change(1).rolling(window=20).std(),
        # Spread features
        "high_low_spread": (high - low) / close,
        "close_open_spread": (close - open_) / open_,
    }, index=df.index)
