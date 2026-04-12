from __future__ import annotations

import numpy as np
import pandas as pd


def compute_volume_features(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"].astype(float)

    # VWAP (cumulative for the session)
    typical_price = (high + low + close) / 3
    cumulative_tp_vol = (typical_price * volume).cumsum()
    cumulative_vol = volume.cumsum()
    vwap = cumulative_tp_vol / cumulative_vol

    # On-Balance Volume
    direction = np.sign(close.diff())
    direction.iloc[0] = 0
    obv = (volume * direction).cumsum()

    # Volume ratio (current volume / rolling average)
    volume_ratio_10 = volume / volume.rolling(window=10).mean()
    volume_ratio_20 = volume / volume.rolling(window=20).mean()

    return pd.DataFrame({
        "vwap": vwap,
        "obv": obv,
        "volume_ratio_10": volume_ratio_10,
        "volume_ratio_20": volume_ratio_20,
    }, index=df.index)
