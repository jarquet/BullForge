from __future__ import annotations

import numpy as np
import pandas as pd


def compute_forward_return_labels(
    df: pd.DataFrame,
    horizons: list[int],
    threshold: float = 0.0,
) -> pd.DataFrame:
    close = df["close"]
    result = {}

    for h in horizons:
        fwd_return = close.shift(-h) / close - 1.0
        if threshold > 0:
            direction = pd.Series(0, index=df.index, dtype=int)
            direction[fwd_return > threshold] = 1
            direction[fwd_return < -threshold] = -1
            direction[fwd_return.isna()] = np.nan
        else:
            direction = np.sign(fwd_return)

        result[f"fwd_return_{h}"] = fwd_return
        result[f"fwd_direction_{h}"] = direction

    return pd.DataFrame(result, index=df.index)
