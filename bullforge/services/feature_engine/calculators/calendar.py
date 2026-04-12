from __future__ import annotations

import pandas as pd


def compute_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    ts = pd.to_datetime(df["timestamp"], utc=True)

    day_of_week = ts.dt.dayofweek
    hour_of_day = ts.dt.hour
    minute_of_hour = ts.dt.minute

    # US equity market open: 9:30 AM ET = 14:30 UTC (during EDT)
    # First 30 minutes: 14:30 - 15:00 UTC
    time_minutes = hour_of_day * 60 + minute_of_hour
    market_open_start = 14 * 60 + 30  # 14:30 UTC
    market_open_end = 15 * 60          # 15:00 UTC
    is_market_open_first_30 = (
        (time_minutes >= market_open_start) & (time_minutes < market_open_end)
    ).astype(int)

    # US equity market close: 4:00 PM ET = 21:00 UTC (during EDT)
    # Last 30 minutes: 20:30 - 21:00 UTC
    market_close_start = 20 * 60 + 30  # 20:30 UTC
    market_close_end = 21 * 60          # 21:00 UTC
    is_market_close_last_30 = (
        (time_minutes >= market_close_start) & (time_minutes < market_close_end)
    ).astype(int)

    return pd.DataFrame({
        "day_of_week": day_of_week.values,
        "hour_of_day": hour_of_day.values,
        "minute_of_hour": minute_of_hour.values,
        "is_market_open_first_30": is_market_open_first_30.values,
        "is_market_close_last_30": is_market_close_last_30.values,
    }, index=df.index)
