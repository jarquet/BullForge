from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from typing import Optional

import pandas as pd
import pandas_market_calendars as mcal

_CALENDAR_NAME = "NYSE"


@lru_cache(maxsize=1)
def _calendar():
    return mcal.get_calendar(_CALENDAR_NAME)


@lru_cache(maxsize=32)
def _schedule_for(start: date, end: date) -> pd.DataFrame:
    return _calendar().schedule(start_date=start, end_date=end)


def _window(now: datetime) -> pd.DataFrame:
    start = (now - timedelta(days=10)).date()
    end = (now + timedelta(days=2)).date()
    return _schedule_for(start, end)


def _as_utc(ts: datetime) -> pd.Timestamp:
    if ts.tzinfo is None:
        return pd.Timestamp(ts, tz="UTC")
    return pd.Timestamp(ts).tz_convert("UTC")


def is_market_open(at: Optional[datetime] = None) -> bool:
    at = at or datetime.now(timezone.utc)
    sched = _window(at)
    if sched.empty:
        return False
    ts = _as_utc(at)
    return bool(((sched["market_open"] <= ts) & (ts < sched["market_close"])).any())


def last_market_close(before: Optional[datetime] = None) -> Optional[datetime]:
    before = before or datetime.now(timezone.utc)
    sched = _window(before)
    if sched.empty:
        return None
    ts = _as_utc(before)
    past = sched[sched["market_close"] <= ts]["market_close"]
    if past.empty:
        return None
    return past.iloc[-1].to_pydatetime()
