from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from shared.symbol_utils import is_crypto

EASTERN = ZoneInfo("America/New_York")
NYSE_OPEN = time(9, 30)
NYSE_CLOSE = time(16, 0)


def is_trading_allowed(
    symbol: str,
    now_utc: datetime,
    *,
    open_buffer_min: int,
    close_buffer_min: int,
) -> tuple[bool, str]:
    """
    Return (allowed, reason).

    - Crypto (symbol containing '/'): always allowed, 24/7.
    - Equity: allowed only on NYSE weekdays, within
      [09:30 ET + open_buffer_min, 16:00 ET - close_buffer_min).
      Holidays are NOT checked here (broker will reject on holidays).
    """
    if now_utc.tzinfo is None:
        raise ValueError(f"now_utc must be timezone-aware UTC, got {now_utc}")

    if is_crypto(symbol):
        return True, "crypto 24/7"

    et = now_utc.astimezone(EASTERN)
    if et.weekday() >= 5:
        return False, f"weekend (ET={et.isoformat()})"

    open_dt = et.replace(
        hour=NYSE_OPEN.hour, minute=NYSE_OPEN.minute, second=0, microsecond=0,
    ) + timedelta(minutes=open_buffer_min)
    close_dt = et.replace(
        hour=NYSE_CLOSE.hour, minute=NYSE_CLOSE.minute, second=0, microsecond=0,
    ) - timedelta(minutes=close_buffer_min)

    if open_dt <= et < close_dt:
        return True, f"within window ({open_dt.time()}-{close_dt.time()} ET)"
    return False, (
        f"outside window (now={et.time()} ET, window={open_dt.time()}-{close_dt.time()} ET)"
    )
