from __future__ import annotations

from datetime import datetime, timedelta

from shared.trading_window import is_trading_allowed


def is_evaluation_window_in_exclusion(
    symbol: str,
    feature_timestamp: datetime,
    horizon_minutes: int,
    *,
    open_buffer_min: int,
    close_buffer_min: int,
) -> bool:
    """Returns True iff the forward-return target timestamp
    (feature_timestamp + horizon_minutes) lands in the equity exclusion
    zone for `symbol`.

    Crypto symbols (containing '/') always return False — they trade 24/7
    and are not subject to the NYSE buffered-window gate. Equity weekends
    return True regardless of buffer values, because is_trading_allowed
    rejects weekends naturally.

    The check looks at the *target* timestamp only, not at
    `feature_timestamp` itself. A row whose feature timestamp lands in the
    open buffer but whose target lands in the buffered window is fine —
    the model will be asked to predict an outcome the live system would
    happily act on.

    Boundary semantics inherited from `shared/trading_window.py`:
    `open_dt <= target < close_dt` (open-inclusive, close-exclusive).
    """
    target = feature_timestamp + timedelta(minutes=horizon_minutes)
    allowed, _ = is_trading_allowed(
        symbol, target,
        open_buffer_min=open_buffer_min,
        close_buffer_min=close_buffer_min,
    )
    return not allowed
