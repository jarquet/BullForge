from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterator

VALID_MODES = ("rolling", "anchored")


@dataclass(frozen=True)
class Fold:
    index: int
    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime


class WalkForwardSplitter:
    def __init__(
        self,
        *,
        train_days: int,
        test_days: int,
        embargo_minutes: int,
        mode: str = "rolling",
    ):
        if mode not in VALID_MODES:
            raise ValueError(f"mode must be one of {VALID_MODES}, got {mode!r}")
        if train_days <= 0:
            raise ValueError(f"train_days must be > 0, got {train_days}")
        if test_days <= 0:
            raise ValueError(f"test_days must be > 0, got {test_days}")
        if embargo_minutes < 0:
            raise ValueError(
                f"embargo_minutes must be >= 0, got {embargo_minutes}"
            )
        self._train = timedelta(days=train_days)
        self._test = timedelta(days=test_days)
        self._embargo = timedelta(minutes=embargo_minutes)
        self._mode = mode

    def split(
        self, range_start: datetime, range_end: datetime,
    ) -> Iterator[Fold]:
        anchor = range_start
        train_start = range_start
        index = 0
        while True:
            train_end = (
                train_start + self._train if self._mode == "rolling"
                else anchor + self._train + self._test * index
            )
            test_start = train_end + self._embargo
            test_end = test_start + self._test
            if test_end > range_end:
                return
            yield Fold(
                index=index,
                train_start=(
                    anchor if self._mode == "anchored" else train_start
                ),
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
            )
            index += 1
            train_start = train_start + self._test
