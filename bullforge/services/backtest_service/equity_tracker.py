from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class EquityPoint:
    timestamp: datetime
    equity: float
    cash: float


class EquityTracker:
    def __init__(self):
        self._points: list[EquityPoint] = []

    def record(self, timestamp: datetime, *, equity: float, cash: float) -> None:
        self._points.append(EquityPoint(timestamp, equity, cash))

    def points(self) -> list[EquityPoint]:
        return list(self._points)

    def returns(self) -> list[float]:
        out = []
        for prev, curr in zip(self._points, self._points[1:]):
            if prev.equity == 0:
                out.append(0.0)
            else:
                out.append((curr.equity - prev.equity) / prev.equity)
        return out
