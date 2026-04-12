from __future__ import annotations

import logging
from typing import Callable, Dict, List

import pandas as pd

logger = logging.getLogger(__name__)

CalculatorFn = Callable[[pd.DataFrame], pd.DataFrame]


class FeatureRegistry:
    def __init__(self):
        self._calculators: Dict[str, CalculatorFn] = {}

    def register(self, name: str, fn: CalculatorFn) -> None:
        if name in self._calculators:
            raise ValueError(f"Calculator '{name}' already registered")
        self._calculators[name] = fn
        logger.debug("Registered feature calculator: %s", name)

    def list_calculators(self) -> List[str]:
        return list(self._calculators.keys())

    def compute_all(self, bars_df: pd.DataFrame) -> pd.DataFrame:
        parts = []
        for name, fn in self._calculators.items():
            logger.debug("Running calculator: %s", name)
            result = fn(bars_df)
            parts.append(result)
        if not parts:
            return pd.DataFrame(index=bars_df.index)
        return pd.concat(parts, axis=1)
