"""One-off benchmark for RegimeBuilder._enrich at ~200K rows.

Settles whether the append_bar docstring claim of "tens of ms" is
accurate or whether the 30-day rolling median is actually closer to
hundreds of ms, a cost paid on every SPY tick. Invoke via:

    cd bullforge && bin/test python scripts/bench_regime_enrich.py

Not a regression test; a measurement script. No CI hookup intended.
"""
from __future__ import annotations

import statistics
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from services.model_service.meta.regime import RegimeBuilder


N_BARS = 200_000  # ~138.9 days of 1m bars
N_ITERS = 5
SEED = 42


def _build_spy_frame(n: int) -> pd.DataFrame:
    """Synthetic 1-minute SPY frame: UTC DatetimeIndex + close column.

    Random-walk closes are reproducible via a seeded default_rng. The
    actual price scale doesn't matter for timing — pandas rolling cost
    is shape-driven, not value-driven.
    """
    rng = np.random.default_rng(SEED)
    start = datetime(2025, 1, 1, 14, 30, tzinfo=timezone.utc)
    index = pd.date_range(start=start, periods=n, freq="1min", tz="UTC",
                          name="timestamp")
    closes = 100.0 + np.cumsum(rng.standard_normal(n) * 0.01)
    return pd.DataFrame({"close": closes}, index=index)


def _verdict(median_ms: float) -> str:
    if median_ms < 50:
        return (f"VERDICT: docstring claim is correct "
                f"(median {median_ms:.1f}ms < 50ms threshold)")
    if median_ms <= 200:
        return (f"VERDICT: borderline (median {median_ms:.1f}ms — soften "
                "docstring to 'tens to low-hundreds of ms')")
    return (f"VERDICT: docstring is wrong (median {median_ms:.1f}ms exceeds "
            "200ms — file follow-up to rewrite incrementally)")


def main() -> None:
    print(f"Building synthetic SPY frame: {N_BARS:,} 1m bars "
          f"(~{N_BARS / (60 * 24):.1f} days)")
    frame = _build_spy_frame(N_BARS)
    print(f"  shape={frame.shape}, index dtype={frame.index.dtype}")

    print(f"Timing RegimeBuilder._enrich over {N_ITERS} iterations "
          "(perf_counter, end-to-end)...")
    timings_ms: list[float] = []
    for i in range(N_ITERS):
        t0 = time.perf_counter()
        out = RegimeBuilder._enrich(frame)
        t1 = time.perf_counter()
        elapsed_ms = (t1 - t0) * 1000.0
        timings_ms.append(elapsed_ms)
        # Touch the output so the optimizer can't elide work.
        assert "vol_regime" in out.columns
        print(f"  iter {i + 1}: {elapsed_ms:.1f} ms")

    median_ms = statistics.median(timings_ms)
    min_ms = min(timings_ms)
    max_ms = max(timings_ms)
    print()
    print(f"median={median_ms:.1f} ms   min={min_ms:.1f} ms   "
          f"max={max_ms:.1f} ms   (n={N_ITERS})")
    print(_verdict(median_ms))


if __name__ == "__main__":
    main()
