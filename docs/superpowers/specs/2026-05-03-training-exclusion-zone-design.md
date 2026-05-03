# Training-time exclusion-zone filter — design

**Status:** approved (auto-mode brainstorm)
**Date:** 2026-05-03
**Author:** Claude (Opus 4.7)
**Source:** TODO.md § "Market open/close exclusion zone", item 2 ("Skip training 1d horizons whose evaluation window lands in the exclusion zone")

## Problem

The live `RiskManager` refuses equity orders whose `now` is outside `[09:30 ET + open_buf, 16:00 ET - close_buf)` via `shared/trading_window.py::is_trading_allowed`. But the per-symbol model trainer (`services/model_service/trainer.py`) is fit on rows whose forward-return *target* timestamp can land inside that excluded window — i.e., the model learns from outcomes the live system would never act on. This is a real label-leakage / live-vs-train mismatch, especially for 1d horizons whose target lands at 16:30 ET (after-close) or on a weekend.

The exclusion-zone gate (A1) is the live-side enforcement; this work (A2) is the training-side counterpart.

## Goals

1. Drop training rows whose `(feature_timestamp + horizon_minutes)` lands in the equity exclusion zone, *as defined by the same `is_trading_allowed` helper the live gate uses*.
2. Crypto symbols pass through unfiltered (24/7 markets).
3. Pull buffer minutes from `RuntimeConfig` so the operator-tuned values flow through to training without code changes. Sandbox/backtest training picks up the override dict the same way.
4. Don't change the on-disk `labels` table or `compute_forward_return_labels` — labels stay pure; filtering happens at training-data selection time.
5. Don't touch `MetaLabelBackfiller`. It runs its own bar-replay path and is out of scope.
6. Backward compat: existing tests that mock `query_training_data` and don't supply buffers must keep passing.

## Design

### Helper

New `bullforge/services/feature_engine/exclusion.py`:

```python
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
    zone for `symbol`. Crypto symbols always return False (always allowed)."""
    target = feature_timestamp + timedelta(minutes=horizon_minutes)
    allowed, _ = is_trading_allowed(
        symbol, target,
        open_buffer_min=open_buffer_min, close_buffer_min=close_buffer_min,
    )
    return not allowed
```

The helper is single-purpose: target-timestamp check only. The feature-timestamp itself is not gated — a row whose `timestamp` lands in the open buffer but whose target lands clean is fine. (A row's *features* are computed from past bars; the gate is about the *outcome* the model is learning to predict.)

### DatasetBuilder.build

`bullforge/services/model_service/dataset.py::DatasetBuilder.build` gains two optional kwargs:

```python
async def build(
    self,
    symbol: str,
    timeframe: str,
    horizon_minutes: int,
    *,
    as_of: datetime,
    limit: int = 10000,
    open_buffer_min: Optional[int] = None,
    close_buffer_min: Optional[int] = None,
) -> Optional[Tuple[pd.DataFrame, pd.Series]]:
```

Both `None` → no filter applied. Both ints → filter rows via `is_evaluation_window_in_exclusion(symbol, row["timestamp"], horizon_minutes, open_buffer_min=…, close_buffer_min=…)` *before* the NaN-mask step. Insufficient-data check after filter, same threshold as today.

The `Optional[None]` default preserves backward compat with the existing `tests/test_trainer.py` and `tests/test_dataset_builder.py` cases that don't pass buffers (filter is a no-op).

### Trainer.train

`bullforge/services/model_service/trainer.py::Trainer.train` reads the buffer keys from the `runtime_config` dict (which is the full `RuntimeConfig.get_all()` snapshot in production via `scheduler.py:185` and `worker.py:48`):

```python
open_buf = runtime_config.get("equity_window_open_buffer_minutes") if runtime_config else None
close_buf = runtime_config.get("equity_window_close_buffer_minutes") if runtime_config else None
result = await self._dataset_builder.build(
    symbol, timeframe, horizon_minutes, as_of=as_of,
    open_buffer_min=open_buf, close_buffer_min=close_buf,
)
```

If `runtime_config` is None → no filter. If `runtime_config` exists but lacks the keys → no filter. Otherwise → filter applied with the operator-set values.

### main.py startup path

`bullforge/services/model_service/main.py:275` currently calls `trainer.train(symbol, …, model_type="gradient_boost")` without `runtime_config`, so the startup path is unfiltered. Fix: snapshot the live RC into a dict and pass it. One line change.

```python
rc_snapshot = await runtime_config.get_all()
…
await trainer.train(
    symbol, timeframe, horizon,
    as_of=startup_as_of, model_type="gradient_boost",
    runtime_config=rc_snapshot,
)
```

This aligns the startup behavior with the nightly retrain.

## Behavior matrix

| Caller | runtime_config | open_buffer | close_buffer | Filter applied? |
|---|---|---|---|---|
| `tests/test_trainer.py` (mocks) | None | — | — | No |
| `tests/test_dataset_builder.py` (mocks) | None | — | — | No |
| `main.py` startup (after fix) | RC snapshot | int (default 60) | int (default 60) | Yes |
| `scheduler.py` nightly retrain | RC snapshot | int | int | Yes |
| `worker.py` ProcessPool | RC snapshot | int | int | Yes |
| `scripts/backtest.py` (sandbox) | override dict | int (override or RC) | int (override or RC) | Yes |

## Tests (TDD)

### Helper tests — `tests/test_exclusion.py` (new file)

Hermetic, no DB. Mirrors `tests/test_trading_window.py`'s style.

- `test_equity_1m_target_in_open_buffer_excluded`: feature_timestamp = 09:25 ET, horizon = 10 → target 09:35 ET (inside 60-min open buffer) → excluded.
- `test_equity_1m_target_midday_survives`: feature_timestamp = 10:50 ET, horizon = 10 → target 11:00 ET → not excluded.
- `test_equity_1m_target_in_close_buffer_excluded`: feature_timestamp = 14:55 ET, horizon = 10 → target 15:05 ET (inside 60-min close buffer) → excluded.
- `test_equity_1d_target_on_saturday_excluded`: feature_timestamp = Friday 14:00 ET, horizon = 1440 → target Saturday 14:00 ET → excluded.
- `test_equity_1d_target_midweek_survives`: feature_timestamp = Monday 14:00 ET, horizon = 1440 → target Tuesday 14:00 ET → not excluded.
- `test_crypto_never_excluded`: feature_timestamp = any, symbol = "BTC/USD" → not excluded.
- `test_boundary_target_at_open_plus_buffer_inclusive`: target == 10:30 ET exactly with buffer = 60 → not excluded (`<=` semantics in `trading_window.py:45`).
- `test_boundary_target_at_close_minus_buffer_minus_1s_excluded`: target == 15:00 ET - 1s with buffer = 60 → excluded (`<` semantics).

### DatasetBuilder tests — extend `tests/test_dataset_builder.py`

- `test_dataset_builder_filters_exclusion_zone`: mock `query_training_data` returning rows where half have `(timestamp + horizon)` in the exclusion zone; call `build` with buffer minutes set; assert filtered count matches the half outside the zone.
- `test_dataset_builder_no_filter_when_buffers_none`: mock same rows; call `build` without buffer kwargs; assert all rows survive (backward compat).
- `test_dataset_builder_crypto_no_filter`: rows for `BTC/USD`; call `build` with buffers set; assert all rows survive.

### Trainer tests — extend `tests/test_trainer.py`

- `test_trainer_passes_buffer_minutes_to_dataset_builder`: spy on `DatasetBuilder.build`; pass `runtime_config={"equity_window_open_buffer_minutes": 60, "equity_window_close_buffer_minutes": 60}`; assert build was called with `open_buffer_min=60, close_buffer_min=60`.
- `test_trainer_passes_none_buffers_when_no_runtime_config`: existing tests already cover the None case implicitly; assert explicitly that build was called with `open_buffer_min=None, close_buffer_min=None`.

## Files changed

- `bullforge/services/feature_engine/exclusion.py` — new helper.
- `bullforge/services/model_service/dataset.py` — add buffer params to `DatasetBuilder.build`.
- `bullforge/services/model_service/trainer.py` — read buffers from `runtime_config` dict and pass through.
- `bullforge/services/model_service/main.py` — snapshot RC and pass through.
- `bullforge/tests/test_exclusion.py` — new helper tests.
- `bullforge/tests/test_dataset_builder.py` — new filter tests.
- `bullforge/tests/test_trainer.py` — assert buffer plumbing.

## Out of scope

- `MetaLabelBackfiller` — bar-replay logic, separate consumer; left alone.
- The labels table itself / `compute_forward_return_labels` — stays pure, no calendar awareness.
- Holiday calendar handling — `is_trading_allowed` deliberately skips it (`trading_window.py:26-27`); the A2 filter mirrors that scope.
- Tightening `is_trading_allowed` boundary semantics — preserve current behavior, no refactor.
- Refactoring `RiskManager`, the dashboard Config tab, or `runtime_config` plumbing.
- Per-symbol exchange calendars (NYSE only today; futures/Asian markets out of scope).
