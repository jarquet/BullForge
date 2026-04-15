# Full-History Backfill on Startup & Symbol Add

**Status:** Draft
**Date:** 2026-04-14
**Owner:** data_pipeline

## Problem

The data pipeline currently backfills a fixed window on startup
(`days_1m=730`, `days_daily=365`) and a smaller window on symbol add
(`days_1m=30`, `days_daily=365`). Both are arbitrary caps that leave
older history unavailable to the feature engine and model service,
even though Alpaca exposes equity bars back to ~2016 and crypto bars
back to ~2021.

We want every configured symbol to converge to *all available* day and
minute history, both on startup and when symbols are added at runtime,
without blocking live streaming for hours on a cold boot.

## Goals

- On startup, every (symbol, timeframe) in
  `trading_symbols_equity` and `trading_symbols_crypto` reaches the
  configured floor date for both `1m` and `1d` bars.
- New symbols added via the `config:symbols_changed` redis channel
  follow the same convergence path.
- Live streaming and the health endpoint come up promptly — they wait
  only on a small forward gap-fill, never on a multi-hour reach-back.
- Restarts are cheap once history is in place (no redundant work).

## Non-Goals

- Concurrent per-symbol backfill. We stay sequential to live well
  inside Alpaca's 200 req/min limit.
- A live Alpaca integration test in CI.
- Any change to the live-tick path or the bars schema.

## Strategy: Two-Phase Convergence

For each (symbol, timeframe) we maintain two invariants:

1. **Forward invariant** — the latest stored bar reaches "now".
2. **Reach-back invariant** — the earliest stored bar reaches the
   configured floor date.

Each startup and each symbol-add runs both phases:

- **Forward fill** is blocking. It pulls
  `(latest_stored + 1) → now`, or `floor → now` if the DB is empty.
  This is small and fast on a warm DB, and it must complete before
  live streaming starts so there is no gap between historical and
  live ticks.
- **Reach-back** is a background `asyncio` task. It pulls
  `floor → earliest_stored` if a gap exists, otherwise no-ops. On a
  cold boot this is the expensive step (potentially hours for 1m
  bars across many symbols); on a warm boot it is a no-op.

`insert_bars` is already upsert-based, so partial work from a
cancelled reach-back is safe to resume on the next startup.

## Configuration

Add to `shared/config.py` (`Settings`):

| Setting | Default | Notes |
| --- | --- | --- |
| `backfill_floor_equity` | `2016-01-01` | Alpaca's earliest equity history |
| `backfill_floor_crypto` | `2021-01-01` | Alpaca's earliest crypto history |

Both are `date`. The floor is selected per-symbol by kind (equity vs
crypto) at backfill time.

## Components & Changes

### `shared/db.py`

Add `get_bar_range(symbol, timeframe) -> tuple[datetime | None, datetime | None]`
returning `(earliest_ts, latest_ts)` from the `bars` table. Both
`None` when the symbol has no bars at that timeframe.

### `services/data_pipeline/backfill.py`

Refactor `Backfiller`:

- `_floor_for(symbol) -> date` — picks equity vs crypto floor based on
  whether the symbol is in `trading_symbols_crypto`.
- `forward_fill(symbol, timeframe) -> int` — looks up the latest
  stored bar; pulls `(latest + 1) → now`, or `floor → now` if empty.
  Returns bars inserted.
- `reach_back(symbol, timeframe) -> int` — looks up the earliest
  stored bar; if `earliest > floor`, pulls `floor → earliest`. No-op
  otherwise. Returns bars inserted.
- `backfill_all_forward()` — runs `forward_fill` for every configured
  (symbol, timeframe). Sequential. Per-symbol failures are logged and
  do not abort the loop. Emits a WARN summary listing failed symbols.
- `backfill_all_reach_back()` — runs `reach_back` for every configured
  (symbol, timeframe). Sequential. Designed to be awaited as a
  background task. Cooperative-cancellation safe.
- `backfill_symbol(symbol, kind) -> int` — runs `forward_fill` for
  both timeframes synchronously. The caller is responsible for
  scheduling `reach_back` as a background task.

The existing `backfill_timeframe(symbol, timeframe, days)` and
`backfill_all(days_1m, days_daily)` are removed — no callers remain
after the migration.

### `services/data_pipeline/main.py`

- Replace the call to `backfiller.backfill_all(days_1m=730, days_daily=365)`
  with `await backfiller.backfill_all_forward()`.
- After streams and the health endpoint are up, schedule
  `asyncio.create_task(backfiller.backfill_all_reach_back())` and
  track it in a `background_tasks: set[asyncio.Task]` so the shutdown
  path can cancel it cleanly.
- Update `handle_symbol_change_event` so that, after the synchronous
  `backfill_symbol` finishes and `subscribe_symbol` is called, it
  schedules a per-symbol `reach_back` for both timeframes as a
  background task tracked in the same set.

## Concurrency

Symbols are processed sequentially within each phase. The Alpaca SDK
pages internally per (symbol, timeframe), so a single symbol's pull
already saturates a small fraction of the rate limit. Sequential
symbols keep total throughput well under 200 req/min and avoid DB
contention with the live-tick writer.

## Error Handling

- Forward-fill: per-symbol failures `logger.exception` and continue.
  At the end of `backfill_all_forward`, emit a single WARN line
  listing failed symbols.
- Reach-back: per-symbol failures `logger.exception`, increment a
  failure counter, continue. The background task itself never crashes
  the process.
- Shutdown: cancel all tracked background tasks, await them,
  swallowing `CancelledError`. Partial inserts are upserted, so the
  next startup resumes correctly.

## Observability

Publish counters to a `monitoring:data_pipeline` redis channel
(matching the `monitoring:reconciler` pattern from the recent
reconciler work):

- `backfill.forward.bars_inserted` (per symbol+timeframe)
- `backfill.reachback.bars_inserted` (per symbol+timeframe)
- `backfill.failures` (per phase)
- `backfill.reachback.complete` event when the startup sweep finishes

Per-call logs include symbol, timeframe, requested range, bars
inserted, and elapsed seconds.

## Testing

Tests live under `bullforge/tests/`. The Alpaca client is mocked; no
live API calls in CI.

Unit:
- `get_bar_range` returns `(None, None)` on empty, correct values on
  populated.
- `forward_fill` pulls `floor → now` when DB is empty.
- `forward_fill` pulls `latest + 1 → now` when DB has bars.
- `reach_back` is a no-op when `earliest == floor`.
- `reach_back` pulls `floor → earliest` when there is a gap.
- `backfill_all_forward` continues past a failing symbol and reports
  it in the summary.

Integration:
- A `config:symbols_changed` add event triggers synchronous
  forward-fill and schedules a background reach-back task.
- Shutdown cancels in-flight reach-back tasks without raising.

## Migration Notes

- No schema changes.
- The hardcoded `days_1m=730 / days_daily=365` startup call and the
  `lookback_days=30` symbol-add window are removed. Operators relying
  on those windows should expect a one-time long reach-back on the
  next deploy; subsequent restarts are fast.
- The two new floor settings have safe defaults; no env changes
  required.

## Open Questions

None at spec time.
