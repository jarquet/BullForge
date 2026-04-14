# Order Reconciliation Design

**Date:** 2026-04-14
**Status:** Approved (design phase)
**Scope:** `services/execution_service`, `shared/db.py`, `shared/models.py`, `migrations/`

## Problem

`OrderExecutor._check_fill` ([order_executor.py:154-196](../../../bullforge/services/execution_service/order_executor.py#L154-L196)) polls Alpaca 3 times at 1-second intervals after submitting an order. If Alpaca has not reached a terminal state (`filled`, `cancelled`, `rejected`) within that ~3s window, the database row is left at `status='submitted'` permanently. Any subsequent transition in Alpaca (e.g. day-order cancellation at market close, late rejection) is never reflected in BullForge.

Observed symptom: 6 orders recorded as `submitted` in the `orders` table are `cancelled` in the Alpaca dashboard.

## Goals

- Keep `orders` in BullForge consistent with Alpaca's authoritative state, indefinitely and across service restarts.
- Maintain `trades` bookkeeping (entry/exit rows, PnL) correctly for fills that arrive after the submit-time window.
- Detect and ingest orders placed directly in the Alpaca UI so position/exposure calculations remain accurate.
- Provide operational visibility into what the reconciler did and why.

## Non-Goals

- Automatic rollback of `trades` rows when a previously-filled order is later marked cancelled by Alpaca. This is rare and operator-reviewable. The reconciler emits an anomaly event instead of silently mutating trade history.
- Reconciling orders older than 48 hours at startup (bounded scan window).
- Replacing `BrokerRelay` or the signal-to-order flow. The reconciler is additive.

## Architecture

A new `OrderReconciler` subsystem inside `execution_service`, composed of three cooperating pieces:

1. **TradeUpdatesStream** — wraps `alpaca.trading.stream.TradingStream`. Subscribes to trade updates and forwards normalized events (matching the dict shape of `AlpacaBroker._sync_get_order`) to the reconciler core. Reconnect loop uses exponential backoff (1s → 30s cap). On reconnect, triggers an immediate sweep to backfill events missed during disconnection.

2. **ReconcilerCore** — stateless handler. Single source of truth for translating an Alpaca order snapshot into BullForge state. Used by the stream, the polling sweeper, and `OrderExecutor` immediately after submit.

3. **PollingSweeper** — background task on a 60-second interval.
   - Every tick: queries non-terminal DB orders, fetches each from Alpaca, passes to `ReconcilerCore`.
   - Every 5th tick (~5 min): lists open Alpaca orders in the last 48h and ingests any whose `alpaca_order_id` is not in our DB as `origin='external'`.

Data flow: Alpaca → (stream **or** sweep) → ReconcilerCore → Postgres + Redis publish → downstream consumers (dashboard, monitoring).

`main.py` instantiates the core, starts the stream and the sweeper alongside `BrokerRelay`. On startup, one immediate polling sweep runs before the stream attaches, to recover any orders the service left in a non-terminal state.

## Schema Changes

Migration `migrations/010_order_reconciliation.sql`:

```sql
ALTER TABLE orders
  ADD COLUMN origin TEXT NOT NULL DEFAULT 'internal'
    CHECK (origin IN ('internal', 'external')),
  ADD COLUMN last_reconciled_at TIMESTAMPTZ,
  ADD COLUMN reconcile_source TEXT
    CHECK (reconcile_source IN ('submit','stream','poll','startup'));

CREATE INDEX idx_orders_non_terminal
  ON orders (submitted_at DESC)
  WHERE status NOT IN ('filled','cancelled','rejected','expired');

CREATE UNIQUE INDEX idx_orders_alpaca_id
  ON orders (alpaca_order_id)
  WHERE alpaca_order_id IS NOT NULL;
```

- `origin` distinguishes BullForge-initiated vs ingested orders.
- `last_reconciled_at` and `reconcile_source` provide operational visibility.
- Partial index on non-terminal orders keeps the sweep query cheap as the table grows.
- Unique index on `alpaca_order_id` makes upserts idempotent and safe under stream/sweep races.

Corresponding changes to `shared/models.py::Order` add the three fields (defaulted). `shared/db.py` gains:
- `query_non_terminal_orders(older_than: timedelta = 48h)` — used by the sweeper.
- `upsert_order_from_alpaca(snapshot, source)` — uses `INSERT ... ON CONFLICT (alpaca_order_id) DO UPDATE ... RETURNING (old_status, new_status, id, entry_order_id_of_existing_trade)` so the caller can detect transitions.

## Components

### `services/execution_service/reconciler_core.py`

```python
class ReconcilerCore:
    def __init__(self, db, redis_client, broker): ...

    async def apply_alpaca_order(
        self,
        alpaca_order: dict,     # normalized snapshot from AlpacaBroker
        source: str,            # 'submit' | 'stream' | 'poll' | 'startup'
    ) -> None:
        """Idempotent. Upserts orders row by alpaca_order_id, handles
        trade side-effects on terminal transitions, publishes Redis
        event only when status actually changed."""
```

Logic:
1. `UPSERT orders ON CONFLICT (alpaca_order_id)` — sets status, fills, `last_reconciled_at`, `reconcile_source=source`. If inserting (no prior row), sets `origin='external'`.
2. Compare returned old status vs new status.
3. On terminal transition to `filled` (and no existing trade for this `entry_order_id`):
   - For buy/sell opens: insert `trades` row (logic extracted from `_execute_open`).
   - For closes (symbol already has an open trade): update the open trade with exit fields and PnL (logic extracted from `_execute_close`).
4. On any status change: publish to `execution:orders` Redis channel.
5. On `filled → cancelled` (pre-existing filled row getting marked cancelled by Alpaca): log ERROR, publish to `execution:orders:anomaly`, do not mutate the trade row.

### `services/execution_service/trade_updates_stream.py`

```python
class TradeUpdatesStream:
    def __init__(self, settings, core: ReconcilerCore,
                 on_reconnect: Callable[[], Awaitable[None]]): ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
```

Normalizes the Alpaca SDK's `TradeUpdate` event into the same dict shape `AlpacaBroker._sync_get_order` returns, then calls `core.apply_alpaca_order(snapshot, source='stream')`. Malformed events are logged and skipped. Connection failures trigger the reconnect loop; on successful reconnect, `on_reconnect()` is invoked (wired to the sweeper's immediate-sweep method).

### `services/execution_service/polling_sweeper.py`

```python
class PollingSweeper:
    def __init__(
        self, db, broker, core,
        interval_s: int = 60,
        orphan_every: int = 5,
    ): ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def sweep_now(self) -> None:   # used on startup + stream reconnect
```

Each tick:
1. `db.query_non_terminal_orders()` → for each row, `broker.get_order(alpaca_id)` wrapped in per-row try/except, pass to core with `source='poll'`.
2. Every `orphan_every` ticks: list open Alpaca orders bounded to last 48h, pass any unknowns to core.

### Refactor to `OrderExecutor`

- Remove `_check_fill` and its retry constants.
- `_execute_open` / `_execute_close`: after `broker.submit_market_order` / `broker.close_position`, fetch the initial snapshot via `broker.get_order(alpaca_id)` and call `core.apply_alpaca_order(snapshot, source='submit')`. The submit path no longer performs trade insertion or fill polling directly — the core does it.
- `process_signal` returns the Order row in its immediate post-submit state. Downstream consumers that need fill notification subscribe to `execution:orders`.

### `services/execution_service/main.py`

Instantiate `ReconcilerCore`, `PollingSweeper`, `TradeUpdatesStream` after `BrokerRelay`. Start order: core → sweeper → immediate `sweep_now()` → stream (with `on_reconnect=sweeper.sweep_now`).

## Error Handling

| Scenario | Behavior |
|---|---|
| Stream disconnects | Exponential backoff reconnect (1s→30s cap); on reconnect, fire `sweep_now()` |
| Malformed stream event | Log, skip, continue — never crash the task |
| Per-order fetch fails during sweep | Log, continue; retried next tick |
| Alpaca list call fails during orphan scan | Log; continue; retried in `orphan_every` ticks |
| Stream and sweep observe same fill | Idempotent via unique index + old/new status guard; second observer is a no-op |
| `filled → cancelled` from Alpaca | ERROR log + anomaly Redis event; no automatic trade rollback |
| Service restart with in-flight orders | Startup `sweep_now()` reconciles them before stream attaches |

## Observability

- Structured log on every `apply_alpaca_order` call: `alpaca_id, old_status, new_status, source`.
- Counters emitted on the existing `monitoring_service` channel:
  - `reconciler_events_total{source,transition}`
  - `reconciler_orphans_ingested_total`
  - `reconciler_anomalies_total`
  - `reconciler_stream_reconnects_total`

## Testing

### Unit

- `test_reconciler_core.py`
  - `submitted → filled` inserts a trade, publishes event.
  - `submitted → cancelled` updates status, no trade row.
  - Duplicate terminal event is a no-op.
  - Entry fill vs exit fill handled correctly (close path updates open trade with PnL).
  - `filled → cancelled` logs ERROR, emits anomaly event, leaves trade row intact.
  - Unknown `alpaca_order_id` upserts with `origin='external'`.

- `test_polling_sweeper.py`
  - Only non-terminal orders fetched (query shape verified).
  - One failing `get_order` doesn't abort the sweep.
  - Orphan scan fires on the Nth tick, not every tick.

- `test_trade_updates_stream.py`
  - Reconnect backoff schedule.
  - Event normalization matches `_sync_get_order` shape.
  - Reconnect triggers `on_reconnect` callback.

- `test_order_executor.py` (updated)
  - `_execute_open` / `_execute_close` delegate to `core.apply_alpaca_order`; no synchronous fill polling remains.

### Integration

`tests/integration/test_order_reconciliation.py` (uses `docker-compose.test.yml`, Alpaca mocked):

1. Submit order; mock holds it `accepted` past 3s, then flips to `cancelled`. Assert DB transitions `submitted → cancelled` within one sweep interval. Direct regression for the reported bug.
2. Restart `execution_service` with an in-flight non-terminal order. Assert startup sweep reconciles it.

### Migration

Run `010_order_reconciliation.sql` on a DB with pre-existing orders: verify `origin` backfills to `'internal'`, indexes are present, no data loss.

## Configuration

New `Settings` fields (env-var driven, sensible defaults):
- `EXECUTION_RECONCILER_POLL_INTERVAL_S=60`
- `EXECUTION_RECONCILER_ORPHAN_EVERY=5`
- `EXECUTION_RECONCILER_ORPHAN_WINDOW_HOURS=48`
- `EXECUTION_RECONCILER_STREAM_ENABLED=true` (escape hatch if stream is unstable in production)

## Migration / Rollout

1. Apply migration `010_order_reconciliation.sql` (backward-compatible; existing rows default to `origin='internal'`).
2. Deploy the new `execution_service`. `_check_fill` is removed; `ReconcilerCore` takes over. The startup `sweep_now()` automatically reconciles the 6 known-stuck orders on first boot.
3. If the Alpaca trade-updates stream proves unstable in production, set `EXECUTION_RECONCILER_STREAM_ENABLED=false` — the polling sweeper alone still closes the consistency gap, just at higher latency.
4. Monitor `reconciler_anomalies_total` for a week before considering follow-up work on automatic trade rollback.
