# Reconciler drain semantics + Alpaca replay CLI — design

**Status:** approved (auto-mode brainstorm)
**Date:** 2026-05-04
**Author:** Claude (Opus 4.7)
**Source:** `NEXT_PROMPT.md` carry-over after the 2026-05-04 crypto long-only fix; production incident around 2026-05-05 00:57–01:02 UTC where four BTC/USD trade rows (ids 150–153) went stale after a manual web-UI liquidation whose sell event never reached the trade-updates stream.

The crypto long-only RiskManager guard is already shipped (`fix/execution-crypto-long-only`, merged 2026-05-04). This design covers the structural follow-up: BullForge tracks per-trade rows (one row per entry signal), Alpaca tracks aggregate positions (one ticker → one quantity). A single broker-side "close position" click emits one sell for the full quantity, so even replaying it through the existing reconciler — which matches each fill against exactly one open trade row — would close only one of the four open rows for BTC/USD.

Two intertwined changes plus one host-side CLI fix this without touching the schema:

1. **Drain semantics in `ReconcilerCore._handle_fill`.** A fill that aggregates N open rows of the same symbol+side closes them all (or the FIFO oldest fitting subset on partial), emitting per-row pnl and per-row `position_closed` alerts.
2. **Closed-orders fetch in `AlpacaBroker`.** Generalize the existing `_sync_list_open_orders` helper so we can also pull `QueryOrderStatus.CLOSED` snapshots.
3. **Replay CLI `bullforge/scripts/reconcile_from_alpaca.py`.** Host-side standalone script (mirrors `scripts/backtest.py` wiring) that fetches closed Alpaca orders since a cutoff and feeds them through `ReconcilerCore.apply_alpaca_order` with `source="manual_replay"`. Idempotent — already-known orders no-op via `upsert_order_from_alpaca`'s `alpaca_order_id` keying.

After both land and merge, run the CLI against production with explicit user confirmation to flatten rows 150–153.

## Bug — `_handle_fill` matches one row, not N

### Problem

`services/execution_service/reconciler_core.py:87-133` (`_handle_fill`):

```python
open_trade = await self._db.get_open_trade(order.symbol)   # LIMIT 1, ORDER BY entry_time DESC

is_closing = open_trade is not None and (
    (open_trade.side == "long" and order.side == "sell")
    or (open_trade.side == "short" and order.side == "buy")
)

if is_closing:
    pnl = (order.filled_price - open_trade.entry_price) * open_trade.quantity
    ...
    await self._db.update_trade_exit(trade_id=open_trade.id, ...)
```

`get_open_trade` returns the most recent single open row. When a fill aggregates N open rows (typical case: user manually closes a position via the broker's web UI; rare case: stream miss followed by a polling sweep over a closed position), only one row gets the exit_time/pnl update. The other N−1 rows stay stale until ops notices.

Live risk gates are unaffected because `PositionTracker.refresh()` reads positions from the broker, not the trades table — but `position_closed` alerts never fire for the stale rows, fidelity tracking under-counts realized pnl until an operator manually fixes the rows, and a future buy fill on the same symbol would create a fifth open row alongside the four stale ones.

### Goals

1. A fill that aggregates N open rows closes all N atomically with the same exit_time/exit_price, per-row pnl computed against each row's own `entry_price` × `quantity`.
2. One `position_closed` alert per row on `monitoring:alerts` — preserves the existing channel contract that every subscriber (Telegram bot, dashboard, fidelity tracker) already relies on. No subscriber changes.
3. Symmetric: applies to `buy + open_short` as well as `sell + open_long`. Equities can short. Cheap to include and avoids leaving a parallel bug latent.
4. Single-open-row case **production behavior** is unchanged: same DB writes, same single `position_closed` alert, same pnl math. Existing unit tests in `tests/test_reconciler_core.py` will need a one-line mechanical swap from `mock_db.get_open_trade.return_value = ...` to `mock_db.get_open_trades.return_value = [...]` (or `[]`) — what they assert about the production behavior stays identical. Same one-line addition in `tests/test_order_reconciliation_integration.py`'s `FakeDB` (line 119 area, sibling to the existing `get_open_trade` method).
5. Anomalies on the under/over branches are surfaced via the existing `monitoring:reconciler` counter publishes (`reconciler_anomalies_total{kind=...}`) and `execution:orders:anomaly` channel — same shape the `filled_reverted` anomaly already uses.

### Design

Replace `get_open_trade` with `get_open_trades` (plural) in `_handle_fill`:

```python
async def _handle_fill(self, order: Order) -> None:
    if order.filled_price is None or order.filled_at is None:
        logger.warning(...)
        return

    open_trades = await self._db.get_open_trades(order.symbol)   # FIFO oldest-first

    if not open_trades:
        # opening fill — create a new trade row (existing else-branch behavior)
        ...
        await self._db.insert_trade(trade)
        return

    side = open_trades[0].side
    is_closing = (
        (side == "long" and order.side == "sell")
        or (side == "short" and order.side == "buy")
    )
    if not is_closing:
        # opening fill in the opposite direction — same insert path as above
        # (this branch is exercised today only via external/manual orders;
        # a sell-with-no-open-long currently inserts a short row, which is
        # by design for equities and out-of-scope for crypto per CLAUDE.md)
        ...
        await self._db.insert_trade(trade)
        return

    # is_closing — drain
    fq = float(order.filled_quantity)
    total_open = sum(float(t.quantity) for t in open_trades)

    if fq >= total_open:
        rows_to_close = open_trades
        if fq > total_open:
            await self._publish_counter(
                "reconciler_anomalies_total", kind="over_close",
            )
            await self._publish("execution:orders:anomaly", order)
    else:
        # fq < total_open — partial drain, FIFO close oldest fitting rows
        rows_to_close = []
        running = 0.0
        for t in open_trades:
            if running + float(t.quantity) <= fq:
                rows_to_close.append(t)
                running += float(t.quantity)
            else:
                break
        residual = fq - running
        if residual > 0:
            await self._publish_counter(
                "reconciler_anomalies_total", kind="partial_drain_residual",
            )
            await self._publish("execution:orders:anomaly", order)
        # If rows_to_close is empty (residual == fq, i.e. fq < smallest row),
        # we close zero rows. The anomaly above is the only side effect.
        # An exact alignment (residual == 0) closes the FIFO subset cleanly
        # with no anomaly — that's the "happy partial" path.

    for t in rows_to_close:
        pnl = (order.filled_price - t.entry_price) * t.quantity
        if t.side == "short":
            pnl = -pnl
        hold_secs = int((order.filled_at - t.entry_time).total_seconds())
        await self._db.update_trade_exit(
            trade_id=t.id,
            exit_time=order.filled_at,
            exit_price=order.filled_price,
            exit_order_id=order.id,
            pnl=pnl,
            hold_duration_seconds=hold_secs,
        )
        await self._publish_position_closed(
            open_trade=t, order=order,
            pnl=pnl, hold_duration_seconds=hold_secs,
        )
```

**Why FIFO for partial.** Matches conventional accounting (oldest lots realize first), gives the longest-held rows the realized pnl, and is deterministic without splitting rows. Row-splitting is explicitly out of scope — too much complexity for a case that hasn't actually occurred yet.

**Why "close all + anomaly" for over-close.** Net DB state matches broker (zero open rows for the symbol), the discrepancy is surfaced for ops review via the anomaly counter and channel, and we don't fabricate trade state we can't justify. The alternative (refuse + anomaly only) leaves rows stale — the same bug we're fixing.

**Why no row-splitting.** A partial fill that lands inside a row would require splitting that row into closed + remaining halves. Doable but multiplies the testing surface and the schema implications (do row-IDs change?). Defer until a real partial case forces our hand.

### Anomaly counter labels

Two new label values for the existing `reconciler_anomalies_total` counter:

| `kind`                     | Trigger                                              | Anomaly publish on `execution:orders:anomaly`? |
|----------------------------|------------------------------------------------------|------------------------------------------------|
| `over_close`               | `filled_qty > sum(open_qty)`                         | yes                                            |
| `partial_drain_residual`   | `filled_qty < sum(open_qty)` AND residual > 0        | yes                                            |

The existing `filled_reverted` kind is unchanged.

## Database helper — `get_open_trades(symbol)`

### Problem

`shared/db.py:1518-1547` (`get_open_trade`) returns at most one row, ordered DESC by `entry_time`. The drain branch needs all open rows for the symbol, FIFO-ordered.

### Design

Add `get_open_trades(symbol: str) -> list[Trade]` next to `get_open_trade`. Same SELECT projection, no LIMIT, `ORDER BY entry_time ASC` so callers iterate FIFO without a re-sort:

```python
async def get_open_trades(self, symbol: str) -> list[Trade]:
    async with self.pool.acquire() as conn:
        rows = await conn.fetchrow(...)  # actually fetch, see below
    return [Trade(...) for r in rows]
```

`get_open_trade` stays as-is. The only production caller is `ReconcilerCore._handle_fill` (which we're retargeting to `get_open_trades`); the rest are tests (`tests/test_monitoring_db.py` keeps testing the singular method, integration test `FakeDB` needs `get_open_trades` added as a sibling). Keeping the singular method around avoids a wide-blast-radius rename for behavior we can preserve.

## AlpacaBroker — closed-orders fetch

### Problem

`alpaca_broker.py:104-111` (`_sync_list_open_orders`) hardcodes `QueryOrderStatus.OPEN`:

```python
def _sync_list_open_orders(self, after) -> list[dict]:
    request = GetOrdersRequest(
        status=QueryOrderStatus.OPEN,
        after=after,
        limit=500,
    )
    orders = self._client.get_orders(request)
    return [self._format_order_snapshot(o) for o in orders]
```

The replay CLI needs `QueryOrderStatus.CLOSED` snapshots since a cutoff.

### Design

Generalize the sync helper to accept a status enum, route the existing open-orders method through it, add a new closed-orders public async method:

```python
def _sync_list_orders(
    self, status: QueryOrderStatus, after, limit: int = 500,
) -> list[dict]:
    request = GetOrdersRequest(status=status, after=after, limit=limit)
    orders = self._client.get_orders(request)
    return [self._format_order_snapshot(o) for o in orders]

def _sync_list_open_orders(self, after) -> list[dict]:
    return self._sync_list_orders(QueryOrderStatus.OPEN, after)

async def list_closed_orders(self, after) -> list[dict]:
    return await asyncio.to_thread(
        self._sync_list_orders, QueryOrderStatus.CLOSED, after, 500,
    )
```

`_format_order_snapshot` is unchanged — already returns the dict shape `apply_alpaca_order` expects.

**Pagination.** `limit=500` matches the existing call, well above any plausible 24h or even multi-day window for our symbol set. If the response hits 500, log a warning and require operator follow-up; pagination beyond that is out of scope for the one-shot recovery script.

## Replay CLI — `bullforge/scripts/reconcile_from_alpaca.py`

### Problem

The four stale BTC/USD rows can't be cleaned up by the live reconciler — the missed sell events are no longer in the open-orders polling window, and even if they were, the existing single-row `_handle_fill` would only close one of the four. We need a one-shot host-side tool that pulls closed orders directly from Alpaca and feeds them into the (now drain-aware) reconciler.

### Goals

1. Standalone host-side execution like `scripts/backtest.py` — connects to host ports 15432 / 16379 via `Settings()`, no docker compose service.
2. Idempotent — re-runnable safely. Already-known orders no-op via `upsert_order_from_alpaca`'s `FOR UPDATE` lookup on `alpaca_order_id`.
3. Reasonable defaults that work for the actual incident (default lookback 24h covers the 2026-05-05 manual liquidation).
4. Dry-run mode for confirming what would be replayed before touching state.
5. End-of-run summary so the operator can confirm the result without separate SQL queries.

### Design

```python
# bullforge/scripts/reconcile_from_alpaca.py
"""One-shot Alpaca → DB reconciliation for closed orders.

Use when the trade-updates websocket missed events (e.g., a manual broker-UI
position close) and the polling sweeper has already moved past the relevant
window. Idempotent: known alpaca_order_ids are no-op'd by upsert.
"""

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Replay Alpaca closed orders into BullForge")
    p.add_argument("--symbol", required=True,
                   help="comma-separated symbols, e.g. BTC/USD or BTC/USD,ETH/USD")
    p.add_argument("--since", type=_parse_iso, default=None,
                   help="ISO datetime UTC; default = now - 24h")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


async def _main() -> None:
    args = _parse_args()
    since = args.since or (datetime.now(timezone.utc) - timedelta(hours=24))

    settings = Settings()
    db = Database(settings); await db.connect()
    redis = RedisClient(settings); await redis.connect()
    broker = AlpacaBroker(settings)
    core = ReconcilerCore(db=db, redis_client=redis, broker=broker)

    symbols = [s.strip() for s in args.symbol.split(",") if s.strip()]
    summary = {"orders_fetched": 0, "orders_dispatched": 0,
               "rows_closed_before": None, "rows_closed_after": None,
               "anomalies": 0}

    # Snapshot the relevant trades' open-count before the run for the summary
    # (per-symbol get_open_trades count). On dry-run, we just print what we'd dispatch.
    ...

    for sym in symbols:
        snaps = await broker.list_closed_orders(since)
        sym_snaps = [s for s in snaps if s["symbol"] == sym]
        summary["orders_fetched"] += len(sym_snaps)
        for snap in sym_snaps:
            if args.dry_run:
                logger.info("[dry-run] would replay %s %s qty=%s status=%s filled=%s",
                            snap["id"], snap["side"], snap["qty"],
                            snap["status"], snap.get("filled_at"))
                continue
            await core.apply_alpaca_order(snap, source="manual_replay")
            summary["orders_dispatched"] += 1

    # Print summary table to stdout
    print("\n=== Reconciliation summary ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")
```

**Anomaly counting in the summary.** Subscribe to `monitoring:reconciler` for the duration of the run and increment `summary["anomalies"]` for each `reconciler_anomalies_total` publish whose `labels.symbol` is in our target set. Alternatively (simpler), query the `monitoring:reconciler` count via the existing dashboard exporter — but that introduces a dependency the standalone script doesn't otherwise need. **Pick the in-process subscribe path.**

**Why `source="manual_replay"`.** Distinct from `submit` / `stream` / `poll`. Surfaces the provenance in `orders.reconcile_source` for any later forensics, and keeps the `reconciler_orphans_ingested_total` counter (which only increments on `source == "poll"`) clean.

**Wiring detail mirrored from `scripts/backtest.py`.** Live (non-sandbox) `Database` because we want the rows to land in `public.orders` / `public.trades`, not `backtest.*`. No `RuntimeConfig` needed — the script has no tunables that belong in `bullforge:config`.

### Out of scope for the CLI

- Pagination beyond the 500-order Alpaca page (warn, require operator if hit).
- A docker compose `tools` service for the script. Standalone host invocation matches `scripts/backtest.py`; the script is a one-shot, not a recurring job.
- A sibling REST endpoint on `dashboard_service`. Operators run the script directly with `--dry-run` first.
- Reconciliation of orders the operator placed that BullForge never tracked (no matching `signal`). The reconciler already handles `external` origin orders via the existing `apply_alpaca_order` path; no script-level change needed.

## Tests

All hermetic, in `bullforge/tests/test_reconciler_core.py` (existing AsyncMock pattern, extends the existing `mock_db`/`mock_redis`/`mock_broker` fixtures with `get_open_trades` returning a configurable list):

| Test                                                                            | What it asserts                                                                                                                |
|---------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| `test_drain_full_close_multiple_longs_emits_per_row_alerts_and_pnls`            | 3 open longs at distinct entry_prices, sell fill with `filled_qty == sum(qty)` → 3 `update_trade_exit` calls + 3 `position_closed` alerts; per-row pnl uses each row's own entry_price |
| `test_drain_over_close_closes_all_and_emits_anomaly`                            | filled_qty > sum(qty) → all rows closed + `reconciler_anomalies_total{kind=over_close}` + `execution:orders:anomaly` published |
| `test_drain_partial_fifo_closes_oldest_fitting_rows_and_emits_residual_anomaly` | 4 rows of [10, 10, 10, 10] qty, sell of 25 → closes oldest 2 rows (sum=20), residual 5 → anomaly emitted, 2 newest rows untouched |
| `test_drain_partial_exact_alignment_no_residual_anomaly`                        | 4 rows of [10, 10, 10, 10], sell of 20 → closes oldest 2 rows, residual 0, no anomaly                                          |
| `test_drain_buy_closes_multiple_shorts_symmetric`                               | 2 open shorts, buy fill `qty == sum(qty)` → 2 closes, per-row pnl negated correctly                                            |
| `test_single_open_trade_drain_path_is_existing_behavior`                        | 1 open long, sell of equal qty → exactly one `update_trade_exit` + one `position_closed` alert with correct pnl (single-row regression guard, complementary to the existing 17 tests after their one-line `get_open_trade` → `get_open_trades` mock swap) |

Plus `bullforge/tests/test_reconcile_from_alpaca.py`:

| Test                                                  | What it asserts                                                                                                |
|-------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `test_dispatches_each_closed_snapshot_to_reconciler`  | Mock broker returns 3 snapshots for the target symbol, mock core records each `apply_alpaca_order` call        |
| `test_dry_run_does_not_dispatch`                      | Mock broker returns snapshots, `--dry-run` causes zero `apply_alpaca_order` calls                              |
| `test_filters_to_target_symbols_only`                 | Mock broker returns mixed-symbol snapshots, only target-symbol ones dispatched                                 |
| `test_default_since_is_24h_lookback`                  | When `--since` omitted, the value passed to `list_closed_orders` is within ±5s of `now() - 24h`                |
| `test_summary_counts_match_dispatch`                  | Summary fields match the number of dispatched snapshots                                                        |

No integration tests. Drain logic is hermetic; the CLI is a thin glue layer whose constituents (`AlpacaBroker.list_closed_orders`, `ReconcilerCore.apply_alpaca_order`, `Database.get_open_trades`) already have unit coverage.

## Doc updates

- `bullforge/docs/architecture/redis-pubsub.md` — under the existing `monitoring:reconciler` counter section, list `kind ∈ {filled_reverted, over_close, partial_drain_residual}` for `reconciler_anomalies_total`. No new channels.
- No CLAUDE.md changes — drain semantics are an internal detail of `ReconcilerCore`, the CLI is a one-shot recovery tool not a routine workflow.
- A short note in `bullforge/docs/runbooks/` would be nice but is **out of scope** for this spec — the script's `--help` output is sufficient documentation for the immediate need; we'll add a runbook if a second incident shows the script gets reused often.

## Out of scope

- `RiskManager` changes (crypto long-only fix already shipped per `89f6daa`).
- `trades` table schema redesign (per-row vs aggregate). The drain logic respects the existing per-row model.
- New HTTP endpoint for reconciliation. CLI is sufficient.
- Meta-signal gating. Meta is shadow-only per CLAUDE.md.
- Pagination for >500 closed orders in a single window. Warn-and-require-operator is acceptable for a recovery tool.

## Production rollout (sequenced after merge)

1. Push `feature/reconciler-drain` → squash-merge or `--no-ff` to master per repo convention. **Explicit user approval at the moment of merge** per CLAUDE.md / NEXT_PROMPT.md guidance.
2. DGX deploy: `cd ~/BullForge/bullforge && git pull && docker compose up -d --build execution_service` (per CLAUDE.md). Does **not** require user approval again — this is the documented deploy path. But auto-mode reminds: confirm at the moment of execution.
3. Replay CLI run on the DGX (or from operator's host) against production:
   ```
   PYTHONPATH=. python scripts/reconcile_from_alpaca.py --symbol BTC/USD --since 2026-05-05T00:00:00+00:00 --dry-run
   # confirm output, then:
   PYTHONPATH=. python scripts/reconcile_from_alpaca.py --symbol BTC/USD --since 2026-05-05T00:00:00+00:00
   ```
   Requires explicit user approval — touches production state.
4. Verify: `bin/status` rows for `trades` should show 0 open BTC/USD rows post-replay, and four new `position_closed` alerts should appear in the operator's Telegram channel. If counts diverge, the anomaly counters will explain why.
