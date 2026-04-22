# Telegram Alert Cleanup and Position-Closed Alerts — Design

**Date:** 2026-04-22
**Status:** Draft (pending user approval)
**Scope:** Telegram alert filtering, new `position_closed` alert type, and a new Redis pub/sub reference doc.

## Goal

1. Stop sending `model_warned` and `model_disabled` alerts to Telegram. Continue publishing them to Redis so future consumers (dashboards, auditors) can still see them.
2. Emit a new `position_closed` Telegram alert each time a trade fills on the closing side, including trade-level P&L and account-level day context (equity, day's P&L, day's win rate).
3. Create a single source of truth for Redis pub/sub channels at `docs/architecture/redis-pubsub.md` and keep it up to date going forward (enforced via auto-memory).

## Motivation

The Telegram stream is too noisy. Fidelity tracking routinely flips models between `active`, `warned`, and `disabled` as live accuracy drifts — this is the system working as designed, not something that requires human attention. Silencing these alerts reclaims Telegram for events that actually warrant a glance.

Position closes are the opposite: low frequency and high signal. Surfacing the trade's P&L plus the account's day context (equity, realized P&L for the day, win rate) gives a full at-a-glance health check every time a trade closes, without the trader having to open the dashboard.

## Non-Goals

- No changes to how `model_warned` / `model_disabled` are generated in `HaltManager.evaluate()`. They keep firing and keep getting published to `monitoring:alerts`. Only Telegram delivery is suppressed.
- No persistent account-snapshot table. Day's P&L is derived from Alpaca's `equity - last_equity`; day's trade stats are derived from `query_closed_trades(since=today_utc_midnight)`.
- No per-alert throttling/batching. Every closing fill produces one Telegram message. Current trade volume does not warrant rate-limiting.
- No new env-var toggles. The suppression set is code-level; flip a line to re-enable.
- No changes to the button/callback system. `position_closed` alerts carry no buttons (informational only).
- Not back-documenting every historical design decision in `docs/architecture/`. The new pub/sub doc is scoped to Redis channels only.

## Design

### 1. Suppress noisy alerts at the Telegram bot

File: [bullforge/services/monitoring_service/telegram_bot.py](../../../bullforge/services/monitoring_service/telegram_bot.py)

Add a module-level set of alert types that should be filtered before delivery:

```python
TELEGRAM_SUPPRESSED_ALERT_TYPES = frozenset({"model_warned", "model_disabled"})
```

Modify `_on_alert` to short-circuit suppressed types:

```python
async def _on_alert(self, message: str) -> None:
    alert = json.loads(message)
    alert_type = alert.get("type")
    if alert_type in TELEGRAM_SUPPRESSED_ALERT_TYPES:
        logger.debug("Suppressed Telegram delivery for alert type %s", alert_type)
        return
    await self.send_alert(alert)
```

`HaltManager.evaluate()` is unchanged. The Redis channel `monitoring:alerts` keeps carrying every tier.

### 2. Expose `last_equity` on the account dict

File: [bullforge/services/execution_service/alpaca_broker.py](../../../bullforge/services/execution_service/alpaca_broker.py)

`_sync_get_account` currently returns `equity`, `buying_power`, `cash`, `portfolio_value`. Add `last_equity`:

```python
def _sync_get_account(self) -> dict:
    account = self._client.get_account()
    return {
        "equity": float(account.equity),
        "last_equity": float(account.last_equity),
        "buying_power": float(account.buying_power),
        "cash": float(account.cash),
        "portfolio_value": float(account.portfolio_value),
    }
```

Alpaca's `last_equity` is the account's equity at the previous trading session's close. `equity - last_equity` is the standard day's P&L definition and matches what the Alpaca web UI shows.

### 3. Day-stats helper

New module: `bullforge/services/execution_service/day_stats.py`

Single pure-ish async function — no class, no state:

```python
async def compute_day_stats(db: Database, broker: AlpacaBroker, now: datetime) -> dict:
    """Returns:
      {
        "equity": float,
        "last_equity": float,
        "day_pnl": float,           # equity - last_equity
        "day_pnl_pct": float,        # (equity - last_equity) / last_equity, 0.0 if last_equity == 0
        "day_trades": int,           # closed trades since UTC midnight
        "day_wins": int,             # subset of day_trades with pnl > 0
        "day_losses": int,           # subset with pnl < 0  (breakevens excluded from both counts)
        "day_win_rate": float,       # wins / (wins + losses), 0.0 if denom == 0
        "day_realized_pnl": float,   # sum(pnl) for day_trades
      }
    """
```

Time window: UTC midnight of `now.date()` to `now`. Matches the convention already used throughout the codebase for `TIMESTAMPTZ` storage. Note this window differs from an equities trading session but aligns with how closed trades are indexed. (Documented in code comment; not surfaced in the message.)

`broker.get_account()` is called once per close alert. This is rare enough (order of magnitude: tens per day) to skip caching.

### 4. Publish `position_closed` from ReconcilerCore

File: [bullforge/services/execution_service/reconciler_core.py](../../../bullforge/services/execution_service/reconciler_core.py)

`ReconcilerCore.__init__` already accepts an optional `redis_client`. `_handle_fill` already computes `pnl` and calls `update_trade_exit` when the fill closes a trade. Extend `__init__` to also accept the broker, and extend `_handle_fill` to publish after `update_trade_exit` succeeds:

```python
def __init__(self, db: Database, redis_client=None, broker: AlpacaBroker | None = None):
    self._db = db
    self._redis = redis_client
    self._broker = broker
```

```python
# After await self._db.update_trade_exit(...)
if self._redis is not None and self._broker is not None:
    try:
        day = await compute_day_stats(self._db, self._broker, datetime.now(timezone.utc))
        alert = {
            "type": "position_closed",
            "tier": 0,
            "trade_id": open_trade.id,
            "symbol": open_trade.symbol,
            "side": open_trade.side,
            "quantity": open_trade.quantity,
            "entry_time": _iso(open_trade.entry_time),
            "entry_price": open_trade.entry_price,
            "exit_time": _iso(exit_time),
            "exit_price": order.filled_price,
            "pnl": pnl,
            "pnl_pct": pnl / (open_trade.entry_price * open_trade.quantity) if open_trade.entry_price and open_trade.quantity else 0.0,
            "hold_duration_seconds": hold_duration_seconds,
            "account": day,
        }
        await self._redis.publish("monitoring:alerts", json.dumps(alert))
    except Exception:
        logger.exception("Failed to publish position_closed alert")
```

Timestamp serialization uses ISO 8601 `YYYY-MM-DD HH:MM:SS` (space separator, UTC implied, matching `fmtTime`/`fmtTradeTime` convention). A local `_iso` helper formats `datetime` instances consistently.

Publish happens **after** the DB write commits. A failure to publish the alert is logged but does not roll back the trade — an unsent Telegram message is preferable to a double-written trade row. The `try/except` is scoped narrowly around the publish block.

File: `bullforge/services/execution_service/main.py`

Pass the broker through when constructing `ReconcilerCore`:

```python
core = ReconcilerCore(db=db, redis_client=redis_client, broker=broker)
```

### 5. Format `position_closed` alerts

File: [bullforge/services/monitoring_service/telegram_alerts.py](../../../bullforge/services/monitoring_service/telegram_alerts.py)

New branch in `format_alert`, keyed on `alert["type"] == "position_closed"`. No buttons — return an empty button layout.

```
[TRADE CLOSED] AAPL long
Entry: 2026-04-22 13:31:15 UTC @ $187.42
Exit:  2026-04-22 15:45:03 UTC @ $189.18
Qty:   100
P&L:   +$176.00 (+0.94%)
Hold:  2h 13m 48s

Account (today, UTC)
Equity:     $104,326.12   (+$426.12, +0.41%)
Realized:   +$352.00 on 4 trades
Win rate:   75.0% (3W / 1L)
```

Formatting rules:

- **Sign-aware P&L:** `+$` for positive, `-$` for negative, using `abs()` for the numeric. Percentage uses the same sign convention with two decimal places.
- **Hold duration:** rendered via `_format_duration(seconds)` helper: `Xh Ym Zs` when ≥ 1h, `Ym Zs` when ≥ 1m, `Zs` otherwise.
- **Win rate denom:** wins + losses (break-evens excluded). If denom is 0, the Win rate line is omitted.
- **Account lines:** grouped under `Account (today, UTC)` so the UTC boundary is explicit.
- **Realized line is omitted** if `day_trades == 0`. (The alert we're formatting is itself the first trade — but the query is "since UTC midnight" and runs *before* the publish, so that trade isn't counted yet. On subsequent closes it will be.)

### 6. Redis pub/sub reference doc

New file: `docs/architecture/redis-pubsub.md`

Single table, one row per channel, covering: `model:predictions`, `models:status`, `market:ticks`, `feature:updates`, `execution:orders`, `execution:orders:anomaly`, `monitoring:reconciler`, `monitoring:data_pipeline`, `monitoring:alerts`, `request:broker`, `response:broker:{correlation_id}`, `config:symbols_changed`, and the alert types carried on `monitoring:alerts` (including the new `position_closed`).

Columns: **Channel | Publisher(s) | Subscriber(s) | Payload shape | Notes**.

Short preamble explains the doc is authoritative and that any PR touching a `.publish()` or `.subscribe()` must update it in the same commit. Auto-memory `feedback_pubsub_docs.md` enforces this going forward.

### 7. Tests

- **`telegram_bot.py`:** unit test that feeds a fake `_on_alert` with `type: "model_warned"` and asserts `send_alert` is not called; a second test with `type: "position_closed"` asserts it is called.
- **`telegram_alerts.py`:** unit tests of `format_alert` for a profitable close, a losing close, a close with day stats, and a close with no day trades yet.
- **`day_stats.py`:** unit test with a mock `db.query_closed_trades` returning a mix of wins, losses, and a break-even, plus a mock broker account dict. Verifies arithmetic and handles the `last_equity == 0` edge case.
- **`reconciler_core.py`:** integration-style test that `_handle_fill` for a closing order results in one published `position_closed` alert with the right shape. An existing test likely asserts `update_trade_exit` is called — extend, don't duplicate.
- No test for "alert delivery failure does not roll back trade write" — covered by the narrow try/except around the publish and manual review.

## Risk / Reversibility

- Suppression is one `frozenset` literal. Revert = add back `model_warned`/`model_disabled` and re-deploy.
- The new alert emission is gated on `self._redis is not None and self._broker is not None`. In environments where either is unconfigured (tests, reduced deployments), nothing fires.
- Publish failures are caught and logged. The trade write is unaffected.
- No schema or API changes. No migrations.
