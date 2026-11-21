# Redis Pub/Sub Channels

**This document is the source of truth.** Any PR that adds, removes, renames, or changes the payload of a Redis `publish`/`subscribe` call must update this table in the same commit.

All channels flow through the shared `RedisClient` in [bullforge/shared/redis.py](../../bullforge/shared/redis.py). Messages are JSON-encoded strings unless otherwise noted.

## Channels

| Channel | Publisher(s) | Subscriber(s) | Payload | Purpose |
|---|---|---|---|---|
| `model:predictions` | `services/model_service/main.py` | `services/execution_service/main.py` (`on_prediction`) | `{symbol, timeframe, predictions: [...]}` | Per-model predictions fan out to execution for signal aggregation. |
| `model:meta_signals` | `services/model_service/main.py` | `services/execution_service/main.py` (`MetaShadowWriter`) | `{symbol, timeframe, timestamp, feature_vector: dict[str, float], signals: [{direction, estimator_type, model_id, entry_probability, kelly_fraction, pair_index, target_pct, stop_pct}]}` | Meta-model shadow decisions per prediction event. Phase 1: log only, never gates execution. `feature_vector` is the assembled L2 dict (`base_*_dir/conf/rolling_acc/cal_err/disabled`, `regime_*`, time/calendar, `sym_*`) that the meta estimator was scored against — `MetaShadowWriter` persists it verbatim into `meta_features.feature_vector`. Empty dicts are dropped on the writer side with a warning rather than masked by a placeholder. |
| `models:status` | `services/monitoring_service/halt_manager.py` (`publish_status`, every monitoring tick) | `services/execution_service/main.py` (`on_model_status`) | `{model_id: "active"|"warned"|"disabled", ...}` | Per-model enable/disable snapshot; execution filters signals from disabled models. Every key is a model id. Halt state is **not** carried here (it used to be, as `{"_halt": "full"}`, which the subscriber could not parse); see `trading:halt`. |
| `trading:halt` | `services/monitoring_service/halt_manager.py` (`evaluate` on a tier-4 trip and on every tick while halted; `trigger_halt`; `resolve_halt`) | `services/execution_service/halt_gate.py` (`HaltGate.handle_message`) | `{halted: bool, tier: int|null, reason: str, ts: ISO-8601 UTC}`. Schema in [bullforge/shared/halt.py](../../bullforge/shared/halt.py). | Full-halt on/off. While `halted` is true, `OrderExecutor` skips new entries; exits still run. See [Full-halt protocol](#full-halt-protocol-tradinghalt). |
| `model:retrained` | `services/model_service/scheduler.py` | `services/model_service/main.py` (`on_model_retrained`) | `{as_of, completed_at, ok, errors, skipped, results: [...]}` | Retrain-cycle summary. The model_service subscriber reloads its in-memory `_models` and `_meta_models` so out-of-process retrains (`retrain_now.py`) refresh the live container without a restart. |
| `market:ticks` | `services/data_pipeline/stream.py` | `services/feature_engine/main.py`; `services/model_service/main.py` (`on_market_tick`, SPY-only) | Bar payload (symbol, timeframe, timestamp, OHLCV) | Live bar updates trigger reactive feature computation. The model_service also subscribes to keep its in-memory `RegimeBuilder` fresh from SPY bars — without this, after `meta_regime_max_staleness_hours` the SPY-derived regime context silently zeros out and corrupts every `meta_features.feature_vector` row. |
| `feature:updates` | `services/feature_engine/engine.py` | `services/model_service/main.py` | `{symbol, timeframe}` | Feature write completion triggers inference. |
| `execution:orders` | `services/execution_service/reconciler_core.py` | *(none currently)* | `{symbol, side, quantity, status, filled_price, alpaca_order_id, origin}` | Order status transitions for logging/monitoring. |
| `execution:orders:anomaly` | `services/execution_service/reconciler_core.py` | *(none currently)* | Same shape as `execution:orders`. | Orders that transitioned `filled → cancelled/rejected/expired`. |
| `monitoring:reconciler` | `services/execution_service/reconciler_core.py` | *(none currently)* | `{counter, labels, value}` | Reconciler counter metrics. Anomaly kinds for `reconciler_anomalies_total{kind=...}`: `filled_reverted` (order went `filled → cancelled/rejected/expired`), `over_close` (closing fill `filled_qty > sum(open_qty)` over the symbol's open rows on the closing side — longs for a sell, shorts for a buy — drains all of them), `partial_drain_residual` (closing fill `filled_qty < sum(open_qty)` and after FIFO-closing the N oldest rows that fit, an unaccounted residual remains). Quantities compare with a 1e-9 relative tolerance. Stale snapshots (an order already terminal in the DB) are logged and ignored, not counted. |
| `monitoring:data_pipeline` | `services/data_pipeline/backfill.py` | *(none currently)* | `{counter, labels, value}` | Data-pipeline counter metrics. |
| `monitoring:alerts` | `services/monitoring_service/main.py` (halt/fidelity alerts); `services/execution_service/reconciler_core.py` (`position_closed`) | `services/monitoring_service/telegram_bot.py` (`_on_alert`) | See [Alert types](#alert-types-on-monitoringalerts) below. | Operator-facing alerts. The Telegram bot filters a subset (see `TELEGRAM_SUPPRESSED_ALERT_TYPES`). |
| `request:broker` | `services/monitoring_service/broker_client.py` | `services/execution_service/broker_relay.py` | `{id, method, params}` | RPC request: broker queries from monitoring → execution. Correlation id is `id`. |
| `response:broker:{correlation_id}` | `services/execution_service/broker_relay.py` | `services/monitoring_service/broker_client.py` (one-shot per request) | `{ok: true, result}` or `{ok: false, error}` | RPC response matched by correlation id. |
| `config:symbols_changed` | `services/dashboard_service/symbols_api.py` | `services/data_pipeline/main.py` (`_on_symbols_changed`) | `{action: "add"|"remove", kind: "equity"|"crypto", symbol}` | Live trading-symbol list change from the dashboard UI. |

## Full-halt protocol (`trading:halt`)

A **full halt** is tier 4 (automated: drawdown at or past `drawdown_halt_threshold`, or more than half of all models disabled) or tier 5 (manual Telegram `/halt`). The durable record is `halt_events`: a full halt is active while an unresolved row with `tier >= 4` exists. `trading:halt` is the low-latency signal to `execution_service`.

Messages:

```json
{"halted": true,  "tier": 4,    "reason": "Drawdown 12.00% exceeds threshold 10.00%", "ts": "2026-04-12T14:00:00+00:00"}
{"halted": false, "tier": null, "reason": "Resumed by operator",                     "ts": "2026-04-12T15:30:00+00:00"}
```

`halted` is required and must be a JSON boolean. `tier` is an int or null. A message that fails these checks is logged and ignored; it never changes state.

Publisher rules (`HaltManager`):

- **On:** published when tier 4 trips in `evaluate`, and by `trigger_halt` (the row is inserted first; the message goes out even if the insert fails).
- **Re-asserted:** published again at the end of every `evaluate` tick while halted. Pub/sub is at-most-once, so a subscriber that missed the transition catches up within one monitoring interval.
- **Off:** published only by `resolve_halt` (Telegram `/resume`), after every active `halt_events` row is resolved. It is sent even when no rows were active, so a repeated `/resume` also clears a subscriber stuck halted. Monitoring never sends "off" on its own.
- A full halt latches. It stays on after the triggering condition clears, until `/resume`. If the condition still holds after `/resume`, the next tick trips it again.
- `evaluate`, `trigger_halt`, and `resolve_halt` hold one `asyncio.Lock`, so a `/resume` cannot interleave with a tick that is recording a new halt.
- On startup, `restore_state()` reloads an active tier-4+ row, so a monitoring restart keeps re-asserting the halt.

Subscriber rules (`HaltGate` in `execution_service`):

- `OrderExecutor.process_signal` blocks **new entries only**. Exit and close signals for open positions still run: orders are plain market orders with no broker-side brackets, so nothing else would close them.
- On startup it subscribes first and then reads `get_active_halts()`, before subscribing to `model:predictions`. If a `trading:halt` message arrives during that read, the message wins (it is newer than the snapshot). If the read fails, the service starts **halted** (fail closed) until an "off" message arrives or a restart reads the table successfully.
- Backtests construct `OrderExecutor` without a gate and are never halted.

Tier 3 (`direction_halt`, half or more of all models disabled) records a `halt_events` row and alerts, but does not block entries. Disabled models are already out of the ensemble (tier 2), and blocking at "half" would make tier 4's "more than half" trigger redundant. The original design's per-direction halt needs long/short model metadata the system doesn't have.

## Alert types on `monitoring:alerts`

| Type | Source | Telegram delivery | Shape |
|---|---|---|---|
| `model_warned` | `HaltManager.evaluate` | **Suppressed** | `{type, tier: 1, model_id, reason}` |
| `model_disabled` | `HaltManager.evaluate` | **Suppressed** | `{type, tier: 2, model_id, reason}` |
| `direction_halt` | `HaltManager.evaluate` | Delivered | `{type, tier: 3, reason}` |
| `drawdown_halt` | `HaltManager.evaluate` | Delivered | `{type, tier: 4, reason}` |
| `majority_halt` | `HaltManager.evaluate` | Delivered | `{type, tier: 4, reason}` |
| `position_closed` | `ReconcilerCore._handle_fill` | Delivered | `{type, tier: 0, trade_id, symbol, side, quantity, entry_time, entry_price, exit_time, exit_price, pnl, pnl_pct, hold_duration_seconds, account: {equity, last_equity, day_pnl, day_pnl_pct, day_trades, day_wins, day_losses, day_win_rate, day_realized_pnl}}` |

Suppression is configured at the bot level: `TELEGRAM_SUPPRESSED_ALERT_TYPES` in [bullforge/services/monitoring_service/telegram_bot.py](../../bullforge/services/monitoring_service/telegram_bot.py). Suppressed alerts still flow on Redis for other consumers.

## Counter keys

Sliding-window observability counters that don't go through pub/sub. Each writer `INCR`s a per-minute bucket key and sets a 30-minute `EXPIRE` on it; the dashboard sums the most recent 15 buckets.

| Key prefix | Bucket suffix | Producer | Consumer | Purpose |
|---|---|---|---|---|
| `bullforge:metrics:meta_inferences` | `:<int(time.time() // 60)>` | `services/model_service/inference.py::predict_meta` | `services/dashboard_service/models_api.py::_handle_meta_shadow_health` | Denominator for the meta-shadow health tile — count of `predict_meta` invocations past the no-meta-models guard. |
| `bullforge:metrics:meta_publisher_finite_drops` | `:<int(time.time() // 60)>` | `services/model_service/inference.py::predict_meta` (one per call with non-finite features) | same as above | Numerator term: publisher dropped non-finite feature(s) before publishing `model:meta_signals`. |
| `bullforge:metrics:meta_shadow_skips` | `:<int(time.time() // 60)>` | `services/execution_service/meta_shadow_writer.py` (one per (signal, direction) pair where `feature_vector` is empty) | same as above | Numerator term: writer skipped a `meta_features` insert because the published vector was empty. |

Tile turns red when `(skips + drops) / inferences > 0.01` over the 15m window. Backtest engines construct `InferenceEngine` without a redis_client so per-fold inference does not pollute the live counters.
