# Redis Pub/Sub Channels

**This document is the source of truth.** Any PR that adds, removes, renames, or changes the payload of a Redis `publish`/`subscribe` call must update this table in the same commit.

All channels flow through the shared `RedisClient` in [bullforge/shared/redis.py](../../bullforge/shared/redis.py). Messages are JSON-encoded strings unless otherwise noted.

## Channels

| Channel | Publisher(s) | Subscriber(s) | Payload | Purpose |
|---|---|---|---|---|
| `model:predictions` | `services/model_service/main.py` | `services/execution_service/main.py` (`on_prediction`) | `{symbol, timeframe, predictions: [...]}` | Per-model predictions fan out to execution for signal aggregation. |
| `model:meta_signals` | `services/model_service/main.py` | `services/execution_service/main.py` (`MetaShadowWriter`) | `{symbol, timeframe, timestamp, feature_vector, signals: [{direction, estimator_type, model_id, entry_probability, kelly_fraction, pair_index, target_pct, stop_pct}]}` | Meta-model shadow decisions per prediction event. Phase 1: log only, never gates execution. |
| `models:status` | `services/monitoring_service/halt_manager.py` | `services/execution_service/main.py` (`on_model_status`) | `{model_id: "active"|"warned"|"disabled", ...}` or `{"_halt": "full"}` | Model enable/disable state; execution filters signals from disabled models. |
| `model:retrained` | `services/model_service/scheduler.py` | *(none currently)* | `{as_of, completed_at, ok, errors, skipped, results: [...]}` | Retrain-cycle summary. Consumed by operators via logs; available for future dashboards. |
| `market:ticks` | `services/data_pipeline/stream.py` | `services/feature_engine/main.py` | Bar payload (symbol, timeframe, timestamp, OHLCV) | Live bar updates trigger reactive feature computation. |
| `feature:updates` | `services/feature_engine/engine.py` | `services/model_service/main.py` | `{symbol, timeframe}` | Feature write completion triggers inference. |
| `execution:orders` | `services/execution_service/reconciler_core.py` | *(none currently)* | `{symbol, side, quantity, status, filled_price, alpaca_order_id, origin}` | Order status transitions for logging/monitoring. |
| `execution:orders:anomaly` | `services/execution_service/reconciler_core.py` | *(none currently)* | Same shape as `execution:orders`. | Orders that transitioned `filled → cancelled/rejected/expired`. |
| `monitoring:reconciler` | `services/execution_service/reconciler_core.py` | *(none currently)* | `{counter, labels, value}` | Reconciler counter metrics. |
| `monitoring:data_pipeline` | `services/data_pipeline/backfill.py` | *(none currently)* | `{counter, labels, value}` | Data-pipeline counter metrics. |
| `monitoring:alerts` | `services/monitoring_service/main.py` (halt/fidelity alerts); `services/execution_service/reconciler_core.py` (`position_closed`) | `services/monitoring_service/telegram_bot.py` (`_on_alert`) | See [Alert types](#alert-types-on-monitoringalerts) below. | Operator-facing alerts. The Telegram bot filters a subset (see `TELEGRAM_SUPPRESSED_ALERT_TYPES`). |
| `request:broker` | `services/monitoring_service/broker_client.py` | `services/execution_service/broker_relay.py` | `{id, method, params}` | RPC request: broker queries from monitoring → execution. Correlation id is `id`. |
| `response:broker:{correlation_id}` | `services/execution_service/broker_relay.py` | `services/monitoring_service/broker_client.py` (one-shot per request) | `{ok: true, result}` or `{ok: false, error}` | RPC response matched by correlation id. |
| `config:symbols_changed` | `services/dashboard_service/symbols_api.py` | `services/data_pipeline/main.py` (`_on_symbols_changed`) | `{action: "add"|"remove", kind: "equity"|"crypto", symbol}` | Live trading-symbol list change from the dashboard UI. |

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
