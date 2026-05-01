# Plan B (Phase 1) — Meta-Model Shadow Mode

**Status**: design approved 2026-04-30
**Branch**: `plan-b-meta-shadow`
**Scope**: real training data + nightly meta-retrain + shadow logging. **No live cutover.**

## 1. Background

The codebase already contains a meta-model framework (`bullforge/services/model_service/meta/`) that was intended to (a) learn which base models are predictive in which conditions, (b) gate trade entry, and (c) size positions and pick (target, stop) pairs. The framework includes:

- Two protocols: `MetaEntryEstimator` (binary `P(trade hits target before stop)`) and `MetaExecEstimator` (multi-task: kelly_fraction regression + pair_index classification).
- Three estimator backends: `lightgbm`, `mlp`, `baseline`.
- A `CalibratedEntryEstimator` wrapper using isotonic regression.
- `MetaFeatureAssembler` that builds a flat feature vector from `(base predictions, rolling stats, regime, symbol_state, timestamp)`.
- DB tables `meta_features` (training rows) and `meta_signals_shadow` (A/B log) created by migration `016_create_meta_tables.sql`.
- A `model_registry.estimator_type` column with a unique index covering `(symbol, timeframe, horizon_minutes, model_type, estimator_type) WHERE is_active = TRUE AND estimator_type IS NOT NULL`.

But none of it is wired in:

- `MetaTrainer._build_entry_dataset` is explicitly a placeholder that uses `np.random` for "predictions" and logs a WARNING saying so.
- `RetrainScheduler.run_once` only enqueues `gradient_boost` and `lstm` jobs.
- `execution_service` has no imports from `services.model_service.meta`.
- `Database.insert_meta_signal_shadow` and `insert_meta_feature` have no production callers.

This spec is "Plan B" — making the framework real.

## 2. Goals (this phase)

1. **Real training-data builder.** Replace `_build_entry_dataset` / `_build_exec_dataset` with bar-walking simulation over historical predictions.
2. **Nightly meta-training.** Extend `RetrainScheduler.run_once` to train one `meta_entry` and one `meta_exec` model per `(symbol, direction, estimator_type)` after base models complete.
3. **Live shadow logging.** When base predictions arrive, run the meta-model and persist its decisions to `meta_signals_shadow` plus a `meta_features` row (`source="live"`, labels NULL). Operator can compare meta-decisions to the legacy `SignalAggregator`'s decisions and to realized outcomes.
4. **Live label backfill.** Periodically fill `entry_label` / `optimal_pair_idx` / `realized_ev` on live `meta_features` rows once enough bars have elapsed to determine the outcome.

## 3. Non-goals (deferred)

- **No execution-path change.** `SignalAggregator`, `RiskManager`, `OrderExecutor` are untouched. Real money is still routed by the legacy ensemble.
- **No dashboard UI.** Shadow data is queryable via SQL only in this phase.
- **No `meta_signals_shadow` analytics views.** Build after we have real shadow data.
- **No promotion mechanism.** A future Phase 2 will introduce `meta_inference_mode` ∈ `{off, shadow, live}` with rollout safeguards.

## 4. Design decisions

### 4.1 Pair grid (per symbol, per direction)

Three ATR-scaled pairs per direction, mirrored short/long: `[(2×, 1×), (3×, 1.5×), (4×, 2×)]` of `ATR-14 / close`. Pair `i` is `(target_pct = mult_target[i] × atr_pct, stop_pct = mult_stop[i] × atr_pct)`. The grid is in `meta_pair_grid` runtime config — a JSON list of `[target_mult, stop_mult]` pairs, default `[[2, 1], [3, 1.5], [4, 2]]`.

`atr_pct` is computed at the **candidate bar** (entry timestamp), not at training time. So a single grid auto-adapts to per-symbol, per-regime volatility.

### 4.2 Replay candidate selection

For every historical bar with `bars_1m + features + ≥1 prediction` rows, we generate **two candidate trades** — one long, one short. No filtering by legacy aggregator decision. Rationale: shadow inference will fire on every prediction event regardless of legacy state, so train and serve distributions match.

### 4.3 Outcome simulation (`PairSimulator`)

For a candidate trade `(symbol, entry_ts, direction, target_pct, stop_pct, max_hold_min)`:

1. Compute `entry_price` = `close` of entry bar.
2. For long: `target_price = entry × (1 + target_pct)`, `stop_price = entry × (1 − stop_pct)`. For short: signs flipped.
3. Walk forward `bars_1m` for up to `max_hold_min` bars after entry:
   - For long: hit_target if any subsequent `bar.high ≥ target_price`; hit_stop if `bar.low ≤ stop_price`.
   - For short: hit_target if `bar.low ≤ target_price`; hit_stop if `bar.high ≥ stop_price`.
   - If both within the same bar, **stop wins** (conservative; matches typical execution since stops fire intra-bar).
4. Outcome:
   - `hit_target` first → `(hit_target=True, exit_idx, realized_return = ±target_pct)`.
   - `hit_stop` first → `(hit_stop=True, exit_idx, realized_return = ∓stop_pct)`.
   - Neither within max_hold → exit at last bar's `close`, `realized_return = (exit_close − entry) / entry × dir`.

### 4.4 Outcome window / max-hold

`max_hold_min` = `meta_max_hold_minutes` runtime config, default `max(label_horizons_1m) = 30`.

### 4.5 Outcome labels (per candidate bar)

For each pair, run the simulator, collect `(hit_target_p, hit_stop_p, ev_p)` where `ev_p` is `target_pct` if target hit, `−stop_pct` if stop hit, else `realized_return` (signed by direction).

- `entry_label = 1 iff any pair hit_target = True, else 0`. (We're asking "is this a tradeable setup at *some* sensible target/stop", which matches the meta-model's role as a gate over both entry and pair selection.)
- `optimal_pair_idx = argmax_p(ev_p)`.
- `kelly_fraction` (regression target) = closed-form Kelly using rolling 200-bar empirical hit-rate per pair: `kelly = max(0, (p × b − (1 − p)) / b)` where `b = target_pct / stop_pct`, then clipped to `[0, 0.25]` (quarter-Kelly cap). If fewer than 200 prior bars exist for the pair, label is set to a configurable cold-start value (default `0.05`).

### 4.6 Regime context (`RegimeBuilder`)

Computes `{spy_trend_1m, spy_trend_1h, spy_trend_1d, spy_momentum, vol_regime}` at a given timestamp:

- `spy_trend_1m` = `SPY.close.pct_change(1)` at ts.
- `spy_trend_1h` = `SPY.close[ts] / SPY.close[ts − 60min] − 1`.
- `spy_trend_1d` = `SPY.close[ts] / SPY.close[ts − 1 trading day] − 1` (closest available 1m SPY bar ≥ 24h prior).
- `spy_momentum` = 12-period EMA of `spy_trend_1m`.
- `vol_regime` = trailing 60-bar std of `spy_trend_1m`, normalized by its own 30-day median (so 1.0 ≈ "normal vol").

For 24/7 crypto bars, **forward-fill from the most recent SPY bar** with a max staleness of `meta_regime_max_staleness_hours` (default 48). If SPY data is missing or staler than that, return zeros and log at DEBUG.

### 4.7 Per-base-model rolling stats

`MetaFeatureAssembler` already takes `rolling_stats: dict[model_id, {accuracy, calibration_error, is_disabled}]`. Source:

- `accuracy` = `query_prediction_outcomes(model_id, limit=200)` → fraction `is_correct`.
- `calibration_error` = `|mean(confidence) − accuracy|` over the same 200 outcomes.
- `is_disabled` = `model_id in current_disabled_set` (from `models:status` channel for live; from `model_status` table snapshot at training time for replay).

For replay we cache per-`as_of` snapshots of these stats so the training loop doesn't re-query for every candidate bar. For live we read once per inference event.

### 4.8 Estimator coverage

`meta_estimator_types` runtime config (JSON list, default `["lightgbm"]`). Nightly trains every estimator type in this list × every `(symbol, direction)`. Operator opts into `mlp` and `baseline` later.

Per-estimator notes:
- `lightgbm`: CPU-friendly; trains in the CPU pool alongside base gradient_boost.
- `mlp`: GPU; trains in the GPU phase next to LSTM.
- `baseline`: trivial heuristic, instant.

### 4.9 Where meta inference runs

In `model_service`, immediately after base inference. Architectural rule "all ML inference lives in `model_service`" is preserved. New channel `model:meta_signals` published; `execution_service` subscribes and persists.

Payload shape (JSON):
```json
{
  "symbol": "SPY",
  "timeframe": "1m",
  "timestamp": "2026-04-30T13:30:00Z",
  "feature_vector": { "base_1_dir": 1.0, "base_1_conf": 0.71, "...": "..." },
  "signals": [
    {
      "direction": 1,
      "estimator_type": "lightgbm",
      "model_id": 4711,
      "entry_probability": 0.62,
      "kelly_fraction": 0.07,
      "pair_index": 1,
      "target_pct": 0.012,
      "stop_pct": 0.006
    },
    { "direction": -1, "estimator_type": "lightgbm", ... }
  ]
}
```

`feature_vector` is direction-agnostic (the `MetaFeatureAssembler` produces one vector per `(symbol, ts)`). The subscriber writes one `meta_signals_shadow` row per `signal` and one `meta_features` row per `direction` (sharing the same feature_vector but with direction-specific NULL labels).

### 4.10 Live label backfill (`MetaLabelBackfiller`)

Periodic loop in `model_service` (`meta_label_backfill_interval_minutes` runtime config, default `15`). Each tick:

1. Query `meta_features` rows where `source='live' AND entry_label IS NULL AND timestamp ≤ now() − (max_hold_minutes + 5)`.
2. For each row, simulate all pairs against `bars_1m`.
3. `UPDATE meta_features SET entry_label, optimal_pair_idx, realized_ev WHERE PRIMARY KEY` (the `(symbol, timestamp, direction)` key, see migration 016).
4. Idempotent — re-running on already-labeled rows is a no-op (filter is `entry_label IS NULL`).

## 5. Architecture

```
                     bars_1m + features + predictions
                                  │
                                  ▼
   ┌──────────────────  meta/replay.py  ──────────────────┐
   │  CandidateBuilder: enumerate (bar, direction) pairs  │
   │  PairSimulator:    walk bars, label target-vs-stop   │  (offline)
   │  RegimeBuilder:    SPY context per timestamp         │
   │  MetaFeatureAssembler (existing)                     │
   └────────────────────────┬─────────────────────────────┘
                            │ meta_features rows (source=replay)
                            ▼
                       MetaTrainer (existing — datasets are real now)
                            │ artifacts
                            ▼
                  model_registry (estimator_type col)

   feature:updates ─► InferenceEngine.predict (base)  ─► model:predictions
                            │
                            ▼
                  InferenceEngine.predict_meta (new)  ─► model:meta_signals
                            │                                  │
                            │                                  ▼
                            │                       execution_service writes:
                            │                         • meta_signals_shadow
                            │                         • meta_features (source=live, labels NULL)
                            ▼
                  MetaLabelBackfiller (periodic) ─► fills NULL labels using bars_1m
```

## 6. File-level changes

### New files

| Path | Purpose |
|---|---|
| `bullforge/services/model_service/meta/replay.py` | `CandidateBuilder`, `PairSimulator`, `compute_atr_pct`, dataset assembly helpers |
| `bullforge/services/model_service/meta/regime.py` | `RegimeBuilder` |
| `bullforge/services/model_service/meta/label_backfill.py` | `MetaLabelBackfiller` (periodic loop) |
| `bullforge/services/execution_service/meta_shadow_writer.py` | Subscriber-side persister |
| `bullforge/tests/meta/test_replay.py` | `PairSimulator`, `CandidateBuilder` |
| `bullforge/tests/meta/test_regime.py` | SPY-present, SPY-stale, SPY-missing |
| `bullforge/tests/meta/test_label_backfill.py` | Idempotency, eligibility filter |
| `bullforge/tests/test_meta_inference.py` | `InferenceEngine.predict_meta` flow |
| `bullforge/tests/test_meta_shadow.py` | execution_service shadow writer |
| `bullforge/tests/integration/test_meta_train_e2e_db.py` | Replay → train → save flow on real DB |

### Modified files

| Path | Change |
|---|---|
| `bullforge/services/model_service/meta/trainer.py` | `_build_entry_dataset` / `_build_exec_dataset` use real replay; pull `pair_grid` from RuntimeConfig; remove placeholder warning |
| `bullforge/services/model_service/scheduler.py` | Add `_train_meta_phase` after GPU phase. Per `(symbol, direction, estimator_type)` call `MetaTrainer.train_entry` + `train_exec` |
| `bullforge/services/model_service/inference.py` | `_meta_models` cache; `load_meta_models()`; `predict_meta(symbol, timeframe, base_predictions)` |
| `bullforge/services/model_service/main.py` | After publishing `model:predictions`, call `predict_meta` and publish `model:meta_signals`. Start `MetaLabelBackfiller` background task |
| `bullforge/services/execution_service/main.py` | Subscribe to `model:meta_signals`, hand off to `MetaShadowWriter` |
| `bullforge/shared/runtime_config.py` | Add `meta_estimator_types`, `meta_pair_grid`, `meta_max_hold_minutes`, `meta_min_train_rows`, `meta_label_backfill_interval_minutes`, `meta_inference_enabled`, `meta_regime_max_staleness_hours` to `CONFIG_SCHEMA` |
| `bullforge/shared/db.py` | Add `query_unlabeled_meta_features(older_than)`, `update_meta_feature_labels(...)`. Existing `insert_meta_*` methods get production callers |
| `docs/architecture/redis-pubsub.md` | Add `model:meta_signals` row (per "maintain Redis pub/sub docs" memory) |

### No new migrations
Schema in `016_create_meta_tables.sql` is sufficient.

## 7. Data flow — offline training (per nightly cycle)

For each `(symbol, direction)`:

1. Pull `bars_1m`, `features`, and `predictions` for the training window (last `meta_train_window_days` days, default `60`). If the resulting candidate count is below `meta_min_train_rows` (default `500`), skip this `(symbol, direction)` and log at WARNING — symmetric with how the base trainer skips on `model_min_samples`. (Add `meta_train_window_days` to the runtime-config table in §9.)
2. Pull cached per-base-model `(accuracy, calibration_error, is_disabled)` snapshot at `as_of`.
3. For each candidate bar:
   - `atr_pct` = ATR-14 / close at this bar.
   - Materialize the pair grid: `[(2, 1), (3, 1.5), (4, 2)] × atr_pct`.
   - For each pair, `PairSimulator.simulate(...)` → `(hit_target, hit_stop, exit_idx, realized_return)`.
   - Aggregate: `entry_label = any(hit_target_p)`, `optimal_pair_idx = argmax(ev_p)`, `kelly_fraction = …`.
   - Build feature vector via `RegimeBuilder` + symbol_state from `features` + `MetaFeatureAssembler`.
   - Insert `meta_features` row (`source="replay"`).
4. After loop: `MetaTrainer.train_entry / train_exec` per estimator_type queries `meta_features` for this `(symbol, direction)` with `entry_label IS NOT NULL`, splits 60/20/20 train/cal/test, fits, calibrates the entry estimator on `cal` slice, evaluates on `test` slice, saves artifact, registers in `model_registry` with `is_active=TRUE` and deactivates the previous active model for that `(symbol, direction, estimator_type)`.

## 8. Data flow — live shadow

1. `feature:updates` → `model_service` runs base inference → publishes `model:predictions` (unchanged).
2. `InferenceEngine.predict_meta(symbol, timeframe, base_predictions)`:
   - Load `current_disabled_set` from in-memory state (kept in sync via `models:status`).
   - Build per-base-model rolling_stats by querying recent prediction outcomes (cached per (model_id) for `meta_rolling_stats_ttl_seconds`, default 600).
   - Compute regime via `RegimeBuilder.regime_at(timestamp)`.
   - Compute symbol_state from latest `features` row.
   - For each `direction ∈ {1, -1}`:
     - For each loaded `(symbol, direction, estimator_type)` meta-model:
       - Build feature vector via `MetaFeatureAssembler.assemble(...)`.
       - Run `entry_estimator.predict_proba(...)[0, 1]` → `entry_probability`.
       - Run `exec_estimator.predict(...)` → `(kelly_fraction, pair_index)`.
       - Resolve `pair_index` → concrete `(target_pct, stop_pct)` via `pair_grid × atr_pct`.
3. Publish a single `model:meta_signals` message containing all signals for this `(symbol, ts)`.
4. `execution_service.MetaShadowWriter.handle_message`:
   - For each `signal`: `db.insert_meta_signal_shadow(MetaSignalShadow(...))`.
   - Once per `(symbol, ts, direction)`: `db.insert_meta_feature(MetaFeatureRow(source="live", entry_label=None, ...))`.
5. `MetaLabelBackfiller` (next tick after horizon elapsed) fills the labels.

## 9. Runtime config additions

Append to `bullforge/shared/runtime_config.py::CONFIG_SCHEMA`:

| Key | Type | Default | Notes |
|---|---|---|---|
| `meta_inference_enabled` | bool | `false` | Master switch for shadow inference. When `false`, `predict_meta` returns an empty list and `model:meta_signals` is not published. Operator flips to `true` after the first successful nightly retrain |
| `meta_estimator_types` | json | `["lightgbm"]` | Subset of `{lightgbm, mlp, baseline}` to train and serve |
| `meta_pair_grid` | json | `[[2,1],[3,1.5],[4,2]]` | `[target_mult, stop_mult]` × ATR |
| `meta_train_window_days` | int | 60 | Replay window for offline training |
| `meta_max_hold_minutes` | int | 30 | Outcome simulation max-hold |
| `meta_min_train_rows` | int | 500 | Skip `(symbol, direction)` if fewer labeled rows |
| `meta_label_backfill_interval_minutes` | int | 15 | `MetaLabelBackfiller` tick interval |
| `meta_regime_max_staleness_hours` | int | 48 | SPY forward-fill staleness cap |
| `meta_rolling_stats_ttl_seconds` | int | 600 | Per-base-model rolling-stats cache TTL during inference |

All keys live in the same Redis hash `bullforge:config` and surface in the dashboard Config tab through the existing schema-driven UI.

## 10. Pub/sub additions

Update `docs/architecture/redis-pubsub.md`:

| Channel | Publisher | Subscriber | Payload | Purpose |
|---|---|---|---|---|
| `model:meta_signals` | `services/model_service/main.py` | `services/execution_service/main.py` (via `MetaShadowWriter`) | See §4.9 | Meta-model shadow decisions per prediction event |

Per the "maintain Redis pub/sub docs" memory, this update lands in the same commit as the publish/subscribe code.

## 11. Testing strategy

### Unit tests
- `tests/meta/test_replay.py`:
  - `PairSimulator`: long-target-first, long-stop-first, long-timeout-with-positive-return, long-timeout-with-negative-return, mirror short cases, both-hit-same-bar (stop wins), zero-volume bar, gap up through target.
  - `CandidateBuilder`: filters bars without ATR / without features / without any prediction, returns both directions.
  - `compute_atr_pct`: matches feature-engine's ATR-14 within float tolerance.
- `tests/meta/test_regime.py`: SPY present (all five fields populated), SPY exactly at staleness cap (last value used), SPY past staleness cap (zeros, DEBUG log), SPY entirely missing.
- `tests/meta/test_label_backfill.py`: only labels rows older than `max_hold + 5min`, idempotent on already-labeled rows, skips if no `bars_1m` past max_hold yet.
- `tests/test_meta_inference.py`: `predict_meta` returns one signal per loaded `(direction, estimator_type)`, gracefully no-ops when no meta models loaded, includes feature_vector in payload.
- `tests/test_meta_shadow.py`: writer inserts both `meta_signals_shadow` and `meta_features` rows, dedups within the same `(symbol, ts, direction)`.

### Integration tests (live DB, in `tests/integration/`)
- `test_meta_train_e2e_db.py`: seed `bars_1m + features + predictions` for one symbol, run `MetaTrainer.train_entry`, assert artifact saved + `model_registry` row inserted with `model_type='meta_entry'` and `estimator_type='lightgbm'`.

### Existing test changes
- `tests/meta/test_trainer.py`: replace placeholder-mock asserts with real-replay-backed inputs; the placeholder WARNING string assertion goes away.

## 12. Failure modes & open items

- **Cold start**: `meta_inference_enabled=false` until at least one nightly cycle produces artifacts. After first successful retrain, operator flips the switch via dashboard Config tab. `predict_meta` no-ops cleanly when no models are loaded for a `(symbol, direction, estimator_type)`.
- **Live row weighting**: `MetaTrainer.live_weight_multiplier=3.0` defaults to 3×. Defer tuning until shadow-mode produces real live `meta_features` rows. Document this in the runbook.
- **Backfill ownership**: `MetaLabelBackfiller` lives in `model_service` because that's where the meta package already imports cleanly. Could be moved to `feature_engine` if it grows.
- **Bar-walking cost during retrain**: 7 symbols × 2 directions × ~50k candidate bars × 3 pairs ≈ 2.1M sim calls per night. Each sim is `O(max_hold)` lookup against an in-memory pandas frame — sub-millisecond. Total wall time estimated <10 min on the GPU box; well under the existing nightly window.
- **Label drift**: simulator uses `bars_1m`; if a symbol's bars are sparse (illiquid crypto), max-hold can elapse with too-few intermediate bars. `PairSimulator` returns `realized_return = 0, hit_target=False, hit_stop=False` if fewer than 2 bars exist post-entry, which will appear as a 0-EV trade. Acceptable for Phase 1; could mark as "insufficient_data" in Phase 2.

## 13. Out-of-scope items captured for Phase 2+

- Live cutover (`meta_inference_mode = live` flag, kill-switch, position-limit safeguards).
- Dashboard surface for shadow comparison (`Models` tab → meta sub-panel).
- Stratified sampling / class balancing during meta-training when `entry_label` distribution is heavily skewed.
- Alternative `kelly_fraction` formulations (e.g., variance-aware, drawdown-adjusted).
- Cross-symbol regime features beyond SPY (DXY, VIX-equivalents for crypto).
