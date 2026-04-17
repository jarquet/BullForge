# Meta-Model Signal Processor — Design Spec

**Date:** 2026-04-16
**Status:** Draft — awaiting approval

## Problem

The current `SignalAggregator` weights all base sub-models equally by confidence. Some models have strong live accuracy, others don't. There is no mechanism for the system to learn which models to trust, when, or by how much — and no way to optimize entry timing, position sizing, or exit-band selection based on base-model agreement patterns.

## Solution

A **pluggable meta-model framework** that sits between base-model inference and trade execution. Base sub-model predictions become input features for a per-symbol meta-model layer that produces higher-quality entry signals, Kelly-derived position sizes, and optimal exit-band selection.

The framework is the permanent infrastructure. Individual estimator types (LightGBM, MLP, future ML technologies) are interchangeable plugins evaluated empirically via shadow-mode A/B/n comparison.

## Design Principles

1. **Estimators are plugins, not architecture.** The feature pipeline, label generation, training loop, calibration, shadow mode, fidelity tracking, guardrails, and dashboard comparison are estimator-agnostic. Adding a new estimator type means implementing a single interface — no infrastructure changes.
2. **Empirical selection over convention.** The system trains all registered estimator types per (symbol, direction), compares them head-to-head in shadow mode, and promotes the best performer. No hardcoded preference for any ML technology.
3. **Graceful degradation.** If no meta-model passes the cold-start gate or fidelity check for a symbol, the existing `SignalAggregator` behavior is the automatic fallback. A bad meta-model never silently takes over.
4. **Safety through hard guardrails.** The meta-model proposes; the execution layer disposes. All existing risk limits (max position %, max concurrent positions, trading windows, blackouts) remain enforced regardless of meta-model output.

## Architecture

### Two-Head Hybrid (per symbol, per direction)

- **`meta_entry`** — binary classifier. Answers: "should we enter a trade right now?" Output: calibrated probability P(target hit before stop).
- **`meta_exec`** — multi-task model, runs only when `meta_entry` fires. Two heads: a regression head outputting `kelly_fraction` (continuous [0, 1]) and a classification head outputting `pair_index` (categorical softmax over the symbol's trained sub-model (target%, stop%) pairs). Shared trunk, separate output layers.

Each (symbol, direction) pair gets one `meta_entry` and one `meta_exec` instance per registered estimator type. Example: `meta_entry_AAPL_long_lightgbm`, `meta_entry_AAPL_long_mlp`, etc.

### Estimator Plugin Interface

All estimators implement a common interface:

```python
class MetaEstimator(Protocol):
    def fit(self, X: pd.DataFrame, y: pd.Series, sample_weight: np.ndarray | None = None) -> None: ...
    def predict(self, X: pd.DataFrame) -> np.ndarray: ...
    def predict_proba(self, X: pd.DataFrame) -> np.ndarray: ...  # entry head only
    def get_params(self) -> dict: ...
    def feature_importances(self) -> np.ndarray: ...
```

### Initial Estimator Plugins

| Plugin | Type | Cold-start min rows | Notes |
|--------|------|---------------------|-------|
| `lightgbm` | LightGBM gradient-boosted trees | 2,000 | Fast, strong on tabular, good feature importance |
| `mlp` | Small feedforward neural net (2 hidden layers, dropout, batchnorm) | 5,000 | Captures non-linear feature interactions trees may miss |
| `baseline_aggregator` | Current `SignalAggregator` logic wrapped as a stub estimator | 0 | Always available as the control group in A/B/n |

Future estimator plugins are added by: (1) implementing `MetaEstimator`, (2) registering in `meta_model_factory.py`, (3) configuring cold-start threshold. No other changes required.

## Feature Vector (Level 2)

~20-40 floats per row, depending on number of active base sub-models for the symbol.

### Per base sub-model features

- `direction` — predicted direction (int).
- `confidence` — predicted probability from the base model.
- `rolling_live_accuracy` — accuracy over configurable rolling window (default 30 days).
- `rolling_calibration_error` — abs(mean predicted confidence - actual hit rate) over rolling window.
- `is_disabled` — binary flag from fidelity tracker.

### Regime features

- SPY trend sign (1m, 1h, 1d timeframes).
- SPY momentum value.
- Current volatility regime bucket (low / normal / high, derived from VIX or realized vol percentile).

### Temporal features

- Time-of-day bucket (e.g., 15-minute bins).
- Minutes since market open.
- Minutes to market close.
- Weekday (0-4).

### Symbol state features

- Current realized volatility (N-bar).
- ATR / price ratio.
- Distance from 50-bar SMA (normalized).

## Labels

### Entry head (`meta_entry`)

Binary triple-barrier label at the symbol's **median** (target, stop) pair (median by target value, e.g., L4 at +0.3%/-0.2% for longs). "Would a trade entered on this bar hit the target before the stop within H bars?" Uses the Feature Engine's existing labeler with pessimistic bar traversal.

### Execution head (`meta_exec`)

Two outputs:

1. **`kelly_fraction`** — `clip(max(0, 2p - 1) * (target / stop), 0, 1)` where `p` is the calibrated entry probability and (target, stop) is the pair that would have realized the highest expected value on that bar's forward window. Continuous in [0, 1].

2. **`pair_index`** — categorical over the symbol's existing sub-model (target%, stop%) pairs (up to 7 long / 7 short). Label is the pair with highest realized profit on that bar's forward window (i.e., which pair's target was hit first, or if none hit, which had the least drawdown). Ties broken by tighter stop (more conservative).

## Training Data

### Backtest-replay bootstrap

For each (symbol, direction):

1. Replay current-generation base models over the symbol's historical feature rows.
2. At each bar, reconstruct the meta-feature vector (base predictions, rolling accuracy computed from replay history, regime, temporal, symbol state).
3. Join to triple-barrier labels from the existing labeler.
4. Produces years of training data per symbol immediately.

**Known limitation:** rolling-live-accuracy in replay is synthetic (no fidelity-driven disables, no production drift). Accepted because live rows are weighted higher during fine-tuning.

### Continuous fine-tuning on live data

- At inference time, the meta-feature vector is persisted to a `meta_features` table.
- Nightly retrain appends new live rows to the replayed dataset.
- Live rows weighted `meta_live_weight_multiplier` × higher than replayed rows in training loss (default 3.0).
- Over time, live data increasingly dominates training signal.

### Cold-start gate

A (symbol, direction, estimator_type) meta-model is only promotable to live when:

- Row count ≥ estimator's `cold_start_min_rows` threshold (2k for LightGBM, 5k for MLP).
- Backtest EV-per-signal ≥ `meta_cold_start_min_backtest_accuracy`.

Below gate → shadow-only. Above gate → eligible for promotion via dashboard or auto-promotion if enabled.

## Inference Path

1. `InferenceEngine` runs base sub-models as today → `Prediction` list.
2. `MetaInferenceEngine` assembles meta-feature vector:
   - Base predictions from step 1.
   - Rolling live accuracy from `prediction_outcomes` table.
   - Regime from Redis `features:latest` (SPY channels).
   - Temporal features from current timestamp.
   - Symbol state from latest feature row.
3. Persist meta-feature row to `meta_features` table (for future retraining).
4. Run the symbol's **active** `meta_entry` estimator → calibrated probability.
5. If `p >= meta_min_confidence` AND symbol's meta flag = `live`:
   - Run `meta_exec` → `(kelly_fraction, pair_index)`.
   - Compute size: `meta_kelly_cap_multiple × kelly_fraction × base_risk_per_trade`.
   - Look up (target%, stop%) from `pair_index`.
   - Emit `TradingSignal` with direction, size, target, stop.
6. If symbol's meta flag = `shadow`:
   - Log meta output to `meta_signals_shadow` table.
   - Emit signal from existing `SignalAggregator` instead (the control).
7. If no meta-model available (missing, cold-start, disabled): fall back to `SignalAggregator`.

### Hysteresis check

Before emitting any meta-sourced signal, check: did this (symbol, direction) emit a signal in the opposite direction within the last `meta_hysteresis_bars` bars? If yes, suppress the new signal and log the suppression.

## Calibration

The entry head's output probability feeds Kelly sizing — miscalibration directly translates to bad sizing. Non-negotiable: every entry-head estimator is wrapped in **isotonic regression calibration**.

- Calibration fitted on a held-out chronological slice (last 20% of training data, after train/validation split).
- Applied at inference time as a post-processing step.
- Calibration error tracked per estimator in `model_metadata.metrics`.
- Dashboard displays reliability diagram (predicted vs actual probability, 10 bins) per active meta-model.

## Training Pipeline (nightly, in `model_service`)

1. **Dependency gate:** wait for base-model nightly retrain to complete (base models must settle before replay uses them).
2. For each (symbol, direction):
   a. Backtest-replay current base models over historical features → meta-feature matrix.
   b. Append live meta-feature rows accumulated since last train.
   c. Apply live-row weight multiplier.
   d. Chronological train/validation/calibration/test split.
   e. For each registered estimator type:
      - Hyperparameter search via Optuna (time-boxed: max 30 trials per estimator per symbol).
      - Fit best params on train set.
      - Fit isotonic calibration on calibration slice.
      - Evaluate on test slice: AUC, calibration error, EV-per-signal, simulated PnL.
      - Save artifact + metrics to `ModelStore` / `model_metadata`.
3. Update shadow-mode leaderboard per (symbol, direction).
4. No auto-promotion by default — dashboard gate. (Configurable: auto-promote if shadow EV exceeds aggregator EV by configurable margin for configurable consecutive days.)

## Shadow Mode & A/B/n Comparison

Every (symbol, direction) has:

- The `baseline_aggregator` control (always runs, zero cold-start).
- Zero or more trained meta-estimators in shadow or live mode.

**Shadow mode** means the estimator runs at inference time and its output is logged to `meta_signals_shadow`, but the actual trade signal comes from whichever source is flagged `live` (initially the `baseline_aggregator`).

**Leaderboard metrics** (computed daily, displayed on dashboard):

- EV per signal (realized PnL / number of signals).
- Win rate.
- Sharpe ratio of signal-level returns.
- Calibration error (entry head only).
- Signal count (more signals at equal EV = better).

**Promotion:** user flips a (symbol, direction) from one estimator to another via dashboard toggle. The previously-live estimator moves to shadow; the promoted one goes live.

## Guardrails

### Configurable parameters (runtime_config, editable on dashboard config page)

All have separate columns for live and backtest override values.

| Key | Default | Description |
|-----|---------|-------------|
| `meta_min_confidence` | 0.55 | Entry-head probability floor |
| `meta_kelly_cap_multiple` | 0.25 | Fraction of full-Kelly to use (range [0, 1]) |
| `meta_hysteresis_bars` | 5 | Suppress direction flips within N bars |
| `meta_cold_start_min_rows` | 2000 | Minimum rows before promotion eligibility (per-estimator override) |
| `meta_cold_start_min_backtest_accuracy` | (per symbol) | EV-per-signal floor on backtest |
| `meta_live_weight_multiplier` | 3.0 | Training weight for live vs replayed rows |
| `meta_rolling_accuracy_window_days` | 30 | Window for per-sub-model live-accuracy feature |

### Hard guardrails (enforced in `execution_service`, not overridable)

- Max position size % of equity — existing limit, applied after Kelly sizing.
- Max concurrent positions — existing limit.
- Target and stop must be one of the symbol's trained sub-model pairs — meta-model soft-selects, never extrapolates beyond trained pairs.
- Hysteresis check as described above.
- All existing trading rules enforced: windows, blackouts, short-close-before-EOD, buying power.

## Fidelity Tracking

`FidelityTracker` extended to handle meta-models:

- Baseline: backtested EV-per-signal from the meta-model's test slice.
- Live: rolling EV-per-signal from actual trades attributed to this meta-model.
- Thresholds: same `warning_margin` and `critical_margin` as base models, applied to EV-per-signal instead of accuracy.
- Degradation path: `active` → `warned` → `disabled`. On disable, (symbol, direction) falls back to `baseline_aggregator`.

## Data Model Additions

### New tables

**`meta_features`**
```
symbol          TEXT
timestamp       TIMESTAMPTZ
direction       INT
feature_vector  JSONB
entry_label     INT (nullable, backfilled by labeler)
optimal_pair_idx INT (nullable, backfilled)
realized_ev     FLOAT (nullable, backfilled)
source          TEXT ('replay' | 'live')
```

**`meta_signals_shadow`**
```
symbol          TEXT
timestamp       TIMESTAMPTZ
direction       INT
estimator_type  TEXT
entry_probability FLOAT
kelly_fraction  FLOAT
pair_index      INT
target_pct      FLOAT
stop_pct        FLOAT
model_id        INT (FK to model_metadata)
```

### Extended tables

**`model_metadata`** — extend `model_type` enum to include `meta_entry`, `meta_exec`. Add `estimator_type` column (`lightgbm`, `mlp`, `baseline_aggregator`, ...).

**`runtime_config`** — add the seven new keys listed in Guardrails section.

## Dashboard Additions

### New "Meta-Models" page

- Per-symbol grid: status (`shadow` / `live` / `disabled` / `cold_start`), active estimator type, live EV-per-signal, calibration error, feature-importance top-10.
- Toggle per (symbol, direction): promote / demote / disable estimators.
- A/B/n leaderboard: ranked comparison of all estimator types per symbol over configurable lookback window.
- Shadow-vs-live comparison chart: "over last N days, each estimator would have produced X signals with realized EV Y."
- Reliability diagram: calibration plot (predicted probability vs actual hit rate) per active entry-head estimator.

### Config page additions

Seven new runtime_config keys with live + backtest-override columns.

## Out of Scope

- **Cross-symbol global meta-model** — single model with symbol embedding. Deferred until per-symbol proves itself.
- **Online/streaming training** — nightly batch is sufficient.
- **Raw-feature inclusion (Level 3)** — base features as meta-model inputs. Deferred until L2 proves insufficient.
- **Sequence models over meta-features** — LSTM/transformer over meta-feature time series. A future estimator plugin, not initial scope.
- **Auto-promotion without dashboard gate** — configurable but off by default.
