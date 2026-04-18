# Dashboard UI Expansion — Design Spec

**Date:** 2026-04-17
**Status:** Draft — awaiting approval

## Problem

The current dashboard gives a surface view of the trading system but lacks the depth needed to actually manage it:

- **Models tab** shows only `model_id / status / reason / updated_at`. No metrics, no time-series, no comparison, no attribution. You cannot answer "is this model actually helping?"
- **Trading tab** shows aggregate cards and a per-symbol grid. No equity curve, no drawdown visibility, no regime awareness, no benchmark comparison. You cannot answer "are we beating buy-and-hold?"
- **Backtests tab** has a sortable list and a detail view, but the detail view is thin. No equity chart, no per-fold breakdown, no cross-run comparison, no sensitivity analysis, no live-vs-backtest overlay. You cannot answer "will this strategy actually work in production?"

## Solution

A four-plan dashboard expansion that builds shared UI infrastructure first, then deepens each of the three weak domains:

- **Plan 1 — Shared Foundation:** reusable components (router, time-window picker, refresh control, chart wrappers, leaderboard primitive, drill-down frame). No user-visible changes; enables downstream plans.
- **Plan 2 — Models domain:** 7 sub-tabs + per-model drill-down + forward-compatible Meta-Models scaffolding.
- **Plan 3 — Trading domain:** 6 sub-tabs + per-symbol drill-down.
- **Plan 4 — Backtests domain:** extends existing tab with 3 new top-level sub-tabs + 7 detail-page sub-tabs + tagging, notes, and promote-to-live.

## Design Principles

1. **Foundation first.** Shared components (routing, charting, time-window, drill-down) are built once and reused across every domain. Each domain plan becomes "wire existing components to new data," not "reinvent the wheel."
2. **Layout pattern — hybrid (D).** Top-level tabs unchanged. Content-heavy tabs get sub-tabs for cross-cutting overview views; clicking a specific entity (model, symbol, backtest) routes to a dedicated drill-down page. URL format: `#models/leaderboard`, `#models/{model_id}`, `#backtests/{run_id}/folds`.
3. **Chart.js + one custom heatmap.** Already loaded. No new frontend frameworks. Heatmaps rendered as colored-cell grids (simpler than Chart.js extensions).
4. **Vanilla JS, no build step.** Current dashboard is script-tag JS served from `bullforge/services/dashboard_service/static/`. Keep it that way.
5. **Backend extends existing patterns.** aiohttp handlers, async Database methods, JSON responses. No new services.
6. **Forward compatibility with meta-models.** Plan 2's shared components are designed so a future Meta-Models tab (wired after Plan B's meta-model inference lands) reuses them verbatim.

## Scope & Phasing

### Plan 1 — Shared Foundation

**Components** (all in `bullforge/services/dashboard_service/static/components/`):

| Component | File | Responsibility |
|-----------|------|----------------|
| Router | `router.js` | Parses `window.location.hash` into `{tab, subtab, entity_id}`, emits `routeChanged` events, tabs register handlers |
| Time-window picker | `timewindow.js` | Dropdown with presets (1d/7d/30d/90d/180d/1y/all/custom) emitting `{from, to}` as ISO strings; persists in sessionStorage per tab |
| Refresh control | `refresh.js` | Pause/resume button + interval selector (10s/30s/60s/manual); replaces hardcoded `REFRESH_INTERVAL` |
| Chart wrappers | `charts.js` | Thin Chart.js wrappers: `lineChart`, `barChart`, `histogramChart`, `scatterChart`, `sparkline`; plus `heatmapChart` as a colored-cell grid (no Chart.js dep) |
| Leaderboard primitive | `leaderboard.js` | Sortable table: `{columns, rows, getRowHref}`, click-row-to-drill-down, pagination, optional sparkline column |
| Drill-down frame | `drilldown.js` | Back-button, entity title header, breadcrumb nav, content slot; renders for routes like `#models/42` |

**Backend foundation work:**

- Add `?from=ISO&to=ISO` query parameter support to all existing `/api/metrics/*`, `/api/trades`, `/api/orders` endpoints. Defaults preserve current behavior.
- Stub new endpoints (return HTTP 501 until downstream plans implement): `/api/models/{id}/history`, `/api/models/{id}/calibration`, `/api/models/{id}/simulated_pnl`, `/api/models/{id}/feature_importance`, `/api/models/{id}/regime_breakdown`, `/api/models/{id}/signal_history`, `/api/trading/equity`, `/api/trading/distributions`, `/api/trading/heatmap`, `/api/trading/streaks`, `/api/trading/exposure`, `/api/trading/benchmark`, `/api/trading/symbols/{sym}`, `/api/backtests/{id}/equity`, `/api/backtests/{id}/folds`, `/api/backtests/{id}/symbols`, `/api/backtests/{id}/distributions`, `/api/backtests/{id}/live-overlay`, `/api/backtests/{id}/funnel`, `/api/backtests/compare`, `/api/backtests/sensitivity`.

**Plan 1 success criteria:** All components unit-testable via a dedicated `static/test/` harness (extends existing `fmttime.html` pattern). URL-based routing works: direct-loading `http://host/#models/42` lands on the models drill-down placeholder. Existing dashboard behavior unchanged for users (refresh still works, current tabs still work).

### Plan 2 — Models domain

**Sub-tabs** (routed `#models/<subtab>`):

| # | Sub-tab | View | Data source | Backs question |
|---|---------|------|-------------|----------------|
| 1 | **Leaderboard** | Sortable table: `symbol / timeframe / horizon / type / status / live_acc_7d / backtest_acc / fidelity_delta / signals_7d / simulated_pnl_7d / sparkline(acc_30d)`. Color-coded fidelity delta (red below `critical_margin`, yellow below `warning_margin`). Row click → drill-down. | Existing `model_registry`, `model_status`, `predictions` + `prediction_outcomes`, plus new daily accuracy aggregation | Q4: which outperform |
| 2 | **Fidelity** | Time-series line chart: live accuracy vs backtested baseline band, multi-model overlay (up to 8). Warning/critical margin lines. | `metric_snapshots` category=`model_fidelity` scope=model_id (Plan 2 adds nightly persistence of `FidelityTracker.evaluate_all_models` results) | Q1: fidelity drift |
| 3 | **Calibration** | Reliability diagram: predicted-confidence bin (10 bins) vs actual-hit-rate, bubble size = bin sample count. Diagonal perfect-calibration reference. Aggregate calibration error above chart. Model selector. | `predictions` + `prediction_outcomes` aggregated on request (LRU cached) | Q2: calibration |
| 4 | **Attribution** | Two stacked panels: signal count per model over window (bar chart), simulated P&L per model (bar chart — "if we'd traded every signal"). | New endpoint `/api/models/{id}/simulated_pnl?from=&to=` joins `predictions` against forward bars at each prediction's horizon | Q3: attribution |
| 5 | **Time-series performance** | Daily-bucketed accuracy over time, multi-model overlay, retrain-event markers. | `metric_snapshots` (extend monitoring to persist daily accuracy) | Q5: degradation over time |
| 6 | **Regime** | Grouped bar chart: accuracy / P&L / hit-rate per regime bucket (Bullish / Bearish / Neutral / HighVol / LowVol). Model selector. | `predictions` joined to regime feature (Plan 2 adds regime column to predictions lookup) | Q6: regime breakdown |
| 7 | **Feature importance** | Horizontal bar chart, top 20 features. Model selector. LSTM shows "not available for sequence models" with optional future gradient-attribution. | Loads artifact via `ModelStore`, reads `feature_importances_` | Q7: feature importance |

**Drill-down page** (`#models/{model_id}`): all seven sub-tab contents scoped to one model in one scrollable page, plus a **signal history table** (last 100 signals: timestamp / direction / confidence / outcome / link to trade that consumed them). Backs Q8 — a trade row in the Trades tab links here filtered by contributing model IDs.

**Meta-Models forward-compatibility (scaffolded in Plan 2, wired after Plan B ships):**

Same 7 sub-tabs under a future **Meta-Models** top-level tab, scoped to meta-models (rows keyed by `(symbol, direction, estimator_type)` instead of base-model identity). Plus a unique **A/B/n comparison panel**: for a selected `(symbol, direction)`, overlay shadow-mode EV-per-signal time series for every registered estimator (lightgbm / mlp / baseline / future) plus the current live `SignalAggregator` baseline. Cumulative signals + PnL bars. Per-row promotion toggle (`shadow` ↔ `live` ↔ `disabled`). Plus a **Shadow vs. live summary view**: "over last N days, estimator X produced N signals with EV Y; live aggregator produced N' with EV Y'."

Plan 2 **builds the components** so Meta-Models tab reuses them when Plan B's shadow-mode inference writes to `meta_signals_shadow`. Plan 2 does **not** wire the Meta-Models tab itself — that's deferred to a follow-up plan after Plan B.

**Plan 2 backend work:**

- Persist daily per-model accuracy snapshots (nightly, part of existing monitoring).
- `/api/models/{id}/calibration?from=&to=` — bin + aggregate error.
- `/api/models/{id}/simulated_pnl?from=&to=` — attribution P&L.
- `/api/models/{id}/feature_importance` — loads artifact, returns array.
- `/api/models/{id}/regime_breakdown?from=&to=` — per-regime metrics.
- `/api/models/{id}/signal_history?limit=100` — drill-down signal table.

### Plan 3 — Trading domain

**Sub-tabs** (routed `#trading/<subtab>`):

| # | Sub-tab | View | Backs feature |
|---|---------|------|---------------|
| 1 | **Overview** | Current card grid unchanged, plus 4 new cards: current streak, longest winning streak, longest losing streak, exposure now (%). | Augments existing |
| 2 | **Equity curve** | Line chart, equity over time; overlaid realized-only and realized+unrealized lines; drawdown shading under trailing-peak; halt-event annotations; optional SPY buy-and-hold overlay. | 1, 9 |
| 3 | **Distributions** | Four visualizations: daily P&L bars, trade P&L histogram, hold-time bucketed bar chart, long-vs-short side breakdown (table + bar chart). | 2, 3, 6, 8 |
| 4 | **Streaks & Exposure** | Exposure-over-time stacked area (colored by symbol); streak-timeline W/L ribbon (each cell = one trade, green/red). | 4, 5 |
| 5 | **Heatmaps** | Two colored-cell grids: time-of-day × day-of-week avg P&L; symbol × day-of-week avg P&L. | 7 |
| 6 | **Benchmark** | Equity-vs-SPY overlay + equal-weighted-traded-basket overlay. Stats table: annualized return, annualized vol, Sharpe, max drawdown, beta, alpha. | 9 |

**Drill-down page** (`#trading/symbols/{sym}`): equity curve scoped to one symbol, trade table, win rate over time, per-side breakdown, symbol-scoped heatmap. Row click in Overview per-symbol grid routes here. Backs feature 10.

**Plan 3 backend work:**

- `/api/trading/equity?from=&to=` — `[{timestamp, realized_pnl, unrealized_pnl, equity, drawdown, exposure_pct, spy_equity_normalized}]`, sampled to ≤2000 points.
- `/api/trading/distributions?from=&to=` — daily P&L + trade P&L histogram + hold-time buckets + side breakdown.
- `/api/trading/heatmap?from=&to=&kind=time|symbol` — cell grid.
- `/api/trading/streaks?from=&to=` — current streak, longest streaks, W/L ribbon.
- `/api/trading/exposure?from=&to=` — exposure series (stacked by symbol).
- `/api/trading/benchmark?from=&to=` — SPY + basket overlay + stats.
- `/api/trading/symbols/{sym}?from=&to=` — symbol-scoped drill-down payload.

### Plan 4 — Backtests domain

**Top-level Backtests sub-tabs:**

| # | Sub-tab | View |
|---|---------|------|
| 1 | **List** (existing, extended) | Current sortable table plus: tags chips column, notes-preview column, multi-select checkboxes feeding Compare. Tag filter in toolbar. |
| 2 | **Compare** | Overlay equity curves for 2–5 selected runs (normalized to shared starting equity). Side-by-side metrics table. Compare set persisted in URL: `#backtests/compare?ids=17,22,34`. |
| 3 | **Sensitivity** | Pick runs sharing a parameter-sweep grouping key. 1-D sweep: three stacked line charts (P&L / Sharpe / Max DD vs. swept param). 2-D sweeps: heatmap. |

**Detail-page sub-tabs** (routed `#backtests/{run_id}/<subtab>`):

| # | Sub-tab | View |
|---|---------|------|
| 1 | **Summary** (existing) | Aggregate metrics, dates, pretty-printed config JSON. |
| 2 | **Equity curve** | Per-fold equity concatenated, fold-boundary dividers, drawdown shading, optional entry/exit markers. |
| 3 | **Folds** | Table: `fold / train_range / test_range / pnl / sharpe / max_dd / trades / hit%`; row expand shows fold's trades; heatmap mode shows deviation from run mean. |
| 4 | **Per-symbol** | Bar chart of P&L per symbol within the run + adjacent table. Reveals whether edge is broad-based. |
| 5 | **Trade distribution** | Same three charts as Trading → Distributions, scoped to this run. |
| 6 | **Live-vs-backtest** | Overlay live equity on simulated equity for the run's symbols/dates. "No overlap" message when live trading didn't run during the backtest window. |
| 7 | **Signal-to-trade funnel** | Horizontal attrition bars: `signals → min-conf → agreement → risk/blackouts → submitted → filled → closed`. Requires backtester to log funnel counts. |

**Tagging & notes:**

- Migration `017_backtest_tags_notes.sql`: `ALTER TABLE backtest_runs ADD COLUMN tags TEXT[], ADD COLUMN notes TEXT`; plus new table `runtime_config_audit (id, key, old_value, new_value, changed_at, source_backtest_run_id)`.
- Inline-editable on the List row (click-to-edit pattern).
- `POST /api/backtests/{id}/tags`, `POST /api/backtests/{id}/notes`.

**Promote-to-live:**

- Button on Summary sub-tab. Opens diff modal showing which runtime_config keys change and from-→to values. Confirm button writes via existing config_api and inserts `runtime_config_audit` row stamped with `source_backtest_run_id`.
- `POST /api/backtests/{id}/promote-config`.

**Plan 4 backend work:**

- `/api/backtests/{id}/equity` — per-fold equity points concatenated.
- `/api/backtests/{id}/folds` — fold table data.
- `/api/backtests/{id}/symbols` — per-symbol breakdown.
- `/api/backtests/{id}/distributions` — same shape as live distributions endpoint.
- `/api/backtests/{id}/live-overlay?window=...` — live trades during backtest window.
- `/api/backtests/{id}/funnel` — funnel counts (requires backtester instrumentation).
- `/api/backtests/compare?ids=...` — overlay-ready payload.
- `/api/backtests/sensitivity?group_key=&swept_param=` — sweep results.
- Tags/notes POST endpoints + promote-config endpoint as above.
- Migration 017.

## Data Flow & Interfaces

### URL routing convention

- Top-level tab: `#<tab>` (e.g., `#models`, `#trading`, `#backtests`).
- Sub-tab: `#<tab>/<subtab>` (e.g., `#models/calibration`, `#trading/equity`).
- Entity drill-down: `#<tab>/<entity-id-or-kind>/<id>` (e.g., `#models/42`, `#trading/symbols/AAPL`, `#backtests/17`).
- Entity sub-tab: `#<tab>/<kind>/<id>/<subtab>` (e.g., `#backtests/17/folds`).
- Query-like params after `?`: `#backtests/compare?ids=17,22,34`.

The Router component is the single parser of this convention.

### Time-window convention

- All data endpoints accept optional `from=<ISO8601>&to=<ISO8601>`.
- When absent, endpoints return their current default (usually "all time" or last N).
- Frontend TimeWindow component emits ISO strings; each tab's fetch layer appends them to URL.
- Backend uses `timestamp >= from` / `timestamp < to` filter on the relevant time column.

### Refresh convention

- Refresh control emits `refresh` events at the chosen interval (10s/30s/60s/manual).
- Tab handlers re-run their data fetches on `refresh`.
- Paused state persists in `sessionStorage[`dashboard.refresh.paused.${tab}`]`.
- Charts use Chart.js `destroy()` before re-creating to prevent leaks.

### Payload shapes (canonical examples)

**Equity series** (shared by trading and backtests):
```json
[
  {"timestamp": "2026-04-17T13:30:00Z", "equity": 100000.00, "realized_pnl": 0.00, "unrealized_pnl": 0.00, "drawdown": 0.00, "exposure_pct": 0.00, "benchmark_spy": 100000.00},
  ...
]
```

**Histogram** (shared by distributions):
```json
{"bins": [-0.05, -0.04, -0.03, ...], "counts": [2, 5, 12, ...], "median": 0.008, "mean": 0.012}
```

**Heatmap cells**:
```json
{"x_labels": ["Mon", "Tue", ...], "y_labels": ["09:30", "10:00", ...], "cells": [[-0.002, 0.005, ...], [0.003, -0.001, ...]]}
```

## Error Handling

- Stubbed endpoints return HTTP 501 with `{"error": "not_implemented", "enabled_after_plan": "plan_N"}`. Frontend detects this and shows a clear "feature coming in Plan N" placeholder, not a red error.
- Empty/insufficient-data cases return `200 OK` with empty arrays; frontend renders an "insufficient data" empty-state message.
- Artifact-load failures (e.g., feature importance on a pre-deleted model) return 404 with a message; frontend shows per-chart "artifact unavailable."
- Backend exceptions logged to existing logger; frontend shows a dismissible error toast, the rest of the dashboard stays usable.

## Testing Strategy

**Frontend (per component, per plan):**

- Each shared component in Plan 1 gets a standalone test page in `bullforge/services/dashboard_service/static/test/` extending the existing `fmttime.html` pattern: loads the component, asserts behavior, reports pass/fail in page body. Plan-specific test pages verify integration (e.g., `test/leaderboard.html` exercises Models leaderboard rendering against a fixture payload).
- Smoke test per plan: manually open each new sub-tab in dev browser and confirm (a) no console errors, (b) charts render, (c) time-window changes propagate, (d) drill-downs route correctly.

**Backend:**

- Each new endpoint gets a unit test under `bullforge/tests/` following the existing `test_dashboard_api.py` pattern (aiohttp test client + mocked Database).
- Time-window filtering tested per endpoint: request without `from/to` returns default behavior; request with `from/to` filters correctly.
- Stubbed endpoints tested to return 501 until their owning plan lands.
- Integration: existing integration tests in `bullforge/tests/integration/` extended to cover new endpoints once the DB is available locally.

## Out of Scope

- **Meta-Models top-level tab wiring.** Plan 2 builds the shared components and forward-compatibility scaffolding. Actual Meta-Models tab wiring is a follow-up plan after Plan B (meta-model inference/shadow mode) lands.
- **Real-time websocket push** for chart updates. Current 30s polling refresh is sufficient; websocket adoption is a separate future project.
- **Dashboard auth / multi-user.** Single-user local dashboard; no user accounts.
- **Theme / style overhaul.** Dark theme stays; no redesign of existing colors, fonts, or spacing.
- **Mobile / small-screen optimization.** Dashboard is desktop-first; responsive work deferred.
- **Export to CSV / PDF.** Out of scope.
- **Real-time alerts on dashboard.** Telegram bot handles alerts; dashboard stays read-only visualization.
- **New frontend framework** (React / Vue / Svelte). Vanilla JS stays.

## Migration Plan

- Plan 1 ships as a silent refactor: routing and components land but no user-visible tab changes.
- Plans 2, 3, 4 ship independently in any order after Plan 1. Each is independently deployable; no cross-plan dependencies.
- Backend API changes are additive only. No breaking changes to existing endpoints.
- The Models/Trading/Backtests tabs remain usable at every step — new sub-tabs appear as their plans land; existing tab content stays until the plan that replaces it ships.
