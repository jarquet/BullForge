# Backtest Dashboard — Design Spec

**Date:** 2026-04-15
**Status:** Approved for planning

## Goal

Give users a dashboard surface to (1) see an overview of all backtest runs with key metrics including P&L, (2) drill into a single run for equity curve and detailed analysis, and (3) kick off new walk-forward runs from the UI.

## Architecture

Extend three existing layers; add one new worker process. No new services beyond the worker.

- **`bullforge/services/backtest_service/results_db.py`** — add read query methods.
- **`bullforge/services/backtest_service/worker.py`** (new) — long-running job consumer.
- **`bullforge/services/dashboard_service/api.py`** — add `/api/backtests/*` routes.
- **Frontend** (`index.html`, `dashboard.js`, `style.css`) — new Backtests tab with overview and detail views, plus a "New Backtest" modal.
- **`bullforge/docker-compose.yml`** — add long-running `backtest-worker` service (reuses the existing `backtest_service/Dockerfile`).

Dashboard reads the backtest tables via the same TimescaleDB pool it already uses. Dashboard never imports `BacktestEngine` (avoids pulling torch/lightgbm/alpaca-py into the dashboard image).

## Data model

Existing tables (unchanged): `backtest_runs`, `backtest_folds`, `backtest_trades`, `backtest_equity`.

New table — schema added via a new numbered SQL migration under `bullforge/migrations/` (next index after `012_create_backtest_tables.sql`):

```sql
CREATE TABLE backtest_jobs (
  id          BIGSERIAL PRIMARY KEY,
  name        TEXT NOT NULL,
  config_json JSONB NOT NULL,
  status      TEXT NOT NULL DEFAULT 'queued',  -- queued | running | done | failed
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at  TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  run_id      BIGINT REFERENCES backtest_runs(id),
  error       TEXT
);
CREATE INDEX ON backtest_jobs (status, created_at);
```

## Worker

`python -m bullforge.services.backtest_service.worker` runs indefinitely:

1. `BEGIN; SELECT … FROM backtest_jobs WHERE status='queued' ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED;`
2. If a row is claimed: set `status='running'`, `started_at=now()`, commit.
3. Build `BacktestConfig` from `config_json`, call `await BacktestEngine(...).run(cfg)`.
4. On success: set `status='done'`, `run_id=<returned id>`, `finished_at=now()`.
5. On exception: set `status='failed'`, `error=<message>`, `finished_at=now()`. Log traceback.
6. Sleep 2s between polls when idle.

One job at a time per worker. Compose runs a single `backtest-worker` replica by default.

## API

All JSON; base path `/api/backtests`. Read-only except the POST.

### GET `/api/backtests`

List runs + in-flight jobs. Query params:

- `status` — one of `all|running|done|failed|queued` (default `all`)
- `q` — case-insensitive substring match on `name`
- `sort` — one of `finished_at|started_at|name|total_pnl|sharpe|max_dd` (default `finished_at`)
- `order` — `asc|desc` (default `desc`)
- `limit` — int, default 50, max 200
- `offset` — int, default 0

Response rows are a union of completed runs (from `backtest_runs`) and pending jobs (from `backtest_jobs` where `run_id` is null). Each row:

```json
{
  "kind": "run" | "job",
  "id": 42,                   // run_id when kind=run, job_id when kind=job
  "name": "wf_aapl_2024",
  "symbols": ["AAPL"],
  "range_start": "...",
  "range_end": "...",
  "status": "done",
  "started_at": "...",
  "finished_at": "...",
  "metrics": {                // null for kind=job
    "total_pnl": 12430.0,
    "sharpe": 1.42,
    "max_dd": -0.083,
    "n_trades": 143,
    "hit_rate": 0.58
  }
}
```

Metric aggregation is computed per-request by SQL over `backtest_trades` + `backtest_equity` for each run in the page. If this becomes slow, introduce a materialized view in a follow-up; not in scope now.

### GET `/api/backtests/{id}`

Run header. 404 if id not found. Response includes the run row + full computed metric set: `total_pnl, sharpe, sortino, max_dd, hit_rate, profit_factor, n_trades, n_folds` + `config_json`.

### GET `/api/backtests/{id}/folds`

Array of fold rows with `fold_index, train_start, train_end, test_start, test_end, metrics_json` (already stored). Server also computes per-fold `total_pnl, sharpe, max_dd, n_trades` from trades+equity.

### GET `/api/backtests/{id}/equity?fold_id=`

Array `[{t, equity, cash}]` sorted by `t`. Omit `fold_id` for combined; pass an id to get a single fold's curve.

### GET `/api/backtests/{id}/trades?limit=&offset=&sort=&order=`

Paginated. Columns: `entry_time, exit_time, symbol, side, entry_price, exit_price, quantity, pnl, signal_strength, fold_id`. Default sort `entry_time desc`, limit 50, max 200.

### POST `/api/backtests`

Create a queued job. Body:

```json
{
  "name": "wf_aapl_2024",
  "symbols": ["AAPL"],
  "timeframe": "1m",
  "horizons": [5, 15, 30],
  "range_start": "2024-01-02T13:30:00Z",
  "range_end":   "2024-12-31T21:00:00Z",
  "train_days": 60,
  "test_days": 5,
  "embargo_min": 30,
  "mode": "rolling"
}
```

Validation (server mirrors client):

- `name` non-empty
- `symbols` non-empty; each in the configured universe from `/api/symbols`
- `timeframe ∈ {1m, 1d}`
- `horizons` non-empty subset of `{5, 10, 15, 30, 60, 75, 90, 105, 120}`
- `range_start < range_end`, both ISO-8601 UTC
- `train_days, test_days, embargo_min` positive integers
- `embargo_min ≤ max(horizons)`
- `mode ∈ {rolling, anchored}`

On success: insert into `backtest_jobs` with `status='queued'`, return `{job_id}`. On failure: `400 {errors: {field: "msg", ...}}`.

Engine-tuning knobs (`starting_cash, slippage_bps, max_volume_pct, periods_per_year`) use CLI defaults; not exposed in the form this round.

## Frontend

### Navigation

New `Backtests` tab added to the existing tab strip in `index.html`. Deep-linkable via URL hash: `#backtests` (list) and `#backtests/{id}` (detail). Browser back-button works.

### Overview view

- **Toolbar:** status dropdown (`all|queued|running|done|failed`), name search (debounced 300ms), "New Backtest" button, refresh button.
- **Table columns:** Name · Symbols · Date range · Total P&L · Sharpe · Max DD · # Trades · Hit % · Status · Finished-at. Sortable headers (server-driven via `sort`/`order` params). Rows clickable → navigate to detail view. P&L cell tinted green/red by sign; Max DD always red. Queued/running rows show `—` for metrics.
- **Polling:** refetch list every 10s while the tab is active (matches existing dashboard tab pattern in `dashboard.js`).
- **Empty state:** "No backtests yet. Click New Backtest to start one."

### Detail view

Shown when URL is `#backtests/{id}` (where `{id}` is a `backtest_runs.id`).

Clicking a row in the overview table:
- `kind=run` → navigate to the detail view below.
- `kind=job` with `status ∈ {queued, running}` → navigate to a minimal detail view showing only the header + `config_json` (no metrics / charts yet). Auto-polls every 5s; when the underlying run completes (`run_id` populated on the job), the URL redirects to `#backtests/{run_id}` and the full detail view renders.
- `kind=job` with `status=failed` → minimal detail view showing header + `config_json` + the `error` text.

1. **Header:** name, symbols, range, mode, fold count, status, ← Back link.
2. **Metric cards row:** Total P&L, Sharpe, Sortino, Max Drawdown, Hit Rate, Profit Factor, # Trades, # Folds.
3. **Equity curve** (Chart.js line): combined curve by default. Legend toggles to overlay each fold's curve on the same axes.
4. **Drawdown chart** (Chart.js area, red): computed client-side from the equity series as `equity / running_max - 1`.
5. **Per-fold metrics table:** fold index, train range, test range, fold P&L, sharpe, max DD, # trades.
6. **Trade P&L histogram** (Chart.js bar): binned P&L-per-trade, fixed bin count 30.
7. **Trades table:** paginated 50/page, columns: entry time, symbol, side, entry, exit, qty, P&L, signal strength, fold. Sortable.
8. **Config panel:** collapsible `<details>` with pretty-printed `config_json`.

### New Backtest modal

Triggered by the toolbar button. Fields per POST body spec above. Notable UI behaviors:

- Symbols multi-select populated from `GET /api/symbols` (union of equity + crypto).
- Horizons multi-select: fixed options `5, 10, 15, 30, 60, 75, 90, 105, 120` minutes.
- Embargo input `max` attribute updated live to `max(selected horizons)`; form rejects submit if exceeded.
- Submit disabled until required fields are valid.
- On 400 response: render per-field error text inline.
- On 200 response: close modal, refetch the list (new queued row appears at top via the default `finished_at desc` — queued rows have null `finished_at`, so sort puts them at top under the `NULLS FIRST` convention used by the list query).

### Charting

Chart.js via CDN `<script>` tag in `index.html`. Loaded only on the Backtests tab to avoid slowing down the rest of the dashboard (conditional script injection or lazy `<script>` append on first tab activation).

## Error handling

- **API:** `404` for missing run/job; `400 {errors: {...}}` for validation; `500 {error: msg}` for unexpected. Structured logging on 500.
- **Worker:** `status='failed'` + `error` text on any engine exception; traceback logged. Worker keeps polling after a failed job.
- **Frontend:** per-section inline error banners — equity curve failure must not blank the whole detail page. Modal shows field errors from 400 response; generic banner for network errors.

## Testing

- **`results_db.py` query methods:** unit tests against the test TimescaleDB, following existing backtest_service test fixtures.
- **Worker:** unit test the claim/update transition; integration test that inserts a tiny job (one symbol, 2-day range, simulated broker) and asserts `status='done'` + a row in `backtest_runs`.
- **Dashboard API routes:** aiohttp test-client tests for list filtering/sorting, detail 404, POST validation (happy path + each field error), equity + trades pagination.
- **Frontend:** manual verification in a browser — load the Backtests tab, run the modal happy path, drill into a completed run, toggle per-fold overlay, paginate trades, deep-link `#backtests/{id}`, back-button. No JS test harness exists today; not adding one this round.

## Out of scope (follow-ups)

- Materialized view for list-page metrics if aggregation gets slow.
- Exposing engine-tuning knobs (starting_cash, slippage_bps, max_volume_pct, periods_per_year) in the form.
- Custom / free-text horizons.
- Deleting or re-running a job from the UI.
- Multi-worker concurrency tuning.
- Live progress streaming (fold-by-fold) — today the run is black-box until `done`.
