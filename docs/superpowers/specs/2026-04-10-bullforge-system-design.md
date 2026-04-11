# BullForge — Algorithmic Trading System Design

**Date:** 2026-04-10
**Status:** Approved for implementation planning

## Overview

BullForge is an ML-driven algorithmic trading system that identifies and rides market trends using an ensemble of triple-barrier sub-models. It trades large-cap equities/ETFs during market hours and BTC/ETH crypto pairs 24/7 via the Alpaca API.

The system runs on two Nvidia DGX Spark nodes (node-1 and node-2) using Docker Compose, with a future migration path to managed Kubernetes (EKS/AKS) on AWS or Azure for production live trading.

## Strategy

### Core Approach

Trend-following with regime awareness:

- Identify current trends and ride them, increasing position size or re-entering as signals indicate the trend continues.
- When the longer-term/overall trend is bullish, weight long entries more heavily and enter fewer/smaller short positions.
- When the longer-term trend is bearish, weight short entries more heavily and long entries more lightly.

### Sub-Model Ensemble

Multiple ML models trained with different profit target / stop loss combinations:

- **Per direction:** Separate model families for LONG and SHORT.
- **Per risk/reward:** Each family contains models trained on different target/stop values (e.g., +0.3%/-0.2%, +0.5%/-0.3%, +1.0%/-0.5%).
- **Consistent features:** All sub-models consume the same feature vector.
- **Binary classification:** Each model predicts "would an entry on this bar hit the profit target before the stop loss?"

### Signal Aggregation

Sub-model outputs are aggregated into entry signals:

1. Run all long sub-models → get probabilities.
2. Run all short sub-models → get probabilities.
3. Apply regime weighting (bullish regime boosts long weight, dampens short; bearish does the reverse).
4. Compute weighted aggregate score per direction.
5. If score exceeds configurable threshold → emit signal.
6. Signal includes: which sub-models agreed, confidence, suggested target/stop from the highest-confidence agreeing sub-model.

Thresholds, regime weights, and sub-model weights are optimized via walk-forward backtesting (not hand-tuned).

### Regime Detection

Uses cross-instrument features (SPY trend, broad momentum) to classify current regime as bullish/bearish/neutral. Initially rule-based (e.g., SPY price above/below 50-day SMA + momentum direction). Can be upgraded to ML later.

## Instruments & Trading Rules

### Equities/ETFs

- **Universe:** SPY, QQQ, and a small configurable list of large-caps (high volume, high liquidity).
- **Trading window:** Core hours only, configurable (default: 11:30 AM - 2:00 PM ET, avoiding 1-2 hours after open and before close).
- **Long positions:** Can hold overnight.
- **Short positions:** Must be closed before market close (by 3:30 PM ET).
- **Blackouts:** No single-equity trades near earnings dates. No equity/ETF trades on Federal Reserve announcement days.
- **Execution:** 1-minute bars for trade execution. Multi-timeframe features for model input.

### Crypto (BTC/USD, ETH/USD)

- **Trading hours:** 24/7.
- **Approach:** More conservative sizing and parameters than equities.
- **Same ML pipeline**, different model parameters and calendar rules.

### General

- Positions are managed with fixed bracket orders (target + stop), submitted as OCO orders via Alpaca.
- No options trading initially. Simple wheel strategy is an acceptable future addition.
- Range bars as an alternative to time bars are deferred — experiment after time-bar pipeline works.

## Architecture

### Approach

7 Docker Compose services (lean services). Each service is a well-bounded container with a clear responsibility. Services split out only when there's a real reason.

### Infrastructure

- **Runtime:** Docker Compose across two DGX Spark nodes.
- **Database:** TimescaleDB for time-series data (bars, features, trades).
- **Messaging:** Redis pub/sub for inter-service real-time events.
- **Model storage:** Shared volume between nodes (simpler than a registry for now).
- **Language:** Python primary (async wherever possible via asyncio/aiohttp). Rust via PyO3 bindings for compute-heavy bottlenecks (deferred until profiling shows need; Rust code should also be async/concurrent where applicable). Python concurrency (threading/multiprocessing) is acceptable where async is not feasible, but these cases should be flagged as candidates for future Rust optimization.
- **Timestamps:** All timestamps system-wide are UTC. No timezone conversions except in the Dashboard UI (deferred — not needed for MVP).

### Node Assignment

| Service | Node | Role |
|---|---|---|
| **Data Pipeline** | node-1 | Ingest market data from Alpaca, normalize, store |
| **Feature Engine** | node-1 | Compute multi-timeframe indicators, generate labels |
| **Model Training** | node-2 | Train/retrain sub-models on GPU, run backtester |
| **Signal Generator** | node-1 | Load models, consume features, emit trade signals |
| **Trade Manager** | node-1 | Execute signals via Alpaca API, manage positions |
| **Monitoring** | node-1 | Track metrics, model fidelity, alerts |
| **Dashboard** | node-1 | Web UI for monitoring and control |

### Shared Infrastructure (node-1)

- **TimescaleDB** — time-series price data, features, trade history.
- **Redis** — pub/sub for real-time events (ticks, features, signals, position updates).

### Inter-Service Communication

- **Redis pub/sub** for real-time events: `market:ticks`, `features:latest`, `signals:trades`, `positions:updates`, `models:status`.
- **REST APIs** for request/response: Dashboard queries Monitoring, Trade Manager checks positions.
- **Shared TimescaleDB** for historical data access across services.

## Service Specifications

### Data Pipeline

**Responsibilities:**

- Connect to Alpaca websocket streams for real-time ticks (equities during market hours, crypto 24/7).
- Pull historical bars via Alpaca REST API for backfill and training data.
- Normalize timestamps to UTC, deduplicate, handle gaps.
- Store raw bars in TimescaleDB.
- Publish live ticks to Redis `market:ticks`.

**Trading calendar awareness:**

- Knows market hours (9:30 AM - 4:00 PM ET).
- Knows the configured trading window (~11:30 AM - 2:00 PM ET for equities).
- Fetches and caches Fed announcement dates and earnings calendars.
- Publishes `market:calendar` events so other services know when blackout periods are active.

**Data storage (TimescaleDB):**

- `bars_1m` — 1-minute OHLCV bars per symbol.
- `bars_daily` — daily bars for longer-timeframe features.
- `calendar_events` — earnings dates, Fed announcements, market holidays.

**Backfill:** On first startup and on-demand, pulls historical data from Alpaca to populate enough history for the longest feature window (e.g., 300-day SMA needs 300+ daily bars).

### Feature Engine

**Responsibilities:**

- Subscribe to `market:ticks` from Redis.
- Compute multi-timeframe technical indicators.
- Write feature rows to TimescaleDB `features` table.
- Publish latest features to Redis `features:latest`.
- Generate triple-barrier labels for training datasets.

**Feature set (consistent across all models):**

| Category | Features | Timeframes |
|---|---|---|
| Trend | SMA, EMA | 10m, 30m, 50m, 300m, 1h, 1d |
| Momentum | RSI, MACD, MACD histogram | 14-bar, 26-bar |
| Volatility | ATR, Bollinger Band width, realized vol | 14-bar, 20-bar |
| Price structure | Distance from recent high/low, support/resistance levels (programmatic local extrema detection) | Multiple lookback windows |
| Volume | Volume SMA ratio, VWAP distance | 20-bar |
| Cross-instrument | SPY trend direction, SPY momentum (regime context) | 1h, 1d |

**Support/resistance detection:** Programmatic identification of repeated local highs/lows — statistically significant price levels where price has reversed multiple times.

**Labeler (triple-barrier):**

- For each bar, determine: "did price hit +X% before hitting -Y%?"
- Generate labels for multiple X/Y combinations.
- Separate labels for long and short directions.
- **When tick data is available:** Follow actual tick sequence to determine target/stop hit order (ground truth).
- **When only minute bars are available:** Apply pessimistic bar traversal:
  - Long entry: assume O→L→H→C (stop tested before target).
  - Short entry: assume O→H→L→C (stop tested before target).
  - If both target and stop would be hit within the same bar under pessimistic ordering, the stop is assumed to hit first.
- Output: labeled Parquet files for Model Training service.

### Model Training (runs on node-2)

**Sub-model matrix:**

```
Direction: LONG
  Model L1: target +0.1%, stop -0.1%
  Model L2: target +0.2%, stop -0.2%
  Model L3: target +0.25%, stop -0.3%
  Model L4: target +0.3%, stop -0.2%
  Model L5: target +0.5%, stop -0.3%
  Model L6: target +1.0%, stop -0.5%
  Model L7: target +1.25%, stop -0.75%

Direction: SHORT
  Model S1: target +0.1%, stop -0.1%
  Model S2: target +0.2%, stop -0.2%
  Model S3: target +0.25%, stop -0.3%
  Model S4: target +0.3%, stop -0.2%
  Model S5: target +0.5%, stop -0.3%
  Model S6: target +1.0%, stop -0.5%
  Model S7: target +1.25%, stop -0.75%
```

Target/stop values are configurable. The above are initial defaults.

**Model type (initial):** LightGBM or XGBoost gradient boosted trees. Fast to train, handles tabular features well, interpretable via feature importance.

**Training pipeline:**

1. Feature Engine exports labeled Parquet files to shared volume.
2. Trainer loads dataset, splits train/validation/test by time (chronological, no random split).
3. Train each sub-model with hyperparameter search (Optuna).
4. Evaluate on held-out test period.
5. Record metrics: accuracy, precision, recall, profit factor on test set.
6. Save model artifact + metadata (training data hash, feature version, test metrics) to shared volume.
7. Model is not auto-deployed — Signal Generator picks up new models only when explicitly promoted via Dashboard.

**Training schedule:**

- Nightly retrain after market close (equities).
- Weekly retrain for crypto (configurable).
- On-demand retraining triggered via Dashboard.

**Backtester:**

Given trained sub-models and historical features, simulates the full signal → trade pipeline to evaluate and optimize:

- Long/short signal emission thresholds.
- Regime weight values.
- Sub-model weight distribution.
- Minimum sub-model agreement count.

Uses walk-forward optimization (train on period A, optimize thresholds on period B, test on period C, slide window forward). Final parameters are those that performed consistently across multiple windows.

Output includes sensitivity analysis — how much does performance degrade if a threshold shifts by ±0.05? Fragile thresholds are flagged.

Backtester applies the same trading rules as live (blackout windows, no shorts overnight, core hours only for equities).

### Signal Generator

**Responsibilities:**

- Load promoted model artifacts from shared volume.
- Subscribe to `features:latest` from Redis.
- Check `models:status` from Redis for active/disabled sub-models.
- Run inference across all active sub-models.
- Aggregate outputs with regime weighting.
- Publish signals to Redis `signals:trades`.

**Signal schema:**

```json
{
  "signal_id": "uuid",
  "timestamp": "...",
  "symbol": "MSFT",
  "direction": "LONG",
  "confidence": 0.76,
  "regime": "BULLISH",
  "sub_models_agreeing": ["L1", "L2"],
  "suggested_target_pct": 0.5,
  "suggested_stop_pct": 0.3,
  "model_version": "2026-04-10-nightly"
}
```

**What Signal Generator does NOT do:** Check buying power, place orders, or decide position size.

### Trade Manager

**Signal → Order flow:**

1. Signal received.
2. Is this within the trading window? (reject if not)
3. Is this symbol in a blackout? (earnings, Fed day)
4. Do we already have a position in this symbol? (reject duplicate)
5. Do we have sufficient buying power? (reject if not)
6. Compute position size.
7. Submit entry order to Alpaca.
8. On fill: submit OCO bracket (take-profit limit + stop-loss stop).
9. Record everything to TimescaleDB with full signal provenance.

**Position sizing:**

- Base size from account equity × configurable risk-per-trade percentage (e.g., 1%).
- Adjusted by signal confidence (higher → larger, up to cap).
- Adjusted by regime alignment (trend-aligned = full size, counter-trend = reduced).
- Hard cap: no single position exceeds configurable % of account equity (e.g., 5%).
- Hard cap: maximum concurrent positions (e.g., 5).

**Trading rules enforced:**

| Rule | Action |
|---|---|
| Outside equity trading window | Reject equity signals |
| Symbol has earnings within N days | Reject signal |
| Fed announcement day | Reject all equity signals |
| Open short equity near market close | Close by 3:30 PM ET |
| Insufficient buying power | Reject, log reason |
| Max concurrent positions reached | Reject, log reason |

**Alpaca integration:**

- Uses `alpaca-py` SDK.
- Paper trading mode by default; configurable switch to live.
- Alpaca websocket for real-time order fill confirmation (not polling).
- Idempotency via unique client_order_id per signal.

**Trade outcome record:**

```json
{
  "trade_id": "uuid",
  "signal_id": "uuid",
  "symbol": "MSFT",
  "direction": "LONG",
  "sub_models_agreeing": ["L1", "L2"],
  "model_version": "2026-04-10-nightly",
  "signal_confidence": 0.76,
  "suggested_target_pct": 0.5,
  "suggested_stop_pct": 0.3,
  "entry_price": 420.15,
  "exit_price": 422.25,
  "exit_type": "TARGET_HIT",
  "pnl": 2.10,
  "hold_duration_minutes": 47,
  "outcome": "WIN"
}
```

**Paper vs live:** Single config flag. All other logic identical. Switching to live requires explicit config change, not toggleable from Dashboard.

### Monitoring

**Metrics maintained:**

| Category | Metrics |
|---|---|
| Account | Total P&L, equity, buying power, max drawdown, current drawdown |
| Trading | Win rate, profit factor, Sharpe ratio, avg win/loss, avg hold duration, trades/day |
| Per-symbol | P&L, win rate, trade count, best/worst trade |
| Per-sub-model | Rolling win rate, profit factor, confidence calibration, trade count, active/disabled |
| Per-direction | Long vs short P&L, long vs short win rate |
| System health | Last tick per symbol, feature freshness, signal latency, Alpaca API health, service heartbeats |

All metrics computed over configurable rolling windows (last 20 trades, last 50, today, this week, all-time).

**Per-sub-model fidelity tracking:**

| Metric | Alert threshold |
|---|---|
| Rolling win rate | Drops below backtested rate by configurable margin |
| Rolling profit factor | Falls below 1.0 |
| Confidence calibration | High-confidence signals winning at same rate as low-confidence |
| Expected vs actual P&L | Divergence exceeds configurable threshold |

**Automatic degradation response:**

- **Tier 1 — Warning:** Win rate drops below backtested rate by warning margin. Alert sent. Sub-model flagged yellow. Trading continues.
- **Tier 2 — Disable sub-model:** Win rate drops below critical margin OR profit factor below 1.0 over N trades. Sub-model excluded from signal aggregation. Alert sent. Remaining models continue.
- **Tier 3 — Halt direction:** Majority of long (or short) sub-models disabled. All signals for that direction halted.
- **Tier 4 — Full halt:** Majority of ALL sub-models disabled OR account drawdown exceeds threshold. Trade Manager stops accepting all signals. Open bracket orders remain (Alpaca manages exits).

Disabling a sub-model does not close existing positions — those have bracket orders managing exits. It only prevents new signals.

Monitoring publishes active/disabled state to Redis `models:status`. Signal Generator reads this before aggregation.

**Telegram bot (polling mode):**

Alerts with action buttons:

| Alert Type | Buttons |
|---|---|
| Sub-model degraded | `[Disable Model]` `[Acknowledge]` |
| Sub-model auto-disabled | `[Acknowledge]` `[Re-enable]` |
| Direction halted | `[Acknowledge]` `[Full Halt]` |
| Drawdown threshold | `[Full Halt]` `[Acknowledge]` |
| System halted | `[Acknowledge]` |

Destructive actions (Full Halt) require a confirmation prompt before executing. Uses `python-telegram-bot` library, bot token in environment config, polling mode (no inbound network exposure).

### Dashboard

**Tech stack:** FastAPI backend + HTMX frontend with Chart.js for graphs. Server-rendered HTML, no build step, no npm.

**Pages:**

1. **Overview** — Account equity chart, current drawdown gauge, today's P&L/win rate/trade count, active positions with live unrealized P&L, system status indicators (green/yellow/red per service).

2. **Trade History** — All trades table (symbol, direction, entry/exit, P&L, hold duration, contributing sub-models). Filterable by symbol, direction, date, outcome. Cumulative P&L chart.

3. **Model Health** — Per-sub-model cards: rolling win rate, profit factor, confidence calibration chart, active/disabled status. Backtested expected vs live actual comparison. Manual enable/disable controls.

4. **Performance Analytics** — Rolling Sharpe ratio, win rate by direction/symbol/time-of-day, P&L distribution histogram, max drawdown waterfall, average win vs average loss.

5. **Controls** — Abort button (Tier 4 full halt), pause/resume per direction, pause/resume per symbol, paper/live mode indicator (prominently displayed, not toggleable), force retrain trigger.

**Live updates:** Redis subscription → Server-Sent Events (SSE) to browser.

**Access:** Local network only. No authentication initially.

## Development Phases

### Phase 1: Foundation & Data (Weeks 1-2)

- Project scaffolding (monorepo, Docker Compose skeleton, shared config).
- Data Pipeline service — Alpaca REST historical backfill + websocket streaming.
- TimescaleDB setup.
- Calendar event tracking.

**Milestone:** `docker compose up` shows live ticks flowing into TimescaleDB.

### Phase 2: Features & Labels (Weeks 3-4)

- Feature Engine service — multi-timeframe indicators.
- Feature storage in TimescaleDB.
- Labeler — triple-barrier with pessimistic bar traversal.
- Parquet export.

**Milestone:** Query feature rows and labels for any symbol/time.

### Phase 3: Model Training & Backtesting (Weeks 5-7)

- Model Training service on node-2 — LightGBM pipeline.
- Sub-model matrix, temporal splitting, Optuna hyperparameter search.
- Backtester with walk-forward threshold optimization.
- Model artifact storage.

**Milestone:** Trained sub-models with backtested metrics. Reality check — iterate if results are poor before proceeding.

### Phase 4: Signal Generation & Paper Trading (Weeks 8-10)

- Signal Generator service.
- Trade Manager service — Alpaca paper API, bracket orders, position tracking.
- Full signal provenance chain.

**Milestone:** System paper trading autonomously during market hours.

### Phase 5: Monitoring, Dashboard & Fidelity (Weeks 11-13)

- Monitoring service — metrics, fidelity tracking, auto-disable.
- Telegram bot with action buttons (polling mode).
- Dashboard — HTMX, all five pages, SSE live updates.
- Tiered halt system.

**Milestone:** Monitor from phone via Telegram, see live P&L in Dashboard, abort from either.

### Phase 6: Hardening & Live Preparation (Weeks 14-16)

- 2-4 weeks continuous paper trading.
- Compare paper vs backtested expectations.
- Add crypto (BTC, ETH).
- Nightly retrain automation.
- Document paper → live config change process.

**Milestone:** Paper results consistent with backtested expectations over sustained period.

### Phase 7: Live Trading (Week 16+)

- Switch to Alpaca live API with minimum capital.
- Reduced position sizing initially.
- Close monitoring for 1-2 weeks.
- Gradually increase sizing.
- Begin planning cloud deployment.

## Deferred

- Kubernetes (until cloud deployment).
- Rust components (until profiling shows a bottleneck).
- Range bars (after time-bar pipeline works).
- Options/wheel strategy (after equities are profitable).
- News ingestion/sentiment (after core price models work).
- Advanced regime models (start with simple SPY SMA rule).
- Raspberry Pi integration (monitoring display or backup alerting).

## Hardware

| Node | Name | Role |
|---|---|---|
| DGX Spark 1 | node-1 | Real-time pipeline: data, features, signals, trading, monitoring, dashboard |
| DGX Spark 2 | node-2 | GPU training workloads, backtesting |
| Raspberry Pi 4 (x2) | TBD | Deferred — potential monitoring display or backup alerting |

Nodes connected via NVIDIA ConnectX-7 NICs and local home ethernet.
