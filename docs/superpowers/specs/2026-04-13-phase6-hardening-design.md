# Phase 6: Hardening & Live Preparation — Design Spec

**Goal:** Transform BullForge from a single-train-at-startup system into a production-hardened paper trading platform with nightly model retraining (parallel CPU + GPU), an expanded model zoo (LSTM), runtime-configurable symbols and hyperparameters via dashboard, improved observability, and expanded Telegram bot control.

**Decomposition:** Three independent sub-projects, ordered by priority:

| Sub-project | Scope | Dependency |
|---|---|---|
| **6A: Retrain + Model Zoo** | Nightly scheduler, parallel LightGBM on CPU, LSTM on GPU, runtime config system | None — blocker for meaningful paper trading |
| **6B: Symbol Management + Dashboard** | Symbols in Redis, dashboard edits with auto-backfill, freshness/fidelity displays, config editor, model labels | Depends on 6A's RuntimeConfig system |
| **6C: Telegram Bot Expansion** | Expanded /status, /halt, /resume, /positions, /trades, /models | Depends on 6A's RuntimeConfig for freshness data |

---

## Sub-project 6A: Nightly Retrain & GPU Model Zoo

### Overview

The model_service currently trains all models once at startup and never retrains. This means models go stale as market conditions change. Phase 6A adds:

1. A **retrain scheduler** that triggers nightly at a configurable hour
2. **Parallel CPU training** using 15 ProcessPoolExecutor workers to train LightGBM models across all symbol/timeframe/horizon combos simultaneously
3. A **PyTorch LSTM model** that trains on GPU, registered in the same model_registry, served by the same InferenceEngine
4. A **RuntimeConfig system** that stores configuration in Redis, editable via the dashboard (built in 6B)

### Runtime Config System

**New file: `shared/runtime_config.py`**

A Redis-backed configuration store that replaces hardcoded Settings values for parameters that should be tunable at runtime.

**RuntimeConfig class:**
- Wraps `RedisClient`, stores all values in a Redis hash `bullforge:config`
- `get(key, default)` / `set(key, value)` — read/write individual keys
- `get_all()` — returns full config dict with current values
- `get_schema()` — returns schema with descriptions, types, defaults, and reference URLs
- On first boot, seeds Redis from defaults if keys don't exist

**Config schema** — a Python dict defining all tunable parameters with metadata:

```python
CONFIG_SCHEMA = {
    "retrain_schedule_hour": {
        "default": 1,
        "type": "int",
        "min": 0,
        "max": 23,
        "label": "Retrain Hour (UTC)",
        "description": "Hour of day (0-23 UTC) when nightly model retraining begins. Default 1 AM UTC = 9 PM EDT.",
        "reference": "https://en.wikipedia.org/wiki/Training,_validation,_and_test_data_sets",
    },
    "retrain_cpu_workers": {
        "default": 15,
        "type": "int",
        "min": 1,
        "max": 20,
        "label": "CPU Workers for Training",
        "description": "Number of parallel processes for gradient boosting model training. Each worker trains one model at a time. Higher values train more models simultaneously but increase memory usage. Set to number of available CPU cores minus headroom for OS and streaming.",
        "reference": "https://en.wikipedia.org/wiki/Parallel_computing",
    },
    "lstm_epochs": {
        "default": 50,
        "type": "int",
        "min": 1,
        "max": 500,
        "label": "LSTM Training Epochs",
        "description": "Number of complete passes through the training dataset for LSTM neural network models. More epochs can improve accuracy but risk overfitting. Training uses early stopping so actual epochs may be fewer.",
        "reference": "https://en.wikipedia.org/wiki/Epoch_(machine_learning)",
    },
    "lstm_hidden_size": {
        "default": 64,
        "type": "int",
        "min": 16,
        "max": 512,
        "label": "LSTM Hidden Layer Size",
        "description": "Number of hidden units in each LSTM layer. Larger values increase model capacity but require more memory and training time. Controls the dimensionality of the learned temporal representation.",
        "reference": "https://en.wikipedia.org/wiki/Long_short-term_memory",
    },
    "lstm_sequence_length": {
        "default": 30,
        "type": "int",
        "min": 5,
        "max": 200,
        "label": "LSTM Lookback Window",
        "description": "Number of consecutive time steps fed into the LSTM as input context. The model sees this many historical feature snapshots when making a prediction. Longer sequences capture more history but increase training time and memory.",
        "reference": "https://en.wikipedia.org/wiki/Sliding_window_protocol",
    },
    "lstm_learning_rate": {
        "default": 0.001,
        "type": "float",
        "min": 0.00001,
        "max": 0.1,
        "label": "LSTM Learning Rate",
        "description": "Step size for the Adam optimizer during LSTM training. Smaller values train more slowly but converge more reliably. Larger values train faster but may overshoot optimal weights.",
        "reference": "https://en.wikipedia.org/wiki/Learning_rate",
    },
    "lstm_batch_size": {
        "default": 64,
        "type": "int",
        "min": 8,
        "max": 512,
        "label": "LSTM Batch Size",
        "description": "Number of training samples processed together in each gradient update. Larger batches use more GPU memory but provide more stable gradient estimates. Smaller batches add regularization noise.",
        "reference": "https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Mini-batch_gradient_descent",
    },
}
```

Services read from RuntimeConfig at each retrain cycle (not cached — always fresh from Redis). This ensures dashboard edits take effect on the next retrain without service restarts.

### Retrain Scheduler

**New file: `services/model_service/scheduler.py`**

An async background task running inside the model_service process:

1. On startup: calculates seconds until next scheduled hour (from `retrain_schedule_hour` in RuntimeConfig)
2. Sleeps until that time using `asyncio.sleep()`
3. On wake: reads fresh config from RuntimeConfig (workers, hyperparams)
4. Builds training matrix: all symbols × timeframes × horizons × model_types
5. **Phase 1 — CPU parallel:** dispatches all `gradient_boost` training jobs to `ProcessPoolExecutor(retrain_cpu_workers)`. Each job is self-contained: loads data from DB, trains, evaluates, saves artifact, registers in DB.
6. **Phase 2 — GPU sequential:** trains `lstm` for each symbol/timeframe/horizon on GPU one at a time (GPU can only efficiently train one model at a time)
7. After all training: calls `inference_engine.load_models()` to hot-swap active models in memory
8. Publishes `model:retrained` event to Redis with timestamp and summary metrics
9. Records last retrain time in RuntimeConfig for freshness display
10. Loops back to step 1

**Startup behavior change:** The existing startup training loop in `main.py` remains for first-boot (when no models exist), but skips if active models already exist (current behavior). The scheduler handles all subsequent retraining.

### LSTM Model

**New file: `services/model_service/lstm_model.py`**

A PyTorch LSTM wrapped in a sklearn-compatible interface so it integrates with the existing Trainer and InferenceEngine without changes to their API.

**LSTMClassifier class:**
- `__init__(input_size, hidden_size, sequence_length, num_classes=3, lr, epochs, batch_size)`
- `fit(X: pd.DataFrame, y: pd.Series)` — reshapes X into sequences of `sequence_length` rows, converts to CUDA tensors, trains with Adam optimizer + CrossEntropyLoss + early stopping (patience=5 on validation loss)
- `predict(X: pd.DataFrame)` — returns direction predictions as numpy array
- `predict_proba(X: pd.DataFrame)` — returns softmax probabilities as numpy array
- Uses GPU when available (`torch.device("cuda" if torch.cuda.is_available() else "cpu")`)

**The PyTorch nn.Module inside:**
```
Input (sequence_length × num_features)
  → LSTM(input_size=num_features, hidden_size=hidden_size, batch_first=True)
  → Take last hidden state
  → Linear(hidden_size → num_classes)
  → Softmax for predict_proba
```

**Sequence construction:** The LSTM needs sequential input. `fit()` converts a flat DataFrame of N rows into (N - sequence_length) samples, each containing `sequence_length` consecutive feature rows. This means the LSTM naturally captures temporal patterns the LightGBM models miss.

**Early stopping:** Splits training data 80/20 (time-based, same as existing `time_based_split`). Monitors validation loss and stops if no improvement for 5 epochs. Restores best weights.

### Model Factory Expansion

**Modified: `services/model_service/model_factory.py`**

Add `"lstm"` to `SUPPORTED_MODELS` and `create_model()`:

```python
SUPPORTED_MODELS = {"gradient_boost", "logistic_regression", "lstm"}

def create_model(model_type: str, runtime_config: dict = None) -> object:
    if model_type == "lstm":
        cfg = runtime_config or {}
        return LSTMClassifier(
            hidden_size=cfg.get("lstm_hidden_size", 64),
            sequence_length=cfg.get("lstm_sequence_length", 30),
            lr=cfg.get("lstm_learning_rate", 0.001),
            epochs=cfg.get("lstm_epochs", 50),
            batch_size=cfg.get("lstm_batch_size", 64),
        )
    # ... existing gradient_boost and logistic_regression unchanged
```

The `runtime_config` dict is passed from the scheduler with fresh values from Redis. The existing startup training path passes `None` and gets defaults.

### ModelStore Changes

**Modified: `services/model_service/model_store.py`**

- `save()`: detects model type — uses `joblib.dump()` for sklearn pipelines, `torch.save()` for LSTMClassifier
- `load()`: detects from file extension — `.joblib` → joblib, `.pt` → torch.load
- Artifact naming: `lstm_SPY_1m_5m_20260413T010000Z.pt`

### Docker/GPU Changes

**Modified: model_service Dockerfile:**
- Base image: `nvidia/cuda:12.4.1-runtime-ubuntu22.04` with Python 3.12 installed
- Adds `torch` with CUDA 12.4 support to requirements
- Adds `--gpus all` equivalent in Docker Compose

**Modified: `docker-compose.yml`:**
```yaml
model_service:
  # ... existing config
  deploy:
    resources:
      reservations:
        devices:
          - driver: nvidia
            count: 1
            capabilities: [gpu]
```

### Training Sequence (Nightly)

1. Scheduler wakes at `retrain_schedule_hour` UTC
2. Reads all config from RuntimeConfig (workers, hyperparams, symbols)
3. Reads symbol list from RuntimeConfig (equity + crypto)
4. Builds job matrix: symbols × ["1m", "1d"] × [5, 10, 30] × ["gradient_boost", "lstm"]
5. **CPU phase:** All `gradient_boost` jobs dispatched to ProcessPoolExecutor(15)
6. **GPU phase:** All `lstm` jobs run sequentially on GPU
7. Each job: query training data → train → evaluate → save artifact → deactivate old model → register new model in DB
8. `inference_engine.load_models()` hot-swaps all active models
9. Publishes `model:retrained` to Redis with summary
10. Stores `last_retrain_time` in RuntimeConfig
11. Sleeps until next scheduled hour

---

## Sub-project 6B: Symbol Management + Dashboard Improvements

### Overview

Move trading symbols from environment variables to Redis (via RuntimeConfig), make them editable via the dashboard, and add freshness/fidelity/model-label displays.

### Symbol Storage & Propagation

**RuntimeConfig additions:**
- `trading_symbols_equity` and `trading_symbols_crypto` stored as JSON arrays in `bullforge:config`
- On first boot: seeds from ENV defaults (`Settings.trading_symbols_equity`, `Settings.trading_symbols_crypto`) if keys don't exist in Redis
- All services read from RuntimeConfig instead of Settings for symbol lists

**Hybrid propagation when a symbol is added:**
1. Dashboard writes updated symbol list to RuntimeConfig
2. Dashboard publishes `config:symbols_changed` event to Redis with the added/removed symbol
3. **Data pipeline** subscribes to `config:symbols_changed`:
   - On add: starts backfilling all available historical data for the new symbol, then subscribes to live stream
   - On remove: unsubscribes from live stream (historical data retained, not deleted)
4. **Feature engine** picks up new bars naturally via existing `market:ticks` subscription
5. **Model service** trains on new symbol at next nightly retrain cycle
6. **Execution service** picks up new symbol at next signal aggregation cycle

**When a symbol is removed:**
- Live streaming stops
- Historical data (bars, features, labels, predictions) is retained
- Active models for that symbol are deactivated in model_registry
- No new trades opened; existing open positions are NOT auto-closed (user manages via Alpaca)

### Dashboard Model Labels

**Current problem:** Model tables show raw `model_id` integers which are meaningless.

**Fix:** Join `model_status` with `model_registry` in the API response. Display format:
```
gradient_boost · SPY · 1m · 5m
```

Composed from: `model_type · symbol · timeframe · horizon_minutes + "m"`

**API change:** `GET /api/models` response includes full model metadata (model_type, symbol, timeframe, horizon_minutes) alongside status. New DB query `query_model_details()` joins `model_status` with `model_registry`.

### New Dashboard Tab: Symbols

A new tab in the dashboard UI showing:

**Equity symbols section:**
- Table: symbol, streaming status, last bar time, feature count, model count, remove button
- Add input: text field + "Add Equity Symbol" button

**Crypto symbols section:**
- Same layout as equity
- Add input: text field + "Add Crypto Pair" button (validates format contains "/")

**Per-symbol status** pulled from new DB queries and RuntimeConfig.

**Backfill progress:** When a symbol is added, the dashboard polls `GET /api/symbols/{symbol}/status`. The endpoint queries the DB for bar count and most recent bar timestamp for that symbol. Status is derived: no bars = "pending", bars exist but latest is old = "backfilling", latest bar is recent = "complete". No explicit progress reporting from data pipeline needed.

### Freshness Indicators

New section on the **Overview tab** showing system health at a glance:

**Price data freshness** (per-symbol):
- Time since last bar inserted
- Query: `SELECT symbol, MAX(timestamp) FROM bars_1m GROUP BY symbol`
- Color: green < 5m, yellow 5-15m, red > 15m (for equities during market hours)
- Crypto: green < 2m, yellow 2-5m, red > 5m (24/7 market)

**Feature freshness** (per-symbol):
- Time since last feature row computed
- Query: `SELECT symbol, MAX(timestamp) FROM features GROUP BY symbol`
- Same color thresholds as price data

**Model training age** (per-model):
- Time since `trained_at` from model_registry
- Color: green < 24h, yellow 24-48h, red > 48h

**Model fidelity** (per-model):
- Latest accuracy and F1 from model metrics
- Color: green if within `fidelity_warning_margin`, yellow if warned, red if disabled

### Config Editor Tab

New dashboard tab rendering the full `CONFIG_SCHEMA` from RuntimeConfig:

- Grouped by category:
  - **Retrain Schedule** — retrain_schedule_hour, retrain_cpu_workers
  - **LSTM Hyperparameters** — lstm_epochs, lstm_hidden_size, lstm_sequence_length, lstm_learning_rate, lstm_batch_size
- Each parameter displays:
  - Label and current value
  - Editable input control (number input with min/max from schema)
  - Description text
  - "Learn more" link to the reference URL
- Save button per parameter (or per group) writes to Redis immediately
- Visual confirmation on save

### New DB Query Methods

- `query_latest_bar_timestamps()` — `SELECT symbol, MAX(timestamp) as last_bar FROM bars_1m GROUP BY symbol`
- `query_latest_feature_timestamps()` — `SELECT symbol, MAX(timestamp) as last_feature FROM features GROUP BY symbol`
- `query_model_details()` — joins `model_registry` with `model_status` to return model metadata + status together

### New API Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/symbols` | Current symbols from RuntimeConfig |
| `POST` | `/api/symbols` | Add symbol (triggers backfill via Redis event) |
| `DELETE` | `/api/symbols/{symbol}` | Remove symbol (stops streaming, deactivates models) |
| `GET` | `/api/symbols/{symbol}/status` | Backfill progress for a symbol |
| `GET` | `/api/freshness` | Price/feature/model freshness data |
| `GET` | `/api/config` | All runtime config with schema metadata |
| `PUT` | `/api/config/{key}` | Update single config value |

---

## Sub-project 6C: Telegram Bot Expansion

### Overview

Expand the Telegram bot from 2 commands (`/start`, `/status`) to 6 commands with richer output and trading control.

### Broker Data Access Pattern

The Telegram bot runs inside monitoring_service, which does NOT have Alpaca API access. To fetch account and position data, a Redis request/reply pattern is used:

**Generic broker request channel:**
- Bot publishes to `request:broker` with payload: `{"method": "get_positions", "correlation_id": "uuid"}`
- Execution service subscribes to `request:broker`, calls the appropriate AlpacaBroker method, publishes response to `response:broker:{correlation_id}`
- Bot awaits response on `response:broker:{correlation_id}` with 5-second timeout
- If timeout: bot responds with "Execution service unavailable" message

**Supported methods:**
- `get_positions` — returns list of open positions
- `get_account` — returns equity, buying_power, cash, portfolio_value

**New file: `services/execution_service/broker_relay.py`**
- Subscribes to `request:broker`
- Dispatches to AlpacaBroker methods
- Publishes responses with correlation ID

### Commands

**`/status` (expanded)**

Current format is a sparse dump. New format:

```
BullForge Status

Account: $100,234.50 equity | +$45.20 (+0.05%) today
Positions: 3 open | $28,500 exposure (28.4%)
Trading: 12 trades today | 67% win rate | +$312.40 P&L

Models: 8 active | 1 warned | 0 disabled
Freshness: All data < 2m old | All models < 24h
Halts: None active

Last retrain: 2026-04-13 01:00 UTC (14h ago)
```

Data sources:
- Account info: broker relay (`get_account`)
- Positions summary: broker relay (`get_positions`) — count and total exposure
- Trading stats: `db.query_latest_metric_snapshot("trading", "", "all")`
- Model summary: `db.get_all_model_statuses()` — count by status
- Freshness: `db.query_latest_bar_timestamps()`, `db.query_latest_feature_timestamps()`
- Halts: `db.get_active_halts()`
- Last retrain: RuntimeConfig `last_retrain_time`

**`/halt` — Stop all trading**

1. Bot sends confirmation message with inline buttons: "Confirm Halt" / "Cancel"
2. On confirm: calls `halt_manager.trigger_halt(reason="Manual halt via Telegram")`
3. Halt event inserted to DB, published to Redis
4. Bot responds: "Trading halted. Use /resume to restart."

**`/resume` — Resume trading**

1. Bot sends confirmation message with inline buttons: "Confirm Resume" / "Cancel"
2. On confirm: calls `halt_manager.resolve_halt()`
3. All active halt events resolved in DB
4. Bot responds: "Trading resumed. All halts cleared."

**`/positions` — Show open positions**

```
Open Positions (3)

SPY: 10 shares LONG @ $421.30
  Current: $423.50 | P&L: +$22.00 (+0.52%)

NVDA: 5 shares LONG @ $890.20
  Current: $885.10 | P&L: -$25.50 (-0.57%)

BTC/USD: 0.15 LONG @ $67,200.00
  Current: $67,450.00 | P&L: +$37.50 (+0.37%)
```

Data source: broker relay (`get_positions`). If no positions: "No open positions."

**`/trades` — Recent trade history**

```
Recent Trades (last 5)

SPY BUY 10 @ $420.50 -> SELL @ $423.10
  P&L: +$26.00 | Hold: 45m | 2h ago

QQQ BUY 8 @ $380.20 -> SELL @ $379.80
  P&L: -$3.20 | Hold: 22m | 3h ago
```

Data source: `db.query_closed_trades(limit=5)`. If no trades: "No recent trades."

**`/models` — Model health summary**

```
Models (9 total)

Active (8):
  gradient_boost SPY 1m 5m - acc: 0.62 | 14h old
  lstm SPY 1m 5m - acc: 0.58 | 14h old
  gradient_boost SPY 1m 10m - acc: 0.59 | 14h old
  ...

Warned (1):
  gradient_boost QQQ 1m 30m - acc: 0.48 | 38h old
```

Data source: `db.query_model_details()` (same join query from 6B). Grouped by status.

### Implementation Changes

**Modified: `services/monitoring_service/telegram_bot.py`**
- Register new command handlers: `/halt`, `/resume`, `/positions`, `/trades`, `/models`
- Add broker relay client methods for request/reply pattern
- Timeout handling for broker relay (5s)

**Modified: `services/monitoring_service/telegram_alerts.py`**
- `format_status_expanded()` — builds the rich status message
- `format_positions()` — formats position list
- `format_trades()` — formats recent trades
- `format_models_summary()` — formats model health grouped by status
- `format_halt_confirmation()` / `format_resume_confirmation()` — confirmation dialogs

**New: `services/execution_service/broker_relay.py`**
- Subscribes to `request:broker` channel
- Dispatches to AlpacaBroker methods based on `method` field
- Publishes JSON response to `response:broker:{correlation_id}`
- Started as a background task in execution_service's `main.py`

**Modified: `services/execution_service/main.py`**
- Starts broker_relay as a background task alongside signal processing

---

## Cross-Cutting Concerns

### Service Startup Order

No changes to Docker Compose dependency chain. RuntimeConfig reads gracefully fall back to defaults if Redis keys don't exist yet.

### Migration Path

- First boot after Phase 6: RuntimeConfig seeds all values from ENV defaults
- Existing `.env` files continue to work for initial seeding
- Once values are in Redis, ENV is only used as fallback for first-time setup

### Testing Strategy

- RuntimeConfig: unit tests with a mock Redis
- LSTM model: unit tests with small synthetic datasets on CPU (CI doesn't need GPU)
- Scheduler: unit tests with mocked time and training
- Broker relay: unit tests with mock Redis pub/sub
- Dashboard API endpoints: integration tests against test DB
- Telegram commands: unit tests with mock bot/DB/relay
