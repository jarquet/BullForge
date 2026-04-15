# Walk-Forward Backtest Runbook

## Prerequisites
- Test stack running: `docker compose -f docker-compose.test.yml up -d`
- Migrations applied (013 + earlier).
- Historical bars + features + labels loaded for target symbols/timeframe.

## Run a backtest

### In Docker (recommended)

The `backtest` compose service bundles the model + execution stacks (torch,
lightgbm, alpaca-py) on the NVIDIA PyTorch base image. It runs under the
`tools` profile so it isn't started by `docker compose up`.

    cd bullforge
    docker compose build backtest     # first time only
    docker compose run --rm backtest \
        --name "spy-1m-q1-2025" \
        --symbols SPY,QQQ \
        --timeframe 1m \
        --horizons 5,10,30 \
        --start 2025-01-01T00:00:00 \
        --end   2025-04-01T00:00:00 \
        --train-days 60 --test-days 5 --embargo-min 30 \
        --mode rolling \
        --slippage-bps 5 --max-volume-pct 0.10

### Locally (requires full deps installed)

    cd bullforge
    PYTHONPATH=. python scripts/backtest.py \
        --name "spy-1m-q1-2025" \
        --symbols SPY,QQQ \
        --timeframe 1m \
        --horizons 5,10,30 \
        --start 2025-01-01T00:00:00 \
        --end   2025-04-01T00:00:00 \
        --train-days 60 --test-days 5 --embargo-min 30 \
        --mode rolling \
        --slippage-bps 5 --max-volume-pct 0.10

## Inspect results

    psql -h localhost -U postgres -d bullforge

    -- summary across folds
    SELECT fold_index,
           (metrics_json->>'sharpe')::float        AS sharpe,
           (metrics_json->>'max_drawdown')::float  AS max_dd,
           (metrics_json->>'total_return_pct')::float AS ret_pct,
           (metrics_json->>'trade_count')::int     AS trades,
           (metrics_json->>'trade_hit_rate')::float AS hit_rate
      FROM backtest_folds
     WHERE run_id = <RUN_ID>
     ORDER BY fold_index;

    -- equity curve for plotting
    SELECT timestamp, equity FROM backtest_equity
     WHERE run_id = <RUN_ID> ORDER BY timestamp;

## Sandbox isolation

The CLI wires a sandbox `Database` (search_path=`backtest,public`) for the
engine, so per-fold writes to `orders`, `model_registry`, and `predictions`
land in the `backtest` schema instead of `public`. Read-only tables
(`bars_1m`, `features`, `labels`) fall through to `public` via search_path.
Backtest results (`backtest_runs`, `backtest_folds`, `backtest_trades`,
`backtest_equity`) live in `public` and are written via a separate live
`Database` instance.

To clear sandbox state between runs:

    psql -h localhost -U postgres -d bullforge -c "
      TRUNCATE backtest.orders, backtest.model_registry, backtest.predictions;
    "

To clear backtest results:

    psql -h localhost -U postgres -d bullforge -c "
      TRUNCATE backtest_equity, backtest_trades, backtest_folds, backtest_runs
        RESTART IDENTITY CASCADE;
    "

## Important caveats
- Embargo must be >= the longest horizon being backtested to prevent label leakage.
- Default annualization assumes 1m equities (390 * 252 periods/year). Pass
  `--periods-per-year` for crypto (1440 * 365) or daily (252).
- The simulated broker only models flat ↔ full-position trades; partial fills
  and scale-out / scale-in are not supported. Strategies that depend on
  position sizing changes mid-trade will not backtest accurately.
