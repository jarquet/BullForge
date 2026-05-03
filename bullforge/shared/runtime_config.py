from __future__ import annotations

import json
import logging
from typing import Any

from shared.redis import RedisClient

logger = logging.getLogger(__name__)

CONFIG_HASH_KEY = "bullforge:config"

CONFIG_SCHEMA: dict[str, dict[str, Any]] = {
    "retrain_schedule_hour": {
        "default": 1, "type": "int", "min": 0, "max": 23,
        "label": "Retrain Hour (UTC)",
        "description": "Hour of day (0-23 UTC) when nightly model retraining begins. Default 1 AM UTC = 9 PM EDT.",
        "reference": "https://en.wikipedia.org/wiki/Training,_validation,_and_test_data_sets",
    },
    "retrain_cpu_workers": {
        "default": 15, "type": "int", "min": 1, "max": 20,
        "label": "CPU Workers for Training",
        "description": "Number of parallel processes for gradient boosting model training. Each worker trains one model at a time. Higher values train more models simultaneously but increase memory usage.",
        "reference": "https://en.wikipedia.org/wiki/Parallel_computing",
    },
    "lstm_epochs": {
        "default": 50, "type": "int", "min": 1, "max": 500,
        "label": "LSTM Training Epochs",
        "description": "Number of complete passes through the training dataset for LSTM neural network models. More epochs can improve accuracy but risk overfitting.",
        "reference": "https://en.wikipedia.org/wiki/Epoch_(machine_learning)",
    },
    "lstm_hidden_size": {
        "default": 64, "type": "int", "min": 16, "max": 512,
        "label": "LSTM Hidden Layer Size",
        "description": "Number of hidden units in each LSTM layer. Larger values increase model capacity but require more memory.",
        "reference": "https://en.wikipedia.org/wiki/Long_short-term_memory",
    },
    "lstm_sequence_length": {
        "default": 30, "type": "int", "min": 5, "max": 200,
        "label": "LSTM Lookback Window",
        "description": "Number of consecutive time steps fed into the LSTM as input context.",
        "reference": "https://en.wikipedia.org/wiki/Sliding_window_protocol",
    },
    "lstm_learning_rate": {
        "default": 0.001, "type": "float", "min": 0.00001, "max": 0.1,
        "label": "LSTM Learning Rate",
        "description": "Step size for the Adam optimizer during LSTM training. Smaller values train more slowly but converge more reliably.",
        "reference": "https://en.wikipedia.org/wiki/Learning_rate",
    },
    "lstm_batch_size": {
        "default": 64, "type": "int", "min": 8, "max": 512,
        "label": "LSTM Batch Size",
        "description": "Number of training samples processed together in each gradient update.",
        "reference": "https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Mini-batch_gradient_descent",
    },
    "trading_symbols_equity": {
        "default": ["SPY", "QQQ", "AAPL", "MSFT", "NVDA"],
        "type": "json",
        "label": "Equity Symbols",
        "description": "Equity/ETF tickers to trade. Adding a symbol triggers historical backfill and live stream subscription. Removing stops streaming (historical data retained).",
        "reference": "https://en.wikipedia.org/wiki/Ticker_symbol",
    },
    "trading_symbols_crypto": {
        "default": ["BTC/USD", "ETH/USD"],
        "type": "json",
        "label": "Crypto Pairs",
        "description": "Crypto trading pairs (base/quote format). Trades 24/7.",
        "reference": "https://en.wikipedia.org/wiki/Cryptocurrency",
    },
    "equity_window_open_buffer_minutes": {
        "default": 60, "type": "int", "min": 0, "max": 240,
        "label": "Equity Open Buffer (minutes)",
        "description": "Minutes after NYSE open (9:30 ET) before equity trading begins. 60 = trade from 10:30 ET onward.",
        "reference": "https://en.wikipedia.org/wiki/Opening_Bell",
    },
    "equity_window_close_buffer_minutes": {
        "default": 60, "type": "int", "min": 0, "max": 240,
        "label": "Equity Close Buffer (minutes)",
        "description": "Minutes before NYSE close (16:00 ET) to stop equity trading. 60 = stop at 15:00 ET.",
        "reference": "https://en.wikipedia.org/wiki/Closing_Bell",
    },
    "execution_min_confidence": {
        "default": 0.6, "type": "float", "min": 0.0, "max": 1.0,
        "label": "Min Prediction Confidence",
        "description": (
            "Minimum per-prediction confidence (max-class probability from "
            "the model) required for a model's vote to count. Predictions "
            "below this are dropped before agreement is tallied. Raising it "
            "makes each model's vote harder to earn; lowering it lets weaker "
            "signals through."
        ),
        "reference": "https://en.wikipedia.org/wiki/Probabilistic_classification",
    },
    "execution_min_agreement": {
        "default": 2, "type": "int", "min": 1, "max": 10000,
        "label": "Min Agreeing Models",
        "description": (
            "Minimum number of confident predictions — after filtering by "
            "Min Prediction Confidence and dropping disabled models — "
            "required to emit a trading signal. With fewer survivors than "
            "this, no trade is considered. Set close to your active model "
            "count for near-unanimity, or lower to allow a plurality vote."
        ),
        "reference": "https://en.wikipedia.org/wiki/Ensemble_learning",
    },
    "execution_direction_band": {
        "default": 0.3, "type": "float", "min": 0.0, "max": 1.0,
        "label": "Direction Neutral Band",
        "description": (
            "Dead-zone around zero for the confidence-weighted average "
            "direction. A weighted average between -band and +band produces "
            "a flat signal (no trade); above +band is buy, below -band is "
            "sell. Widening the band filters out low-conviction crowd votes."
        ),
        "reference": "https://en.wikipedia.org/wiki/Dead_zone_(signal_processing)",
    },
    "execution_min_signal_strength": {
        "default": 0.5, "type": "float", "min": 0.0, "max": 1.0,
        "label": "Min Signal Strength",
        "description": (
            "Minimum |weighted_avg_direction| (0-1) required by the Risk "
            "Manager to approve an entry after the Direction Neutral Band "
            "check passes. Example: two models agreeing at confidence 0.6 "
            "produce strength=0.6 — just clearing the default 0.5."
        ),
        "reference": "https://en.wikipedia.org/wiki/Signal-to-noise_ratio",
    },
    "execution_max_position_pct": {
        "default": 0.02, "type": "float", "min": 0.0, "max": 1.0,
        "label": "Max Position Size (fraction of equity)",
        "description": (
            "Fraction of account equity allocated to a single new position. "
            "0.02 = 2% of equity per entry. Quantity is "
            "(equity * pct / current_price) as a fractional share (crypto "
            "and fractional-equity orders); entries sized below Alpaca's "
            "$1 minimum notional are rejected."
        ),
        "reference": "https://en.wikipedia.org/wiki/Position_sizing",
    },
    "execution_max_positions": {
        "default": 5, "type": "int", "min": 1, "max": 100,
        "label": "Max Concurrent Positions",
        "description": (
            "Maximum number of simultaneous open positions across all "
            "symbols. New entries beyond this count are rejected until an "
            "existing position exits."
        ),
        "reference": "https://en.wikipedia.org/wiki/Diversification_(finance)",
    },
    "execution_max_exposure": {
        "default": 0.2, "type": "float", "min": 0.0, "max": 1.0,
        "label": "Max Portfolio Exposure (fraction of equity)",
        "description": (
            "Upper bound on total portfolio exposure as a fraction of "
            "equity (sum of |market_value| across open positions divided by "
            "equity). New entries that would push exposure to or past this "
            "value are rejected. 0.2 = up to 20% of equity deployed at any "
            "time."
        ),
        "reference": "https://en.wikipedia.org/wiki/Market_exposure",
    },
    "label_horizons_1m": {
        "default": [1, 2, 3, 5, 8, 10, 20, 40, 60, 80, 100, 120],
        "type": "json",
        "label": "1-min Forecast Horizons (bars)",
        "description": "Forward-return horizons (in 1-minute bars) for label generation and model training on 1m data. Backtests using timeframe=1m can only select horizons from this list.",
        "reference": "https://en.wikipedia.org/wiki/Forecasting",
    },
    "label_horizons_1d": {
        "default": [1, 2, 3, 5, 12, 15],
        "type": "json",
        "label": "1-day Forecast Horizons (bars)",
        "description": "Forward-return horizons (in daily bars) for label generation and model training on 1d data. Backtests using timeframe=1d can only select horizons from this list.",
        "reference": "https://en.wikipedia.org/wiki/Forecasting",
    },
    "last_retrain_time": {
        "default": "", "type": "str",
        "label": "Last Retrain Time (UTC ISO)",
        "description": "ISO-8601 UTC timestamp of the most recent completed retrain cycle. Read-only from user perspective; written by the scheduler.",
        "reference": "https://en.wikipedia.org/wiki/ISO_8601",
    },
    "meta_min_confidence": {
        "default": 0.55, "type": "float", "min": 0.0, "max": 1.0,
        "label": "Meta Entry Min Confidence",
        "description": "Minimum calibrated probability from the meta-entry model required to emit a trade signal. Higher values = fewer but more confident trades.",
        "reference": "https://en.wikipedia.org/wiki/Probabilistic_classification",
    },
    "meta_kelly_cap_multiple": {
        "default": 0.25, "type": "float", "min": 0.0, "max": 1.0,
        "label": "Kelly Cap Multiple",
        "description": "Fraction of full-Kelly sizing to use. 0.25 = quarter-Kelly (conservative). Full Kelly (1.0) is mathematically optimal but extremely aggressive on estimated probabilities.",
        "reference": "https://en.wikipedia.org/wiki/Kelly_criterion",
    },
    "meta_hysteresis_bars": {
        "default": 5, "type": "int", "min": 0, "max": 100,
        "label": "Meta Hysteresis Window (bars)",
        "description": "Suppress direction flips on the same symbol within N bars. Prevents flip-flopping when base predictions are noisy.",
        "reference": "https://en.wikipedia.org/wiki/Hysteresis",
    },
    "meta_shadow_red_threshold": {
        "default": 0.01, "type": "float", "min": 0.0, "max": 1.0,
        "label": "Meta Shadow Health Red Threshold",
        "description": (
            "Combined skip+drop rate over the trailing 15-minute window above "
            "which the dashboard's Meta shadow health tile flips red. "
            "Comparison is strict-> so a steady-state rate exactly at the "
            "threshold does NOT trigger — the tile is for regression "
            "detection, not enforcement of a hard SLO."
        ),
        "reference": "https://en.wikipedia.org/wiki/Anomaly_detection",
    },
    "meta_cold_start_min_rows": {
        "default": 2000, "type": "int", "min": 100, "max": 100000,
        "label": "Meta Cold-Start Min Rows",
        "description": "Minimum labeled training rows (replay + live) before a meta-model is eligible for promotion. LightGBM default: 2000; MLP may need higher (5000+).",
        "reference": "https://en.wikipedia.org/wiki/Cold_start_(recommender_systems)",
    },
    "meta_cold_start_min_backtest_ev": {
        "default": 0.001, "type": "float", "min": 0.0, "max": 1.0,
        "label": "Meta Cold-Start Min Backtest EV",
        "description": "Minimum expected value per signal on backtest required for promotion eligibility. Configurable per symbol via dashboard overrides.",
        "reference": "https://en.wikipedia.org/wiki/Expected_value",
    },
    "meta_live_weight_multiplier": {
        "default": 3.0, "type": "float", "min": 1.0, "max": 20.0,
        "label": "Meta Live Row Weight Multiplier",
        "description": "Training weight applied to live inference rows relative to backtest-replay rows. Higher values make the meta-model adapt faster to live conditions.",
        "reference": "https://en.wikipedia.org/wiki/Weighted_least_squares",
    },
    "meta_rolling_accuracy_window_days": {
        "default": 30, "type": "int", "min": 7, "max": 365,
        "label": "Meta Rolling Accuracy Window (days)",
        "description": "Rolling window in calendar days for computing per-sub-model live accuracy and calibration error features.",
        "reference": "https://en.wikipedia.org/wiki/Moving_average",
    },
    "meta_inference_enabled": {
        "default": False, "type": "bool",
        "label": "Meta-Model Shadow Inference",
        "description": (
            "Master switch for meta-model shadow inference. When off, "
            "predict_meta returns empty and model:meta_signals is not "
            "published. Flip on after the first successful nightly retrain."
        ),
        "reference": "https://en.wikipedia.org/wiki/Stacked_generalization",
    },
    "meta_estimator_types": {
        "default": ["lightgbm"], "type": "json",
        "label": "Meta Estimator Types",
        "description": (
            "Subset of {lightgbm, mlp, baseline} to train and serve. "
            "Each entry produces one active artifact per (symbol, direction)."
        ),
        "reference": "https://en.wikipedia.org/wiki/Ensemble_learning",
    },
    "meta_pair_grid": {
        "default": [[2, 1], [3, 1.5], [4, 2]], "type": "json",
        "label": "Meta (Target, Stop) Multipliers x ATR-14",
        "description": (
            "List of [target_mult, stop_mult] pairs. Per candidate bar, "
            "target_pct = target_mult x ATR-14/close; stop_pct similarly. "
            "Adapts pair sizes per-symbol via ATR scaling."
        ),
        "reference": "https://en.wikipedia.org/wiki/Average_true_range",
    },
    "meta_train_window_days": {
        "default": 60, "type": "int", "min": 7, "max": 365,
        "label": "Meta Training Window (days)",
        "description": "Replay window for offline meta-training.",
        "reference": "https://en.wikipedia.org/wiki/Walk_forward_optimization",
    },
    "meta_max_hold_minutes": {
        "default": 30, "type": "int", "min": 1, "max": 1440,
        "label": "Meta Outcome Max-Hold (minutes)",
        "description": (
            "Bar-walking simulation cuts off after this many minutes if "
            "neither target nor stop has been hit."
        ),
        "reference": "https://en.wikipedia.org/wiki/Position_(finance)",
    },
    "meta_min_train_rows": {
        "default": 500, "type": "int", "min": 100, "max": 100000,
        "label": "Meta Min Training Rows",
        "description": "Skip a (symbol, direction) if fewer labeled rows exist.",
        "reference": "https://en.wikipedia.org/wiki/Sample_size_determination",
    },
    "meta_label_backfill_interval_minutes": {
        "default": 15, "type": "int", "min": 1, "max": 1440,
        "label": "Meta Label-Backfill Interval (minutes)",
        "description": (
            "How often the backfill loop fills entry_label / "
            "optimal_pair_idx / realized_ev on live meta_features rows."
        ),
        "reference": "https://en.wikipedia.org/wiki/Label_(computer_science)",
    },
    "meta_regime_max_staleness_hours": {
        "default": 48, "type": "int", "min": 1, "max": 720,
        "label": "Meta Regime SPY Staleness Cap (hours)",
        "description": (
            "Forward-fill SPY regime values for crypto bars up to this "
            "many hours; older than this returns zeros."
        ),
        "reference": "https://en.wikipedia.org/wiki/SPDR_S%26P_500_ETF_Trust",
    },
    "meta_rolling_stats_ttl_seconds": {
        "default": 600, "type": "int", "min": 10, "max": 86400,
        "label": "Meta Rolling-Stats Cache TTL (seconds)",
        "description": (
            "Per-base-model accuracy/calibration cache TTL during meta "
            "inference. Rebuilds when expired."
        ),
        "reference": "https://en.wikipedia.org/wiki/Cache_(computing)",
    },
}


def _parse(value: str, type_: str) -> Any:
    if type_ == "bool":
        if value == "true":
            return True
        if value == "false":
            return False
        raise ValueError(f"invalid bool value: {value!r}")
    if type_ == "int":
        return int(value)
    if type_ == "float":
        return float(value)
    if type_ == "json":
        return json.loads(value)
    return value


def _serialize(value: Any, type_: str) -> str:
    if type_ == "bool":
        return "true" if value else "false"
    if type_ == "json":
        return json.dumps(value)
    return str(value)


class RuntimeConfig:
    def __init__(self, redis_client: RedisClient):
        self._redis = redis_client

    async def get(self, key: str) -> Any:
        if key not in CONFIG_SCHEMA:
            raise KeyError(f"unknown config key: {key}")
        meta = CONFIG_SCHEMA[key]
        raw = await self._redis.client.hget(CONFIG_HASH_KEY, key)
        if raw is None:
            return meta["default"]
        return _parse(raw, meta["type"])

    async def set(self, key: str, value: Any) -> None:
        if key not in CONFIG_SCHEMA:
            raise KeyError(f"unknown config key: {key}")
        meta = CONFIG_SCHEMA[key]
        if "min" in meta and value < meta["min"]:
            raise ValueError(
                f"{key}={value} must be between {meta['min']} and {meta['max']}"
            )
        if "max" in meta and value > meta["max"]:
            raise ValueError(
                f"{key}={value} must be between {meta['min']} and {meta['max']}"
            )
        await self._redis.client.hset(
            CONFIG_HASH_KEY, key, _serialize(value, meta["type"])
        )

    async def get_all(self) -> dict[str, Any]:
        stored = await self._redis.client.hgetall(CONFIG_HASH_KEY)
        result: dict[str, Any] = {}
        for key, meta in CONFIG_SCHEMA.items():
            if key in stored:
                result[key] = _parse(stored[key], meta["type"])
            else:
                result[key] = meta["default"]
        return result

    def get_schema(self) -> dict[str, dict[str, Any]]:
        return CONFIG_SCHEMA
