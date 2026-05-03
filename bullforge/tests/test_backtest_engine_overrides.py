from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from services.backtest_service.engine import BacktestConfig
from services.execution_service.signal_aggregator import SignalAggregator
from services.execution_service.risk_manager import RiskManager


def _rc():
    defaults = {
        "execution_min_confidence": 0.6,
        "execution_min_agreement": 2,
        "execution_direction_band": 0.3,
        "execution_min_signal_strength": 0.5,
        "execution_max_position_pct": 0.10,
        "execution_max_positions": 5,
        "execution_max_exposure": 0.5,
        "equity_window_open_buffer_minutes": 60,
        "equity_window_close_buffer_minutes": 60,
    }
    rc = AsyncMock()

    async def get(key):
        return defaults[key]

    rc.get = get
    return rc


def test_backtest_config_has_all_seven_override_fields():
    cfg = BacktestConfig(
        name="t",
        symbols=["SPY"],
        timeframe="1m",
        horizons=[5],
        range_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        range_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        train_days=1, test_days=1, embargo_minutes=5, mode="rolling",
        min_confidence=0.7,
        min_agreement=3,
        direction_band=0.4,
        min_signal_strength=0.6,
        max_position_pct=0.05,
        max_positions=2,
        max_exposure=0.1,
    )
    assert cfg.min_confidence == 0.7
    assert cfg.min_agreement == 3
    assert cfg.direction_band == 0.4
    assert cfg.min_signal_strength == 0.6
    assert cfg.max_position_pct == 0.05
    assert cfg.max_positions == 2
    assert cfg.max_exposure == 0.1


def test_backtest_config_override_fields_default_none():
    cfg = BacktestConfig(
        name="t",
        symbols=["SPY"],
        timeframe="1m",
        horizons=[5],
        range_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        range_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        train_days=1, test_days=1, embargo_minutes=5, mode="rolling",
    )
    assert cfg.min_confidence is None
    assert cfg.min_agreement is None
    assert cfg.direction_band is None
    assert cfg.min_signal_strength is None
    assert cfg.max_position_pct is None
    assert cfg.max_positions is None
    assert cfg.max_exposure is None


def test_backtest_config_volume_caps_have_asset_class_defaults():
    """Equity cap stays at 0.10; crypto cap defaults to 0.25 — see
    bullforge/docs/research/crypto-bar-volume-units.md for rationale."""
    cfg = BacktestConfig(
        name="t",
        symbols=["SPY"],
        timeframe="1m",
        horizons=[5],
        range_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        range_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        train_days=1, test_days=1, embargo_minutes=5, mode="rolling",
    )
    assert cfg.max_volume_pct == 0.10
    assert cfg.max_volume_pct_crypto == 0.25


def test_backtest_config_legacy_max_volume_pct_only_still_constructs():
    """Backwards-compat: pre-existing callers that pass only max_volume_pct
    (no max_volume_pct_crypto) must still work."""
    cfg = BacktestConfig(
        name="t",
        symbols=["SPY"],
        timeframe="1m",
        horizons=[5],
        range_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        range_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        train_days=1, test_days=1, embargo_minutes=5, mode="rolling",
        max_volume_pct=0.05,
    )
    assert cfg.max_volume_pct == 0.05
    # Crypto cap still gets the asset-class-aware default.
    assert cfg.max_volume_pct_crypto == 0.25


@pytest.mark.asyncio
async def test_signal_aggregator_honors_backtest_overrides_end_to_end():
    """Mimics how engine._run_fold builds overrides from cfg."""
    cfg = BacktestConfig(
        name="t",
        symbols=["SPY"],
        timeframe="1m",
        horizons=[5],
        range_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        range_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        train_days=1, test_days=1, embargo_minutes=5, mode="rolling",
        min_confidence=0.9,
    )
    overrides = {
        "min_confidence": cfg.min_confidence,
        "min_agreement": cfg.min_agreement,
        "direction_band": cfg.direction_band,
        "min_signal_strength": cfg.min_signal_strength,
        "max_position_pct": cfg.max_position_pct,
        "max_positions": cfg.max_positions,
        "max_exposure": cfg.max_exposure,
    }
    agg = SignalAggregator(runtime_config=_rc(), overrides=overrides)
    predictions = [
        {"direction": 1, "confidence": 0.80},
        {"direction": 1, "confidence": 0.75},
    ]
    ts = datetime(2026, 4, 16, 14, 0, tzinfo=timezone.utc)
    # min_confidence=0.9 override filters out both → no signal.
    assert await agg.aggregate("SPY", ts, predictions) is None


@pytest.mark.asyncio
async def test_risk_manager_honors_backtest_overrides_end_to_end():
    cfg = BacktestConfig(
        name="t",
        symbols=["SPY"],
        timeframe="1m",
        horizons=[5],
        range_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        range_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        train_days=1, test_days=1, embargo_minutes=5, mode="rolling",
        min_signal_strength=0.95,
    )
    overrides = {
        "min_confidence": cfg.min_confidence,
        "min_agreement": cfg.min_agreement,
        "direction_band": cfg.direction_band,
        "min_signal_strength": cfg.min_signal_strength,
        "max_position_pct": cfg.max_position_pct,
        "max_positions": cfg.max_positions,
        "max_exposure": cfg.max_exposure,
    }
    from shared.models import TradingSignal
    rm = RiskManager(runtime_config=_rc(), overrides=overrides)
    sig = TradingSignal(
        symbol="SPY",
        timestamp=datetime(2026, 4, 13, 16, 0, tzinfo=timezone.utc),
        direction=1, strength=0.9, contributing_predictions=3,
    )
    # min_signal_strength=0.95 override rejects a 0.9 strength signal.
    approved, reason, _ = await rm.check_entry(
        sig, {"equity": 100000.0, "buying_power": 200000.0}, [], current_price=420.0,
    )
    assert approved is False
    assert "weak" in reason.lower()


@pytest.mark.asyncio
async def test_backtest_engine_passes_buffer_minutes_to_trainer():
    """Backtest folds must thread RuntimeConfig through to Trainer.train so
    the equity exclusion zone filter runs at training time, matching the
    sim broker's at-action gate. Without this the backtest trains on rows
    whose targets land in the buffered window and then refuses to act on
    them at eval time — exactly the train/eval mismatch the filter exists
    to remove."""
    from datetime import timedelta
    from unittest.mock import MagicMock, patch

    from services.backtest_service.engine import BacktestEngine
    from services.model_service.dataset import DatasetBuilder
    from shared.models import BacktestScope
    from services.backtest_service.splitter import Fold

    # _rc with the buffer keys; mock get_all so the engine's snapshot works.
    rc = _rc()
    snapshot_payload = {
        "execution_min_confidence": 0.6,
        "execution_min_agreement": 2,
        "execution_direction_band": 0.3,
        "execution_min_signal_strength": 0.5,
        "execution_max_position_pct": 0.10,
        "execution_max_positions": 5,
        "execution_max_exposure": 0.5,
        "equity_window_open_buffer_minutes": 60,
        "equity_window_close_buffer_minutes": 45,
    }

    async def get_all():
        return snapshot_payload

    rc.get_all = get_all

    # Spy on DatasetBuilder.build to capture the buffer kwargs that reach it.
    captured_kwargs = {}

    original_build = DatasetBuilder.build

    async def spy_build(self, *args, **kwargs):
        captured_kwargs.update(kwargs)
        return None  # short-circuit — we only care about the kwargs.

    cfg = BacktestConfig(
        name="t",
        symbols=["SPY"],
        timeframe="1m",
        horizons=[5],
        range_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        range_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        train_days=1, test_days=1, embargo_minutes=5, mode="rolling",
    )
    fold = Fold(
        index=0,
        train_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        train_end=datetime(2025, 1, 1, 12, tzinfo=timezone.utc),
        test_start=datetime(2025, 1, 1, 12, tzinfo=timezone.utc) + timedelta(minutes=5),
        test_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
    )

    db = MagicMock()
    db.query_bars_between = AsyncMock(return_value=[])
    model_store = MagicMock()
    results_db = MagicMock()
    results_db.insert_fold = AsyncMock(return_value=99)

    engine = BacktestEngine(
        db=db, runtime_config=rc, model_store=model_store, results_db=results_db,
    )

    with patch.object(DatasetBuilder, "build", spy_build):
        # _run_fold short-circuits when training returns None (no models),
        # but we only need to reach the trainer.train call — bail early after.
        try:
            await engine._run_fold(run_id=42, cfg=cfg, fold=fold)
        except Exception:
            # Downstream fold steps depend on having trained models; without
            # them the test path may raise. We've already captured what we
            # care about by then.
            pass

    assert captured_kwargs.get("open_buffer_min") == 60, (
        "Backtest engine did not thread equity_window_open_buffer_minutes "
        "from RuntimeConfig snapshot to Trainer → DatasetBuilder.build"
    )
    assert captured_kwargs.get("close_buffer_min") == 45
