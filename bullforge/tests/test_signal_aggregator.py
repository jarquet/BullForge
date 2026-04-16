from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from services.execution_service.signal_aggregator import SignalAggregator


def _rc(values: dict | None = None):
    """Build a fake RuntimeConfig whose .get returns `values[key]` or a default."""
    defaults = {
        "execution_min_confidence": 0.6,
        "execution_min_agreement": 2,
        "execution_direction_band": 0.3,
    }
    data = {**defaults, **(values or {})}
    rc = AsyncMock()

    async def get(key):
        return data[key]

    rc.get = get
    return rc


def _ts():
    return datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_aggregate_strong_buy():
    agg = SignalAggregator(runtime_config=_rc())
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.80},
        {"horizon_minutes": 10, "direction": 1, "confidence": 0.75},
        {"horizon_minutes": 30, "direction": 1, "confidence": 0.70},
    ]
    signal = await agg.aggregate("SPY", _ts(), predictions)
    assert signal is not None
    assert signal.direction == 1
    assert signal.strength > 0.5
    assert signal.contributing_predictions == 3


@pytest.mark.asyncio
async def test_aggregate_strong_sell():
    agg = SignalAggregator(runtime_config=_rc())
    predictions = [
        {"horizon_minutes": 5, "direction": -1, "confidence": 0.80},
        {"horizon_minutes": 10, "direction": -1, "confidence": 0.70},
    ]
    signal = await agg.aggregate("SPY", _ts(), predictions)
    assert signal is not None
    assert signal.direction == -1
    assert signal.strength > 0.5


@pytest.mark.asyncio
async def test_aggregate_mixed_produces_flat():
    agg = SignalAggregator(runtime_config=_rc())
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.75},
        {"horizon_minutes": 10, "direction": -1, "confidence": 0.75},
    ]
    signal = await agg.aggregate("SPY", _ts(), predictions)
    assert signal is not None
    assert signal.direction == 0


@pytest.mark.asyncio
async def test_aggregate_filters_low_confidence():
    agg = SignalAggregator(runtime_config=_rc())
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.80},
        {"horizon_minutes": 10, "direction": 1, "confidence": 0.70},
        {"horizon_minutes": 30, "direction": -1, "confidence": 0.40},
    ]
    signal = await agg.aggregate("SPY", _ts(), predictions)
    assert signal is not None
    assert signal.direction == 1
    assert signal.contributing_predictions == 2


@pytest.mark.asyncio
async def test_aggregate_insufficient_agreement_returns_none():
    agg = SignalAggregator(runtime_config=_rc())
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.80},
        {"horizon_minutes": 10, "direction": -1, "confidence": 0.40},
    ]
    signal = await agg.aggregate("SPY", _ts(), predictions)
    assert signal is None


@pytest.mark.asyncio
async def test_aggregate_empty_returns_none():
    agg = SignalAggregator(runtime_config=_rc())
    assert await agg.aggregate("SPY", _ts(), []) is None


@pytest.mark.asyncio
async def test_aggregate_all_below_confidence_returns_none():
    agg = SignalAggregator(runtime_config=_rc())
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.40},
        {"horizon_minutes": 10, "direction": 1, "confidence": 0.50},
    ]
    assert await agg.aggregate("SPY", _ts(), predictions) is None


@pytest.mark.asyncio
async def test_aggregate_filters_disabled_models():
    agg = SignalAggregator(runtime_config=_rc())
    agg.update_disabled_models({2})
    predictions = [
        {"direction": 1, "confidence": 0.8, "model_id": 1},
        {"direction": 1, "confidence": 0.7, "model_id": 2},
        {"direction": 1, "confidence": 0.9, "model_id": 3},
    ]
    result = await agg.aggregate("SPY", _ts(), predictions)
    assert result is not None
    assert result.contributing_predictions == 2


@pytest.mark.asyncio
async def test_aggregate_disabled_models_cause_no_signal():
    agg = SignalAggregator(runtime_config=_rc())
    agg.update_disabled_models({1, 2})
    predictions = [
        {"direction": 1, "confidence": 0.8, "model_id": 1},
        {"direction": 1, "confidence": 0.7, "model_id": 2},
        {"direction": 1, "confidence": 0.9, "model_id": 3},
    ]
    assert await agg.aggregate("SPY", _ts(), predictions) is None


@pytest.mark.asyncio
async def test_aggregate_no_model_id_not_filtered():
    agg = SignalAggregator(runtime_config=_rc())
    agg.update_disabled_models({99})
    predictions = [
        {"direction": 1, "confidence": 0.8},
        {"direction": 1, "confidence": 0.7},
    ]
    result = await agg.aggregate("SPY", _ts(), predictions)
    assert result is not None
    assert result.contributing_predictions == 2


@pytest.mark.asyncio
async def test_direction_band_wide_produces_flat():
    """Same inputs that would produce direction=1 at band=0.3 produce 0 at band=0.9."""
    agg = SignalAggregator(
        runtime_config=_rc({"execution_direction_band": 0.9}),
    )
    predictions = [
        {"direction": 1, "confidence": 0.7},
        {"direction": 0, "confidence": 0.7},
    ]
    signal = await agg.aggregate("SPY", _ts(), predictions)
    assert signal is not None
    assert signal.direction == 0


@pytest.mark.asyncio
async def test_direction_band_narrow_produces_buy():
    """Same inputs that are flat at band=0.3 buy at band=0.05."""
    agg = SignalAggregator(
        runtime_config=_rc({"execution_direction_band": 0.05}),
    )
    predictions = [
        {"direction": 1, "confidence": 0.7},
        {"direction": 0, "confidence": 0.7},
    ]
    signal = await agg.aggregate("SPY", _ts(), predictions)
    assert signal is not None
    assert signal.direction == 1


@pytest.mark.asyncio
async def test_overrides_win_over_runtime_config():
    """If overrides={'min_confidence': 0.9}, the 0.6 from runtime_config is ignored."""
    agg = SignalAggregator(
        runtime_config=_rc(),
        overrides={"min_confidence": 0.9},
    )
    predictions = [
        {"direction": 1, "confidence": 0.80},
        {"direction": 1, "confidence": 0.75},
    ]
    assert await agg.aggregate("SPY", _ts(), predictions) is None


@pytest.mark.asyncio
async def test_override_none_falls_back_to_runtime_config():
    """overrides={'min_confidence': None} = unset; use runtime_config."""
    agg = SignalAggregator(
        runtime_config=_rc(),
        overrides={"min_confidence": None, "min_agreement": None, "direction_band": None},
    )
    predictions = [
        {"direction": 1, "confidence": 0.80},
        {"direction": 1, "confidence": 0.70},
    ]
    signal = await agg.aggregate("SPY", _ts(), predictions)
    assert signal is not None
    assert signal.direction == 1


@pytest.mark.asyncio
async def test_live_reload_uses_latest_runtime_config_value():
    """Changing runtime_config between calls changes behavior."""
    data = {
        "execution_min_confidence": 0.5,
        "execution_min_agreement": 2,
        "execution_direction_band": 0.3,
    }
    rc = AsyncMock()

    async def get(key):
        return data[key]

    rc.get = get
    agg = SignalAggregator(runtime_config=rc)
    predictions = [
        {"direction": 1, "confidence": 0.55},
        {"direction": 1, "confidence": 0.55},
    ]
    first = await agg.aggregate("SPY", _ts(), predictions)
    assert first is not None and first.direction == 1

    data["execution_min_confidence"] = 0.9
    second = await agg.aggregate("SPY", _ts(), predictions)
    assert second is None
