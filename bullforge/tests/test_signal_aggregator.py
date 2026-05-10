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
        "execution_hysteresis_bars": 0,
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
        "execution_hysteresis_bars": 0,
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


# ----- Hysteresis tests (execution_hysteresis_bars) -----

def _strong_buy_preds():
    """Predictions whose weighted avg comfortably clears band=0.3 → raw=+1."""
    return [
        {"direction": 1, "confidence": 0.80},
        {"direction": 1, "confidence": 0.75},
    ]


def _strong_sell_preds():
    """Predictions whose weighted avg comfortably crosses -band → raw=-1."""
    return [
        {"direction": -1, "confidence": 0.80},
        {"direction": -1, "confidence": 0.75},
    ]


def _flat_preds():
    """Mixed predictions producing weighted avg ~0 → raw=0."""
    return [
        {"direction": 1, "confidence": 0.75},
        {"direction": -1, "confidence": 0.75},
    ]


@pytest.mark.asyncio
async def test_hysteresis_disabled_passes_raw_direction():
    """With hysteresis_bars=0, every raw direction passes through untouched.

    Regression guard for the default — existing deployments must see no
    behavior change after this feature lands.
    """
    agg = SignalAggregator(runtime_config=_rc({"execution_hysteresis_bars": 0}))
    s1 = await agg.aggregate("SPY", _ts(), _strong_buy_preds())
    assert s1 is not None and s1.direction == 1
    s2 = await agg.aggregate("SPY", _ts(), _strong_sell_preds())
    assert s2 is not None and s2.direction == -1
    s3 = await agg.aggregate("SPY", _ts(), _flat_preds())
    assert s3 is not None and s3.direction == 0


@pytest.mark.asyncio
async def test_hysteresis_requires_consecutive_same_direction_to_commit():
    """With hysteresis_bars=5, starting from committed=0, a +1 raw must
    persist for 5 consecutive bars before the aggregator emits +1.

    Bars 1-4 return None (silent hold); bar 5 commits and returns +1.
    """
    agg = SignalAggregator(runtime_config=_rc({"execution_hysteresis_bars": 5}))
    for i in range(1, 5):
        result = await agg.aggregate("SPY", _ts(), _strong_buy_preds())
        assert result is None, f"bar {i} should be suppressed (streak < 5)"
    final = await agg.aggregate("SPY", _ts(), _strong_buy_preds())
    assert final is not None
    assert final.direction == 1


@pytest.mark.asyncio
async def test_hysteresis_resets_streak_on_revert_to_committed():
    """A bar that matches the committed direction resets the candidate streak.

    Sequence: +1, +1, +1, +1, 0, +1, +1, +1, +1 (9 bars). With committed=0
    initially: bars 1-4 build streak to 4 (none commit); bar 5 is raw=0
    which equals committed=0, so the candidate is cleared and streak
    resets; bars 6-9 only build a fresh streak to 4. With bars=5, no
    commit ever happens. Demonstrates the reset path — without bar 5's
    reset, bar 5's input would have committed +1.
    """
    agg = SignalAggregator(runtime_config=_rc({"execution_hysteresis_bars": 5}))
    sequence = [
        _strong_buy_preds(), _strong_buy_preds(),
        _strong_buy_preds(), _strong_buy_preds(),  # streak 1..4, all None
        _flat_preds(),                              # raw=0 matches committed=0 → reset
        _strong_buy_preds(), _strong_buy_preds(),
        _strong_buy_preds(), _strong_buy_preds(),  # new streak 1..4, still None
    ]
    results = []
    for preds in sequence:
        results.append(await agg.aggregate("SPY", _ts(), preds))
    # Bar 5 emits committed=0 (a real signal, direction=0); the rest are None.
    assert results[4] is not None and results[4].direction == 0
    for i, r in enumerate(results):
        if i == 4:
            continue
        assert r is None, f"bar {i + 1} should have been suppressed"


@pytest.mark.asyncio
async def test_hysteresis_resets_streak_on_change_in_candidate():
    """A change in candidate direction restarts the streak from 1.

    With hysteresis_bars=5 and committed=0, feed +1, +1, -1, -1, -1.
    Bars 1-2 build candidate=+1 streak=2. Bar 3 has raw=-1 which
    doesn't match committed (0) and doesn't match candidate (+1), so
    candidate becomes -1, streak=1. Bars 4-5 reach streak=3. Five bars
    in, no commit yet (candidate=-1 needs 5 consecutive to commit).
    """
    agg = SignalAggregator(runtime_config=_rc({"execution_hysteresis_bars": 5}))
    sequence = [
        _strong_buy_preds(), _strong_buy_preds(),
        _strong_sell_preds(), _strong_sell_preds(), _strong_sell_preds(),
    ]
    for i, preds in enumerate(sequence, start=1):
        result = await agg.aggregate("SPY", _ts(), preds)
        assert result is None, f"bar {i} should be suppressed"


@pytest.mark.asyncio
async def test_hysteresis_per_symbol_state_isolated():
    """Hysteresis state is keyed per symbol; one symbol's streak does not
    influence another's.

    With hysteresis_bars=3, feed BTC and ETH alternating buys. After 3
    BTC calls, BTC commits +1. ETH, having only seen 2 calls, is still
    suppressed. After ETH's 3rd call, ETH commits.
    """
    agg = SignalAggregator(runtime_config=_rc({"execution_hysteresis_bars": 3}))

    btc1 = await agg.aggregate("BTC/USD", _ts(), _strong_buy_preds())
    eth1 = await agg.aggregate("ETH/USD", _ts(), _strong_buy_preds())
    btc2 = await agg.aggregate("BTC/USD", _ts(), _strong_buy_preds())
    eth2 = await agg.aggregate("ETH/USD", _ts(), _strong_buy_preds())
    btc3 = await agg.aggregate("BTC/USD", _ts(), _strong_buy_preds())
    # BTC has now hit streak=3; should commit on this call.
    assert btc1 is None and btc2 is None
    assert btc3 is not None and btc3.direction == 1

    # ETH has only seen 2 of its own calls; still suppressed.
    assert eth1 is None and eth2 is None
    eth3 = await agg.aggregate("ETH/USD", _ts(), _strong_buy_preds())
    assert eth3 is not None and eth3.direction == 1


@pytest.mark.asyncio
async def test_hysteresis_suppressed_tick_returns_none_not_flat_signal():
    """During the streak buildup the aggregator returns None, NOT a
    TradingSignal with direction=0.

    OrderExecutor distinguishes 'no signal at all' (silent return,
    nothing happens) from 'flat signal' (direction=0, which triggers
    check_exit -> 'Flat signal' -> close on a held position). Hysteresis
    suppression must use the former.
    """
    agg = SignalAggregator(runtime_config=_rc({"execution_hysteresis_bars": 3}))
    result = await agg.aggregate("SPY", _ts(), _strong_buy_preds())
    assert result is None  # not TradingSignal(direction=0)


@pytest.mark.asyncio
async def test_hysteresis_strength_uses_current_raw_when_committing():
    """When the streak completes, the emitted signal's strength reflects
    the current bar's weighted avg, not a stale value from earlier in
    the streak.

    Build streak=2 with high-conviction inputs (avg ~0.422), then
    commit on bar 3 with weaker inputs (avg ~0.333). Both produce
    raw_direction=+1 (above band=0.3), so the streak progresses. The
    emitted strength on bar 3 must equal bar 3's avg ~0.333, NOT
    bar 1's ~0.422 — the strength must not be cached from when the
    candidate was first seen.

    Mixed direction votes are required because uniform-direction
    predictions always produce |avg|=1.0 regardless of confidences,
    making this test indistinguishable.
    """
    agg = SignalAggregator(runtime_config=_rc({"execution_hysteresis_bars": 3}))
    high_conf = [
        {"direction": 1, "confidence": 0.80},
        {"direction": 1, "confidence": 0.80},
        {"direction": -1, "confidence": 0.65},
    ]
    low_conf = [
        {"direction": 1, "confidence": 0.65},
        {"direction": 1, "confidence": 0.65},
        {"direction": -1, "confidence": 0.65},
    ]
    # Sanity: high_conf avg = (0.80+0.80-0.65)/(0.80+0.80+0.65) ≈ 0.422
    #        low_conf avg  = (0.65+0.65-0.65)/(0.65+0.65+0.65) ≈ 0.333
    assert await agg.aggregate("SPY", _ts(), high_conf) is None  # streak=1
    assert await agg.aggregate("SPY", _ts(), high_conf) is None  # streak=2
    final = await agg.aggregate("SPY", _ts(), low_conf)          # streak=3, commit
    assert final is not None
    assert final.direction == 1
    assert final.strength == pytest.approx(0.65 / 1.95, abs=1e-6)
    # Sanity-guard: strength is the low_conf value, NOT the high_conf one.
    assert final.strength != pytest.approx(0.95 / 2.25, abs=1e-6)
