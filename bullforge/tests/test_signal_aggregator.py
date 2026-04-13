import pytest
from datetime import datetime, timezone
from shared.models import TradingSignal
from services.execution_service.signal_aggregator import SignalAggregator


@pytest.fixture
def aggregator():
    return SignalAggregator(min_confidence=0.6, min_agreement=2)


def _ts():
    return datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc)


def test_aggregate_strong_buy(aggregator):
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.80},
        {"horizon_minutes": 10, "direction": 1, "confidence": 0.75},
        {"horizon_minutes": 30, "direction": 1, "confidence": 0.70},
    ]
    signal = aggregator.aggregate("SPY", _ts(), predictions)

    assert signal is not None
    assert signal.direction == 1
    assert signal.strength > 0.5
    assert signal.contributing_predictions == 3


def test_aggregate_strong_sell(aggregator):
    predictions = [
        {"horizon_minutes": 5, "direction": -1, "confidence": 0.80},
        {"horizon_minutes": 10, "direction": -1, "confidence": 0.70},
    ]
    signal = aggregator.aggregate("SPY", _ts(), predictions)

    assert signal is not None
    assert signal.direction == -1
    assert signal.strength > 0.5
    assert signal.contributing_predictions == 2


def test_aggregate_mixed_produces_flat(aggregator):
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.75},
        {"horizon_minutes": 10, "direction": -1, "confidence": 0.75},
    ]
    signal = aggregator.aggregate("SPY", _ts(), predictions)

    assert signal is not None
    assert signal.direction == 0


def test_aggregate_filters_low_confidence(aggregator):
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.80},
        {"horizon_minutes": 10, "direction": 1, "confidence": 0.70},
        {"horizon_minutes": 30, "direction": -1, "confidence": 0.40},  # below threshold
    ]
    signal = aggregator.aggregate("SPY", _ts(), predictions)

    assert signal is not None
    assert signal.direction == 1  # low-confidence -1 filtered out
    assert signal.contributing_predictions == 2


def test_aggregate_insufficient_agreement(aggregator):
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.80},
        {"horizon_minutes": 10, "direction": -1, "confidence": 0.40},  # below threshold
    ]
    # Only 1 confident prediction, min_agreement=2
    signal = aggregator.aggregate("SPY", _ts(), predictions)

    assert signal is None


def test_aggregate_empty_predictions(aggregator):
    signal = aggregator.aggregate("SPY", _ts(), [])
    assert signal is None


def test_aggregate_all_below_confidence(aggregator):
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.40},
        {"horizon_minutes": 10, "direction": 1, "confidence": 0.50},
    ]
    signal = aggregator.aggregate("SPY", _ts(), predictions)
    assert signal is None
