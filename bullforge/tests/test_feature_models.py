from datetime import datetime, timezone
import pytest
from shared.models import FeatureRow, Label


def test_feature_row_creation():
    row = FeatureRow(
        symbol="SPY",
        timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
        timeframe="1m",
        features={"sma_20": 420.5, "rsi_14": 55.3, "log_return": 0.001},
    )
    assert row.symbol == "SPY"
    assert row.features["sma_20"] == 420.5
    assert row.timeframe == "1m"


def test_feature_row_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="UTC"):
        FeatureRow(
            symbol="SPY",
            timestamp=datetime(2026, 4, 10, 15, 30, 0),
            timeframe="1m",
            features={"sma_20": 420.5},
        )


def test_feature_row_rejects_empty_features():
    with pytest.raises(ValueError, match="empty"):
        FeatureRow(
            symbol="SPY",
            timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
            timeframe="1m",
            features={},
        )


def test_label_creation():
    label = Label(
        symbol="SPY",
        timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
        timeframe="1m",
        horizon_minutes=30,
        forward_return=0.0025,
        direction=1,
    )
    assert label.symbol == "SPY"
    assert label.forward_return == 0.0025
    assert label.direction == 1
    assert label.horizon_minutes == 30


def test_label_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="UTC"):
        Label(
            symbol="SPY",
            timestamp=datetime(2026, 4, 10, 15, 30, 0),
            timeframe="1m",
            horizon_minutes=30,
            forward_return=0.0025,
            direction=1,
        )


def test_label_direction_values():
    """Direction must be -1, 0, or 1."""
    with pytest.raises(ValueError, match="direction"):
        Label(
            symbol="SPY",
            timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
            timeframe="1m",
            horizon_minutes=30,
            forward_return=0.0025,
            direction=2,
        )


def test_label_rejects_non_positive_horizon():
    with pytest.raises(ValueError, match="horizon_minutes"):
        Label(
            symbol="SPY",
            timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
            timeframe="1m",
            horizon_minutes=0,
            forward_return=0.0025,
            direction=1,
        )
