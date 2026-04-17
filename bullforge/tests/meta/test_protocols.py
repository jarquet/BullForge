from datetime import datetime, timezone

import pytest

from shared.models import MetaFeatureRow, MetaSignalShadow


def _ts():
    return datetime(2026, 4, 17, 14, 0, 0, tzinfo=timezone.utc)


class TestMetaFeatureRow:
    def test_valid_construction(self):
        row = MetaFeatureRow(
            symbol="AAPL",
            timestamp=_ts(),
            direction=1,
            feature_vector={"base_L1_conf": 0.8, "regime_spy_trend": 1.0},
            source="replay",
        )
        assert row.symbol == "AAPL"
        assert row.direction == 1
        assert row.source == "replay"

    def test_rejects_naive_timestamp(self):
        with pytest.raises(ValueError, match="UTC"):
            MetaFeatureRow(
                symbol="AAPL",
                timestamp=datetime(2026, 4, 17, 14, 0, 0),
                direction=1,
                feature_vector={"x": 1.0},
                source="replay",
            )

    def test_rejects_invalid_direction(self):
        with pytest.raises(ValueError, match="direction"):
            MetaFeatureRow(
                symbol="AAPL",
                timestamp=_ts(),
                direction=2,
                feature_vector={"x": 1.0},
                source="replay",
            )

    def test_rejects_empty_feature_vector(self):
        with pytest.raises(ValueError, match="feature_vector"):
            MetaFeatureRow(
                symbol="AAPL",
                timestamp=_ts(),
                direction=1,
                feature_vector={},
                source="replay",
            )

    def test_rejects_invalid_source(self):
        with pytest.raises(ValueError, match="source"):
            MetaFeatureRow(
                symbol="AAPL",
                timestamp=_ts(),
                direction=1,
                feature_vector={"x": 1.0},
                source="unknown",
            )


class TestMetaSignalShadow:
    def test_valid_construction(self):
        sig = MetaSignalShadow(
            symbol="AAPL",
            timestamp=_ts(),
            direction=1,
            estimator_type="lightgbm",
            entry_probability=0.72,
            kelly_fraction=0.15,
            pair_index=3,
            target_pct=0.005,
            stop_pct=0.003,
            model_id=42,
        )
        assert sig.entry_probability == 0.72
        assert sig.estimator_type == "lightgbm"

    def test_rejects_probability_out_of_range(self):
        with pytest.raises(ValueError, match="entry_probability"):
            MetaSignalShadow(
                symbol="AAPL",
                timestamp=_ts(),
                direction=1,
                estimator_type="lightgbm",
                entry_probability=1.5,
            )
