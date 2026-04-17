from datetime import datetime, timezone
import numpy as np
import pytest
from services.model_service.meta.feature_assembler import MetaFeatureAssembler

def _ts():
    return datetime(2026, 4, 17, 14, 30, 0, tzinfo=timezone.utc)

def _make_predictions():
    return [
        {"model_id": 1, "direction": 1, "confidence": 0.82},
        {"model_id": 2, "direction": 1, "confidence": 0.65},
        {"model_id": 3, "direction": -1, "confidence": 0.55},
    ]

def _make_rolling_stats():
    return {
        1: {"accuracy": 0.72, "calibration_error": 0.05, "is_disabled": False},
        2: {"accuracy": 0.58, "calibration_error": 0.12, "is_disabled": False},
        3: {"accuracy": 0.45, "calibration_error": 0.20, "is_disabled": True},
    }

def _make_regime():
    return {"spy_trend_1m": 1.0, "spy_trend_1h": 1.0, "spy_trend_1d": 1.0, "spy_momentum": 0.3, "vol_regime": 1.0}

def _make_symbol_state():
    return {"realized_vol": 0.025, "atr_price_ratio": 0.012, "dist_sma_50": 0.015, "dist_sma_200bar": -0.005, "dist_sma_200day": 0.008}

class TestMetaFeatureAssembler:
    def test_assemble_returns_dict(self):
        assembler = MetaFeatureAssembler(model_ids=[1, 2, 3])
        vec = assembler.assemble(_make_predictions(), _make_rolling_stats(), _make_regime(), _make_symbol_state(), _ts())
        assert isinstance(vec, dict)
        assert len(vec) > 0
        assert all(isinstance(v, float) for v in vec.values())

    def test_base_model_features_present(self):
        assembler = MetaFeatureAssembler(model_ids=[1, 2, 3])
        vec = assembler.assemble(_make_predictions(), _make_rolling_stats(), _make_regime(), _make_symbol_state(), _ts())
        assert "base_1_dir" in vec
        assert "base_1_conf" in vec
        assert "base_1_rolling_acc" in vec
        assert "base_1_cal_err" in vec
        assert "base_1_disabled" in vec

    def test_regime_features_present(self):
        assembler = MetaFeatureAssembler(model_ids=[1, 2, 3])
        vec = assembler.assemble(_make_predictions(), _make_rolling_stats(), _make_regime(), _make_symbol_state(), _ts())
        assert "regime_spy_trend_1m" in vec
        assert "regime_vol" in vec

    def test_temporal_features_present(self):
        assembler = MetaFeatureAssembler(model_ids=[1, 2, 3])
        vec = assembler.assemble(_make_predictions(), _make_rolling_stats(), _make_regime(), _make_symbol_state(), _ts())
        assert "time_bucket" in vec
        assert "minutes_since_open" in vec
        assert "minutes_to_close" in vec
        assert "weekday" in vec

    def test_symbol_state_features_present(self):
        assembler = MetaFeatureAssembler(model_ids=[1, 2, 3])
        vec = assembler.assemble(_make_predictions(), _make_rolling_stats(), _make_regime(), _make_symbol_state(), _ts())
        assert "sym_realized_vol" in vec
        assert "sym_atr_price_ratio" in vec
        assert "sym_dist_sma_50" in vec
        assert "sym_dist_sma_200bar" in vec
        assert "sym_dist_sma_200day" in vec

    def test_missing_prediction_gets_zeros(self):
        assembler = MetaFeatureAssembler(model_ids=[1, 2, 3])
        vec = assembler.assemble([{"model_id": 1, "direction": 1, "confidence": 0.82}],
                                  _make_rolling_stats(), _make_regime(), _make_symbol_state(), _ts())
        assert vec["base_2_dir"] == 0.0
        assert vec["base_2_conf"] == 0.0

    def test_feature_names_returns_ordered_list(self):
        assembler = MetaFeatureAssembler(model_ids=[1, 2, 3])
        names = assembler.feature_names()
        assert isinstance(names, list)
        assert len(names) > 0
        assert names[0] == "base_1_dir"
