import pytest
from services.model_service.meta.factory import create_meta_entry, create_meta_exec, list_estimator_types, get_cold_start_min_rows

def test_list_estimator_types():
    types = list_estimator_types()
    assert "lightgbm" in types
    assert "mlp" in types
    assert "baseline" in types

def test_create_meta_entry_lightgbm():
    est = create_meta_entry("lightgbm")
    assert hasattr(est, "fit")
    assert hasattr(est, "predict_proba")
    assert hasattr(est, "feature_importances")

def test_create_meta_exec_lightgbm():
    est = create_meta_exec("lightgbm", num_pairs=7)
    assert hasattr(est, "fit")
    assert hasattr(est, "predict")
    assert hasattr(est, "feature_importances")

def test_create_unknown_type_raises():
    with pytest.raises(ValueError, match="Unknown estimator type"):
        create_meta_entry("unknown_type")

def test_cold_start_min_rows():
    assert get_cold_start_min_rows("lightgbm") == 2000
    assert get_cold_start_min_rows("mlp") == 5000
    assert get_cold_start_min_rows("baseline") == 0
