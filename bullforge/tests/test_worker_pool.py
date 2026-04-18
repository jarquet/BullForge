import pytest

from services.backtest_service.worker_pool import cpu_or_gpu


@pytest.mark.parametrize("model_type,expected", [
    (None, "cpu"),
    ("gradient_boost", "cpu"),
    ("hist_gradient_boost", "cpu"),
    ("logistic_regression", "cpu"),
    ("lightgbm", "cpu"),
    ("baseline", "cpu"),
    ("lstm", "gpu"),
    ("mlp", "gpu"),
    ("meta_entry", "gpu"),
    ("meta_exec", "gpu"),
    ("LSTM", "gpu"),
    ("unknown_future", "cpu"),
    ("", "cpu"),
])
def test_cpu_or_gpu(model_type, expected):
    assert cpu_or_gpu(model_type) == expected
