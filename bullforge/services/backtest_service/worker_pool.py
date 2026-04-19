"""Classifies a backtest job's required worker pool based on its model_type."""
from __future__ import annotations

_GPU_MODEL_TYPES = frozenset({"lstm", "mlp", "meta_entry", "meta_exec"})


def cpu_or_gpu(model_type: str | None) -> str:
    """Maps a model_type string to the worker pool it should run on.

    None / empty / unknown / CPU-native types (gradient_boost,
    hist_gradient_boost, logistic_regression, lightgbm, baseline) → 'cpu'.
    LSTM / MLP / meta-models → 'gpu'.
    """
    if model_type and model_type.lower() in _GPU_MODEL_TYPES:
        return "gpu"
    return "cpu"
