from __future__ import annotations
from typing import Any
from services.model_service.meta.protocols import MetaEntryEstimator, MetaExecEstimator

_ENTRY_REGISTRY: dict[str, type] = {}
_EXEC_REGISTRY: dict[str, type] = {}
_COLD_START: dict[str, int] = {}

def register_estimator(name: str, entry_cls: type, exec_cls: type, cold_start_min_rows: int) -> None:
    _ENTRY_REGISTRY[name] = entry_cls
    _EXEC_REGISTRY[name] = exec_cls
    _COLD_START[name] = cold_start_min_rows

def create_meta_entry(estimator_type: str, **kwargs: Any) -> MetaEntryEstimator:
    if estimator_type not in _ENTRY_REGISTRY:
        raise ValueError(f"Unknown estimator type: {estimator_type!r}. Available: {list(_ENTRY_REGISTRY)}")
    return _ENTRY_REGISTRY[estimator_type](**kwargs)

def create_meta_exec(estimator_type: str, *, num_pairs: int, **kwargs: Any) -> MetaExecEstimator:
    if estimator_type not in _EXEC_REGISTRY:
        raise ValueError(f"Unknown estimator type: {estimator_type!r}. Available: {list(_EXEC_REGISTRY)}")
    return _EXEC_REGISTRY[estimator_type](num_pairs=num_pairs, **kwargs)

def list_estimator_types() -> list[str]:
    return list(_ENTRY_REGISTRY)

def get_cold_start_min_rows(estimator_type: str) -> int:
    return _COLD_START.get(estimator_type, 0)

def _auto_register() -> None:
    from services.model_service.meta.estimators.lightgbm import LightGBMEntryEstimator, LightGBMExecEstimator
    from services.model_service.meta.estimators.mlp import MLPEntryEstimator, MLPExecEstimator
    from services.model_service.meta.estimators.baseline import BaselineEntryEstimator, BaselineExecEstimator
    register_estimator("lightgbm", LightGBMEntryEstimator, LightGBMExecEstimator, 2000)
    register_estimator("mlp", MLPEntryEstimator, MLPExecEstimator, 5000)
    register_estimator("baseline", BaselineEntryEstimator, BaselineExecEstimator, 0)

_auto_register()
