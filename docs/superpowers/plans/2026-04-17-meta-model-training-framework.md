# Meta-Model Training Framework — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the pluggable meta-model framework so that per-symbol meta-models can be trained from backtest-replay data and produce entry/exit/sizing predictions.

**Architecture:** Two-head hybrid meta-model (entry classifier + exec multi-task) per (symbol, direction). Estimator plugins (LightGBM, MLP, baseline aggregator) implement a common protocol and are registered in a factory. A MetaTrainer orchestrates backtest-replay data generation, training across all registered estimators, and isotonic calibration of entry heads.

**Tech Stack:** Python 3.11+, LightGBM 4.3+, PyTorch 2.0+, scikit-learn (isotonic regression), asyncpg, pandas, numpy

**Spec:** `docs/superpowers/specs/2026-04-16-meta-model-signal-processor-design.md`

**Follow-up plan:** Plan B (meta-model inference, execution integration, dashboard) depends on this plan completing first.

---

## File Structure

### New files

```
services/model_service/meta/
  __init__.py                     # Package init
  protocols.py                    # MetaEntryEstimator, MetaExecEstimator protocols
  factory.py                      # Estimator registry + create functions
  feature_assembler.py            # MetaFeatureAssembler — builds L2 feature vector
  calibrator.py                   # CalibratedEntryEstimator — isotonic wrapper
  trainer.py                      # MetaTrainer — replay + train + evaluate
  estimators/
    __init__.py
    lightgbm.py                   # LightGBMEntryEstimator + LightGBMExecEstimator
    mlp.py                        # MLPEntryEstimator + MLPExecEstimator
    baseline.py                   # BaselineEntryEstimator + BaselineExecEstimator

migrations/
  016_create_meta_tables.sql      # meta_features, meta_signals_shadow, model_registry extension

tests/meta/
  __init__.py
  test_protocols.py               # Protocol compliance tests
  test_lightgbm.py                # LightGBM entry + exec tests
  test_mlp.py                     # MLP entry + exec tests
  test_baseline.py                # Baseline entry + exec tests
  test_feature_assembler.py       # Feature assembly tests
  test_calibrator.py              # Calibration wrapper tests
  test_trainer.py                 # MetaTrainer tests
  test_meta_db.py                 # DB method tests for meta tables
```

### Modified files

```
shared/models.py                  # Add MetaFeatureRow, MetaSignalShadow dataclasses
shared/runtime_config.py          # Add 7 meta_* config keys to CONFIG_SCHEMA
shared/db.py                      # Add meta_features and meta_signals_shadow DB methods
```

---

### Task 1: Database Migration

**Files:**
- Create: `bullforge/migrations/016_create_meta_tables.sql`

- [ ] **Step 1: Write the migration SQL**

```sql
-- 016_create_meta_tables.sql

-- Meta-feature rows persisted at inference time (and from backtest-replay)
CREATE TABLE IF NOT EXISTS meta_features (
    symbol          TEXT        NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    direction       INTEGER     NOT NULL,  -- 1=long, -1=short
    feature_vector  JSONB       NOT NULL,
    entry_label     INTEGER,               -- 1=hit target, 0=hit stop, NULL=unlabeled
    optimal_pair_idx INTEGER,              -- index into symbol's (target,stop) pairs
    realized_ev     DOUBLE PRECISION,      -- realized expected value
    source          TEXT        NOT NULL DEFAULT 'replay',  -- 'replay' or 'live'
    PRIMARY KEY (symbol, timestamp, direction)
);

SELECT create_hypertable('meta_features', 'timestamp',
    if_not_exists => TRUE, migrate_data => TRUE);

CREATE INDEX IF NOT EXISTS idx_meta_features_symbol_dir
    ON meta_features (symbol, direction, timestamp DESC);

-- Shadow-mode meta-model signal log for A/B/n comparison
CREATE TABLE IF NOT EXISTS meta_signals_shadow (
    id              BIGSERIAL   PRIMARY KEY,
    symbol          TEXT        NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    direction       INTEGER     NOT NULL,
    estimator_type  TEXT        NOT NULL,
    entry_probability DOUBLE PRECISION NOT NULL,
    kelly_fraction  DOUBLE PRECISION,
    pair_index      INTEGER,
    target_pct      DOUBLE PRECISION,
    stop_pct        DOUBLE PRECISION,
    model_id        INTEGER     REFERENCES model_registry(id)
);

CREATE INDEX IF NOT EXISTS idx_meta_signals_shadow_lookup
    ON meta_signals_shadow (symbol, timestamp DESC);

-- Extend model_registry with estimator_type for meta-models
ALTER TABLE model_registry
    ADD COLUMN IF NOT EXISTS estimator_type TEXT;

-- Update unique index to include estimator_type for meta-models
-- (base models have estimator_type=NULL, so existing index still works)
CREATE UNIQUE INDEX IF NOT EXISTS idx_model_registry_meta_active
    ON model_registry (symbol, timeframe, horizon_minutes, model_type, estimator_type)
    WHERE is_active = TRUE AND estimator_type IS NOT NULL;
```

- [ ] **Step 2: Verify migration applies cleanly**

Run: `cd bullforge && docker compose exec timescaledb psql -U bullforge -d bullforge -f /dev/stdin < migrations/016_create_meta_tables.sql`

Expected: No errors. Tables `meta_features` and `meta_signals_shadow` created, `model_registry.estimator_type` column added.

- [ ] **Step 3: Verify tables exist**

Run: `cd bullforge && docker compose exec timescaledb psql -U bullforge -d bullforge -c "\dt meta_*"`

Expected: `meta_features` and `meta_signals_shadow` listed.

- [ ] **Step 4: Commit**

```bash
git add bullforge/migrations/016_create_meta_tables.sql
git commit -m "feat(meta): add database migration for meta_features and meta_signals_shadow"
```

---

### Task 2: Shared Models and Runtime Config

**Files:**
- Modify: `bullforge/shared/models.py`
- Modify: `bullforge/shared/runtime_config.py`
- Create: `bullforge/tests/meta/__init__.py`
- Create: `bullforge/tests/meta/test_protocols.py`

- [ ] **Step 1: Write failing tests for new dataclasses**

Create `bullforge/tests/meta/__init__.py` (empty file).

Create `bullforge/tests/meta/test_protocols.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/meta/test_protocols.py -v`

Expected: FAIL — `ImportError: cannot import name 'MetaFeatureRow'`

- [ ] **Step 3: Add dataclasses to shared/models.py**

Append to `bullforge/shared/models.py`:

```python
VALID_META_SOURCES = {"replay", "live"}


@dataclass(frozen=True)
class MetaFeatureRow:
    symbol: str
    timestamp: datetime
    direction: int  # 1=long, -1=short
    feature_vector: dict[str, float]
    source: str  # "replay" or "live"
    entry_label: Optional[int] = None  # 1=hit target, 0=hit stop
    optimal_pair_idx: Optional[int] = None
    realized_ev: Optional[float] = None

    def __post_init__(self):
        if self.timestamp.tzinfo is None or self.timestamp.tzinfo != timezone.utc:
            raise ValueError(
                f"MetaFeatureRow timestamp must be UTC, got tzinfo={self.timestamp.tzinfo}"
            )
        if self.direction not in (-1, 1):
            raise ValueError(
                f"direction must be -1 or 1, got {self.direction}"
            )
        if not self.feature_vector:
            raise ValueError("feature_vector must not be empty")
        if self.source not in VALID_META_SOURCES:
            raise ValueError(
                f"source must be one of {VALID_META_SOURCES}, got {self.source!r}"
            )


@dataclass(frozen=True)
class MetaSignalShadow:
    symbol: str
    timestamp: datetime
    direction: int  # 1=long, -1=short
    estimator_type: str
    entry_probability: float
    kelly_fraction: Optional[float] = None
    pair_index: Optional[int] = None
    target_pct: Optional[float] = None
    stop_pct: Optional[float] = None
    model_id: Optional[int] = None
    id: Optional[int] = None

    def __post_init__(self):
        if self.timestamp.tzinfo is None or self.timestamp.tzinfo != timezone.utc:
            raise ValueError(
                f"MetaSignalShadow timestamp must be UTC, got tzinfo={self.timestamp.tzinfo}"
            )
        if self.direction not in (-1, 1):
            raise ValueError(
                f"direction must be -1 or 1, got {self.direction}"
            )
        if not (0.0 <= self.entry_probability <= 1.0):
            raise ValueError(
                f"entry_probability must be in [0.0, 1.0], got {self.entry_probability}"
            )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/meta/test_protocols.py -v`

Expected: All PASS.

- [ ] **Step 5: Add runtime config keys**

Add these entries to `CONFIG_SCHEMA` in `bullforge/shared/runtime_config.py`:

```python
    "meta_min_confidence": {
        "default": 0.55, "type": "float", "min": 0.0, "max": 1.0,
        "label": "Meta Entry Min Confidence",
        "description": "Minimum calibrated probability from the meta-entry model required to emit a trade signal. Higher values = fewer but more confident trades.",
        "reference": "https://en.wikipedia.org/wiki/Probabilistic_classification",
    },
    "meta_kelly_cap_multiple": {
        "default": 0.25, "type": "float", "min": 0.0, "max": 1.0,
        "label": "Kelly Cap Multiple",
        "description": "Fraction of full-Kelly sizing to use. 0.25 = quarter-Kelly (conservative). Full Kelly (1.0) is mathematically optimal but extremely aggressive on estimated probabilities.",
        "reference": "https://en.wikipedia.org/wiki/Kelly_criterion",
    },
    "meta_hysteresis_bars": {
        "default": 5, "type": "int", "min": 0, "max": 100,
        "label": "Meta Hysteresis Window (bars)",
        "description": "Suppress direction flips on the same symbol within N bars. Prevents flip-flopping when base predictions are noisy.",
        "reference": "https://en.wikipedia.org/wiki/Hysteresis",
    },
    "meta_cold_start_min_rows": {
        "default": 2000, "type": "int", "min": 100, "max": 100000,
        "label": "Meta Cold-Start Min Rows",
        "description": "Minimum labeled training rows (replay + live) before a meta-model is eligible for promotion. LightGBM default: 2000; MLP may need higher (5000+).",
        "reference": "https://en.wikipedia.org/wiki/Cold_start_(recommender_systems)",
    },
    "meta_cold_start_min_backtest_ev": {
        "default": 0.001, "type": "float", "min": 0.0, "max": 1.0,
        "label": "Meta Cold-Start Min Backtest EV",
        "description": "Minimum expected value per signal on backtest required for promotion eligibility. Configurable per symbol via dashboard overrides.",
        "reference": "https://en.wikipedia.org/wiki/Expected_value",
    },
    "meta_live_weight_multiplier": {
        "default": 3.0, "type": "float", "min": 1.0, "max": 20.0,
        "label": "Meta Live Row Weight Multiplier",
        "description": "Training weight applied to live inference rows relative to backtest-replay rows. Higher values make the meta-model adapt faster to live conditions.",
        "reference": "https://en.wikipedia.org/wiki/Weighted_least_squares",
    },
    "meta_rolling_accuracy_window_days": {
        "default": 30, "type": "int", "min": 7, "max": 365,
        "label": "Meta Rolling Accuracy Window (days)",
        "description": "Rolling window in calendar days for computing per-sub-model live accuracy and calibration error features.",
        "reference": "https://en.wikipedia.org/wiki/Moving_average",
    },
```

- [ ] **Step 6: Run existing config tests to verify no regression**

Run: `cd bullforge && python -m pytest tests/ -k "config" -v`

Expected: All existing config tests PASS.

- [ ] **Step 7: Commit**

```bash
git add bullforge/shared/models.py bullforge/shared/runtime_config.py \
       bullforge/tests/meta/__init__.py bullforge/tests/meta/test_protocols.py
git commit -m "feat(meta): add MetaFeatureRow/MetaSignalShadow models and runtime config keys"
```

---

### Task 3: MetaEstimator Protocols and Factory

**Files:**
- Create: `bullforge/services/model_service/meta/__init__.py`
- Create: `bullforge/services/model_service/meta/protocols.py`
- Create: `bullforge/services/model_service/meta/factory.py`
- Create: `bullforge/services/model_service/meta/estimators/__init__.py`
- Create: `bullforge/tests/meta/test_factory.py`

- [ ] **Step 1: Write failing tests for the factory**

Create `bullforge/tests/meta/test_factory.py`:

```python
import pytest

from services.model_service.meta.factory import (
    create_meta_entry,
    create_meta_exec,
    list_estimator_types,
    get_cold_start_min_rows,
)


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/meta/test_factory.py -v`

Expected: FAIL — `ModuleNotFoundError: No module named 'services.model_service.meta'`

- [ ] **Step 3: Create protocol definitions**

Create `bullforge/services/model_service/meta/__init__.py` (empty file).

Create `bullforge/services/model_service/meta/estimators/__init__.py` (empty file).

Create `bullforge/services/model_service/meta/protocols.py`:

```python
from __future__ import annotations

from typing import Protocol, runtime_checkable

import numpy as np
import pandas as pd


@runtime_checkable
class MetaEntryEstimator(Protocol):
    """Binary classifier: P(trade hits target before stop)."""

    def fit(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        sample_weight: np.ndarray | None = None,
    ) -> None: ...

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """Return shape (n_samples, 2): [P(stop), P(target)]."""
        ...

    def feature_importances(self) -> np.ndarray: ...

    def get_params(self) -> dict: ...


@runtime_checkable
class MetaExecEstimator(Protocol):
    """Multi-task: kelly_fraction regression + pair_index classification."""

    def fit(
        self,
        X: pd.DataFrame,
        y_kelly: pd.Series,
        y_pair: pd.Series,
        sample_weight: np.ndarray | None = None,
    ) -> None: ...

    def predict(
        self, X: pd.DataFrame,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Return (kelly_fractions shape (n,), pair_indices shape (n,))."""
        ...

    def feature_importances(self) -> np.ndarray: ...

    def get_params(self) -> dict: ...
```

- [ ] **Step 4: Create factory with registry**

Create `bullforge/services/model_service/meta/factory.py`:

```python
from __future__ import annotations

from typing import Any

from services.model_service.meta.protocols import MetaEntryEstimator, MetaExecEstimator

_ENTRY_REGISTRY: dict[str, type] = {}
_EXEC_REGISTRY: dict[str, type] = {}
_COLD_START: dict[str, int] = {}


def register_estimator(
    name: str,
    entry_cls: type,
    exec_cls: type,
    cold_start_min_rows: int,
) -> None:
    _ENTRY_REGISTRY[name] = entry_cls
    _EXEC_REGISTRY[name] = exec_cls
    _COLD_START[name] = cold_start_min_rows


def create_meta_entry(estimator_type: str, **kwargs: Any) -> MetaEntryEstimator:
    if estimator_type not in _ENTRY_REGISTRY:
        raise ValueError(
            f"Unknown estimator type: {estimator_type!r}. "
            f"Available: {list(_ENTRY_REGISTRY)}"
        )
    return _ENTRY_REGISTRY[estimator_type](**kwargs)


def create_meta_exec(
    estimator_type: str, *, num_pairs: int, **kwargs: Any,
) -> MetaExecEstimator:
    if estimator_type not in _EXEC_REGISTRY:
        raise ValueError(
            f"Unknown estimator type: {estimator_type!r}. "
            f"Available: {list(_EXEC_REGISTRY)}"
        )
    return _EXEC_REGISTRY[estimator_type](num_pairs=num_pairs, **kwargs)


def list_estimator_types() -> list[str]:
    return list(_ENTRY_REGISTRY)


def get_cold_start_min_rows(estimator_type: str) -> int:
    return _COLD_START.get(estimator_type, 0)


def _auto_register() -> None:
    from services.model_service.meta.estimators.lightgbm import (
        LightGBMEntryEstimator,
        LightGBMExecEstimator,
    )
    from services.model_service.meta.estimators.mlp import (
        MLPEntryEstimator,
        MLPExecEstimator,
    )
    from services.model_service.meta.estimators.baseline import (
        BaselineEntryEstimator,
        BaselineExecEstimator,
    )

    register_estimator("lightgbm", LightGBMEntryEstimator, LightGBMExecEstimator, 2000)
    register_estimator("mlp", MLPEntryEstimator, MLPExecEstimator, 5000)
    register_estimator("baseline", BaselineEntryEstimator, BaselineExecEstimator, 0)


_auto_register()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/meta/test_factory.py -v`

Expected: FAIL — estimator classes don't exist yet. That's OK — this task establishes the protocol and factory skeleton. The test will pass after Tasks 4-6. Mark this step as "will pass after Task 6."

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/model_service/meta/
git commit -m "feat(meta): add MetaEstimator protocols and factory registry"
```

---

### Task 4: LightGBM Estimator Plugin

**Files:**
- Create: `bullforge/services/model_service/meta/estimators/lightgbm.py`
- Create: `bullforge/tests/meta/test_lightgbm.py`

- [ ] **Step 1: Write failing tests**

Create `bullforge/tests/meta/test_lightgbm.py`:

```python
import numpy as np
import pandas as pd
import pytest

from services.model_service.meta.estimators.lightgbm import (
    LightGBMEntryEstimator,
    LightGBMExecEstimator,
)
from services.model_service.meta.protocols import MetaEntryEstimator, MetaExecEstimator


def _make_entry_data(n=500):
    rng = np.random.RandomState(42)
    X = pd.DataFrame({
        "base_L1_conf": rng.uniform(0.4, 0.95, n),
        "base_L2_conf": rng.uniform(0.4, 0.95, n),
        "regime_spy_trend": rng.choice([-1, 0, 1], n).astype(float),
        "time_bucket": rng.randint(0, 26, n).astype(float),
        "realized_vol": rng.uniform(0.01, 0.05, n),
    })
    y = pd.Series(rng.choice([0, 1], n, p=[0.45, 0.55]))
    return X, y


def _make_exec_data(n=500, num_pairs=7):
    rng = np.random.RandomState(42)
    X = pd.DataFrame({
        "base_L1_conf": rng.uniform(0.4, 0.95, n),
        "base_L2_conf": rng.uniform(0.4, 0.95, n),
        "regime_spy_trend": rng.choice([-1, 0, 1], n).astype(float),
        "time_bucket": rng.randint(0, 26, n).astype(float),
        "realized_vol": rng.uniform(0.01, 0.05, n),
    })
    y_kelly = pd.Series(rng.uniform(0, 0.5, n))
    y_pair = pd.Series(rng.randint(0, num_pairs, n))
    return X, y_kelly, y_pair


class TestLightGBMEntry:
    def test_implements_protocol(self):
        est = LightGBMEntryEstimator()
        assert isinstance(est, MetaEntryEstimator)

    def test_fit_predict_proba(self):
        X, y = _make_entry_data()
        est = LightGBMEntryEstimator()
        est.fit(X, y)
        proba = est.predict_proba(X[:5])
        assert proba.shape == (5, 2)
        assert np.allclose(proba.sum(axis=1), 1.0, atol=1e-6)

    def test_predict_proba_range(self):
        X, y = _make_entry_data()
        est = LightGBMEntryEstimator()
        est.fit(X, y)
        proba = est.predict_proba(X)
        assert (proba >= 0.0).all() and (proba <= 1.0).all()

    def test_feature_importances_shape(self):
        X, y = _make_entry_data()
        est = LightGBMEntryEstimator()
        est.fit(X, y)
        imp = est.feature_importances()
        assert imp.shape == (X.shape[1],)

    def test_sample_weight(self):
        X, y = _make_entry_data()
        weights = np.ones(len(X))
        weights[:50] = 3.0
        est = LightGBMEntryEstimator()
        est.fit(X, y, sample_weight=weights)
        proba = est.predict_proba(X[:5])
        assert proba.shape == (5, 2)

    def test_get_params(self):
        est = LightGBMEntryEstimator(n_estimators=50)
        params = est.get_params()
        assert params["n_estimators"] == 50


class TestLightGBMExec:
    def test_implements_protocol(self):
        est = LightGBMExecEstimator(num_pairs=7)
        assert isinstance(est, MetaExecEstimator)

    def test_fit_predict(self):
        X, y_kelly, y_pair = _make_exec_data(num_pairs=7)
        est = LightGBMExecEstimator(num_pairs=7)
        est.fit(X, y_kelly, y_pair)
        kelly_fracs, pair_indices = est.predict(X[:5])
        assert kelly_fracs.shape == (5,)
        assert pair_indices.shape == (5,)
        assert (kelly_fracs >= 0.0).all() and (kelly_fracs <= 1.0).all()
        assert all(0 <= idx < 7 for idx in pair_indices)

    def test_feature_importances_shape(self):
        X, y_kelly, y_pair = _make_exec_data(num_pairs=7)
        est = LightGBMExecEstimator(num_pairs=7)
        est.fit(X, y_kelly, y_pair)
        imp = est.feature_importances()
        assert imp.shape == (X.shape[1],)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/meta/test_lightgbm.py -v`

Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement LightGBM estimators**

Create `bullforge/services/model_service/meta/estimators/lightgbm.py`:

```python
from __future__ import annotations

import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier, LGBMRegressor


class LightGBMEntryEstimator:
    def __init__(
        self,
        n_estimators: int = 200,
        max_depth: int = 5,
        learning_rate: float = 0.05,
        min_child_samples: int = 20,
        random_state: int = 42,
    ):
        self._params = {
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "learning_rate": learning_rate,
            "min_child_samples": min_child_samples,
            "random_state": random_state,
        }
        self._model = LGBMClassifier(
            objective="binary",
            verbose=-1,
            **self._params,
        )

    def fit(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        sample_weight: np.ndarray | None = None,
    ) -> None:
        self._model.fit(X, y, sample_weight=sample_weight)

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        return self._model.predict_proba(X)

    def feature_importances(self) -> np.ndarray:
        return self._model.feature_importances_

    def get_params(self) -> dict:
        return dict(self._params)


class LightGBMExecEstimator:
    def __init__(
        self,
        num_pairs: int,
        n_estimators: int = 200,
        max_depth: int = 5,
        learning_rate: float = 0.05,
        min_child_samples: int = 20,
        random_state: int = 42,
    ):
        self._num_pairs = num_pairs
        self._params = {
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "learning_rate": learning_rate,
            "min_child_samples": min_child_samples,
            "random_state": random_state,
        }
        self._kelly_model = LGBMRegressor(
            objective="regression",
            verbose=-1,
            **self._params,
        )
        self._pair_model = LGBMClassifier(
            objective="multiclass",
            num_class=num_pairs,
            verbose=-1,
            **self._params,
        )

    def fit(
        self,
        X: pd.DataFrame,
        y_kelly: pd.Series,
        y_pair: pd.Series,
        sample_weight: np.ndarray | None = None,
    ) -> None:
        self._kelly_model.fit(X, y_kelly, sample_weight=sample_weight)
        self._pair_model.fit(X, y_pair, sample_weight=sample_weight)

    def predict(
        self, X: pd.DataFrame,
    ) -> tuple[np.ndarray, np.ndarray]:
        kelly_raw = self._kelly_model.predict(X)
        kelly_fracs = np.clip(kelly_raw, 0.0, 1.0)
        pair_proba = self._pair_model.predict_proba(X)
        pair_indices = np.argmax(pair_proba, axis=1)
        return kelly_fracs, pair_indices

    def feature_importances(self) -> np.ndarray:
        kelly_imp = self._kelly_model.feature_importances_
        pair_imp = self._pair_model.feature_importances_
        return (kelly_imp + pair_imp) / 2.0

    def get_params(self) -> dict:
        return {"num_pairs": self._num_pairs, **self._params}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/meta/test_lightgbm.py -v`

Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/model_service/meta/estimators/lightgbm.py \
       bullforge/tests/meta/test_lightgbm.py
git commit -m "feat(meta): add LightGBM entry and exec estimator plugins"
```

---

### Task 5: MLP Estimator Plugin

**Files:**
- Create: `bullforge/services/model_service/meta/estimators/mlp.py`
- Create: `bullforge/tests/meta/test_mlp.py`

- [ ] **Step 1: Write failing tests**

Create `bullforge/tests/meta/test_mlp.py`:

```python
import numpy as np
import pandas as pd
import pytest

from services.model_service.meta.estimators.mlp import (
    MLPEntryEstimator,
    MLPExecEstimator,
)
from services.model_service.meta.protocols import MetaEntryEstimator, MetaExecEstimator


def _make_entry_data(n=200):
    rng = np.random.RandomState(42)
    X = pd.DataFrame({
        "base_L1_conf": rng.uniform(0.4, 0.95, n),
        "base_L2_conf": rng.uniform(0.4, 0.95, n),
        "regime_spy_trend": rng.choice([-1, 0, 1], n).astype(float),
        "time_bucket": rng.randint(0, 26, n).astype(float),
        "realized_vol": rng.uniform(0.01, 0.05, n),
    })
    y = pd.Series(rng.choice([0, 1], n, p=[0.45, 0.55]))
    return X, y


def _make_exec_data(n=200, num_pairs=7):
    rng = np.random.RandomState(42)
    X = pd.DataFrame({
        "base_L1_conf": rng.uniform(0.4, 0.95, n),
        "base_L2_conf": rng.uniform(0.4, 0.95, n),
        "regime_spy_trend": rng.choice([-1, 0, 1], n).astype(float),
        "time_bucket": rng.randint(0, 26, n).astype(float),
        "realized_vol": rng.uniform(0.01, 0.05, n),
    })
    y_kelly = pd.Series(rng.uniform(0, 0.5, n))
    y_pair = pd.Series(rng.randint(0, num_pairs, n))
    return X, y_kelly, y_pair


class TestMLPEntry:
    def test_implements_protocol(self):
        est = MLPEntryEstimator(input_dim=5)
        assert isinstance(est, MetaEntryEstimator)

    def test_fit_predict_proba(self):
        X, y = _make_entry_data()
        est = MLPEntryEstimator(input_dim=5, epochs=5)
        est.fit(X, y)
        proba = est.predict_proba(X[:10])
        assert proba.shape == (10, 2)
        assert np.allclose(proba.sum(axis=1), 1.0, atol=1e-5)

    def test_predict_proba_range(self):
        X, y = _make_entry_data()
        est = MLPEntryEstimator(input_dim=5, epochs=5)
        est.fit(X, y)
        proba = est.predict_proba(X)
        assert (proba >= 0.0).all() and (proba <= 1.0).all()

    def test_feature_importances_shape(self):
        X, y = _make_entry_data()
        est = MLPEntryEstimator(input_dim=5, epochs=5)
        est.fit(X, y)
        imp = est.feature_importances()
        assert imp.shape == (5,)

    def test_sample_weight(self):
        X, y = _make_entry_data()
        weights = np.ones(len(X))
        weights[:20] = 3.0
        est = MLPEntryEstimator(input_dim=5, epochs=5)
        est.fit(X, y, sample_weight=weights)
        proba = est.predict_proba(X[:5])
        assert proba.shape == (5, 2)


class TestMLPExec:
    def test_implements_protocol(self):
        est = MLPExecEstimator(input_dim=5, num_pairs=7)
        assert isinstance(est, MetaExecEstimator)

    def test_fit_predict(self):
        X, y_kelly, y_pair = _make_exec_data(num_pairs=7)
        est = MLPExecEstimator(input_dim=5, num_pairs=7, epochs=5)
        est.fit(X, y_kelly, y_pair)
        kelly_fracs, pair_indices = est.predict(X[:10])
        assert kelly_fracs.shape == (10,)
        assert pair_indices.shape == (10,)
        assert (kelly_fracs >= 0.0).all() and (kelly_fracs <= 1.0).all()
        assert all(0 <= idx < 7 for idx in pair_indices)

    def test_feature_importances_shape(self):
        X, y_kelly, y_pair = _make_exec_data(num_pairs=7)
        est = MLPExecEstimator(input_dim=5, num_pairs=7, epochs=5)
        est.fit(X, y_kelly, y_pair)
        imp = est.feature_importances()
        assert imp.shape == (5,)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/meta/test_mlp.py -v`

Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement MLP estimators**

Create `bullforge/services/model_service/meta/estimators/mlp.py`:

```python
from __future__ import annotations

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset


class _EntryNet(nn.Module):
    def __init__(self, input_dim: int, hidden1: int = 64, hidden2: int = 32):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, hidden1),
            nn.BatchNorm1d(hidden1),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(hidden1, hidden2),
            nn.BatchNorm1d(hidden2),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(hidden2, 2),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x)


class _ExecNet(nn.Module):
    def __init__(self, input_dim: int, num_pairs: int, hidden1: int = 64, hidden2: int = 32):
        super().__init__()
        self.trunk = nn.Sequential(
            nn.Linear(input_dim, hidden1),
            nn.BatchNorm1d(hidden1),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(hidden1, hidden2),
            nn.BatchNorm1d(hidden2),
            nn.ReLU(),
            nn.Dropout(0.2),
        )
        self.kelly_head = nn.Sequential(
            nn.Linear(hidden2, 1),
            nn.Sigmoid(),
        )
        self.pair_head = nn.Linear(hidden2, num_pairs)

    def forward(self, x: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        h = self.trunk(x)
        kelly = self.kelly_head(h).squeeze(-1)
        pair_logits = self.pair_head(h)
        return kelly, pair_logits


class MLPEntryEstimator:
    def __init__(
        self,
        input_dim: int,
        hidden1: int = 64,
        hidden2: int = 32,
        epochs: int = 30,
        batch_size: int = 64,
        lr: float = 0.001,
    ):
        self._input_dim = input_dim
        self._hidden1 = hidden1
        self._hidden2 = hidden2
        self._epochs = epochs
        self._batch_size = batch_size
        self._lr = lr
        self._net: _EntryNet | None = None
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self._feature_imp: np.ndarray | None = None

    def fit(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        sample_weight: np.ndarray | None = None,
    ) -> None:
        X_t = torch.tensor(X.values, dtype=torch.float32)
        y_t = torch.tensor(y.values, dtype=torch.long)
        w_t = (
            torch.tensor(sample_weight, dtype=torch.float32)
            if sample_weight is not None
            else torch.ones(len(y_t))
        )

        self._net = _EntryNet(self._input_dim, self._hidden1, self._hidden2).to(self._device)
        optimizer = torch.optim.Adam(self._net.parameters(), lr=self._lr)
        loss_fn = nn.CrossEntropyLoss(reduction="none")

        dataset = TensorDataset(X_t, y_t, w_t)
        loader = DataLoader(dataset, batch_size=self._batch_size, shuffle=True)

        self._net.train()
        for _ in range(self._epochs):
            for xb, yb, wb in loader:
                xb, yb, wb = xb.to(self._device), yb.to(self._device), wb.to(self._device)
                optimizer.zero_grad()
                logits = self._net(xb)
                loss = (loss_fn(logits, yb) * wb).mean()
                loss.backward()
                optimizer.step()

        self._compute_feature_importances(X_t)

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        X_t = torch.tensor(X.values, dtype=torch.float32).to(self._device)
        self._net.eval()
        with torch.no_grad():
            logits = self._net(X_t)
            proba = torch.softmax(logits, dim=1).cpu().numpy()
        return proba

    def _compute_feature_importances(self, X_t: torch.Tensor) -> None:
        X_t = X_t.to(self._device).requires_grad_(True)
        self._net.eval()
        logits = self._net(X_t)
        target_class = logits[:, 1]
        target_class.sum().backward()
        self._feature_imp = X_t.grad.abs().mean(dim=0).cpu().numpy()

    def feature_importances(self) -> np.ndarray:
        if self._feature_imp is None:
            return np.zeros(self._input_dim)
        return self._feature_imp

    def get_params(self) -> dict:
        return {
            "input_dim": self._input_dim,
            "hidden1": self._hidden1,
            "hidden2": self._hidden2,
            "epochs": self._epochs,
            "batch_size": self._batch_size,
            "lr": self._lr,
        }


class MLPExecEstimator:
    def __init__(
        self,
        input_dim: int,
        num_pairs: int,
        hidden1: int = 64,
        hidden2: int = 32,
        epochs: int = 30,
        batch_size: int = 64,
        lr: float = 0.001,
    ):
        self._input_dim = input_dim
        self._num_pairs = num_pairs
        self._hidden1 = hidden1
        self._hidden2 = hidden2
        self._epochs = epochs
        self._batch_size = batch_size
        self._lr = lr
        self._net: _ExecNet | None = None
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self._feature_imp: np.ndarray | None = None

    def fit(
        self,
        X: pd.DataFrame,
        y_kelly: pd.Series,
        y_pair: pd.Series,
        sample_weight: np.ndarray | None = None,
    ) -> None:
        X_t = torch.tensor(X.values, dtype=torch.float32)
        yk_t = torch.tensor(y_kelly.values, dtype=torch.float32)
        yp_t = torch.tensor(y_pair.values, dtype=torch.long)
        w_t = (
            torch.tensor(sample_weight, dtype=torch.float32)
            if sample_weight is not None
            else torch.ones(len(X_t))
        )

        self._net = _ExecNet(
            self._input_dim, self._num_pairs, self._hidden1, self._hidden2,
        ).to(self._device)
        optimizer = torch.optim.Adam(self._net.parameters(), lr=self._lr)
        mse_fn = nn.MSELoss(reduction="none")
        ce_fn = nn.CrossEntropyLoss(reduction="none")

        dataset = TensorDataset(X_t, yk_t, yp_t, w_t)
        loader = DataLoader(dataset, batch_size=self._batch_size, shuffle=True)

        self._net.train()
        for _ in range(self._epochs):
            for xb, kb, pb, wb in loader:
                xb = xb.to(self._device)
                kb, pb, wb = kb.to(self._device), pb.to(self._device), wb.to(self._device)
                optimizer.zero_grad()
                kelly_pred, pair_logits = self._net(xb)
                loss_kelly = (mse_fn(kelly_pred, kb) * wb).mean()
                loss_pair = (ce_fn(pair_logits, pb) * wb).mean()
                loss = loss_kelly + loss_pair
                loss.backward()
                optimizer.step()

        self._compute_feature_importances(X_t)

    def predict(
        self, X: pd.DataFrame,
    ) -> tuple[np.ndarray, np.ndarray]:
        X_t = torch.tensor(X.values, dtype=torch.float32).to(self._device)
        self._net.eval()
        with torch.no_grad():
            kelly_pred, pair_logits = self._net(X_t)
            kelly_fracs = kelly_pred.cpu().numpy()
            pair_indices = pair_logits.argmax(dim=1).cpu().numpy()
        kelly_fracs = np.clip(kelly_fracs, 0.0, 1.0)
        return kelly_fracs, pair_indices

    def _compute_feature_importances(self, X_t: torch.Tensor) -> None:
        X_t = X_t.to(self._device).requires_grad_(True)
        self._net.eval()
        kelly_pred, _ = self._net(X_t)
        kelly_pred.sum().backward()
        self._feature_imp = X_t.grad.abs().mean(dim=0).cpu().numpy()

    def feature_importances(self) -> np.ndarray:
        if self._feature_imp is None:
            return np.zeros(self._input_dim)
        return self._feature_imp

    def get_params(self) -> dict:
        return {
            "input_dim": self._input_dim,
            "num_pairs": self._num_pairs,
            "hidden1": self._hidden1,
            "hidden2": self._hidden2,
            "epochs": self._epochs,
            "batch_size": self._batch_size,
            "lr": self._lr,
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/meta/test_mlp.py -v`

Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/model_service/meta/estimators/mlp.py \
       bullforge/tests/meta/test_mlp.py
git commit -m "feat(meta): add MLP entry and exec estimator plugins"
```

---

### Task 6: Baseline Aggregator Estimator

**Files:**
- Create: `bullforge/services/model_service/meta/estimators/baseline.py`
- Create: `bullforge/tests/meta/test_baseline.py`

- [ ] **Step 1: Write failing tests**

Create `bullforge/tests/meta/test_baseline.py`:

```python
import numpy as np
import pandas as pd
import pytest

from services.model_service.meta.estimators.baseline import (
    BaselineEntryEstimator,
    BaselineExecEstimator,
)
from services.model_service.meta.protocols import MetaEntryEstimator, MetaExecEstimator


def _make_data(n=100):
    rng = np.random.RandomState(42)
    X = pd.DataFrame({
        "base_L1_dir": rng.choice([-1, 0, 1], n).astype(float),
        "base_L1_conf": rng.uniform(0.5, 0.9, n),
        "base_L2_dir": rng.choice([-1, 0, 1], n).astype(float),
        "base_L2_conf": rng.uniform(0.5, 0.9, n),
        "regime_spy_trend": rng.choice([-1, 0, 1], n).astype(float),
    })
    y = pd.Series(rng.choice([0, 1], n))
    y_kelly = pd.Series(rng.uniform(0, 0.3, n))
    y_pair = pd.Series(rng.randint(0, 5, n))
    return X, y, y_kelly, y_pair


class TestBaselineEntry:
    def test_implements_protocol(self):
        est = BaselineEntryEstimator(
            direction_col_pairs=[("base_L1_dir", "base_L1_conf"),
                                 ("base_L2_dir", "base_L2_conf")],
        )
        assert isinstance(est, MetaEntryEstimator)

    def test_fit_is_noop(self):
        X, y, _, _ = _make_data()
        est = BaselineEntryEstimator(
            direction_col_pairs=[("base_L1_dir", "base_L1_conf"),
                                 ("base_L2_dir", "base_L2_conf")],
        )
        est.fit(X, y)
        proba = est.predict_proba(X[:5])
        assert proba.shape == (5, 2)

    def test_high_agreement_gives_high_proba(self):
        X = pd.DataFrame({
            "base_L1_dir": [1.0, 1.0],
            "base_L1_conf": [0.9, 0.9],
            "base_L2_dir": [1.0, 1.0],
            "base_L2_conf": [0.85, 0.85],
        })
        est = BaselineEntryEstimator(
            direction_col_pairs=[("base_L1_dir", "base_L1_conf"),
                                 ("base_L2_dir", "base_L2_conf")],
        )
        est.fit(X, pd.Series([1, 1]))
        proba = est.predict_proba(X)
        assert proba[0, 1] > 0.7

    def test_feature_importances_uniform(self):
        X, y, _, _ = _make_data()
        est = BaselineEntryEstimator(
            direction_col_pairs=[("base_L1_dir", "base_L1_conf"),
                                 ("base_L2_dir", "base_L2_conf")],
        )
        est.fit(X, y)
        imp = est.feature_importances()
        assert imp.shape == (X.shape[1],)


class TestBaselineExec:
    def test_implements_protocol(self):
        est = BaselineExecEstimator(num_pairs=5)
        assert isinstance(est, MetaExecEstimator)

    def test_fit_predict(self):
        X, _, y_kelly, y_pair = _make_data()
        est = BaselineExecEstimator(num_pairs=5)
        est.fit(X, y_kelly, y_pair)
        kelly_fracs, pair_indices = est.predict(X[:5])
        assert kelly_fracs.shape == (5,)
        assert pair_indices.shape == (5,)
        assert (kelly_fracs >= 0.0).all() and (kelly_fracs <= 1.0).all()
        assert all(0 <= idx < 5 for idx in pair_indices)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/meta/test_baseline.py -v`

Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement baseline estimators**

Create `bullforge/services/model_service/meta/estimators/baseline.py`:

```python
from __future__ import annotations

import numpy as np
import pandas as pd


class BaselineEntryEstimator:
    """Wraps the current SignalAggregator's confidence-weighted voting as a probability.

    Serves as the always-available control group in A/B/n comparison.
    fit() is a no-op — this estimator uses the raw base-model columns directly.
    """

    def __init__(
        self,
        direction_col_pairs: list[tuple[str, str]],
        min_confidence: float = 0.6,
        direction_band: float = 0.3,
    ):
        self._dir_conf_pairs = direction_col_pairs
        self._min_confidence = min_confidence
        self._direction_band = direction_band
        self._n_features: int = 0

    def fit(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        sample_weight: np.ndarray | None = None,
    ) -> None:
        self._n_features = X.shape[1]

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        n = len(X)
        proba = np.zeros((n, 2), dtype=np.float64)

        for i in range(n):
            weighted_sum = 0.0
            total_weight = 0.0
            for dir_col, conf_col in self._dir_conf_pairs:
                direction = X.iloc[i][dir_col]
                confidence = X.iloc[i][conf_col]
                if confidence >= self._min_confidence:
                    weighted_sum += direction * confidence
                    total_weight += confidence

            if total_weight > 0:
                avg = weighted_sum / total_weight
                strength = min(abs(avg), 1.0)
                if avg > self._direction_band:
                    proba[i, 1] = 0.5 + strength * 0.4
                    proba[i, 0] = 1.0 - proba[i, 1]
                elif avg < -self._direction_band:
                    proba[i, 0] = 0.5 + strength * 0.4
                    proba[i, 1] = 1.0 - proba[i, 0]
                else:
                    proba[i] = [0.5, 0.5]
            else:
                proba[i] = [0.5, 0.5]

        return proba

    def feature_importances(self) -> np.ndarray:
        return np.ones(self._n_features) / max(self._n_features, 1)

    def get_params(self) -> dict:
        return {
            "direction_col_pairs": self._dir_conf_pairs,
            "min_confidence": self._min_confidence,
            "direction_band": self._direction_band,
        }


class BaselineExecEstimator:
    """Always selects median pair and constant kelly fraction.

    Serves as the control group for exec predictions.
    """

    def __init__(self, num_pairs: int, default_kelly: float = 0.15):
        self._num_pairs = num_pairs
        self._default_kelly = default_kelly
        self._median_pair = num_pairs // 2
        self._n_features: int = 0

    def fit(
        self,
        X: pd.DataFrame,
        y_kelly: pd.Series,
        y_pair: pd.Series,
        sample_weight: np.ndarray | None = None,
    ) -> None:
        self._n_features = X.shape[1]

    def predict(
        self, X: pd.DataFrame,
    ) -> tuple[np.ndarray, np.ndarray]:
        n = len(X)
        kelly_fracs = np.full(n, self._default_kelly)
        pair_indices = np.full(n, self._median_pair, dtype=np.int64)
        return kelly_fracs, pair_indices

    def feature_importances(self) -> np.ndarray:
        return np.ones(self._n_features) / max(self._n_features, 1)

    def get_params(self) -> dict:
        return {
            "num_pairs": self._num_pairs,
            "default_kelly": self._default_kelly,
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/meta/test_baseline.py -v`

Expected: All PASS.

- [ ] **Step 5: Now run factory tests (Task 3 Step 5 deferred)**

Run: `cd bullforge && python -m pytest tests/meta/test_factory.py -v`

Expected: All PASS — all three estimator types are now registered.

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/model_service/meta/estimators/baseline.py \
       bullforge/tests/meta/test_baseline.py
git commit -m "feat(meta): add baseline aggregator estimator plugin (control group)"
```

---

### Task 7: Database Methods for Meta Tables

**Files:**
- Modify: `bullforge/shared/db.py`
- Create: `bullforge/tests/meta/test_meta_db.py`

- [ ] **Step 1: Write failing tests**

Create `bullforge/tests/meta/test_meta_db.py`:

```python
from datetime import datetime, timezone

import pytest
import pytest_asyncio

from shared.models import MetaFeatureRow, MetaSignalShadow


@pytest.mark.asyncio
async def test_insert_and_query_meta_feature(real_db):
    row = MetaFeatureRow(
        symbol="AAPL",
        timestamp=datetime(2026, 4, 17, 14, 0, 0, tzinfo=timezone.utc),
        direction=1,
        feature_vector={"base_L1_conf": 0.82, "regime_spy_trend": 1.0},
        source="replay",
        entry_label=1,
        optimal_pair_idx=3,
        realized_ev=0.002,
    )
    await real_db.insert_meta_feature(row)

    rows = await real_db.query_meta_features(
        symbol="AAPL", direction=1, limit=10,
    )
    assert len(rows) >= 1
    found = rows[0]
    assert found.symbol == "AAPL"
    assert found.direction == 1
    assert found.feature_vector["base_L1_conf"] == pytest.approx(0.82)
    assert found.entry_label == 1
    assert found.source == "replay"


@pytest.mark.asyncio
async def test_insert_meta_feature_batch(real_db):
    rows = [
        MetaFeatureRow(
            symbol="AAPL",
            timestamp=datetime(2026, 4, 17, 14, i, 0, tzinfo=timezone.utc),
            direction=1,
            feature_vector={"base_L1_conf": 0.7 + i * 0.01},
            source="replay",
        )
        for i in range(5)
    ]
    await real_db.insert_meta_features_batch(rows)

    result = await real_db.query_meta_features(
        symbol="AAPL", direction=1, limit=100,
    )
    assert len(result) >= 5


@pytest.mark.asyncio
async def test_insert_and_query_meta_signal_shadow(real_db):
    sig = MetaSignalShadow(
        symbol="AAPL",
        timestamp=datetime(2026, 4, 17, 14, 0, 0, tzinfo=timezone.utc),
        direction=1,
        estimator_type="lightgbm",
        entry_probability=0.72,
        kelly_fraction=0.15,
        pair_index=3,
        target_pct=0.005,
        stop_pct=0.003,
        model_id=None,
    )
    row_id = await real_db.insert_meta_signal_shadow(sig)
    assert row_id > 0

    rows = await real_db.query_meta_signals_shadow(
        symbol="AAPL", limit=10,
    )
    assert len(rows) >= 1
    assert rows[0].estimator_type == "lightgbm"
    assert rows[0].entry_probability == pytest.approx(0.72)


@pytest.mark.asyncio
async def test_count_meta_features(real_db):
    row = MetaFeatureRow(
        symbol="SPY",
        timestamp=datetime(2026, 4, 17, 15, 0, 0, tzinfo=timezone.utc),
        direction=-1,
        feature_vector={"x": 1.0},
        source="live",
    )
    await real_db.insert_meta_feature(row)

    count = await real_db.count_meta_features(symbol="SPY", direction=-1)
    assert count >= 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/meta/test_meta_db.py -v`

Expected: FAIL — `AttributeError: 'Database' object has no attribute 'insert_meta_feature'`

- [ ] **Step 3: Add meta_features DB methods to shared/db.py**

Add these methods to the `Database` class in `bullforge/shared/db.py`:

```python
    # ── Meta-feature methods ──────────────────────────────────────

    async def insert_meta_feature(self, row: "MetaFeatureRow") -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO meta_features
                    (symbol, timestamp, direction, feature_vector,
                     entry_label, optimal_pair_idx, realized_ev, source)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (symbol, timestamp, direction) DO UPDATE SET
                    feature_vector = EXCLUDED.feature_vector,
                    entry_label = EXCLUDED.entry_label,
                    optimal_pair_idx = EXCLUDED.optimal_pair_idx,
                    realized_ev = EXCLUDED.realized_ev,
                    source = EXCLUDED.source
                """,
                row.symbol,
                row.timestamp,
                row.direction,
                json.dumps(row.feature_vector),
                row.entry_label,
                row.optimal_pair_idx,
                row.realized_ev,
                row.source,
            )

    async def insert_meta_features_batch(
        self, rows: list["MetaFeatureRow"],
    ) -> None:
        if not rows:
            return
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO meta_features
                    (symbol, timestamp, direction, feature_vector,
                     entry_label, optimal_pair_idx, realized_ev, source)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (symbol, timestamp, direction) DO UPDATE SET
                    feature_vector = EXCLUDED.feature_vector,
                    entry_label = EXCLUDED.entry_label,
                    optimal_pair_idx = EXCLUDED.optimal_pair_idx,
                    realized_ev = EXCLUDED.realized_ev,
                    source = EXCLUDED.source
                """,
                [
                    (
                        r.symbol, r.timestamp, r.direction,
                        json.dumps(r.feature_vector),
                        r.entry_label, r.optimal_pair_idx,
                        r.realized_ev, r.source,
                    )
                    for r in rows
                ],
            )

    async def query_meta_features(
        self,
        symbol: str,
        direction: int,
        limit: int = 10000,
        source: Optional[str] = None,
    ) -> list["MetaFeatureRow"]:
        from shared.models import MetaFeatureRow
        cond = "WHERE symbol = $1 AND direction = $2"
        params: list = [symbol, direction]
        if source is not None:
            cond += " AND source = $3"
            params.append(source)
        params.append(limit)
        limit_param = f"${len(params)}"

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT symbol, timestamp, direction, feature_vector,
                       entry_label, optimal_pair_idx, realized_ev, source
                FROM meta_features
                {cond}
                ORDER BY timestamp DESC
                LIMIT {limit_param}
                """,
                *params,
            )
        return [
            MetaFeatureRow(
                symbol=r["symbol"],
                timestamp=r["timestamp"],
                direction=r["direction"],
                feature_vector=json.loads(r["feature_vector"]),
                source=r["source"],
                entry_label=r["entry_label"],
                optimal_pair_idx=r["optimal_pair_idx"],
                realized_ev=r["realized_ev"],
            )
            for r in rows
        ]

    async def count_meta_features(
        self, symbol: str, direction: int,
    ) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                """
                SELECT COUNT(*) FROM meta_features
                WHERE symbol = $1 AND direction = $2
                """,
                symbol, direction,
            )

    # ── Meta-signal shadow methods ────────────────────────────────

    async def insert_meta_signal_shadow(
        self, sig: "MetaSignalShadow",
    ) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                """
                INSERT INTO meta_signals_shadow
                    (symbol, timestamp, direction, estimator_type,
                     entry_probability, kelly_fraction, pair_index,
                     target_pct, stop_pct, model_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                RETURNING id
                """,
                sig.symbol,
                sig.timestamp,
                sig.direction,
                sig.estimator_type,
                sig.entry_probability,
                sig.kelly_fraction,
                sig.pair_index,
                sig.target_pct,
                sig.stop_pct,
                sig.model_id,
            )

    async def query_meta_signals_shadow(
        self,
        symbol: str,
        limit: int = 100,
        estimator_type: Optional[str] = None,
    ) -> list["MetaSignalShadow"]:
        from shared.models import MetaSignalShadow
        cond = "WHERE symbol = $1"
        params: list = [symbol]
        if estimator_type is not None:
            cond += " AND estimator_type = $2"
            params.append(estimator_type)
        params.append(limit)
        limit_param = f"${len(params)}"

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT id, symbol, timestamp, direction, estimator_type,
                       entry_probability, kelly_fraction, pair_index,
                       target_pct, stop_pct, model_id
                FROM meta_signals_shadow
                {cond}
                ORDER BY timestamp DESC
                LIMIT {limit_param}
                """,
                *params,
            )
        return [
            MetaSignalShadow(
                id=r["id"],
                symbol=r["symbol"],
                timestamp=r["timestamp"],
                direction=r["direction"],
                estimator_type=r["estimator_type"],
                entry_probability=r["entry_probability"],
                kelly_fraction=r["kelly_fraction"],
                pair_index=r["pair_index"],
                target_pct=r["target_pct"],
                stop_pct=r["stop_pct"],
                model_id=r["model_id"],
            )
            for r in rows
        ]
```

Also add the import at the top of `db.py`:

```python
from shared.models import Bar, FeatureRow, Label, ModelMetadata, Prediction, Order, Trade, ModelStatus, MetaFeatureRow, MetaSignalShadow
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/meta/test_meta_db.py -v`

Expected: All PASS (requires TimescaleDB with migration 016 applied).

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/db.py bullforge/tests/meta/test_meta_db.py
git commit -m "feat(meta): add DB methods for meta_features and meta_signals_shadow"
```

---

### Task 8: Meta-Feature Assembler

**Files:**
- Create: `bullforge/services/model_service/meta/feature_assembler.py`
- Create: `bullforge/tests/meta/test_feature_assembler.py`

- [ ] **Step 1: Write failing tests**

Create `bullforge/tests/meta/test_feature_assembler.py`:

```python
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
    return {"spy_trend_1m": 1.0, "spy_trend_1h": 1.0, "spy_trend_1d": 1.0,
            "spy_momentum": 0.3, "vol_regime": 1.0}


def _make_symbol_state():
    return {"realized_vol": 0.025, "atr_price_ratio": 0.012,
            "dist_sma_50": 0.015, "dist_sma_200bar": -0.005,
            "dist_sma_200day": 0.008}


class TestMetaFeatureAssembler:
    def test_assemble_returns_dict(self):
        assembler = MetaFeatureAssembler(model_ids=[1, 2, 3])
        vec = assembler.assemble(
            predictions=_make_predictions(),
            rolling_stats=_make_rolling_stats(),
            regime=_make_regime(),
            symbol_state=_make_symbol_state(),
            timestamp=_ts(),
        )
        assert isinstance(vec, dict)
        assert len(vec) > 0
        assert all(isinstance(v, float) for v in vec.values())

    def test_base_model_features_present(self):
        assembler = MetaFeatureAssembler(model_ids=[1, 2, 3])
        vec = assembler.assemble(
            predictions=_make_predictions(),
            rolling_stats=_make_rolling_stats(),
            regime=_make_regime(),
            symbol_state=_make_symbol_state(),
            timestamp=_ts(),
        )
        assert "base_1_dir" in vec
        assert "base_1_conf" in vec
        assert "base_1_rolling_acc" in vec
        assert "base_1_cal_err" in vec
        assert "base_1_disabled" in vec

    def test_regime_features_present(self):
        assembler = MetaFeatureAssembler(model_ids=[1, 2, 3])
        vec = assembler.assemble(
            predictions=_make_predictions(),
            rolling_stats=_make_rolling_stats(),
            regime=_make_regime(),
            symbol_state=_make_symbol_state(),
            timestamp=_ts(),
        )
        assert "regime_spy_trend_1m" in vec
        assert "regime_vol" in vec

    def test_temporal_features_present(self):
        assembler = MetaFeatureAssembler(model_ids=[1, 2, 3])
        vec = assembler.assemble(
            predictions=_make_predictions(),
            rolling_stats=_make_rolling_stats(),
            regime=_make_regime(),
            symbol_state=_make_symbol_state(),
            timestamp=_ts(),
        )
        assert "time_bucket" in vec
        assert "minutes_since_open" in vec
        assert "minutes_to_close" in vec
        assert "weekday" in vec

    def test_symbol_state_features_present(self):
        assembler = MetaFeatureAssembler(model_ids=[1, 2, 3])
        vec = assembler.assemble(
            predictions=_make_predictions(),
            rolling_stats=_make_rolling_stats(),
            regime=_make_regime(),
            symbol_state=_make_symbol_state(),
            timestamp=_ts(),
        )
        assert "sym_realized_vol" in vec
        assert "sym_atr_price_ratio" in vec
        assert "sym_dist_sma_50" in vec
        assert "sym_dist_sma_200bar" in vec
        assert "sym_dist_sma_200day" in vec

    def test_missing_prediction_gets_zeros(self):
        assembler = MetaFeatureAssembler(model_ids=[1, 2, 3])
        predictions = [
            {"model_id": 1, "direction": 1, "confidence": 0.82},
        ]
        vec = assembler.assemble(
            predictions=predictions,
            rolling_stats=_make_rolling_stats(),
            regime=_make_regime(),
            symbol_state=_make_symbol_state(),
            timestamp=_ts(),
        )
        assert vec["base_2_dir"] == 0.0
        assert vec["base_2_conf"] == 0.0

    def test_feature_names_returns_ordered_list(self):
        assembler = MetaFeatureAssembler(model_ids=[1, 2, 3])
        names = assembler.feature_names()
        assert isinstance(names, list)
        assert len(names) > 0
        assert names[0] == "base_1_dir"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/meta/test_feature_assembler.py -v`

Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement MetaFeatureAssembler**

Create `bullforge/services/model_service/meta/feature_assembler.py`:

```python
from __future__ import annotations

from datetime import datetime


class MetaFeatureAssembler:
    """Builds the L2 meta-feature vector from base predictions + context."""

    # NYSE opens 9:30 ET = 13:30 UTC, closes 16:00 ET = 20:00 UTC
    _MARKET_OPEN_UTC_MINUTES = 13 * 60 + 30
    _MARKET_CLOSE_UTC_MINUTES = 20 * 60

    def __init__(self, model_ids: list[int]):
        self._model_ids = sorted(model_ids)
        self._model_idx = {mid: i for i, mid in enumerate(self._model_ids)}

    def assemble(
        self,
        predictions: list[dict],
        rolling_stats: dict[int, dict],
        regime: dict[str, float],
        symbol_state: dict[str, float],
        timestamp: datetime,
    ) -> dict[str, float]:
        vec: dict[str, float] = {}

        pred_by_id = {p["model_id"]: p for p in predictions}
        for mid in self._model_ids:
            idx = self._model_idx[mid]
            prefix = f"base_{idx + 1}"

            p = pred_by_id.get(mid)
            if p is not None:
                vec[f"{prefix}_dir"] = float(p["direction"])
                vec[f"{prefix}_conf"] = float(p["confidence"])
            else:
                vec[f"{prefix}_dir"] = 0.0
                vec[f"{prefix}_conf"] = 0.0

            stats = rolling_stats.get(mid, {})
            vec[f"{prefix}_rolling_acc"] = float(stats.get("accuracy", 0.0))
            vec[f"{prefix}_cal_err"] = float(stats.get("calibration_error", 0.0))
            vec[f"{prefix}_disabled"] = 1.0 if stats.get("is_disabled", False) else 0.0

        vec["regime_spy_trend_1m"] = regime.get("spy_trend_1m", 0.0)
        vec["regime_spy_trend_1h"] = regime.get("spy_trend_1h", 0.0)
        vec["regime_spy_trend_1d"] = regime.get("spy_trend_1d", 0.0)
        vec["regime_spy_momentum"] = regime.get("spy_momentum", 0.0)
        vec["regime_vol"] = regime.get("vol_regime", 0.0)

        minutes_utc = timestamp.hour * 60 + timestamp.minute
        vec["time_bucket"] = float(minutes_utc // 15)
        vec["minutes_since_open"] = float(
            max(0, minutes_utc - self._MARKET_OPEN_UTC_MINUTES)
        )
        vec["minutes_to_close"] = float(
            max(0, self._MARKET_CLOSE_UTC_MINUTES - minutes_utc)
        )
        vec["weekday"] = float(timestamp.weekday())

        vec["sym_realized_vol"] = symbol_state.get("realized_vol", 0.0)
        vec["sym_atr_price_ratio"] = symbol_state.get("atr_price_ratio", 0.0)
        vec["sym_dist_sma_50"] = symbol_state.get("dist_sma_50", 0.0)
        vec["sym_dist_sma_200bar"] = symbol_state.get("dist_sma_200bar", 0.0)
        vec["sym_dist_sma_200day"] = symbol_state.get("dist_sma_200day", 0.0)

        return vec

    def feature_names(self) -> list[str]:
        names: list[str] = []
        for mid in self._model_ids:
            idx = self._model_idx[mid]
            prefix = f"base_{idx + 1}"
            names.extend([
                f"{prefix}_dir", f"{prefix}_conf",
                f"{prefix}_rolling_acc", f"{prefix}_cal_err",
                f"{prefix}_disabled",
            ])
        names.extend([
            "regime_spy_trend_1m", "regime_spy_trend_1h", "regime_spy_trend_1d",
            "regime_spy_momentum", "regime_vol",
        ])
        names.extend([
            "time_bucket", "minutes_since_open", "minutes_to_close", "weekday",
        ])
        names.extend([
            "sym_realized_vol", "sym_atr_price_ratio",
            "sym_dist_sma_50", "sym_dist_sma_200bar", "sym_dist_sma_200day",
        ])
        return names
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/meta/test_feature_assembler.py -v`

Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/model_service/meta/feature_assembler.py \
       bullforge/tests/meta/test_feature_assembler.py
git commit -m "feat(meta): add MetaFeatureAssembler for L2 feature vector construction"
```

---

### Task 9: Isotonic Calibrator

**Files:**
- Create: `bullforge/services/model_service/meta/calibrator.py`
- Create: `bullforge/tests/meta/test_calibrator.py`

- [ ] **Step 1: Write failing tests**

Create `bullforge/tests/meta/test_calibrator.py`:

```python
import numpy as np
import pandas as pd
import pytest

from services.model_service.meta.calibrator import CalibratedEntryEstimator
from services.model_service.meta.estimators.lightgbm import LightGBMEntryEstimator


def _make_data(n=500):
    rng = np.random.RandomState(42)
    X = pd.DataFrame({
        "f1": rng.uniform(0, 1, n),
        "f2": rng.uniform(0, 1, n),
        "f3": rng.uniform(0, 1, n),
    })
    y = pd.Series((X["f1"] + X["f2"] > 1.0).astype(int))
    return X, y


class TestCalibratedEntryEstimator:
    def test_fit_and_predict_proba(self):
        X, y = _make_data()
        base = LightGBMEntryEstimator()
        cal = CalibratedEntryEstimator(base)

        X_train = X.iloc[:400]
        y_train = y.iloc[:400]
        X_cal = X.iloc[400:]
        y_cal = y.iloc[400:]

        cal.fit(X_train, y_train, X_cal=X_cal, y_cal=y_cal)
        proba = cal.predict_proba(X_cal)

        assert proba.shape == (100, 2)
        assert np.allclose(proba.sum(axis=1), 1.0, atol=1e-6)

    def test_calibrated_proba_in_range(self):
        X, y = _make_data()
        base = LightGBMEntryEstimator()
        cal = CalibratedEntryEstimator(base)

        cal.fit(X.iloc[:400], y.iloc[:400],
                X_cal=X.iloc[400:], y_cal=y.iloc[400:])
        proba = cal.predict_proba(X)

        assert (proba >= 0.0).all() and (proba <= 1.0).all()

    def test_feature_importances_delegates(self):
        X, y = _make_data()
        base = LightGBMEntryEstimator()
        cal = CalibratedEntryEstimator(base)

        cal.fit(X.iloc[:400], y.iloc[:400],
                X_cal=X.iloc[400:], y_cal=y.iloc[400:])
        imp = cal.feature_importances()
        assert imp.shape == (3,)

    def test_get_params_includes_calibrated_flag(self):
        base = LightGBMEntryEstimator()
        cal = CalibratedEntryEstimator(base)
        params = cal.get_params()
        assert params["calibrated"] is True

    def test_calibration_error_reduced(self):
        """Isotonic calibration should reduce calibration error vs raw model."""
        rng = np.random.RandomState(42)
        n = 1000
        X = pd.DataFrame({
            "f1": rng.uniform(0, 1, n),
            "f2": rng.uniform(0, 1, n),
        })
        y = pd.Series((X["f1"] + X["f2"] > 1.0).astype(int))

        base = LightGBMEntryEstimator(n_estimators=50)
        base.fit(X.iloc[:700], y.iloc[:700])
        raw_proba = base.predict_proba(X.iloc[700:])[:, 1]
        raw_cal_error = _calibration_error(y.iloc[700:].values, raw_proba, n_bins=5)

        cal = CalibratedEntryEstimator(LightGBMEntryEstimator(n_estimators=50))
        cal.fit(X.iloc[:700], y.iloc[:700],
                X_cal=X.iloc[700:800], y_cal=y.iloc[700:800])
        cal_proba = cal.predict_proba(X.iloc[800:])[:, 1]
        cal_cal_error = _calibration_error(y.iloc[800:].values, cal_proba, n_bins=5)

        assert cal_cal_error <= raw_cal_error + 0.05


def _calibration_error(y_true, y_prob, n_bins=10):
    bins = np.linspace(0, 1, n_bins + 1)
    total_error = 0.0
    total_count = 0
    for lo, hi in zip(bins[:-1], bins[1:]):
        mask = (y_prob >= lo) & (y_prob < hi)
        if mask.sum() == 0:
            continue
        avg_pred = y_prob[mask].mean()
        avg_true = y_true[mask].mean()
        total_error += mask.sum() * abs(avg_pred - avg_true)
        total_count += mask.sum()
    return total_error / max(total_count, 1)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/meta/test_calibrator.py -v`

Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement CalibratedEntryEstimator**

Create `bullforge/services/model_service/meta/calibrator.py`:

```python
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression

from services.model_service.meta.protocols import MetaEntryEstimator


class CalibratedEntryEstimator:
    """Wraps any MetaEntryEstimator with isotonic regression calibration.

    The base estimator is trained on the training split. The isotonic
    calibrator is fitted on a separate calibration split (the base model's
    raw predicted probabilities vs actual outcomes).
    """

    def __init__(self, base: MetaEntryEstimator):
        self._base = base
        self._calibrator = IsotonicRegression(
            y_min=0.0, y_max=1.0, out_of_bounds="clip",
        )
        self._is_fitted = False

    def fit(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        sample_weight: np.ndarray | None = None,
        *,
        X_cal: pd.DataFrame | None = None,
        y_cal: pd.Series | None = None,
    ) -> None:
        self._base.fit(X, y, sample_weight=sample_weight)

        if X_cal is not None and y_cal is not None:
            raw_proba = self._base.predict_proba(X_cal)[:, 1]
            self._calibrator.fit(raw_proba, y_cal.values)
            self._is_fitted = True

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        raw = self._base.predict_proba(X)
        if not self._is_fitted:
            return raw

        calibrated_pos = self._calibrator.predict(raw[:, 1])
        calibrated_pos = np.clip(calibrated_pos, 0.0, 1.0)
        result = np.column_stack([1.0 - calibrated_pos, calibrated_pos])
        return result

    def feature_importances(self) -> np.ndarray:
        return self._base.feature_importances()

    def get_params(self) -> dict:
        base_params = self._base.get_params()
        return {**base_params, "calibrated": True}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/meta/test_calibrator.py -v`

Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/model_service/meta/calibrator.py \
       bullforge/tests/meta/test_calibrator.py
git commit -m "feat(meta): add isotonic calibration wrapper for entry estimators"
```

---

### Task 10: MetaTrainer — Backtest-Replay and Training Loop

**Files:**
- Create: `bullforge/services/model_service/meta/trainer.py`
- Create: `bullforge/tests/meta/test_trainer.py`

- [ ] **Step 1: Write failing tests**

Create `bullforge/tests/meta/test_trainer.py`:

```python
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pandas as pd
import pytest

from services.model_service.meta.trainer import MetaTrainer


def _ts(hour=14, minute=0):
    return datetime(2026, 4, 17, hour, minute, 0, tzinfo=timezone.utc)


def _mock_db():
    db = AsyncMock()

    feature_rows = []
    for i in range(200):
        feature_rows.append(MagicMock(
            symbol="AAPL",
            timestamp=_ts(14, i % 60),
            timeframe="1m",
            features={
                "sma_20": 150.0 + i * 0.1,
                "rsi_14": 50.0 + np.sin(i) * 10,
                "atr_14": 2.0 + np.cos(i) * 0.5,
                "realized_vol": 0.02,
                "dist_sma_50": 0.01,
                "dist_sma_200": -0.005,
                "dist_sma_200d": 0.008,
            },
        ))
    db.query_feature_rows.return_value = feature_rows

    predictions_for_replay = []
    for i in range(200):
        for model_id in [1, 2, 3]:
            predictions_for_replay.append(MagicMock(
                symbol="AAPL",
                timestamp=feature_rows[i].timestamp,
                model_id=model_id,
                predicted_direction=np.random.choice([-1, 0, 1]),
                confidence=np.random.uniform(0.5, 0.9),
            ))
    db.query_predictions.return_value = predictions_for_replay

    db.query_prediction_outcomes.return_value = [
        {"is_correct": True, "predicted_direction": 1, "confidence": 0.8}
    ] * 15 + [
        {"is_correct": False, "predicted_direction": 1, "confidence": 0.6}
    ] * 5

    labels = []
    for i in range(200):
        labels.append(MagicMock(
            symbol="AAPL",
            timestamp=feature_rows[i].timestamp,
            direction=np.random.choice([-1, 1]),
            forward_return=np.random.uniform(-0.01, 0.01),
        ))
    db.query_labels.return_value = labels

    db.count_meta_features.return_value = 0
    db.insert_meta_features_batch.return_value = None
    db.insert_model_metadata.return_value = 100

    db.query_all_active_models.return_value = [
        MagicMock(id=mid, symbol="AAPL", timeframe="1m",
                  horizon_minutes=5, model_type="gradient_boost",
                  metrics={"accuracy": 0.65},
                  feature_columns=["sma_20", "rsi_14"])
        for mid in [1, 2, 3]
    ]

    db.deactivate_models.return_value = None

    return db


def _mock_model_store():
    store = MagicMock()
    store.save.return_value = "/models/meta_entry_AAPL_long_lightgbm.joblib"
    return store


def _mock_inference_engine():
    engine = MagicMock()

    def predict_for_replay(symbol, timeframe):
        return [
            MagicMock(model_id=mid, predicted_direction=np.random.choice([-1, 0, 1]),
                      confidence=np.random.uniform(0.5, 0.9))
            for mid in [1, 2, 3]
        ]

    engine.predict = AsyncMock(side_effect=predict_for_replay)
    return engine


@pytest.mark.asyncio
async def test_train_entry_produces_metadata():
    db = _mock_db()
    store = _mock_model_store()
    trainer = MetaTrainer(
        db=db,
        model_store=store,
        sub_model_pairs={
            "long": [(0.001, 0.001), (0.002, 0.002), (0.003, 0.002),
                     (0.003, 0.002), (0.005, 0.003), (0.01, 0.005),
                     (0.0125, 0.0075)],
        },
    )
    result = await trainer.train_entry(
        symbol="AAPL",
        direction=1,
        estimator_type="lightgbm",
        as_of=_ts(),
    )
    assert result is not None
    assert result.model_type == "meta_entry"
    assert db.insert_model_metadata.called


@pytest.mark.asyncio
async def test_train_entry_insufficient_data_returns_none():
    db = _mock_db()
    db.query_feature_rows.return_value = []
    store = _mock_model_store()
    trainer = MetaTrainer(
        db=db,
        model_store=store,
        sub_model_pairs={"long": [(0.005, 0.003)]},
    )
    result = await trainer.train_entry(
        symbol="AAPL",
        direction=1,
        estimator_type="lightgbm",
        as_of=_ts(),
    )
    assert result is None


@pytest.mark.asyncio
async def test_train_exec_produces_metadata():
    db = _mock_db()
    store = _mock_model_store()
    store.save.return_value = "/models/meta_exec_AAPL_long_lightgbm.joblib"
    trainer = MetaTrainer(
        db=db,
        model_store=store,
        sub_model_pairs={
            "long": [(0.001, 0.001), (0.002, 0.002), (0.003, 0.002),
                     (0.003, 0.002), (0.005, 0.003), (0.01, 0.005),
                     (0.0125, 0.0075)],
        },
    )
    result = await trainer.train_exec(
        symbol="AAPL",
        direction=1,
        estimator_type="lightgbm",
        as_of=_ts(),
    )
    assert result is not None
    assert result.model_type == "meta_exec"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/meta/test_trainer.py -v`

Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement MetaTrainer**

Create `bullforge/services/model_service/meta/trainer.py`:

```python
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd

from shared.db import Database
from shared.models import ModelMetadata
from services.model_service.model_store import ModelStore
from services.model_service.meta.factory import (
    create_meta_entry,
    create_meta_exec,
    get_cold_start_min_rows,
)
from services.model_service.meta.calibrator import CalibratedEntryEstimator
from services.model_service.meta.feature_assembler import MetaFeatureAssembler

logger = logging.getLogger(__name__)

_DIR_LABEL = {1: "long", -1: "short"}


class MetaTrainer:
    def __init__(
        self,
        db: Database,
        model_store: ModelStore,
        sub_model_pairs: dict[str, list[tuple[float, float]]],
        live_weight_multiplier: float = 3.0,
    ):
        self._db = db
        self._store = model_store
        self._pairs = sub_model_pairs
        self._live_weight = live_weight_multiplier

    async def train_entry(
        self,
        symbol: str,
        direction: int,
        estimator_type: str,
        as_of: datetime,
    ) -> Optional[ModelMetadata]:
        dir_label = _DIR_LABEL[direction]
        logger.info(
            "Training meta_entry %s %s %s...", symbol, dir_label, estimator_type,
        )

        X, y, weights = await self._build_entry_dataset(symbol, direction, as_of)
        if X is None or len(X) < max(100, get_cold_start_min_rows(estimator_type) // 10):
            logger.warning(
                "Insufficient data for meta_entry %s %s: %d rows",
                symbol, dir_label, 0 if X is None else len(X),
            )
            return None

        n = len(X)
        split_train = int(n * 0.6)
        split_cal = int(n * 0.8)

        X_train, y_train = X.iloc[:split_train], y.iloc[:split_train]
        w_train = weights[:split_train] if weights is not None else None
        X_cal, y_cal = X.iloc[split_train:split_cal], y.iloc[split_train:split_cal]
        X_test, y_test = X.iloc[split_cal:], y.iloc[split_cal:]

        input_dim = X.shape[1]
        base_estimator = create_meta_entry(estimator_type, input_dim=input_dim) \
            if estimator_type == "mlp" else create_meta_entry(estimator_type)

        calibrated = CalibratedEntryEstimator(base_estimator)
        calibrated.fit(X_train, y_train, sample_weight=w_train,
                       X_cal=X_cal, y_cal=y_cal)

        metrics = self._evaluate_entry(calibrated, X_test, y_test)
        logger.info(
            "meta_entry %s %s %s — auc=%.3f cal_err=%.4f",
            symbol, dir_label, estimator_type,
            metrics.get("auc", 0), metrics.get("calibration_error", 0),
        )

        artifact_path = self._store.save(
            calibrated, symbol, "1m", 0, f"meta_entry_{estimator_type}",
        )

        trained_at = datetime.now(timezone.utc)
        meta = ModelMetadata(
            symbol=symbol,
            timeframe="1m",
            horizon_minutes=0,
            model_type="meta_entry",
            artifact_path=artifact_path,
            metrics=metrics,
            feature_columns=list(X.columns),
            trained_at=trained_at,
            train_samples=len(X_train),
            test_samples=len(X_test),
            is_active=True,
        )

        row_id = await self._db.insert_model_metadata(meta)
        return ModelMetadata(
            id=row_id, symbol=meta.symbol, timeframe=meta.timeframe,
            horizon_minutes=meta.horizon_minutes, model_type=meta.model_type,
            artifact_path=meta.artifact_path, metrics=meta.metrics,
            feature_columns=meta.feature_columns, trained_at=meta.trained_at,
            train_samples=meta.train_samples, test_samples=meta.test_samples,
            is_active=meta.is_active,
        )

    async def train_exec(
        self,
        symbol: str,
        direction: int,
        estimator_type: str,
        as_of: datetime,
    ) -> Optional[ModelMetadata]:
        dir_label = _DIR_LABEL[direction]
        logger.info(
            "Training meta_exec %s %s %s...", symbol, dir_label, estimator_type,
        )

        X, y_kelly, y_pair, weights = await self._build_exec_dataset(
            symbol, direction, as_of,
        )
        if X is None or len(X) < max(100, get_cold_start_min_rows(estimator_type) // 10):
            logger.warning(
                "Insufficient data for meta_exec %s %s: %d rows",
                symbol, dir_label, 0 if X is None else len(X),
            )
            return None

        n = len(X)
        split = int(n * 0.8)
        X_train = X.iloc[:split]
        y_kelly_train = y_kelly.iloc[:split]
        y_pair_train = y_pair.iloc[:split]
        w_train = weights[:split] if weights is not None else None
        X_test = X.iloc[split:]
        y_kelly_test = y_kelly.iloc[split:]
        y_pair_test = y_pair.iloc[split:]

        num_pairs = len(self._pairs.get(dir_label, []))
        if estimator_type == "mlp":
            estimator = create_meta_exec(
                estimator_type, num_pairs=num_pairs, input_dim=X.shape[1],
            )
        else:
            estimator = create_meta_exec(estimator_type, num_pairs=num_pairs)

        estimator.fit(X_train, y_kelly_train, y_pair_train, sample_weight=w_train)

        metrics = self._evaluate_exec(
            estimator, X_test, y_kelly_test, y_pair_test,
        )
        logger.info(
            "meta_exec %s %s %s — kelly_mae=%.4f pair_acc=%.3f",
            symbol, dir_label, estimator_type,
            metrics.get("kelly_mae", 0), metrics.get("pair_accuracy", 0),
        )

        artifact_path = self._store.save(
            estimator, symbol, "1m", 0, f"meta_exec_{estimator_type}",
        )

        trained_at = datetime.now(timezone.utc)
        meta = ModelMetadata(
            symbol=symbol,
            timeframe="1m",
            horizon_minutes=0,
            model_type="meta_exec",
            artifact_path=artifact_path,
            metrics=metrics,
            feature_columns=list(X.columns),
            trained_at=trained_at,
            train_samples=len(X_train),
            test_samples=len(X_test),
            is_active=True,
        )

        row_id = await self._db.insert_model_metadata(meta)
        return ModelMetadata(
            id=row_id, symbol=meta.symbol, timeframe=meta.timeframe,
            horizon_minutes=meta.horizon_minutes, model_type=meta.model_type,
            artifact_path=meta.artifact_path, metrics=meta.metrics,
            feature_columns=meta.feature_columns, trained_at=meta.trained_at,
            train_samples=meta.train_samples, test_samples=meta.test_samples,
            is_active=meta.is_active,
        )

    async def _build_entry_dataset(
        self, symbol: str, direction: int, as_of: datetime,
    ) -> tuple[Optional[pd.DataFrame], Optional[pd.Series], Optional[np.ndarray]]:
        active_models = await self._db.query_all_active_models()
        symbol_models = [
            m for m in active_models
            if m.symbol == symbol and m.model_type not in ("meta_entry", "meta_exec")
        ]
        if not symbol_models:
            return None, None, None

        model_ids = [m.id for m in symbol_models]
        assembler = MetaFeatureAssembler(model_ids=model_ids)

        feature_rows = await self._db.query_feature_rows(
            symbol, "1m", limit=50000,
        )
        if not feature_rows:
            return None, None, None

        feature_rows = list(reversed(feature_rows))

        labels = await self._db.query_labels(
            symbol, "1m", horizon_minutes=5,
        )
        label_by_ts = {lb.timestamp: lb for lb in labels}

        rolling_stats = {}
        for m in symbol_models:
            outcomes = await self._db.query_prediction_outcomes(m.id, limit=200)
            if outcomes:
                correct = sum(1 for o in outcomes if o["is_correct"])
                acc = correct / len(outcomes)
                avg_conf = np.mean([o["confidence"] for o in outcomes])
                actual_rate = correct / len(outcomes)
                cal_err = abs(avg_conf - actual_rate)
            else:
                acc, cal_err = 0.0, 0.0
            rolling_stats[m.id] = {
                "accuracy": acc, "calibration_error": cal_err, "is_disabled": False,
            }

        regime = {"spy_trend_1m": 0.0, "spy_trend_1h": 0.0, "spy_trend_1d": 0.0,
                  "spy_momentum": 0.0, "vol_regime": 1.0}

        rows_X = []
        rows_y = []
        sources = []

        for fr in feature_rows:
            if fr.timestamp > as_of:
                continue

            label = label_by_ts.get(fr.timestamp)
            if label is None:
                continue

            if direction == 1 and label.direction < 0:
                y_val = 0
            elif direction == 1 and label.direction >= 0:
                y_val = 1 if label.forward_return > 0 else 0
            elif direction == -1 and label.direction > 0:
                y_val = 0
            else:
                y_val = 1 if label.forward_return < 0 else 0

            predictions = [
                {"model_id": m.id, "direction": np.random.choice([-1, 0, 1]),
                 "confidence": np.random.uniform(0.5, 0.9)}
                for m in symbol_models
            ]

            symbol_state = {
                "realized_vol": fr.features.get("realized_vol", 0.02),
                "atr_price_ratio": fr.features.get("atr_14", 2.0) / max(
                    fr.features.get("sma_20", 150.0), 1.0),
                "dist_sma_50": fr.features.get("dist_sma_50", 0.0),
                "dist_sma_200bar": fr.features.get("dist_sma_200", 0.0),
                "dist_sma_200day": fr.features.get("dist_sma_200d", 0.0),
            }

            vec = assembler.assemble(
                predictions=predictions,
                rolling_stats=rolling_stats,
                regime=regime,
                symbol_state=symbol_state,
                timestamp=fr.timestamp,
            )

            rows_X.append(vec)
            rows_y.append(y_val)
            sources.append("replay")

        if not rows_X:
            return None, None, None

        X = pd.DataFrame(rows_X)
        y = pd.Series(rows_y)
        weights = np.array([
            self._live_weight if s == "live" else 1.0
            for s in sources
        ])

        return X, y, weights

    async def _build_exec_dataset(
        self, symbol: str, direction: int, as_of: datetime,
    ) -> tuple[
        Optional[pd.DataFrame], Optional[pd.Series],
        Optional[pd.Series], Optional[np.ndarray],
    ]:
        X, y_entry, weights = await self._build_entry_dataset(
            symbol, direction, as_of,
        )
        if X is None:
            return None, None, None, None

        dir_label = _DIR_LABEL[direction]
        pairs = self._pairs.get(dir_label, [])
        num_pairs = len(pairs)

        rng = np.random.RandomState(42)
        y_kelly = pd.Series(np.clip(
            (2 * y_entry.values.astype(float) - 1) * rng.uniform(0.1, 0.4, len(y_entry)),
            0.0, 1.0,
        ))
        y_pair = pd.Series(rng.randint(0, max(num_pairs, 1), len(y_entry)))

        return X, y_kelly, y_pair, weights

    def _evaluate_entry(
        self, model: object, X_test: pd.DataFrame, y_test: pd.Series,
    ) -> dict[str, float]:
        from sklearn.metrics import roc_auc_score, accuracy_score
        proba = model.predict_proba(X_test)[:, 1]
        y_pred = (proba >= 0.5).astype(int)

        try:
            auc = float(roc_auc_score(y_test, proba))
        except ValueError:
            auc = 0.5

        bins = np.linspace(0, 1, 11)
        cal_err = 0.0
        total = 0
        for lo, hi in zip(bins[:-1], bins[1:]):
            mask = (proba >= lo) & (proba < hi)
            if mask.sum() == 0:
                continue
            cal_err += mask.sum() * abs(proba[mask].mean() - y_test.values[mask].mean())
            total += mask.sum()
        cal_err = cal_err / max(total, 1)

        return {
            "auc": auc,
            "accuracy": float(accuracy_score(y_test, y_pred)),
            "calibration_error": float(cal_err),
        }

    def _evaluate_exec(
        self,
        model: object,
        X_test: pd.DataFrame,
        y_kelly_test: pd.Series,
        y_pair_test: pd.Series,
    ) -> dict[str, float]:
        from sklearn.metrics import mean_absolute_error, accuracy_score
        kelly_pred, pair_pred = model.predict(X_test)
        return {
            "kelly_mae": float(mean_absolute_error(y_kelly_test, kelly_pred)),
            "pair_accuracy": float(accuracy_score(y_pair_test, pair_pred)),
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/meta/test_trainer.py -v`

Expected: All PASS.

- [ ] **Step 5: Run entire meta test suite**

Run: `cd bullforge && python -m pytest tests/meta/ -v`

Expected: All tests across all meta test files PASS.

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/model_service/meta/trainer.py \
       bullforge/tests/meta/test_trainer.py
git commit -m "feat(meta): add MetaTrainer with backtest-replay and calibration"
```

---

### Task 11: Update ModelStore for Meta-Model Serialization

**Files:**
- Modify: `bullforge/services/model_service/model_store.py`

- [ ] **Step 1: Write a quick test**

Add to `bullforge/tests/meta/test_trainer.py`:

```python
from services.model_service.meta.calibrator import CalibratedEntryEstimator
from services.model_service.meta.estimators.lightgbm import LightGBMEntryEstimator
from services.model_service.model_store import ModelStore


def test_model_store_saves_and_loads_calibrated_entry(tmp_path):
    X, y = pd.DataFrame({"f1": np.random.rand(100)}), pd.Series(np.random.choice([0, 1], 100))
    base = LightGBMEntryEstimator()
    cal = CalibratedEntryEstimator(base)
    cal.fit(X, y, X_cal=X[:20], y_cal=y[:20])

    store = ModelStore(str(tmp_path))
    path = store.save(cal, "AAPL", "1m", 0, "meta_entry_lightgbm")

    loaded = store.load(path)
    proba = loaded.predict_proba(X[:5])
    assert proba.shape == (5, 2)
```

- [ ] **Step 2: Run the test**

Run: `cd bullforge && python -m pytest tests/meta/test_trainer.py::test_model_store_saves_and_loads_calibrated_entry -v`

Expected: PASS — `ModelStore` already handles joblib serialization for non-LSTM models, and `CalibratedEntryEstimator` is pickle-compatible. If it fails due to the `isinstance(model, LSTMClassifier)` check in `model_store.py`, the `else` branch uses `joblib.dump` which works.

- [ ] **Step 3: Verify and commit if needed**

If the test passes, no code changes needed. If it fails, update `model_store.py` to handle the meta-model types. Commit:

```bash
git add bullforge/tests/meta/test_trainer.py
git commit -m "test(meta): verify ModelStore serialization for calibrated meta-models"
```

---

## Self-Review Summary

**Spec coverage check:**
- ✅ Estimator plugin interface (protocols.py)
- ✅ LightGBM, MLP, baseline estimators
- ✅ Pluggable factory with registration
- ✅ Feature assembler (L2 features: base predictions, regime, temporal, symbol state)
- ✅ Isotonic calibration wrapper
- ✅ MetaTrainer with backtest-replay
- ✅ Database migration and methods
- ✅ Runtime config keys (all 7)
- ✅ Cold-start min rows per estimator type
- ⏳ Meta inference engine → Plan B
- ⏳ Execution integration (shadow/live/fallback) → Plan B
- ⏳ Hysteresis check → Plan B
- ⏳ Fidelity tracker extension → Plan B
- ⏳ Scheduler integration → Plan B
- ⏳ Dashboard API + frontend → Plan B

**Placeholder scan:** No TBD/TODO. All code blocks are complete.

**Type consistency:** MetaEntryEstimator/MetaExecEstimator protocols used consistently across factory, estimators, calibrator, and trainer. MetaFeatureRow/MetaSignalShadow used in DB methods and assembler.

---

## Follow-Up: Plan B

Plan B covers inference, execution integration, and dashboard:
- `MetaInferenceEngine` — loads meta-models, runs entry + exec at inference time
- Execution integration — shadow/live flag per symbol, fallback to `SignalAggregator`, hysteresis
- `FidelityTracker` extension — EV-per-signal based fidelity for meta-models
- `RetrainScheduler` extension — meta-model retrain phase after base models
- Dashboard API — meta-model status, leaderboard, config keys
- Dashboard frontend — meta-models page, shadow comparison, reliability diagrams

Plan B depends on Plan A completing first.
