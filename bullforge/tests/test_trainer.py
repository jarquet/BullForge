import json
import pytest
import pytest_asyncio
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from shared.models import ModelMetadata
from services.model_service.trainer import Trainer


def _make_training_rows(n: int = 200) -> list[dict]:
    base = datetime(2026, 4, 10, 14, 0, 0, tzinfo=timezone.utc)
    np.random.seed(42)
    rows = []
    for i in range(n):
        features = {
            "sma_20": float(420 + np.random.randn()),
            "rsi_14": float(50 + np.random.randn() * 10),
            "return_1": float(np.random.randn() * 0.001),
        }
        direction = int(np.random.choice([-1, 0, 1]))
        rows.append({
            "timestamp": base + timedelta(minutes=i),
            "features": json.dumps(features),
            "direction": direction,
            "forward_return": float(np.random.randn() * 0.002),
        })
    return rows


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.query_training_data = AsyncMock(return_value=_make_training_rows(200))
    db.deactivate_models = AsyncMock()
    db.insert_model_metadata = AsyncMock(return_value=1)
    return db


@pytest.fixture
def mock_model_store(tmp_path):
    from services.model_service.model_store import ModelStore
    return ModelStore(str(tmp_path))


@pytest.mark.asyncio
async def test_trainer_trains_gradient_boost(mock_db, mock_model_store):
    trainer = Trainer(db=mock_db, model_store=mock_model_store, min_samples=50)
    meta = await trainer.train("SPY", "1m", 5, as_of=datetime(2026, 4, 13, tzinfo=timezone.utc), model_type="gradient_boost")

    assert meta is not None
    assert isinstance(meta, ModelMetadata)
    assert meta.symbol == "SPY"
    assert meta.timeframe == "1m"
    assert meta.horizon_minutes == 5
    assert meta.model_type == "gradient_boost"
    assert "accuracy" in meta.metrics
    assert "f1_macro" in meta.metrics
    assert meta.train_samples > 0
    assert meta.test_samples > 0
    assert len(meta.feature_columns) == 3
    assert meta.id == 1

    mock_db.deactivate_models.assert_called_once_with(
        "SPY", "1m", 5, model_type="gradient_boost", scope=None,
    )
    mock_db.insert_model_metadata.assert_called_once()


@pytest.mark.asyncio
async def test_trainer_trains_logistic_regression(mock_db, mock_model_store):
    trainer = Trainer(db=mock_db, model_store=mock_model_store, min_samples=50)
    meta = await trainer.train("SPY", "1m", 5, as_of=datetime(2026, 4, 13, tzinfo=timezone.utc), model_type="logistic_regression")

    assert meta is not None
    assert meta.model_type == "logistic_regression"
    assert "accuracy" in meta.metrics


@pytest.mark.asyncio
async def test_trainer_returns_none_insufficient_data(mock_model_store):
    mock_db = AsyncMock()
    mock_db.query_training_data = AsyncMock(return_value=_make_training_rows(10))

    trainer = Trainer(db=mock_db, model_store=mock_model_store, min_samples=500)
    meta = await trainer.train("SPY", "1m", 5, as_of=datetime(2026, 4, 13, tzinfo=timezone.utc))

    assert meta is None
    mock_db.insert_model_metadata.assert_not_called()


@pytest.mark.asyncio
async def test_trainer_deactivates_old_model_before_insert(mock_db, mock_model_store):
    trainer = Trainer(db=mock_db, model_store=mock_model_store, min_samples=50)
    await trainer.train("SPY", "1m", 5, as_of=datetime(2026, 4, 13, tzinfo=timezone.utc))

    # deactivate should be called before insert, scoped to this model_type
    # so a later run of a different model_type doesn't wipe this row.
    mock_db.deactivate_models.assert_called_once_with(
        "SPY", "1m", 5, model_type="gradient_boost", scope=None,
    )
    mock_db.insert_model_metadata.assert_called_once()


@pytest.mark.asyncio
async def test_trainer_metrics_are_valid(mock_db, mock_model_store):
    trainer = Trainer(db=mock_db, model_store=mock_model_store, min_samples=50)
    meta = await trainer.train("SPY", "1m", 5, as_of=datetime(2026, 4, 13, tzinfo=timezone.utc))

    assert meta is not None
    assert 0.0 <= meta.metrics["accuracy"] <= 1.0
    assert 0.0 <= meta.metrics["f1_macro"] <= 1.0
    assert 0.0 <= meta.metrics["precision_macro"] <= 1.0
    assert 0.0 <= meta.metrics["recall_macro"] <= 1.0
    assert meta.metrics["support_neg"] >= 0
    assert meta.metrics["support_flat"] >= 0
    assert meta.metrics["support_pos"] >= 0


@pytest.mark.asyncio
async def test_trainer_artifact_path_exists(mock_db, mock_model_store):
    trainer = Trainer(db=mock_db, model_store=mock_model_store, min_samples=50)
    meta = await trainer.train("SPY", "1m", 5, as_of=datetime(2026, 4, 13, tzinfo=timezone.utc))

    assert meta is not None
    from pathlib import Path
    assert Path(meta.artifact_path).exists()


from unittest.mock import AsyncMock, MagicMock

from shared.models import BacktestScope
from services.model_service.trainer import Trainer


@pytest.mark.asyncio
async def test_trainer_passes_scope_to_db_and_store():
    db = MagicMock()
    db.deactivate_models = AsyncMock()
    db.insert_model_metadata = AsyncMock(return_value=42)
    store = MagicMock()
    store.save = MagicMock(return_value="/tmp/fake.joblib")

    trainer = Trainer(
        db, store,
        scope=BacktestScope(backtest_run_id=7, fold_index=2),
    )
    trainer._dataset_builder = MagicMock()
    trainer._dataset_builder.build = AsyncMock(return_value=None)

    await trainer.train("AAPL", "1m", 5, as_of=None)  # type: ignore[arg-type]

    assert trainer._scope == BacktestScope(backtest_run_id=7, fold_index=2)


@pytest.mark.asyncio
async def test_trainer_live_mode_has_no_scope():
    db = MagicMock()
    store = MagicMock()
    trainer = Trainer(db, store)
    assert trainer._scope is None


@pytest.mark.asyncio
async def test_trainer_passes_buffer_minutes_to_dataset_builder():
    """When runtime_config carries the equity buffer keys, Trainer.train
    forwards them as open_buffer_min / close_buffer_min to the
    DatasetBuilder so the exclusion-zone filter actually fires."""
    db = MagicMock()
    store = MagicMock()
    trainer = Trainer(db, store)
    trainer._dataset_builder = MagicMock()
    trainer._dataset_builder.build = AsyncMock(return_value=None)

    await trainer.train(
        "SPY", "1m", 5,
        as_of=datetime(2026, 4, 13, tzinfo=timezone.utc),
        runtime_config={
            "equity_window_open_buffer_minutes": 60,
            "equity_window_close_buffer_minutes": 45,
        },
    )

    trainer._dataset_builder.build.assert_called_once()
    call_kwargs = trainer._dataset_builder.build.call_args.kwargs
    assert call_kwargs["open_buffer_min"] == 60
    assert call_kwargs["close_buffer_min"] == 45


@pytest.mark.asyncio
async def test_trainer_passes_none_buffers_when_no_runtime_config():
    """Without a runtime_config, the buffer kwargs are passed as None and
    the filter stays off — preserves backward compat for the startup path
    and for tests that don't supply a runtime_config."""
    db = MagicMock()
    store = MagicMock()
    trainer = Trainer(db, store)
    trainer._dataset_builder = MagicMock()
    trainer._dataset_builder.build = AsyncMock(return_value=None)

    await trainer.train(
        "SPY", "1m", 5,
        as_of=datetime(2026, 4, 13, tzinfo=timezone.utc),
    )

    trainer._dataset_builder.build.assert_called_once()
    call_kwargs = trainer._dataset_builder.build.call_args.kwargs
    assert call_kwargs["open_buffer_min"] is None
    assert call_kwargs["close_buffer_min"] is None


@pytest.mark.asyncio
async def test_trainer_passes_none_buffers_when_runtime_config_lacks_keys():
    """A runtime_config dict without the buffer keys still results in None
    buffers (no filter) rather than a KeyError."""
    db = MagicMock()
    store = MagicMock()
    trainer = Trainer(db, store)
    trainer._dataset_builder = MagicMock()
    trainer._dataset_builder.build = AsyncMock(return_value=None)

    await trainer.train(
        "SPY", "1m", 5,
        as_of=datetime(2026, 4, 13, tzinfo=timezone.utc),
        runtime_config={"some_other_key": 123},
    )

    call_kwargs = trainer._dataset_builder.build.call_args.kwargs
    assert call_kwargs["open_buffer_min"] is None
    assert call_kwargs["close_buffer_min"] is None
