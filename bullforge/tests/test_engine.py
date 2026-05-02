import logging

import pytest
import pytest_asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from shared.config import Settings
from shared.models import Bar, FeatureRow, Label
from services.feature_engine.engine import FeatureEngine
from services.feature_engine.registry import FeatureRegistry


def _make_bars(symbol: str, n: int = 60) -> list[Bar]:
    base = datetime(2026, 4, 10, 14, 0, 0, tzinfo=timezone.utc)
    np.random.seed(42)
    close = 420.0 + np.cumsum(np.random.randn(n) * 0.1)
    return [
        Bar(
            symbol=symbol,
            timestamp=base + timedelta(minutes=i),
            open=float(close[i] - np.random.rand() * 0.2),
            high=float(close[i] + np.random.rand() * 0.3),
            low=float(close[i] - np.random.rand() * 0.3),
            close=float(close[i]),
            volume=int(np.random.randint(1000, 50000)),
            timeframe="1m",
        )
        for i in range(n)
    ]


@pytest.fixture
def mock_settings(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test_key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test_secret")
    monkeypatch.setenv("TRADING_SYMBOLS_EQUITY", "SPY")
    monkeypatch.setenv("TRADING_SYMBOLS_CRYPTO", "")
    return Settings()


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.query_bars = AsyncMock(return_value=_make_bars("SPY", 60))
    db.insert_feature_rows = AsyncMock()
    db.insert_labels = AsyncMock()
    return db


@pytest.fixture
def simple_registry():
    registry = FeatureRegistry()

    def double_close(df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"close_x2": df["close"] * 2}, index=df.index)

    registry.register("double", double_close)
    return registry


def test_engine_creation(mock_settings, mock_db, simple_registry):
    engine = FeatureEngine(
        settings=mock_settings,
        db=mock_db,
        registry=simple_registry,
        label_horizons={"1m": [5, 10], "1d": [5, 10]},
    )
    assert engine is not None


@pytest.mark.asyncio
async def test_compute_features_for_symbol(mock_settings, mock_db, simple_registry):
    engine = FeatureEngine(
        settings=mock_settings,
        db=mock_db,
        registry=simple_registry,
        label_horizons={"1m": [5], "1d": [5]},
    )

    await engine.compute_features_for_symbol("SPY", "1m")

    mock_db.query_bars.assert_called_once_with("SPY", "1m", limit=500)
    mock_db.insert_feature_rows.assert_called_once()
    inserted_rows = mock_db.insert_feature_rows.call_args[0][0]
    assert len(inserted_rows) > 0
    assert all(isinstance(r, FeatureRow) for r in inserted_rows)
    assert all(r.symbol == "SPY" for r in inserted_rows)
    # The simple calculator adds close_x2
    assert "close_x2" in inserted_rows[0].features


@pytest.mark.asyncio
async def test_compute_labels_for_symbol(mock_settings, mock_db, simple_registry):
    engine = FeatureEngine(
        settings=mock_settings,
        db=mock_db,
        registry=simple_registry,
        label_horizons={"1m": [5, 10], "1d": [5, 10]},
    )

    await engine.compute_labels_for_symbol("SPY", "1m")

    mock_db.query_bars.assert_called_once()
    mock_db.insert_labels.assert_called_once()
    inserted_labels = mock_db.insert_labels.call_args[0][0]
    assert len(inserted_labels) > 0
    assert all(isinstance(l, Label) for l in inserted_labels)
    # Should have labels for both horizons
    horizons = {l.horizon_minutes for l in inserted_labels}
    assert 5 in horizons
    assert 10 in horizons


@pytest.mark.asyncio
async def test_compute_all_symbols(mock_settings, mock_db, simple_registry):
    engine = FeatureEngine(
        settings=mock_settings,
        db=mock_db,
        registry=simple_registry,
        label_horizons={"1m": [5], "1d": [5]},
    )

    await engine.compute_all(["SPY"])

    # Should have been called for SPY (only equity, no crypto since empty string)
    assert mock_db.query_bars.call_count >= 1


@pytest.mark.asyncio
async def test_compute_skips_insufficient_bars(mock_settings, simple_registry):
    mock_db = AsyncMock()
    mock_db.query_bars = AsyncMock(return_value=_make_bars("SPY", 3))
    mock_db.insert_feature_rows = AsyncMock()
    mock_db.insert_labels = AsyncMock()

    engine = FeatureEngine(
        settings=mock_settings,
        db=mock_db,
        registry=simple_registry,
        label_horizons={"1m": [5], "1d": [5]},
        min_bars=10,
    )

    await engine.compute_features_for_symbol("SPY", "1m")

    # Should not insert anything with insufficient bars
    mock_db.insert_feature_rows.assert_not_called()


@pytest.mark.asyncio
async def test_compute_features_publishes_update(mock_settings, mock_db, simple_registry):
    mock_redis = AsyncMock()
    mock_redis.publish = AsyncMock()

    engine = FeatureEngine(
        settings=mock_settings,
        db=mock_db,
        registry=simple_registry,
        label_horizons={"1m": [5], "1d": [5]},
        redis_client=mock_redis,
    )

    await engine.compute_features_for_symbol("SPY", "1m")

    mock_redis.publish.assert_called_once()
    call_args = mock_redis.publish.call_args
    assert call_args[0][0] == "feature:updates"
    import json
    payload = json.loads(call_args[0][1])
    assert payload["symbol"] == "SPY"
    assert payload["timeframe"] == "1m"


@pytest.mark.asyncio
async def test_compute_features_no_publish_without_redis(mock_settings, mock_db, simple_registry):
    engine = FeatureEngine(
        settings=mock_settings,
        db=mock_db,
        registry=simple_registry,
        label_horizons={"1m": [5], "1d": [5]},
    )

    # Should not raise even without redis_client
    await engine.compute_features_for_symbol("SPY", "1m")
    mock_db.insert_feature_rows.assert_called_once()


@pytest.mark.asyncio
async def test_compute_features_warns_per_column_when_nan_drops_rows(
    caplog, mock_settings, mock_db,
):
    """When a calculator NaNs out a column, the engine must emit a WARNING
    naming the offending column, its NaN count, and the bar window — so an
    operator can diagnose a stalled pipeline without re-running with debug
    logs."""
    registry = FeatureRegistry()

    def half_nan(df: pd.DataFrame) -> pd.DataFrame:
        n = len(df)
        nan_count = n // 2
        col = pd.Series(
            [float("nan")] * nan_count + [1.0] * (n - nan_count),
            index=df.index,
        )
        return pd.DataFrame({"unstable_col": col}, index=df.index)

    registry.register("half_nan", half_nan)

    engine = FeatureEngine(
        settings=mock_settings,
        db=mock_db,
        registry=registry,
        label_horizons={"1m": [5], "1d": [5]},
    )

    with caplog.at_level(logging.WARNING, logger="services.feature_engine.engine"):
        await engine.compute_features_for_symbol("SPY", "1m")

    nan_warnings = [
        r for r in caplog.records
        if r.levelno == logging.WARNING
        and r.name == "services.feature_engine.engine"
    ]
    assert nan_warnings, "expected at least one WARNING when rows are dropped"

    # The diagnostic warning should mention the symbol, the NaN'ing column,
    # the count of dropped/NaN rows (60 bars; 30 NaN'd), and the bar count.
    warning_messages = [r.getMessage() for r in nan_warnings]
    assert any("SPY" in m for m in warning_messages)
    assert any("unstable_col" in m for m in warning_messages)
    assert any("30" in m for m in warning_messages), (
        f"expected NaN count 30 in warning; got {warning_messages!r}"
    )
    assert any("1m" in m for m in warning_messages)


@pytest.mark.asyncio
async def test_compute_features_no_warning_when_all_rows_valid(
    caplog, mock_settings, mock_db, simple_registry,
):
    """The diagnostic warning must NOT fire on the happy path — the existing
    INFO ``Computed N feature rows`` log already covers it. A noisy WARNING
    on every successful call would drown out real signal."""
    engine = FeatureEngine(
        settings=mock_settings,
        db=mock_db,
        registry=simple_registry,
        label_horizons={"1m": [5], "1d": [5]},
    )

    with caplog.at_level(logging.WARNING, logger="services.feature_engine.engine"):
        await engine.compute_features_for_symbol("SPY", "1m")

    nan_warnings = [
        r for r in caplog.records
        if r.levelno == logging.WARNING
        and r.name == "services.feature_engine.engine"
    ]
    assert not nan_warnings, (
        f"expected no WARNING when all rows are valid; got {[r.getMessage() for r in nan_warnings]!r}"
    )
