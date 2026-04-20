"""Tests for the DB query methods backing /api/models/{id}/*.

Uses the real-DB fixture from tests/integration/conftest.py when DB is up.
Skipped by ConnectionRefusedError when DB is not running locally.
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

from shared.models import Bar, Prediction


pytestmark = pytest.mark.asyncio(loop_scope="session")


async def test_query_accuracy_history_buckets_by_day(real_db):
    """query_accuracy_history returns one row per day in window with hit rate."""
    # Need: a prediction + matching entry and exit bar per day for 3 days.
    base = datetime(2026, 4, 10, 14, 0, 0, tzinfo=timezone.utc)
    for day in range(3):
        ts = base + timedelta(days=day)
        await real_db.insert_bar(Bar(
            symbol="ACCHIST", timestamp=ts,
            open=100.0, high=101.0, low=99.5, close=100.5,
            volume=1000, timeframe="1m",
        ))
        await real_db.insert_bar(Bar(
            symbol="ACCHIST", timestamp=ts + timedelta(minutes=5),
            open=100.5, high=101.5, low=100.0, close=101.0,
            volume=1000, timeframe="1m",
        ))
        await real_db.insert_prediction(Prediction(
            symbol="ACCHIST", timestamp=ts, timeframe="1m",
            horizon_minutes=5, model_id=999_901,
            predicted_direction=1, confidence=0.75,
        ))

    rows = await real_db.query_accuracy_history(
        model_id=999_901, from_dt=base, to_dt=base + timedelta(days=4),
    )
    assert len(rows) == 3
    # All 3 predictions should be correct (direction=1, exit > entry).
    for r in rows:
        assert r["total"] == 1
        assert r["correct"] == 1
        assert r["accuracy"] == 1.0


async def test_query_calibration_bins_buckets_by_confidence(real_db):
    """query_calibration_bins groups predictions into 10 confidence bins."""
    base = datetime(2026, 4, 10, 14, 0, 0, tzinfo=timezone.utc)
    # Insert 20 predictions with varying confidence, all with bars so they're labeled.
    for i in range(20):
        ts = base + timedelta(minutes=i)
        await real_db.insert_bar(Bar(
            symbol="CALTEST", timestamp=ts,
            open=100.0, high=101.0, low=99.5, close=100.0,
            volume=1000, timeframe="1m",
        ))
        # Half go up, half down; confidence ranges 0.55 to 0.94.
        direction = 1 if i % 2 == 0 else -1
        exit_close = 101.0 if direction == 1 else 99.0
        await real_db.insert_bar(Bar(
            symbol="CALTEST", timestamp=ts + timedelta(minutes=5),
            open=100.0, high=101.5, low=99.0, close=exit_close,
            volume=1000, timeframe="1m",
        ))
        await real_db.insert_prediction(Prediction(
            symbol="CALTEST", timestamp=ts, timeframe="1m",
            horizon_minutes=5, model_id=999_902,
            predicted_direction=direction, confidence=0.55 + i * 0.02,
        ))

    bins = await real_db.query_calibration_bins(
        model_id=999_902, num_bins=10,
        from_dt=base, to_dt=base + timedelta(hours=1),
    )
    # Bins are 0.0-0.1, 0.1-0.2, ..., 0.9-1.0. We populated 0.55-0.93.
    total_count = sum(b["count"] for b in bins)
    assert total_count == 20
    # Each populated bin's avg_confidence should fall within its range.
    for b in bins:
        if b["count"] > 0:
            lo, hi = b["bin_low"], b["bin_high"]
            assert lo <= b["avg_confidence"] <= hi


async def test_query_simulated_pnl_sums_directional_returns(real_db):
    """query_simulated_pnl returns per-prediction P&L assuming unit position."""
    base = datetime(2026, 4, 10, 14, 0, 0, tzinfo=timezone.utc)
    # One prediction: direction=1, entry=100, exit=102 → pnl_pct = +2%.
    await real_db.insert_bar(Bar(
        symbol="PNLTEST", timestamp=base,
        open=100.0, high=100.5, low=99.5, close=100.0,
        volume=1000, timeframe="1m",
    ))
    await real_db.insert_bar(Bar(
        symbol="PNLTEST", timestamp=base + timedelta(minutes=5),
        open=101.0, high=102.5, low=100.5, close=102.0,
        volume=1000, timeframe="1m",
    ))
    await real_db.insert_prediction(Prediction(
        symbol="PNLTEST", timestamp=base, timeframe="1m",
        horizon_minutes=5, model_id=999_903,
        predicted_direction=1, confidence=0.8,
    ))

    summary = await real_db.query_simulated_pnl(
        model_id=999_903,
        from_dt=base, to_dt=base + timedelta(hours=1),
    )
    assert summary["signal_count"] == 1
    assert summary["cumulative_pnl_pct"] == pytest.approx(0.02, abs=1e-6)
    assert summary["avg_pnl_pct"] == pytest.approx(0.02, abs=1e-6)


async def test_query_regime_breakdown_groups_by_spy_trend(real_db):
    """query_regime_breakdown classifies each prediction's timestamp by SPY trend.

    Bull: SPY close > SPY 50-bar SMA. Bear: <. Neutral: within 0.1%.
    Returns per-regime accuracy + count.
    """
    base = datetime(2026, 4, 10, 14, 0, 0, tzinfo=timezone.utc)
    # Seed 60 SPY bars with a clear uptrend so latest close > 50-SMA → Bull.
    for i in range(60):
        await real_db.insert_bar(Bar(
            symbol="SPY", timestamp=base + timedelta(minutes=i),
            open=100.0 + i * 0.1, high=100.2 + i * 0.1,
            low=99.8 + i * 0.1, close=100.1 + i * 0.1,
            volume=1000, timeframe="1m",
        ))
    # Target symbol bars + prediction at minute 55 (when SPY has 50 prior bars).
    target_ts = base + timedelta(minutes=55)
    await real_db.insert_bar(Bar(
        symbol="REGIMETEST", timestamp=target_ts,
        open=50.0, high=50.5, low=49.5, close=50.2,
        volume=1000, timeframe="1m",
    ))
    await real_db.insert_bar(Bar(
        symbol="REGIMETEST", timestamp=target_ts + timedelta(minutes=5),
        open=50.2, high=50.7, low=50.0, close=50.5,
        volume=1000, timeframe="1m",
    ))
    await real_db.insert_prediction(Prediction(
        symbol="REGIMETEST", timestamp=target_ts, timeframe="1m",
        horizon_minutes=5, model_id=999_904,
        predicted_direction=1, confidence=0.8,
    ))

    rows = await real_db.query_regime_breakdown(
        model_id=999_904,
        from_dt=base, to_dt=base + timedelta(hours=2),
    )
    # The one prediction should land in Bull.
    total = sum(r["count"] for r in rows)
    assert total == 1
    bull_row = next(r for r in rows if r["regime"] == "bull")
    assert bull_row["count"] == 1
    assert bull_row["correct"] == 1  # direction=1, close rose


async def test_query_signal_history_returns_recent_predictions(real_db):
    """query_signal_history returns last N predictions for a model with outcome info."""
    base = datetime(2026, 4, 10, 14, 0, 0, tzinfo=timezone.utc)
    for i in range(5):
        ts = base + timedelta(minutes=i)
        await real_db.insert_bar(Bar(
            symbol="SIGHIST", timestamp=ts,
            open=100.0, high=100.5, low=99.5, close=100.0,
            volume=1000, timeframe="1m",
        ))
        await real_db.insert_bar(Bar(
            symbol="SIGHIST", timestamp=ts + timedelta(minutes=5),
            open=100.0, high=101.0, low=99.5, close=100.8,
            volume=1000, timeframe="1m",
        ))
        await real_db.insert_prediction(Prediction(
            symbol="SIGHIST", timestamp=ts, timeframe="1m",
            horizon_minutes=5, model_id=999_905,
            predicted_direction=1, confidence=0.7 + i * 0.01,
        ))

    rows = await real_db.query_signal_history(model_id=999_905, limit=3)
    assert len(rows) == 3
    # Most recent first.
    assert rows[0]["timestamp"] > rows[1]["timestamp"]
    for r in rows:
        assert "predicted_direction" in r
        assert "confidence" in r
        assert "is_correct" in r
