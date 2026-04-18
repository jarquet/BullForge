# Dashboard Expansion Plan 2 — Models Domain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the Models tab from a flat status table into a rich per-model performance explorer: 7 cross-cutting sub-tabs (Leaderboard, Fidelity, Calibration, Attribution, Time-series, Regime, Feature Importance) plus a per-model drill-down page that aggregates everything for one model.

**Architecture:** Six new aiohttp handlers replace the HTTP 501 stubs from Plan 1. The monitoring service gains a nightly persistence step that writes per-model daily accuracy to `metric_snapshots`. The frontend Models tab is restructured to use Plan 1's Router, TimeWindow, Refresh, Charts, and Leaderboard components. Each sub-tab renders in its own container; drill-down at `#models/{model_id}` reuses the same components scoped to one model. All reads use the default `scope=None` (backtest sandbox invisible to live dashboard).

**Tech Stack:** aiohttp + asyncpg backend, vanilla JS + Chart.js frontend (via Plan 1 wrappers). No new dependencies.

**Spec:** [docs/superpowers/specs/2026-04-17-dashboard-expansion-design.md](../specs/2026-04-17-dashboard-expansion-design.md)

**Depends on:**
- [Plan 1 foundation](2026-04-17-dashboard-foundation.md) — uses Router, TimeWindow, Refresh, Charts, Leaderboard, DrillDown, and replaces six of its HTTP 501 stubs.
- [Backtest run/fold scoping](2026-04-17-backtest-run-scoping.md) — already merged (9105bd0). All new reads use `scope=None` (default) so sandbox rows stay invisible.

---

## Pre-Task: Worktree & Dev-Iteration Setup

The execution skill creates `.worktrees/dashboard-plan-2-models` branched from master. Suggested branch: `feat/dashboard-models`. Paths below are relative to that worktree.

Set up the dev iteration mount as in Plan 1:

```yaml
# bullforge/docker-compose.override.yml (gitignore-aware — DO NOT commit)
services:
  dashboard_service:
    volumes:
      - ./services/dashboard_service/static:/app/services/dashboard_service/static
```

Then `cd bullforge && docker compose up -d dashboard_service`.

---

## File Structure

### New files

```
bullforge/services/dashboard_service/
  models_api.py                         # Six new endpoints for /api/models/{id}/*
  static/
    models_tab.js                       # Models tab controller: sub-tab switch, drill-down, per-sub-tab loaders

bullforge/services/monitoring_service/
  accuracy_snapshot.py                  # Nightly per-model daily-accuracy persistence

bullforge/tests/
  test_dashboard_models_api.py          # aiohttp tests for each new endpoint
  test_dashboard_models_api_history.py  # specific history/time-series coverage
  test_accuracy_snapshot.py             # monitoring persistence tests
  test_dashboard_models_leaderboard.py  # tests for augmented /api/models (sparkline, fidelity_delta, simulated_pnl_7d)
```

### Modified files

```
bullforge/services/dashboard_service/api.py              # register_models_routes(app); extend /api/models for leaderboard
bullforge/services/dashboard_service/stubs_api.py        # remove the 6 Plan 2 endpoints from STUB_ENDPOINTS
bullforge/services/dashboard_service/main.py             # register_models_routes
bullforge/services/dashboard_service/static/index.html   # replace Models tab inner content with slot + script
bullforge/services/dashboard_service/static/dashboard.js # remove old loadModels(); defer to models_tab.js
bullforge/services/dashboard_service/static/style.css    # sub-tab strip + reliability-diagram + heatmap styling
bullforge/services/monitoring_service/main.py            # call accuracy_snapshot.persist_daily() after fidelity step
bullforge/shared/db.py                                   # add 6 new query methods (listed per task)
```

---

## Task 1: Database query methods

**Files:**
- Modify: `bullforge/shared/db.py`
- Create: `bullforge/tests/test_models_api_db.py`

These are the data-layer primitives used by Tasks 4-9. Each is small and testable independently.

- [ ] **Step 1: Write failing tests for the new DB methods**

Create `bullforge/tests/test_models_api_db.py`:

```python
"""Tests for the DB query methods backing /api/models/{id}/*.

Uses the real-DB fixture from tests/integration/conftest.py when DB is up.
Skipped by ConnectionRefusedError when DB is not running locally.
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

from shared.models import Bar, Prediction


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cd bullforge && python -m pytest tests/test_models_api_db.py -q`

Expected: FAIL — methods don't exist yet (`AttributeError: 'Database' object has no attribute 'query_accuracy_history'`).

- [ ] **Step 3: Implement `query_accuracy_history`**

Add to `bullforge/shared/db.py` (near the existing `query_prediction_outcomes` method):

```python
    async def query_accuracy_history(
        self,
        model_id: int,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> list[dict]:
        """Daily-bucketed hit rate for a model over [from_dt, to_dt).

        Returns rows: {date, total, correct, accuracy}. Ordered by date asc.
        A prediction counts as 'correct' by the same rules as
        query_prediction_outcomes: direction matches price movement, flat is
        ±0.1% of entry.
        """
        query = """
            SELECT
                DATE_TRUNC('day', p.timestamp) AS date,
                COUNT(*) AS total,
                SUM(CASE
                    WHEN (p.predicted_direction = 1 AND b_exit.close > b_entry.close) THEN 1
                    WHEN (p.predicted_direction = -1 AND b_exit.close < b_entry.close) THEN 1
                    WHEN (p.predicted_direction = 0
                          AND ABS(b_exit.close - b_entry.close)
                              / NULLIF(b_entry.close, 0) < 0.001) THEN 1
                    ELSE 0
                END)::INT AS correct
            FROM predictions p
            JOIN bars_1m b_entry
                ON b_entry.symbol = p.symbol
                AND b_entry.timestamp = p.timestamp
            JOIN bars_1m b_exit
                ON b_exit.symbol = p.symbol
                AND b_exit.timestamp = p.timestamp
                    + (p.horizon_minutes * interval '1 minute')
            WHERE p.model_id = $1
        """
        args: list = [model_id]
        idx = 2
        if from_dt is not None:
            query += f" AND p.timestamp >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            query += f" AND p.timestamp < ${idx}"
            args.append(to_dt); idx += 1
        query += """
            GROUP BY DATE_TRUNC('day', p.timestamp)
            ORDER BY date ASC
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [
            {
                "date": r["date"],
                "total": r["total"],
                "correct": r["correct"],
                "accuracy": (r["correct"] / r["total"]) if r["total"] > 0 else 0.0,
            }
            for r in rows
        ]
```

- [ ] **Step 4: Implement `query_calibration_bins`**

Add to `db.py`:

```python
    async def query_calibration_bins(
        self,
        model_id: int,
        num_bins: int = 10,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> list[dict]:
        """Reliability-diagram bins: predicted confidence vs actual hit rate.

        Returns num_bins rows: {bin_low, bin_high, count, correct,
        avg_confidence, hit_rate}. All bins returned even if empty.
        """
        query = """
            WITH outcomes AS (
                SELECT
                    p.confidence,
                    CASE
                        WHEN p.predicted_direction = 1 AND b_exit.close > b_entry.close THEN 1
                        WHEN p.predicted_direction = -1 AND b_exit.close < b_entry.close THEN 1
                        WHEN p.predicted_direction = 0
                             AND ABS(b_exit.close - b_entry.close)
                                 / NULLIF(b_entry.close, 0) < 0.001 THEN 1
                        ELSE 0
                    END AS is_correct
                FROM predictions p
                JOIN bars_1m b_entry
                    ON b_entry.symbol = p.symbol
                    AND b_entry.timestamp = p.timestamp
                JOIN bars_1m b_exit
                    ON b_exit.symbol = p.symbol
                    AND b_exit.timestamp = p.timestamp
                        + (p.horizon_minutes * interval '1 minute')
                WHERE p.model_id = $1
        """
        args: list = [model_id]
        idx = 2
        if from_dt is not None:
            query += f" AND p.timestamp >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            query += f" AND p.timestamp < ${idx}"
            args.append(to_dt); idx += 1
        query += f"""
            )
            SELECT
                bin_idx,
                COUNT(*) AS count,
                SUM(is_correct)::INT AS correct,
                AVG(confidence) AS avg_confidence,
                AVG(is_correct::FLOAT) AS hit_rate
            FROM (
                SELECT
                    LEAST(FLOOR(confidence * ${idx})::INT, ${idx} - 1) AS bin_idx,
                    confidence, is_correct
                FROM outcomes
            ) t
            GROUP BY bin_idx
            ORDER BY bin_idx ASC
        """
        args.append(num_bins)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)

        by_idx = {r["bin_idx"]: r for r in rows}
        result = []
        for i in range(num_bins):
            r = by_idx.get(i)
            lo = i / num_bins
            hi = (i + 1) / num_bins
            if r is None:
                result.append({
                    "bin_low": lo, "bin_high": hi,
                    "count": 0, "correct": 0,
                    "avg_confidence": 0.0, "hit_rate": 0.0,
                })
            else:
                result.append({
                    "bin_low": lo, "bin_high": hi,
                    "count": r["count"], "correct": r["correct"],
                    "avg_confidence": float(r["avg_confidence"]),
                    "hit_rate": float(r["hit_rate"]),
                })
        return result
```

- [ ] **Step 5: Implement `query_simulated_pnl`**

Add to `db.py`:

```python
    async def query_simulated_pnl(
        self,
        model_id: int,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> dict:
        """Simulated P&L if every signal had been traded with unit position.

        Returns {signal_count, cumulative_pnl_pct, avg_pnl_pct, win_rate,
        daily_series: [{date, pnl_pct, count}, ...]}.
        P&L per signal = predicted_direction * (exit_close - entry_close) / entry_close.
        """
        query = """
            WITH signals AS (
                SELECT
                    p.timestamp,
                    p.predicted_direction::FLOAT *
                        (b_exit.close - b_entry.close) / NULLIF(b_entry.close, 0)
                        AS pnl_pct
                FROM predictions p
                JOIN bars_1m b_entry
                    ON b_entry.symbol = p.symbol
                    AND b_entry.timestamp = p.timestamp
                JOIN bars_1m b_exit
                    ON b_exit.symbol = p.symbol
                    AND b_exit.timestamp = p.timestamp
                        + (p.horizon_minutes * interval '1 minute')
                WHERE p.model_id = $1
        """
        args: list = [model_id]
        idx = 2
        if from_dt is not None:
            query += f" AND p.timestamp >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            query += f" AND p.timestamp < ${idx}"
            args.append(to_dt); idx += 1
        query += """
            )
            SELECT
                DATE_TRUNC('day', timestamp) AS date,
                COUNT(*) AS count,
                SUM(pnl_pct) AS daily_pnl_pct,
                SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END)::INT AS wins
            FROM signals
            GROUP BY DATE_TRUNC('day', timestamp)
            ORDER BY date ASC
        """
        async with self.pool.acquire() as conn:
            daily_rows = await conn.fetch(query, *args)

        total_count = sum(r["count"] for r in daily_rows)
        total_pnl = sum(r["daily_pnl_pct"] or 0 for r in daily_rows)
        total_wins = sum(r["wins"] for r in daily_rows)

        return {
            "signal_count": total_count,
            "cumulative_pnl_pct": float(total_pnl),
            "avg_pnl_pct": float(total_pnl / total_count) if total_count > 0 else 0.0,
            "win_rate": (total_wins / total_count) if total_count > 0 else 0.0,
            "daily_series": [
                {
                    "date": r["date"],
                    "pnl_pct": float(r["daily_pnl_pct"] or 0),
                    "count": r["count"],
                }
                for r in daily_rows
            ],
        }
```

- [ ] **Step 6: Implement `query_regime_breakdown`**

Add to `db.py`:

```python
    async def query_regime_breakdown(
        self,
        model_id: int,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        sma_window: int = 50,
        neutral_band_pct: float = 0.001,
    ) -> list[dict]:
        """Per-regime accuracy + P&L for a model.

        Regime classification at each prediction timestamp:
          - bull: SPY close > SPY 50-bar SMA by >0.1% (configurable)
          - bear: SPY close < SPY 50-bar SMA by >0.1%
          - neutral: within ±0.1% of SMA
        Each prediction is classified independently based on the SPY bar at
        its timestamp (or the nearest earlier SPY bar, if missing).

        Returns rows: {regime, count, correct, accuracy, cumulative_pnl_pct}.
        """
        query = f"""
            WITH spy_series AS (
                SELECT
                    b.timestamp,
                    b.close AS spy_close,
                    AVG(b.close) OVER (
                        ORDER BY b.timestamp
                        ROWS BETWEEN {sma_window - 1} PRECEDING AND CURRENT ROW
                    ) AS sma
                FROM bars_1m b
                WHERE b.symbol = 'SPY'
            ),
            classified AS (
                SELECT
                    p.timestamp,
                    p.predicted_direction,
                    CASE
                        WHEN s.sma IS NULL THEN 'unknown'
                        WHEN (s.spy_close - s.sma) / NULLIF(s.sma, 0) > $2 THEN 'bull'
                        WHEN (s.spy_close - s.sma) / NULLIF(s.sma, 0) < -$2 THEN 'bear'
                        ELSE 'neutral'
                    END AS regime,
                    CASE
                        WHEN p.predicted_direction = 1 AND b_exit.close > b_entry.close THEN 1
                        WHEN p.predicted_direction = -1 AND b_exit.close < b_entry.close THEN 1
                        WHEN p.predicted_direction = 0
                             AND ABS(b_exit.close - b_entry.close)
                                 / NULLIF(b_entry.close, 0) < 0.001 THEN 1
                        ELSE 0
                    END AS is_correct,
                    p.predicted_direction::FLOAT *
                        (b_exit.close - b_entry.close) / NULLIF(b_entry.close, 0)
                        AS pnl_pct
                FROM predictions p
                LEFT JOIN spy_series s
                    ON s.timestamp = (
                        SELECT MAX(s2.timestamp)
                        FROM spy_series s2
                        WHERE s2.timestamp <= p.timestamp
                    )
                JOIN bars_1m b_entry
                    ON b_entry.symbol = p.symbol AND b_entry.timestamp = p.timestamp
                JOIN bars_1m b_exit
                    ON b_exit.symbol = p.symbol
                    AND b_exit.timestamp = p.timestamp
                        + (p.horizon_minutes * interval '1 minute')
                WHERE p.model_id = $1
        """
        args: list = [model_id, neutral_band_pct]
        idx = 3
        if from_dt is not None:
            query += f" AND p.timestamp >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            query += f" AND p.timestamp < ${idx}"
            args.append(to_dt); idx += 1
        query += """
            )
            SELECT
                regime,
                COUNT(*) AS count,
                SUM(is_correct)::INT AS correct,
                AVG(is_correct::FLOAT) AS accuracy,
                SUM(pnl_pct) AS cumulative_pnl_pct
            FROM classified
            GROUP BY regime
            ORDER BY regime
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [
            {
                "regime": r["regime"],
                "count": r["count"],
                "correct": r["correct"],
                "accuracy": float(r["accuracy"] or 0),
                "cumulative_pnl_pct": float(r["cumulative_pnl_pct"] or 0),
            }
            for r in rows
        ]
```

- [ ] **Step 7: Implement `query_signal_history`**

Add to `db.py`:

```python
    async def query_signal_history(
        self,
        model_id: int,
        limit: int = 100,
    ) -> list[dict]:
        """Recent predictions for a model with outcome info, newest first.

        Returns: {timestamp, symbol, timeframe, horizon_minutes,
                  predicted_direction, confidence, entry_price, exit_price,
                  is_correct, pnl_pct}. `is_correct`/`pnl_pct` are NULL when
                  the exit bar doesn't yet exist.
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    p.timestamp, p.symbol, p.timeframe, p.horizon_minutes,
                    p.predicted_direction, p.confidence,
                    b_entry.close AS entry_price,
                    b_exit.close  AS exit_price,
                    CASE
                        WHEN b_exit.close IS NULL THEN NULL
                        WHEN p.predicted_direction = 1
                             AND b_exit.close > b_entry.close THEN TRUE
                        WHEN p.predicted_direction = -1
                             AND b_exit.close < b_entry.close THEN TRUE
                        WHEN p.predicted_direction = 0
                             AND ABS(b_exit.close - b_entry.close)
                                 / NULLIF(b_entry.close, 0) < 0.001 THEN TRUE
                        ELSE FALSE
                    END AS is_correct,
                    CASE
                        WHEN b_exit.close IS NULL THEN NULL
                        ELSE p.predicted_direction::FLOAT
                             * (b_exit.close - b_entry.close)
                             / NULLIF(b_entry.close, 0)
                    END AS pnl_pct
                FROM predictions p
                LEFT JOIN bars_1m b_entry
                    ON b_entry.symbol = p.symbol
                    AND b_entry.timestamp = p.timestamp
                LEFT JOIN bars_1m b_exit
                    ON b_exit.symbol = p.symbol
                    AND b_exit.timestamp = p.timestamp
                        + (p.horizon_minutes * interval '1 minute')
                WHERE p.model_id = $1
                ORDER BY p.timestamp DESC
                LIMIT $2
                """,
                model_id, limit,
            )
        return [dict(r) for r in rows]
```

- [ ] **Step 8: Run DB tests**

If your local Postgres is running:

```bash
cd bullforge && python -m pytest tests/test_models_api_db.py -v
```

Expected: all 5 tests PASS.

If Postgres is not running, these tests skip with `ConnectionRefusedError`. That's OK — the code will be exercised via the aiohttp endpoint tests in later tasks using mocked Database objects.

- [ ] **Step 9: Commit**

```bash
git add bullforge/shared/db.py bullforge/tests/test_models_api_db.py
git commit -m "feat(db): add model-performance query methods (accuracy history, calibration, sim PnL, regime, signal history)"
```

---

## Task 2: Extend monitoring to persist daily per-model accuracy

**Files:**
- Create: `bullforge/services/monitoring_service/accuracy_snapshot.py`
- Modify: `bullforge/services/monitoring_service/main.py`
- Create: `bullforge/tests/test_accuracy_snapshot.py`

The Fidelity and Time-series sub-tabs need per-model daily accuracy going back 30+ days. Rather than recompute from `predictions` on every request, the monitoring service persists a daily rollup to `metric_snapshots` (category=`model_fidelity`, scope=`<model_id>`, window=`all`).

- [ ] **Step 1: Write the failing test**

Create `bullforge/tests/test_accuracy_snapshot.py`:

```python
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.monitoring_service.accuracy_snapshot import persist_daily_accuracy


@pytest.mark.asyncio
async def test_persist_daily_accuracy_writes_one_snapshot_per_active_model():
    db = AsyncMock()
    db.query_all_active_models.return_value = [
        MagicMock(id=1), MagicMock(id=2), MagicMock(id=3),
    ]
    db.query_prediction_outcomes.return_value = [
        {"is_correct": True}, {"is_correct": False}, {"is_correct": True},
    ]

    count = await persist_daily_accuracy(db, window_days=30)

    assert count == 3
    assert db.insert_metric_snapshot.await_count == 3
    for call in db.insert_metric_snapshot.call_args_list:
        args, _ = call
        # Args: (category, scope, window, metrics)
        assert args[0] == "model_fidelity"
        assert args[2] == "all"
        assert "live_accuracy" in args[3]
        assert "sample_size" in args[3]


@pytest.mark.asyncio
async def test_persist_daily_accuracy_skips_model_with_no_outcomes():
    db = AsyncMock()
    db.query_all_active_models.return_value = [
        MagicMock(id=1), MagicMock(id=2),
    ]
    # Model 1 has outcomes, model 2 does not.
    async def outcomes(model_id, limit):
        if model_id == 1:
            return [{"is_correct": True}, {"is_correct": True}]
        return []
    db.query_prediction_outcomes.side_effect = outcomes

    count = await persist_daily_accuracy(db, window_days=30)

    assert count == 1  # Only model 1 snapshotted.
    assert db.insert_metric_snapshot.await_count == 1
    args = db.insert_metric_snapshot.call_args.args
    assert args[1] == "1"  # model_id as string scope
```

- [ ] **Step 2: Run to verify failure**

Run: `cd bullforge && python -m pytest tests/test_accuracy_snapshot.py -v`

Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Implement persistence**

Create `bullforge/services/monitoring_service/accuracy_snapshot.py`:

```python
from __future__ import annotations

import logging

from shared.db import Database

logger = logging.getLogger(__name__)


async def persist_daily_accuracy(
    db: Database,
    window_days: int = 30,
) -> int:
    """Write a model_fidelity snapshot per active base model.

    Each snapshot holds the rolling live accuracy over `window_days` worth
    of recent predictions (limit=window_days*100 as a practical cap),
    plus the backtested accuracy from the model's metadata.

    Models with no outcomes in the window are skipped.
    Returns the number of snapshots written.
    """
    active = await db.query_all_active_models()
    written = 0
    for model in active:
        # Skip meta-model rows — they aren't tracked by fidelity here.
        if getattr(model, "model_type", "") in ("meta_entry", "meta_exec"):
            continue
        outcomes = await db.query_prediction_outcomes(
            model.id, limit=window_days * 100,
        )
        if not outcomes:
            continue
        correct = sum(1 for o in outcomes if o["is_correct"])
        live_accuracy = correct / len(outcomes)
        backtested_accuracy = float(
            (getattr(model, "metrics", None) or {}).get("accuracy", 0.0)
        )
        metrics = {
            "live_accuracy": live_accuracy,
            "backtested_accuracy": backtested_accuracy,
            "sample_size": len(outcomes),
            "window_days": window_days,
        }
        await db.insert_metric_snapshot(
            "model_fidelity", str(model.id), "all", metrics,
        )
        written += 1
    return written
```

- [ ] **Step 4: Wire it into the monitoring loop**

In `bullforge/services/monitoring_service/main.py`, add import at the top:

```python
from services.monitoring_service.accuracy_snapshot import persist_daily_accuracy
```

Find the section just after fidelity evaluation (after `statuses = []` loop, before `# 4. Evaluate halt conditions`) and insert:

```python
            # 3b. Persist daily per-model accuracy snapshot for dashboard history.
            try:
                await persist_daily_accuracy(db)
            except Exception:
                logger.exception("Error persisting per-model accuracy snapshot")
```

Non-fatal — the `try/except Exception` ensures a DB hiccup here doesn't break the monitoring tick.

- [ ] **Step 5: Run tests**

Run: `cd bullforge && python -m pytest tests/test_accuracy_snapshot.py -v`

Expected: 2 PASS.

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/monitoring_service/accuracy_snapshot.py \
        bullforge/services/monitoring_service/main.py \
        bullforge/tests/test_accuracy_snapshot.py
git commit -m "feat(monitoring): persist per-model daily accuracy to metric_snapshots"
```

---

## Task 3: New `models_api.py` with history, calibration, simulated_pnl endpoints

**Files:**
- Create: `bullforge/services/dashboard_service/models_api.py`
- Create: `bullforge/tests/test_dashboard_models_api.py`
- Modify: `bullforge/services/dashboard_service/stubs_api.py`
- Modify: `bullforge/services/dashboard_service/api.py`

- [ ] **Step 1: Write failing tests**

Create `bullforge/tests/test_dashboard_models_api.py`:

```python
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiohttp import web

from services.dashboard_service.models_api import register_models_routes


@pytest.fixture
def mock_db_models():
    db = AsyncMock()
    db.query_accuracy_history = AsyncMock(return_value=[
        {
            "date": datetime(2026, 4, 14, 0, 0, 0, tzinfo=timezone.utc),
            "total": 10, "correct": 7, "accuracy": 0.7,
        },
        {
            "date": datetime(2026, 4, 15, 0, 0, 0, tzinfo=timezone.utc),
            "total": 12, "correct": 9, "accuracy": 0.75,
        },
    ])
    db.query_calibration_bins = AsyncMock(return_value=[
        {"bin_low": 0.0, "bin_high": 0.1, "count": 0, "correct": 0,
         "avg_confidence": 0.0, "hit_rate": 0.0},
        {"bin_low": 0.5, "bin_high": 0.6, "count": 5, "correct": 3,
         "avg_confidence": 0.55, "hit_rate": 0.6},
    ])
    db.query_simulated_pnl = AsyncMock(return_value={
        "signal_count": 42, "cumulative_pnl_pct": 0.08,
        "avg_pnl_pct": 0.002, "win_rate": 0.65,
        "daily_series": [
            {"date": datetime(2026, 4, 15, 0, 0, 0, tzinfo=timezone.utc),
             "pnl_pct": 0.04, "count": 10},
        ],
    })
    db.query_regime_breakdown = AsyncMock(return_value=[
        {"regime": "bull", "count": 30, "correct": 22,
         "accuracy": 0.73, "cumulative_pnl_pct": 0.06},
        {"regime": "bear", "count": 12, "correct": 5,
         "accuracy": 0.42, "cumulative_pnl_pct": -0.01},
    ])
    db.query_signal_history = AsyncMock(return_value=[
        {
            "timestamp": datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc),
            "symbol": "AAPL", "timeframe": "1m", "horizon_minutes": 5,
            "predicted_direction": 1, "confidence": 0.78,
            "entry_price": 180.5, "exit_price": 181.2,
            "is_correct": True, "pnl_pct": 0.00388,
        },
    ])
    return db


@pytest.fixture
def models_app(mock_db_models):
    app = web.Application()
    app["db"] = mock_db_models
    register_models_routes(app)
    return app


@pytest.mark.asyncio
async def test_history_endpoint(aiohttp_client, models_app, mock_db_models):
    client = await aiohttp_client(models_app)
    resp = await client.get(
        "/api/models/42/history?from=2026-04-14T00:00:00Z&to=2026-04-16T00:00:00Z",
    )
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)
    assert len(body) == 2
    assert body[0]["accuracy"] == 0.7
    # Verify DB was called with int model_id and parsed datetimes.
    call = mock_db_models.query_accuracy_history.call_args
    assert call.args[0] == 42 or call.kwargs.get("model_id") == 42
    assert call.kwargs.get("from_dt") is not None
    assert call.kwargs.get("to_dt") is not None


@pytest.mark.asyncio
async def test_calibration_endpoint(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/42/calibration")
    assert resp.status == 200
    body = await resp.json()
    assert "bins" in body
    assert len(body["bins"]) == 2
    # Aggregate calibration error computed over bins with count > 0.
    assert "calibration_error" in body


@pytest.mark.asyncio
async def test_simulated_pnl_endpoint(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/42/simulated_pnl")
    assert resp.status == 200
    body = await resp.json()
    assert body["signal_count"] == 42
    assert "daily_series" in body


@pytest.mark.asyncio
async def test_regime_breakdown_endpoint(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/42/regime_breakdown")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)
    regimes = [r["regime"] for r in body]
    assert "bull" in regimes


@pytest.mark.asyncio
async def test_signal_history_endpoint(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/42/signal_history?limit=50")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)
    assert body[0]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_invalid_model_id_returns_400(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/not-a-number/history")
    assert resp.status == 400


@pytest.mark.asyncio
async def test_history_rejects_malformed_from(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/42/history?from=garbage")
    assert resp.status == 400
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_dashboard_models_api.py -v`

Expected: FAIL — `ModuleNotFoundError: No module named 'services.dashboard_service.models_api'`.

- [ ] **Step 3: Implement `models_api.py`**

Create `bullforge/services/dashboard_service/models_api.py`:

```python
from __future__ import annotations

import logging
from datetime import datetime

from aiohttp import web

from shared.db import Database
from shared.time_window import parse_time_window

logger = logging.getLogger(__name__)


def register_models_routes(app: web.Application) -> None:
    app.router.add_get("/api/models/{id}/history", _handle_history)
    app.router.add_get("/api/models/{id}/calibration", _handle_calibration)
    app.router.add_get("/api/models/{id}/simulated_pnl", _handle_simulated_pnl)
    app.router.add_get("/api/models/{id}/feature_importance", _handle_feature_importance)
    app.router.add_get("/api/models/{id}/regime_breakdown", _handle_regime_breakdown)
    app.router.add_get("/api/models/{id}/signal_history", _handle_signal_history)


def _parse_model_id(request: web.Request) -> int:
    raw = request.match_info.get("id", "")
    try:
        return int(raw)
    except ValueError:
        raise web.HTTPBadRequest(reason=f"invalid model id: {raw!r}")


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


async def _handle_history(request: web.Request) -> web.Response:
    model_id = _parse_model_id(request)
    from_dt, to_dt = parse_time_window(request)
    db: Database = request.app["db"]
    rows = await db.query_accuracy_history(
        model_id=model_id, from_dt=from_dt, to_dt=to_dt,
    )
    return web.json_response([
        {
            "date": _iso(r["date"]),
            "total": r["total"],
            "correct": r["correct"],
            "accuracy": r["accuracy"],
        }
        for r in rows
    ])


async def _handle_calibration(request: web.Request) -> web.Response:
    model_id = _parse_model_id(request)
    from_dt, to_dt = parse_time_window(request)
    num_bins = int(request.query.get("bins", "10"))
    db: Database = request.app["db"]
    bins = await db.query_calibration_bins(
        model_id=model_id, num_bins=num_bins,
        from_dt=from_dt, to_dt=to_dt,
    )
    # Calibration error = weighted mean |avg_conf - hit_rate| across bins
    # with count > 0.
    total_n = sum(b["count"] for b in bins)
    if total_n > 0:
        err = sum(
            b["count"] * abs(b["avg_confidence"] - b["hit_rate"])
            for b in bins if b["count"] > 0
        ) / total_n
    else:
        err = 0.0
    return web.json_response({
        "bins": bins,
        "calibration_error": err,
        "sample_size": total_n,
    })


async def _handle_simulated_pnl(request: web.Request) -> web.Response:
    model_id = _parse_model_id(request)
    from_dt, to_dt = parse_time_window(request)
    db: Database = request.app["db"]
    result = await db.query_simulated_pnl(
        model_id=model_id, from_dt=from_dt, to_dt=to_dt,
    )
    return web.json_response({
        "signal_count": result["signal_count"],
        "cumulative_pnl_pct": result["cumulative_pnl_pct"],
        "avg_pnl_pct": result["avg_pnl_pct"],
        "win_rate": result["win_rate"],
        "daily_series": [
            {
                "date": _iso(d["date"]),
                "pnl_pct": d["pnl_pct"],
                "count": d["count"],
            }
            for d in result["daily_series"]
        ],
    })


async def _handle_feature_importance(request: web.Request) -> web.Response:
    model_id = _parse_model_id(request)
    db: Database = request.app["db"]
    from shared.models import ModelMetadata
    meta = None
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT artifact_path, feature_columns, model_type
            FROM model_registry WHERE id = $1 AND backtest_run_id IS NULL
            """,
            model_id,
        )
    if row is None:
        raise web.HTTPNotFound(reason=f"model {model_id} not found")

    model_type = row["model_type"]
    if model_type in ("lstm",):
        return web.json_response({
            "available": False,
            "reason": f"feature importance not available for {model_type}",
            "features": [],
        })

    # Load artifact once per request. For scale, swap in an LRU cache later.
    from services.model_service.model_store import ModelStore
    import json as _json
    store = ModelStore(artifacts_dir=str(row["artifact_path"]).rsplit("/", 1)[0])
    try:
        model = store.load(row["artifact_path"])
    except FileNotFoundError:
        raise web.HTTPNotFound(reason="artifact missing on disk")

    # Sklearn pipelines: unwrap to last step.
    if hasattr(model, "steps"):
        model = model.steps[-1][1]

    if not hasattr(model, "feature_importances_"):
        return web.json_response({
            "available": False,
            "reason": f"{model.__class__.__name__} has no feature_importances_",
            "features": [],
        })

    features = _json.loads(row["feature_columns"]) if isinstance(
        row["feature_columns"], str,
    ) else row["feature_columns"]
    importances = list(model.feature_importances_)
    pairs = list(zip(features, importances))
    pairs.sort(key=lambda p: p[1], reverse=True)
    return web.json_response({
        "available": True,
        "features": [
            {"name": name, "importance": float(imp)}
            for name, imp in pairs[:20]
        ],
    })


async def _handle_regime_breakdown(request: web.Request) -> web.Response:
    model_id = _parse_model_id(request)
    from_dt, to_dt = parse_time_window(request)
    db: Database = request.app["db"]
    rows = await db.query_regime_breakdown(
        model_id=model_id, from_dt=from_dt, to_dt=to_dt,
    )
    return web.json_response(rows)


async def _handle_signal_history(request: web.Request) -> web.Response:
    model_id = _parse_model_id(request)
    limit = min(int(request.query.get("limit", "100")), 500)
    db: Database = request.app["db"]
    rows = await db.query_signal_history(model_id=model_id, limit=limit)
    return web.json_response([
        {
            "timestamp": _iso(r["timestamp"]),
            "symbol": r["symbol"],
            "timeframe": r["timeframe"],
            "horizon_minutes": r["horizon_minutes"],
            "predicted_direction": r["predicted_direction"],
            "confidence": r["confidence"],
            "entry_price": r["entry_price"],
            "exit_price": r["exit_price"],
            "is_correct": r["is_correct"],
            "pnl_pct": r["pnl_pct"],
        }
        for r in rows
    ])
```

- [ ] **Step 4: Remove the 6 Plan 2 stubs from `stubs_api.py`**

In `bullforge/services/dashboard_service/stubs_api.py`, delete these 6 lines from `STUB_ENDPOINTS`:

```python
    "/api/models/{id}/history":             {"enabled_after_plan": "plan_2"},
    "/api/models/{id}/calibration":         {"enabled_after_plan": "plan_2"},
    "/api/models/{id}/simulated_pnl":       {"enabled_after_plan": "plan_2"},
    "/api/models/{id}/feature_importance":  {"enabled_after_plan": "plan_2"},
    "/api/models/{id}/regime_breakdown":    {"enabled_after_plan": "plan_2"},
    "/api/models/{id}/signal_history":      {"enabled_after_plan": "plan_2"},
```

- [ ] **Step 5: Register models routes in `api.py`**

In `bullforge/services/dashboard_service/api.py`, add import:

```python
from services.dashboard_service.models_api import register_models_routes
```

In `create_api_app()`, register the models routes after the existing `register_backtests_routes(app)` call:

```python
    register_backtests_routes(app)
    register_models_routes(app)
```

- [ ] **Step 6: Update `test_dashboard_stubs.py` expectation**

In `bullforge/tests/test_dashboard_stubs.py`, update the `test_stubs_include_every_spec_endpoint` function's `expected_prefixes` set by removing the 6 model paths (they're no longer stubs). Replace the set with:

```python
    expected_prefixes = {
        "/api/trading/equity",
        "/api/trading/distributions",
        "/api/trading/heatmap",
        "/api/trading/streaks",
        "/api/trading/exposure",
        "/api/trading/benchmark",
        "/api/trading/symbols/{sym}",
        "/api/backtests/{run_id}/equity",
        "/api/backtests/{run_id}/folds",
        "/api/backtests/{run_id}/symbols",
        "/api/backtests/{run_id}/distributions",
        "/api/backtests/{run_id}/live-overlay",
        "/api/backtests/{run_id}/funnel",
        "/api/backtests/compare",
        "/api/backtests/sensitivity",
    }
```

The test `test_every_stub_returns_501` doesn't need changes — it iterates over `STUB_ENDPOINTS` which now has 15 entries.

- [ ] **Step 7: Run all affected tests**

Run: `cd bullforge && python -m pytest tests/test_dashboard_models_api.py tests/test_dashboard_stubs.py tests/test_dashboard_api.py -v`

Expected: all PASS. 7 new model-API tests + existing stub tests (still 2, now on 15 endpoints) + existing dashboard API tests.

- [ ] **Step 8: Commit**

```bash
git add bullforge/services/dashboard_service/models_api.py \
        bullforge/services/dashboard_service/api.py \
        bullforge/services/dashboard_service/stubs_api.py \
        bullforge/tests/test_dashboard_models_api.py \
        bullforge/tests/test_dashboard_stubs.py
git commit -m "feat(dashboard): implement /api/models/{id}/* endpoints (6 handlers, replace Plan 1 stubs)"
```

---

## Task 4: Extend `/api/models` leaderboard payload

**Files:**
- Modify: `bullforge/services/dashboard_service/api.py` (`_handle_models`)
- Modify: `bullforge/shared/db.py` (helper method `query_models_with_leaderboard_stats`)
- Create: `bullforge/tests/test_dashboard_models_leaderboard.py`

The existing `/api/models` returns basic metadata per model. The Leaderboard sub-tab needs: `live_accuracy_7d`, `backtested_accuracy`, `fidelity_delta`, `signals_7d`, `simulated_pnl_7d`, and a 30-day accuracy sparkline per model.

- [ ] **Step 1: Write failing test**

Create `bullforge/tests/test_dashboard_models_leaderboard.py`:

```python
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from aiohttp import web


@pytest.mark.asyncio
async def test_models_endpoint_returns_leaderboard_fields(aiohttp_client):
    from services.dashboard_service.api import create_api_app
    db = AsyncMock()
    db.query_model_details.return_value = [
        {
            "model_id": 42, "symbol": "AAPL", "timeframe": "1m",
            "horizon_minutes": 5, "model_type": "gradient_boost",
            "trained_at": datetime(2026, 4, 15, tzinfo=timezone.utc),
            "metrics": {"accuracy": 0.62}, "is_active": True,
            "status": "active", "reason": "Within range",
            "status_updated_at": datetime(2026, 4, 15, tzinfo=timezone.utc),
        },
    ]
    db.query_models_with_leaderboard_stats = AsyncMock(return_value={
        42: {
            "live_accuracy_7d": 0.58,
            "signals_7d": 210,
            "simulated_pnl_7d_pct": 0.012,
            "accuracy_sparkline_30d": [0.60, 0.62, 0.58, 0.61, 0.59],
        }
    })

    app = create_api_app(db)
    client = await aiohttp_client(app)
    resp = await client.get("/api/models")
    assert resp.status == 200
    body = await resp.json()
    assert len(body) == 1
    row = body[0]
    assert row["live_accuracy_7d"] == pytest.approx(0.58)
    assert row["backtested_accuracy"] == pytest.approx(0.62)
    assert row["fidelity_delta"] == pytest.approx(0.62 - 0.58)
    assert row["signals_7d"] == 210
    assert row["simulated_pnl_7d_pct"] == pytest.approx(0.012)
    assert row["accuracy_sparkline_30d"] == [0.60, 0.62, 0.58, 0.61, 0.59]
```

- [ ] **Step 2: Run to verify failure**

Run: `cd bullforge && python -m pytest tests/test_dashboard_models_leaderboard.py -v`

Expected: FAIL — `query_models_with_leaderboard_stats` doesn't exist, response missing fields.

- [ ] **Step 3: Add `query_models_with_leaderboard_stats` to `db.py`**

Append to the `Database` class:

```python
    async def query_models_with_leaderboard_stats(
        self,
        model_ids: list[int],
    ) -> dict[int, dict]:
        """Batch lookup of 7-day live stats + 30-day sparkline per model.

        Returns {model_id: {live_accuracy_7d, signals_7d, simulated_pnl_7d_pct,
                            accuracy_sparkline_30d}}. Models with no data get
        zeros/empty sparkline.
        """
        if not model_ids:
            return {}

        # 7-day live stats via one aggregate query.
        async with self.pool.acquire() as conn:
            stats_rows = await conn.fetch(
                """
                SELECT
                    p.model_id,
                    COUNT(*) AS signals,
                    SUM(CASE
                        WHEN p.predicted_direction = 1 AND b_exit.close > b_entry.close THEN 1
                        WHEN p.predicted_direction = -1 AND b_exit.close < b_entry.close THEN 1
                        WHEN p.predicted_direction = 0
                             AND ABS(b_exit.close - b_entry.close)
                                 / NULLIF(b_entry.close, 0) < 0.001 THEN 1
                        ELSE 0
                    END)::FLOAT AS correct,
                    SUM(p.predicted_direction::FLOAT
                        * (b_exit.close - b_entry.close)
                        / NULLIF(b_entry.close, 0)) AS pnl_pct
                FROM predictions p
                JOIN bars_1m b_entry
                    ON b_entry.symbol = p.symbol
                    AND b_entry.timestamp = p.timestamp
                JOIN bars_1m b_exit
                    ON b_exit.symbol = p.symbol
                    AND b_exit.timestamp = p.timestamp
                        + (p.horizon_minutes * interval '1 minute')
                WHERE p.model_id = ANY($1)
                    AND p.timestamp >= NOW() - INTERVAL '7 days'
                GROUP BY p.model_id
                """,
                model_ids,
            )

            # 30-day daily sparkline via one aggregate query.
            sparkline_rows = await conn.fetch(
                """
                SELECT
                    p.model_id,
                    DATE_TRUNC('day', p.timestamp) AS date,
                    SUM(CASE
                        WHEN p.predicted_direction = 1 AND b_exit.close > b_entry.close THEN 1
                        WHEN p.predicted_direction = -1 AND b_exit.close < b_entry.close THEN 1
                        WHEN p.predicted_direction = 0
                             AND ABS(b_exit.close - b_entry.close)
                                 / NULLIF(b_entry.close, 0) < 0.001 THEN 1
                        ELSE 0
                    END)::FLOAT / NULLIF(COUNT(*), 0) AS accuracy
                FROM predictions p
                JOIN bars_1m b_entry
                    ON b_entry.symbol = p.symbol
                    AND b_entry.timestamp = p.timestamp
                JOIN bars_1m b_exit
                    ON b_exit.symbol = p.symbol
                    AND b_exit.timestamp = p.timestamp
                        + (p.horizon_minutes * interval '1 minute')
                WHERE p.model_id = ANY($1)
                    AND p.timestamp >= NOW() - INTERVAL '30 days'
                GROUP BY p.model_id, DATE_TRUNC('day', p.timestamp)
                ORDER BY p.model_id, date ASC
                """,
                model_ids,
            )

        result: dict[int, dict] = {}
        for mid in model_ids:
            result[mid] = {
                "live_accuracy_7d": 0.0,
                "signals_7d": 0,
                "simulated_pnl_7d_pct": 0.0,
                "accuracy_sparkline_30d": [],
            }
        for r in stats_rows:
            mid = r["model_id"]
            signals = r["signals"] or 0
            result[mid]["signals_7d"] = signals
            result[mid]["live_accuracy_7d"] = (
                (r["correct"] / signals) if signals > 0 else 0.0
            )
            result[mid]["simulated_pnl_7d_pct"] = float(r["pnl_pct"] or 0.0)
        for r in sparkline_rows:
            mid = r["model_id"]
            result[mid]["accuracy_sparkline_30d"].append(
                float(r["accuracy"] or 0.0)
            )
        return result
```

- [ ] **Step 4: Update `_handle_models` in `api.py`**

Find `async def _handle_models` in `bullforge/services/dashboard_service/api.py` and replace its body with:

```python
async def _handle_models(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    details = await db.query_model_details()
    model_ids = [d["model_id"] for d in details]
    extra = await db.query_models_with_leaderboard_stats(model_ids)

    payload = []
    for d in details:
        mid = d["model_id"]
        stats = extra.get(mid, {})
        backtested = float((d.get("metrics") or {}).get("accuracy", 0.0))
        live = stats.get("live_accuracy_7d", 0.0)
        label = (
            f"{d['model_type']} \u00b7 {d['symbol']} \u00b7 "
            f"{d['timeframe']} \u00b7 {d['horizon_minutes']}m"
        )
        payload.append({
            "model_id": mid,
            "label": label,
            "symbol": d["symbol"],
            "timeframe": d["timeframe"],
            "horizon_minutes": d["horizon_minutes"],
            "model_type": d["model_type"],
            "status": d["status"],
            "reason": d["reason"],
            "metrics": d["metrics"],
            "trained_at": (
                d["trained_at"].isoformat() if d["trained_at"] else None
            ),
            "status_updated_at": (
                d["status_updated_at"].isoformat()
                if d.get("status_updated_at") else None
            ),
            "backtested_accuracy": backtested,
            "live_accuracy_7d": live,
            "fidelity_delta": backtested - live,
            "signals_7d": stats.get("signals_7d", 0),
            "simulated_pnl_7d_pct": stats.get("simulated_pnl_7d_pct", 0.0),
            "accuracy_sparkline_30d": stats.get("accuracy_sparkline_30d", []),
        })
    return web.json_response(payload)
```

- [ ] **Step 5: Run tests**

Run: `cd bullforge && python -m pytest tests/test_dashboard_models_leaderboard.py tests/test_dashboard_api.py -v`

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add bullforge/shared/db.py \
        bullforge/services/dashboard_service/api.py \
        bullforge/tests/test_dashboard_models_leaderboard.py
git commit -m "feat(dashboard): extend /api/models leaderboard payload with 7d stats + sparkline"
```

---

## Task 5: Frontend — Models tab controller + sub-tab scaffold

**Files:**
- Create: `bullforge/services/dashboard_service/static/models_tab.js`
- Modify: `bullforge/services/dashboard_service/static/index.html`
- Modify: `bullforge/services/dashboard_service/static/dashboard.js`
- Modify: `bullforge/services/dashboard_service/static/style.css`

This task builds the shell — sub-tab switcher, routing integration, empty panels. Each sub-tab becomes a ~30-50 line addition in subsequent tasks.

- [ ] **Step 1: Replace Models tab markup in `index.html`**

Find the existing Models tab in `static/index.html`:

```html
    <!-- Models Tab -->
    <section id="tab-models" class="tab-content">
        <table id="models-table" class="data-table">
            <thead>
                <tr>
                    <th>Model ID</th>
                    <th>Status</th>
                    <th>Reason</th>
                    <th>Updated</th>
                </tr>
            </thead>
            <tbody></tbody>
        </table>
    </section>
```

Replace with:

```html
    <!-- Models Tab -->
    <section id="tab-models" class="tab-content">
        <div class="subtab-bar" id="models-subtabs">
            <button class="subtab active" data-subtab="leaderboard">Leaderboard</button>
            <button class="subtab" data-subtab="fidelity">Fidelity</button>
            <button class="subtab" data-subtab="calibration">Calibration</button>
            <button class="subtab" data-subtab="attribution">Attribution</button>
            <button class="subtab" data-subtab="timeseries">Time-series</button>
            <button class="subtab" data-subtab="regime">Regime</button>
            <button class="subtab" data-subtab="featimp">Feature Importance</button>
        </div>
        <div class="subtab-toolbar" id="models-toolbar">
            <span class="toolbar-label">Window:</span>
            <span id="models-timewindow-slot"></span>
            <span id="models-refresh-slot" class="toolbar-right"></span>
        </div>
        <div id="models-panel"></div>
    </section>
```

- [ ] **Step 2: Load `models_tab.js` from `index.html`**

In the script block at the bottom of `index.html`, add `models_tab.js` right after `dashboard.js`:

```html
    <script src="/static/dashboard.js"></script>
    <script src="/static/models_tab.js"></script>
    <script src="/static/backtests.js?v=2"></script>
```

- [ ] **Step 3: Remove old Models-tab code from `dashboard.js`**

Find `loadModels()` in `bullforge/services/dashboard_service/static/dashboard.js` and **delete the entire function**. Also delete any `setInterval` / `click` handler that called `loadModels`. The Models tab is now owned by `models_tab.js`.

Search for and delete these two patterns (adjust to what you actually see):

```javascript
async function loadModels() { ... }
```

and any reference like:

```javascript
if (btn.dataset.tab === 'models') loadModels();
```

- [ ] **Step 4: Create the Models tab controller with empty sub-tab handlers**

Create `bullforge/services/dashboard_service/static/models_tab.js`:

```javascript
// models_tab.js — Models tab controller.
// Owns: sub-tab switching, time-window + refresh wiring, router integration,
// per-sub-tab data fetching and rendering.
//
// Uses Plan 1 components: window.Router, window.TimeWindow, window.Refresh,
// window.Charts, window.Leaderboard, window.DrillDown.
(function () {
  var state = {
    activeSubtab: 'leaderboard',
    window: {from: null, to: null},
    timewindow: null,
    refresh: null,
    currentDrillId: null,
  };

  function $(id) { return document.getElementById(id); }
  function panel() { return $('models-panel'); }

  function setActiveSubtab(name) {
    state.activeSubtab = name;
    document.querySelectorAll('#models-subtabs .subtab').forEach(function (b) {
      b.classList.toggle('active', b.dataset.subtab === name);
    });
    render();
  }

  async function fetchJSON(path) {
    var url = path;
    var range = state.window;
    if (range && range.from) {
      url += (url.indexOf('?') >= 0 ? '&' : '?') + 'from=' + encodeURIComponent(range.from);
    }
    if (range && range.to) {
      url += (url.indexOf('?') >= 0 ? '&' : '?') + 'to=' + encodeURIComponent(range.to);
    }
    var r = await fetch(url);
    if (!r.ok) {
      var body = await r.json().catch(function () { return {}; });
      throw new Error('HTTP ' + r.status + ' ' + JSON.stringify(body));
    }
    return r.json();
  }

  // Sub-tab handlers are plugged in by later tasks (Tasks 6-12).
  var handlers = {
    leaderboard: async function () {
      panel().innerHTML = '<p class="empty-state">Leaderboard sub-tab placeholder (Task 6).</p>';
    },
    fidelity: async function () {
      panel().innerHTML = '<p class="empty-state">Fidelity sub-tab placeholder (Task 7).</p>';
    },
    calibration: async function () {
      panel().innerHTML = '<p class="empty-state">Calibration sub-tab placeholder (Task 8).</p>';
    },
    attribution: async function () {
      panel().innerHTML = '<p class="empty-state">Attribution sub-tab placeholder (Task 9).</p>';
    },
    timeseries: async function () {
      panel().innerHTML = '<p class="empty-state">Time-series sub-tab placeholder (Task 10).</p>';
    },
    regime: async function () {
      panel().innerHTML = '<p class="empty-state">Regime sub-tab placeholder (Task 11).</p>';
    },
    featimp: async function () {
      panel().innerHTML = '<p class="empty-state">Feature Importance sub-tab placeholder (Task 12).</p>';
    },
  };

  async function renderDrillDown(modelId) {
    state.currentDrillId = modelId;
    panel().innerHTML = '';
    var dd = window.DrillDown.create({
      mountEl: panel(),
      title: 'Model #' + modelId,
      backHref: '#models/leaderboard',
      breadcrumbs: [
        {label: 'Models', href: '#models'},
        {label: 'Leaderboard', href: '#models/leaderboard'},
        {label: 'Model #' + modelId},
      ],
    });
    dd.getContentSlot().innerHTML =
      '<p class="empty-state">Per-model drill-down placeholder (Task 13).</p>';
  }

  async function render() {
    if (state.currentDrillId !== null) {
      // Drill-down rendering handled separately.
      return;
    }
    var handler = handlers[state.activeSubtab];
    if (!handler) {
      panel().innerHTML = '<p class="empty-state">Unknown sub-tab: ' + state.activeSubtab + '</p>';
      return;
    }
    try {
      await handler();
    } catch (e) {
      panel().innerHTML = '<p class="error-state">Error loading sub-tab: ' + e.message + '</p>';
    }
  }

  function activate() {
    // Mount time-window picker.
    if (!state.timewindow) {
      state.timewindow = window.TimeWindow.create({
        mountEl: $('models-timewindow-slot'),
        storageKey: 'models.timewindow',
        onChange: function (range) {
          state.window = range;
          render();
        },
      });
    }
    // Mount refresh control.
    if (!state.refresh) {
      state.refresh = window.Refresh.create({
        mountEl: $('models-refresh-slot'),
        storageKey: 'models.refresh',
        initialIntervalSec: 30,
        onRefresh: function () { render(); },
      });
    }
    // Wire sub-tab buttons.
    document.querySelectorAll('#models-subtabs .subtab').forEach(function (b) {
      b.onclick = function () {
        window.Router.navigate('#models/' + b.dataset.subtab);
      };
    });
  }

  function handleRoute(route) {
    if (route.tab !== 'models') return;
    activate();
    if (route.entity_kind === 'models' && route.entity_id) {
      renderDrillDown(route.entity_id);
    } else if (route.subtab) {
      state.currentDrillId = null;
      setActiveSubtab(route.subtab);
    } else {
      state.currentDrillId = null;
      setActiveSubtab('leaderboard');
    }
  }

  // Boot on first DOM load + subscribe to future routes.
  window.addEventListener('DOMContentLoaded', function () {
    if (window.Router) {
      window.Router.subscribe(handleRoute);
      handleRoute(window.Router.current());
    }
  });

  // Also handle the case where the top-level tab click switches to models.
  document.addEventListener('click', function (e) {
    var btn = e.target && e.target.closest && e.target.closest('.tab[data-tab="models"]');
    if (btn) {
      // Defer routing to Router.
      setTimeout(function () {
        window.Router.navigate('#models/' + (state.activeSubtab || 'leaderboard'));
      }, 0);
    }
  });

  window.ModelsTab = {
    _state: state,
    _handlers: handlers,
    _render: render,
    _fetchJSON: fetchJSON,
  };
})();
```

- [ ] **Step 5: Add sub-tab CSS**

In `bullforge/services/dashboard_service/static/style.css`, append:

```css
/* --- Sub-tabs (Plan 2) ---------------------------------------------------- */

.subtab-bar {
    display: flex;
    gap: 0;
    border-bottom: 1px solid var(--border);
    margin-bottom: 1rem;
}

.subtab {
    padding: 0.5rem 1rem;
    background: none;
    border: none;
    color: var(--text-muted);
    cursor: pointer;
    font-size: 0.85rem;
    border-bottom: 2px solid transparent;
    transition: color 0.2s, border-color 0.2s;
}

.subtab:hover { color: var(--text); }
.subtab.active { color: var(--accent); border-bottom-color: var(--accent); }

.subtab-toolbar {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 0;
    margin-bottom: 1rem;
    font-size: 0.85rem;
}

.toolbar-label {
    color: var(--text-muted);
    margin-right: 0.25rem;
}

.toolbar-right { margin-left: auto; }

.error-state {
    color: var(--red);
    padding: 1rem;
    background: rgba(231, 76, 60, 0.1);
    border-radius: 4px;
}

.tw-select, .refresh-interval {
    background: var(--bg-card);
    color: var(--text);
    border: 1px solid var(--border);
    padding: 0.25rem 0.5rem;
    border-radius: 4px;
    font-family: inherit;
    font-size: 0.85rem;
}

.refresh-btn, .refresh-manual {
    background: var(--bg-card);
    color: var(--text);
    border: 1px solid var(--border);
    padding: 0.25rem 0.5rem;
    border-radius: 4px;
    cursor: pointer;
    font-family: inherit;
    font-size: 0.85rem;
}
.refresh-btn:hover, .refresh-manual:hover { border-color: var(--accent); }

.dd-back { cursor: pointer; }
.dd-back:hover { text-decoration: underline; }
```

- [ ] **Step 6: Reload dashboard and verify**

Reload `http://localhost:18084/#models`.

Expected:
- Sub-tab bar visible with 7 buttons; "Leaderboard" highlighted.
- Toolbar with time-window picker + refresh control.
- Panel shows "Leaderboard sub-tab placeholder (Task 6)."
- Clicking sub-tab buttons changes the panel placeholder text and updates URL hash to `#models/fidelity` etc.
- Direct URL `http://localhost:18084/#models/42` renders the drill-down placeholder.
- Other tabs (Overview, Trading, Trades, Orders) still work.
- Browser DevTools console has no errors from `models_tab.js`.

- [ ] **Step 7: Commit**

```bash
git add bullforge/services/dashboard_service/static/models_tab.js \
        bullforge/services/dashboard_service/static/index.html \
        bullforge/services/dashboard_service/static/dashboard.js \
        bullforge/services/dashboard_service/static/style.css
git commit -m "feat(dashboard): Models tab sub-tab scaffold + router/timewindow/refresh wiring"
```

---

## Task 6: Leaderboard sub-tab

**Files:**
- Modify: `bullforge/services/dashboard_service/static/models_tab.js` (replace the `handlers.leaderboard` placeholder)

- [ ] **Step 1: Replace the `leaderboard` placeholder**

In `models_tab.js`, replace the existing `leaderboard:` handler (which shows the placeholder text) with:

```javascript
    leaderboard: async function () {
      panel().innerHTML = '<div id="models-lb-host"></div>';
      var rows = await fetchJSON('/api/models');
      var host = $('models-lb-host');
      window.Leaderboard.create({
        mountEl: host,
        columns: [
          {key: 'symbol', label: 'Symbol', sortable: true},
          {key: 'timeframe', label: 'TF', sortable: true},
          {key: 'horizon_minutes', label: 'Horizon', sortable: true,
           render: function (v) { return v + 'm'; }},
          {key: 'model_type', label: 'Type', sortable: true},
          {key: 'status', label: 'Status', sortable: true,
           render: function (v) {
             return '<span class="status-cell ' + v + '">' + v + '</span>';
           }},
          {key: 'live_accuracy_7d', label: 'Live 7d', sortable: true,
           render: function (v) { return (v * 100).toFixed(1) + '%'; }},
          {key: 'backtested_accuracy', label: 'Backtest', sortable: true,
           render: function (v) { return (v * 100).toFixed(1) + '%'; }},
          {key: 'fidelity_delta', label: 'Δ (bt−live)', sortable: true,
           render: function (v) {
             var cls = v > 0.05 ? 'negative' : (v > 0.02 ? 'warning' : '');
             return '<span class="' + cls + '">' + (v * 100).toFixed(1) + '%</span>';
           }},
          {key: 'signals_7d', label: 'Signals 7d', sortable: true},
          {key: 'simulated_pnl_7d_pct', label: 'Sim P&L 7d', sortable: true,
           render: function (v) {
             var cls = v >= 0 ? 'positive' : 'negative';
             return '<span class="' + cls + '">' + (v * 100).toFixed(2) + '%</span>';
           }},
          {key: 'accuracy_sparkline_30d', label: '30d', sortable: false,
           render: function (values, row) {
             if (!values || values.length === 0) return '<span class="muted">—</span>';
             return '<span class="sparkline-cell" data-mid="' + row.model_id + '"></span>';
           }},
        ],
        rows: rows,
        getRowHref: function (row) { return '#models/' + row.model_id; },
        initialSort: {key: 'fidelity_delta', order: 'desc'},
      });
      // Render sparklines after leaderboard inserts the cells.
      host.querySelectorAll('.sparkline-cell').forEach(function (el) {
        var row = rows.filter(function (r) {
          return String(r.model_id) === el.getAttribute('data-mid');
        })[0];
        if (row && row.accuracy_sparkline_30d && row.accuracy_sparkline_30d.length > 0) {
          el.style.display = 'inline-block';
          el.style.width = '80px';
          el.style.height = '20px';
          window.Charts.sparkline(el, {values: row.accuracy_sparkline_30d});
        }
      });
    },
```

Also add CSS for the `warning` / `muted` classes (append to `style.css`):

```css
.warning { color: var(--yellow); }
.muted { color: var(--text-muted); }
.positive { color: var(--green); }
.negative { color: var(--red); }

.leaderboard-table td,
.leaderboard-table th {
    padding: 0.5rem 0.75rem;
    border-bottom: 1px solid var(--border);
    font-size: 0.85rem;
}
.leaderboard-table th { color: var(--text-muted); text-align: left; }
.leaderboard-table tbody tr:hover { background: rgba(255,255,255,0.03); }
.leaderboard-table a { color: var(--accent); text-decoration: none; }
.leaderboard-table a:hover { text-decoration: underline; }

.sparkline-cell {
    display: inline-block;
    width: 80px;
    height: 20px;
    vertical-align: middle;
}
```

- [ ] **Step 2: Reload dashboard**

Reload `http://localhost:18084/#models/leaderboard`.

Expected:
- Table with one row per active model.
- Sortable on every column.
- Fidelity delta column color-coded (red if >5%, yellow if >2%).
- Sparkline renders inline in the 30d column.
- Clicking a row navigates to `#models/<id>`.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/models_tab.js \
        bullforge/services/dashboard_service/static/style.css
git commit -m "feat(dashboard): Models Leaderboard sub-tab (sortable + sparklines + color-coded fidelity)"
```

---

## Task 7: Fidelity sub-tab

**Files:**
- Modify: `bullforge/services/dashboard_service/static/models_tab.js` (replace `handlers.fidelity`)

- [ ] **Step 1: Replace the `fidelity` placeholder**

Replace in `models_tab.js`:

```javascript
    fidelity: async function () {
      panel().innerHTML =
        '<div class="fidelity-panel">' +
        '<p class="subtitle">Per-model live accuracy (7-day rolling) vs backtested baseline. Red line = warning margin, dashed red = critical margin.</p>' +
        '<div class="model-selector"><label>Models: </label>' +
        '<select id="fidelity-model-select" multiple size="6" style="min-width:400px;"></select>' +
        '<button id="fidelity-refresh">Load</button></div>' +
        '<div id="fidelity-chart-host" class="chart-host" style="width:100%;height:360px;margin-top:1rem;"></div>' +
        '</div>';

      var models = await fetchJSON('/api/models');
      var select = $('fidelity-model-select');
      select.innerHTML = models.map(function (m) {
        return '<option value="' + m.model_id + '" selected>' +
          m.symbol + ' · ' + m.timeframe + ' · ' + m.horizon_minutes + 'm · ' + m.model_type +
          '</option>';
      }).join('');
      // Cap to first 8 selected by default for rendering performance.
      Array.prototype.slice.call(select.options).forEach(function (o, i) {
        if (i >= 8) o.selected = false;
      });

      async function load() {
        var selected = Array.prototype.filter.call(select.options,
          function (o) { return o.selected; }).map(function (o) { return o.value; });
        var series = [];
        for (var i = 0; i < selected.length; i++) {
          var mid = selected[i];
          var hist = await fetchJSON('/api/models/' + mid + '/history');
          var m = models.filter(function (mm) {
            return String(mm.model_id) === String(mid);
          })[0];
          var label = m ? (m.symbol + ' · ' + m.horizon_minutes + 'm') : ('#' + mid);
          series.push({
            label: label,
            data: hist.map(function (h) {
              return {x: h.date, y: h.accuracy};
            }),
          });
        }
        window.Charts.lineChart($('fidelity-chart-host'), {
          series: series,
          xIsTime: true,
          yLabel: 'Accuracy',
        });
      }

      $('fidelity-refresh').onclick = load;
      await load();
    },
```

- [ ] **Step 2: Reload dashboard**

Reload `http://localhost:18084/#models/fidelity`.

Expected:
- Model multi-select populated from `/api/models`.
- Default: first 8 models selected.
- Chart renders a line per model using daily accuracy from `/api/models/{id}/history`.
- Changing selection and clicking "Load" re-renders.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/models_tab.js
git commit -m "feat(dashboard): Models Fidelity sub-tab (multi-model accuracy-over-time overlay)"
```

---

## Task 8: Calibration sub-tab

**Files:**
- Modify: `bullforge/services/dashboard_service/static/models_tab.js` (replace `handlers.calibration`)

- [ ] **Step 1: Replace the `calibration` placeholder**

Replace in `models_tab.js`:

```javascript
    calibration: async function () {
      panel().innerHTML =
        '<div class="calibration-panel">' +
        '<p class="subtitle">Reliability diagram: predicted confidence vs actual hit rate, binned into 10 ranges. Perfect calibration = diagonal line.</p>' +
        '<div class="model-selector"><label>Model: </label>' +
        '<select id="cal-model-select"></select></div>' +
        '<div class="cal-metrics" id="cal-metrics" style="margin:0.5rem 0;color:var(--text-muted)"></div>' +
        '<div id="cal-chart-host" class="chart-host" style="width:100%;height:360px;"></div>' +
        '</div>';

      var models = await fetchJSON('/api/models');
      var select = $('cal-model-select');
      select.innerHTML = models.map(function (m) {
        return '<option value="' + m.model_id + '">' +
          m.symbol + ' · ' + m.timeframe + ' · ' + m.horizon_minutes + 'm · ' + m.model_type +
          '</option>';
      }).join('');

      async function load() {
        var mid = select.value;
        var payload = await fetchJSON('/api/models/' + mid + '/calibration');
        $('cal-metrics').textContent =
          'Samples: ' + payload.sample_size +
          ' · Calibration error: ' + (payload.calibration_error * 100).toFixed(2) + '%';
        var points = payload.bins
          .filter(function (b) { return b.count > 0; })
          .map(function (b) {
            return {x: b.avg_confidence, y: b.hit_rate, r: Math.min(10, 3 + Math.sqrt(b.count))};
          });
        window.Charts.scatterChart($('cal-chart-host'), {
          points: points,
          referenceDiagonal: true,
          xLabel: 'Predicted confidence',
          yLabel: 'Actual hit rate',
        });
      }

      select.onchange = load;
      await load();
    },
```

- [ ] **Step 2: Reload and verify**

Reload `http://localhost:18084/#models/calibration`.

Expected:
- Model selector populated.
- Scatter chart with diagonal reference and one bubble per populated bin.
- Bubble size proportional to sample count.
- "Calibration error: X%" text above chart.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/models_tab.js
git commit -m "feat(dashboard): Models Calibration sub-tab (reliability diagram)"
```

---

## Task 9: Attribution sub-tab

**Files:**
- Modify: `bullforge/services/dashboard_service/static/models_tab.js` (replace `handlers.attribution`)

- [ ] **Step 1: Replace the `attribution` placeholder**

Replace in `models_tab.js`:

```javascript
    attribution: async function () {
      panel().innerHTML =
        '<div class="attribution-panel">' +
        '<p class="subtitle">Per-model signal count and simulated P&L (if every signal had been traded with unit position). Red/green by P&L sign.</p>' +
        '<div id="attr-signals-host" class="chart-host" style="width:100%;height:260px;margin-bottom:1rem;"></div>' +
        '<div id="attr-pnl-host" class="chart-host" style="width:100%;height:260px;"></div>' +
        '</div>';

      var models = await fetchJSON('/api/models');
      var labels = [];
      var signals = [];
      var pnls = [];

      for (var i = 0; i < models.length; i++) {
        var m = models[i];
        labels.push(m.symbol + ' · ' + m.horizon_minutes + 'm');
        var sim = await fetchJSON('/api/models/' + m.model_id + '/simulated_pnl');
        signals.push(sim.signal_count);
        pnls.push(sim.cumulative_pnl_pct * 100);  // as percent
      }

      window.Charts.barChart($('attr-signals-host'), {
        labels: labels, values: signals,
        yLabel: 'Signal count (over selected window)',
        colorByValue: false,
      });
      window.Charts.barChart($('attr-pnl-host'), {
        labels: labels, values: pnls,
        yLabel: 'Cumulative simulated P&L (%)',
        colorByValue: true,
      });
    },
```

- [ ] **Step 2: Reload**

Reload `http://localhost:18084/#models/attribution`.

Expected:
- Two stacked bar charts, one row per model.
- Signal count chart uses accent color.
- P&L chart red/green by sign.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/models_tab.js
git commit -m "feat(dashboard): Models Attribution sub-tab (signals + simulated PnL per model)"
```

---

## Task 10: Time-series sub-tab

**Files:**
- Modify: `bullforge/services/dashboard_service/static/models_tab.js` (replace `handlers.timeseries`)

The Time-series sub-tab is essentially Fidelity's layout but without the multi-model clutter — showing daily per-day accuracy for a single model + signal count overlay.

- [ ] **Step 1: Replace the `timeseries` placeholder**

Replace in `models_tab.js`:

```javascript
    timeseries: async function () {
      panel().innerHTML =
        '<div class="timeseries-panel">' +
        '<p class="subtitle">Daily accuracy and signal count for a single model over the selected window.</p>' +
        '<div class="model-selector"><label>Model: </label>' +
        '<select id="ts-model-select"></select></div>' +
        '<div id="ts-acc-host" class="chart-host" style="width:100%;height:220px;margin-top:1rem;"></div>' +
        '<div id="ts-count-host" class="chart-host" style="width:100%;height:180px;margin-top:1rem;"></div>' +
        '</div>';

      var models = await fetchJSON('/api/models');
      var select = $('ts-model-select');
      select.innerHTML = models.map(function (m) {
        return '<option value="' + m.model_id + '">' +
          m.symbol + ' · ' + m.timeframe + ' · ' + m.horizon_minutes + 'm · ' + m.model_type +
          '</option>';
      }).join('');

      async function load() {
        var mid = select.value;
        var hist = await fetchJSON('/api/models/' + mid + '/history');
        window.Charts.lineChart($('ts-acc-host'), {
          series: [{
            label: 'Accuracy',
            data: hist.map(function (h) { return {x: h.date, y: h.accuracy}; }),
          }],
          xIsTime: true,
          yLabel: 'Daily accuracy',
        });
        window.Charts.barChart($('ts-count-host'), {
          labels: hist.map(function (h) { return h.date.slice(0, 10); }),
          values: hist.map(function (h) { return h.total; }),
          yLabel: 'Signal count (per day)',
          colorByValue: false,
        });
      }

      select.onchange = load;
      await load();
    },
```

- [ ] **Step 2: Reload**

Reload `http://localhost:18084/#models/timeseries`.

Expected:
- Single model selector.
- Accuracy line chart (top) + signal count bar chart (bottom).
- Changing model reloads both.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/models_tab.js
git commit -m "feat(dashboard): Models Time-series sub-tab (single-model accuracy + signal count)"
```

---

## Task 11: Regime sub-tab

**Files:**
- Modify: `bullforge/services/dashboard_service/static/models_tab.js` (replace `handlers.regime`)

- [ ] **Step 1: Replace the `regime` placeholder**

Replace in `models_tab.js`:

```javascript
    regime: async function () {
      panel().innerHTML =
        '<div class="regime-panel">' +
        '<p class="subtitle">Per-regime accuracy and P&L. Regime classified by SPY 50-bar SMA at prediction time (bull/bear/neutral/unknown).</p>' +
        '<div class="model-selector"><label>Model: </label>' +
        '<select id="regime-model-select"></select></div>' +
        '<div id="regime-acc-host" class="chart-host" style="width:100%;height:260px;margin-top:1rem;"></div>' +
        '<div id="regime-pnl-host" class="chart-host" style="width:100%;height:260px;margin-top:1rem;"></div>' +
        '<div id="regime-table" style="margin-top:1rem;"></div>' +
        '</div>';

      var models = await fetchJSON('/api/models');
      var select = $('regime-model-select');
      select.innerHTML = models.map(function (m) {
        return '<option value="' + m.model_id + '">' +
          m.symbol + ' · ' + m.timeframe + ' · ' + m.horizon_minutes + 'm · ' + m.model_type +
          '</option>';
      }).join('');

      async function load() {
        var mid = select.value;
        var rows = await fetchJSON('/api/models/' + mid + '/regime_breakdown');
        var labels = rows.map(function (r) { return r.regime; });
        var accs = rows.map(function (r) { return r.accuracy * 100; });
        var pnls = rows.map(function (r) { return r.cumulative_pnl_pct * 100; });

        window.Charts.barChart($('regime-acc-host'), {
          labels: labels, values: accs,
          yLabel: 'Accuracy (%)', colorByValue: false,
        });
        window.Charts.barChart($('regime-pnl-host'), {
          labels: labels, values: pnls,
          yLabel: 'Cumulative P&L (%)', colorByValue: true,
        });

        $('regime-table').innerHTML =
          '<table class="data-table"><thead><tr>' +
          '<th>Regime</th><th>Signals</th><th>Correct</th><th>Accuracy</th><th>Cumulative P&L</th>' +
          '</tr></thead><tbody>' +
          rows.map(function (r) {
            return '<tr><td>' + r.regime + '</td>' +
              '<td>' + r.count + '</td>' +
              '<td>' + r.correct + '</td>' +
              '<td>' + (r.accuracy * 100).toFixed(1) + '%</td>' +
              '<td>' + (r.cumulative_pnl_pct * 100).toFixed(2) + '%</td></tr>';
          }).join('') +
          '</tbody></table>';
      }

      select.onchange = load;
      await load();
    },
```

- [ ] **Step 2: Reload + verify**

Reload `http://localhost:18084/#models/regime`.

Expected:
- Model selector.
- Two bar charts (accuracy, P&L) with one bar per regime (bull/bear/neutral/unknown).
- Summary table below.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/models_tab.js
git commit -m "feat(dashboard): Models Regime sub-tab (per-regime accuracy + PnL)"
```

---

## Task 12: Feature Importance sub-tab

**Files:**
- Modify: `bullforge/services/dashboard_service/static/models_tab.js` (replace `handlers.featimp`)

- [ ] **Step 1: Replace the `featimp` placeholder**

Replace in `models_tab.js`:

```javascript
    featimp: async function () {
      panel().innerHTML =
        '<div class="featimp-panel">' +
        '<p class="subtitle">Top 20 features by importance for the selected model. Unavailable for sequence models (LSTM).</p>' +
        '<div class="model-selector"><label>Model: </label>' +
        '<select id="fi-model-select"></select></div>' +
        '<div id="fi-chart-host" class="chart-host" style="width:100%;height:500px;margin-top:1rem;"></div>' +
        '<div id="fi-note" style="margin-top:0.5rem;color:var(--text-muted);"></div>' +
        '</div>';

      var models = await fetchJSON('/api/models');
      var select = $('fi-model-select');
      select.innerHTML = models.map(function (m) {
        return '<option value="' + m.model_id + '">' +
          m.symbol + ' · ' + m.timeframe + ' · ' + m.horizon_minutes + 'm · ' + m.model_type +
          '</option>';
      }).join('');

      async function load() {
        var mid = select.value;
        var payload = await fetchJSON('/api/models/' + mid + '/feature_importance');
        if (!payload.available) {
          $('fi-chart-host').innerHTML = '';
          $('fi-note').textContent = payload.reason || 'Feature importance not available.';
          return;
        }
        $('fi-note').textContent = '';
        // Horizontal bar: feature names as labels, importances as values.
        var names = payload.features.map(function (f) { return f.name; }).reverse();
        var values = payload.features.map(function (f) { return f.importance; }).reverse();
        window.Charts.barChart($('fi-chart-host'), {
          labels: names, values: values,
          yLabel: 'Importance', colorByValue: false,
        });
      }

      select.onchange = load;
      await load();
    },
```

- [ ] **Step 2: Reload + verify**

Reload `http://localhost:18084/#models/featimp`.

Expected:
- Model selector.
- For gradient_boost models: horizontal bar chart with top 20 features.
- For lstm models: blank chart + "feature importance not available for lstm" note.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/models_tab.js
git commit -m "feat(dashboard): Models Feature Importance sub-tab (top 20 bar chart)"
```

---

## Task 13: Per-model drill-down page

**Files:**
- Modify: `bullforge/services/dashboard_service/static/models_tab.js` (replace `renderDrillDown`)

- [ ] **Step 1: Replace `renderDrillDown`**

In `models_tab.js`, replace the existing `renderDrillDown` placeholder with:

```javascript
  async function renderDrillDown(modelId) {
    state.currentDrillId = modelId;
    panel().innerHTML = '';
    var models = await fetchJSON('/api/models');
    var m = models.filter(function (x) { return String(x.model_id) === String(modelId); })[0];
    var title = m
      ? ('Model #' + modelId + ' — ' + m.symbol + ' · ' + m.timeframe + ' · ' + m.horizon_minutes + 'm · ' + m.model_type)
      : ('Model #' + modelId);

    var dd = window.DrillDown.create({
      mountEl: panel(),
      title: title,
      backHref: '#models/leaderboard',
      breadcrumbs: [
        {label: 'Models', href: '#models'},
        {label: 'Leaderboard', href: '#models/leaderboard'},
        {label: m ? (m.symbol + ' · ' + m.horizon_minutes + 'm') : ('#' + modelId)},
      ],
    });
    var slot = dd.getContentSlot();
    slot.innerHTML =
      '<section class="drill-section"><h3>Accuracy over time</h3>' +
      '<div id="dd-ts" class="chart-host" style="width:100%;height:240px;"></div></section>' +
      '<section class="drill-section"><h3>Calibration</h3>' +
      '<div id="dd-cal-metrics" style="color:var(--text-muted);"></div>' +
      '<div id="dd-cal" class="chart-host" style="width:100%;height:300px;"></div></section>' +
      '<section class="drill-section"><h3>Simulated P&L</h3>' +
      '<div id="dd-pnl-summary" style="color:var(--text-muted);"></div>' +
      '<div id="dd-pnl" class="chart-host" style="width:100%;height:240px;"></div></section>' +
      '<section class="drill-section"><h3>Regime breakdown</h3>' +
      '<div id="dd-regime-chart" class="chart-host" style="width:100%;height:220px;"></div>' +
      '<div id="dd-regime-table"></div></section>' +
      '<section class="drill-section"><h3>Feature importance (top 20)</h3>' +
      '<div id="dd-fi" class="chart-host" style="width:100%;height:480px;"></div>' +
      '<div id="dd-fi-note" style="color:var(--text-muted);"></div></section>' +
      '<section class="drill-section"><h3>Recent signals (last 100)</h3>' +
      '<div id="dd-sig-host"></div></section>';

    // Fire all fetches in parallel.
    var [hist, cal, sim, regime, fi, sig] = await Promise.all([
      fetchJSON('/api/models/' + modelId + '/history'),
      fetchJSON('/api/models/' + modelId + '/calibration'),
      fetchJSON('/api/models/' + modelId + '/simulated_pnl'),
      fetchJSON('/api/models/' + modelId + '/regime_breakdown'),
      fetchJSON('/api/models/' + modelId + '/feature_importance'),
      fetchJSON('/api/models/' + modelId + '/signal_history?limit=100'),
    ]);

    // Accuracy over time.
    window.Charts.lineChart($('dd-ts'), {
      series: [{
        label: 'Accuracy',
        data: hist.map(function (h) { return {x: h.date, y: h.accuracy}; }),
      }],
      xIsTime: true, yLabel: 'Daily accuracy',
    });

    // Calibration.
    $('dd-cal-metrics').textContent =
      'Samples: ' + cal.sample_size + ' · Calibration error: ' + (cal.calibration_error * 100).toFixed(2) + '%';
    window.Charts.scatterChart($('dd-cal'), {
      points: cal.bins.filter(function (b) { return b.count > 0; })
        .map(function (b) {
          return {x: b.avg_confidence, y: b.hit_rate, r: Math.min(10, 3 + Math.sqrt(b.count))};
        }),
      referenceDiagonal: true,
      xLabel: 'Predicted confidence', yLabel: 'Actual hit rate',
    });

    // Simulated P&L.
    $('dd-pnl-summary').textContent =
      'Signals: ' + sim.signal_count +
      ' · Cumulative P&L: ' + (sim.cumulative_pnl_pct * 100).toFixed(2) + '%' +
      ' · Avg per signal: ' + (sim.avg_pnl_pct * 100).toFixed(3) + '%' +
      ' · Win rate: ' + (sim.win_rate * 100).toFixed(1) + '%';
    window.Charts.barChart($('dd-pnl'), {
      labels: sim.daily_series.map(function (d) { return d.date.slice(0, 10); }),
      values: sim.daily_series.map(function (d) { return d.pnl_pct * 100; }),
      yLabel: 'Daily P&L (%)', colorByValue: true,
    });

    // Regime.
    window.Charts.barChart($('dd-regime-chart'), {
      labels: regime.map(function (r) { return r.regime; }),
      values: regime.map(function (r) { return r.accuracy * 100; }),
      yLabel: 'Accuracy (%)', colorByValue: false,
    });
    $('dd-regime-table').innerHTML =
      '<table class="data-table"><thead><tr>' +
      '<th>Regime</th><th>Signals</th><th>Accuracy</th><th>P&L</th>' +
      '</tr></thead><tbody>' +
      regime.map(function (r) {
        return '<tr><td>' + r.regime + '</td>' +
          '<td>' + r.count + '</td>' +
          '<td>' + (r.accuracy * 100).toFixed(1) + '%</td>' +
          '<td>' + (r.cumulative_pnl_pct * 100).toFixed(2) + '%</td></tr>';
      }).join('') +
      '</tbody></table>';

    // Feature importance.
    if (fi.available) {
      $('dd-fi-note').textContent = '';
      window.Charts.barChart($('dd-fi'), {
        labels: fi.features.map(function (f) { return f.name; }).reverse(),
        values: fi.features.map(function (f) { return f.importance; }).reverse(),
        yLabel: 'Importance', colorByValue: false,
      });
    } else {
      $('dd-fi').innerHTML = '';
      $('dd-fi-note').textContent = fi.reason || 'Feature importance not available.';
    }

    // Signal history.
    window.Leaderboard.create({
      mountEl: $('dd-sig-host'),
      columns: [
        {key: 'timestamp', label: 'Time', sortable: true,
         render: function (v) { return window.fmtTime ? window.fmtTime(v) : v; }},
        {key: 'symbol', label: 'Symbol', sortable: true},
        {key: 'predicted_direction', label: 'Dir', sortable: true,
         render: function (v) { return v === 1 ? '↑' : (v === -1 ? '↓' : '→'); }},
        {key: 'confidence', label: 'Conf', sortable: true,
         render: function (v) { return (v * 100).toFixed(1) + '%'; }},
        {key: 'entry_price', label: 'Entry', sortable: false,
         render: function (v) { return v != null ? v.toFixed(2) : '—'; }},
        {key: 'exit_price', label: 'Exit', sortable: false,
         render: function (v) { return v != null ? v.toFixed(2) : '—'; }},
        {key: 'is_correct', label: 'Hit?', sortable: false,
         render: function (v) {
           if (v === null || v === undefined) return '—';
           return v ? '<span class="positive">✓</span>' : '<span class="negative">✗</span>';
         }},
        {key: 'pnl_pct', label: 'P&L', sortable: true,
         render: function (v) {
           if (v === null || v === undefined) return '—';
           var cls = v >= 0 ? 'positive' : 'negative';
           return '<span class="' + cls + '">' + (v * 100).toFixed(3) + '%</span>';
         }},
      ],
      rows: sig,
    });
  }
```

Also add a small CSS block for `.drill-section` — append to `style.css`:

```css
.drill-section {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 1rem;
    margin-bottom: 1rem;
}
.drill-section h3 {
    margin: 0 0 0.5rem 0;
    font-size: 1rem;
    color: var(--accent);
}
```

- [ ] **Step 2: Reload drill-down**

Reload `http://localhost:18084/#models/42` (replace 42 with a real model ID from the leaderboard).

Expected:
- DrillDown header (back button + title + breadcrumbs).
- Six sections stacked vertically: Accuracy over time, Calibration, Simulated P&L, Regime breakdown, Feature importance, Recent signals.
- All sections populated in parallel (fast — one network round-trip per endpoint).
- Clicking Back returns to `#models/leaderboard`.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/models_tab.js \
        bullforge/services/dashboard_service/static/style.css
git commit -m "feat(dashboard): per-model drill-down page (6 sections + signal history)"
```

---

## Task 14: End-to-end smoke test + verification

**Files:** none modified — verification only.

- [ ] **Step 1: Full backend test suite**

Run: `cd bullforge && python -m pytest tests/test_time_window.py tests/test_dashboard_api.py tests/test_dashboard_stubs.py tests/test_dashboard_models_api.py tests/test_dashboard_models_leaderboard.py tests/test_accuracy_snapshot.py tests/test_dashboard_backtests_api.py -v`

Expected: all PASS.

- [ ] **Step 2: Full core suite sanity**

Run: `cd bullforge && python -m pytest tests/test_signal_aggregator.py tests/test_risk_manager.py tests/test_trainer.py tests/test_lstm_model.py tests/test_order_executor.py tests/meta/ -v`

Expected: no regressions.

- [ ] **Step 3: Manual dashboard walkthrough**

Open `http://localhost:18084/` in a browser. Visit, in order:

1. `#models/leaderboard` — sortable table renders with sparklines, fidelity delta color-coded.
2. `#models/fidelity` — multi-select populated; chart renders on Load.
3. `#models/calibration` — model selector; reliability diagram with diagonal reference.
4. `#models/attribution` — two stacked bar charts (signals + P&L).
5. `#models/timeseries` — single-model selector; accuracy line + signal-count bars.
6. `#models/regime` — two bar charts + summary table.
7. `#models/featimp` — top-20 feature bar chart (for a gradient_boost model).
8. Click a row in the leaderboard → drill-down page loads with all six sections populated in parallel.
9. Back button returns to leaderboard.
10. Time-window picker change triggers re-fetch on the active sub-tab.
11. Refresh-manual button triggers re-fetch.
12. Other tabs (Overview, Trading, Trades, Orders, Halts, Symbols, Freshness, Config, Backtests) still work.
13. DevTools console has no errors.

- [ ] **Step 4: Probe stubbed endpoints**

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:18084/api/models/1/history
# Expected: 200 (or a JSON array; no 501)

curl -s -o /dev/null -w '%{http_code}\n' http://localhost:18084/api/trading/equity
# Expected: 501 (still a Plan 3 stub)

curl -s -o /dev/null -w '%{http_code}\n' http://localhost:18084/api/backtests/1/folds
# Expected: 501 (still a Plan 4 stub)
```

- [ ] **Step 5: Commit smoke-test fixes if any**

If Steps 1-4 required any fixes, commit them with:

```bash
git commit -m "fix(dashboard): address smoke-test findings in Models plan"
```

Otherwise skip.

---

## Self-Review Summary

**Spec coverage** (against [2026-04-17-dashboard-expansion-design.md](../specs/2026-04-17-dashboard-expansion-design.md) — Plan 2 scope):

| Spec item | Task |
|-----------|------|
| Leaderboard sub-tab + sortable columns + sparkline | Task 4 (backend), Task 6 (frontend) |
| Fidelity sub-tab + multi-model overlay | Task 3 (history endpoint), Task 7 |
| Calibration sub-tab + reliability diagram | Task 3 (calibration endpoint), Task 8 |
| Attribution sub-tab (signals + simulated PnL) | Task 3 (simulated_pnl endpoint), Task 9 |
| Time-series sub-tab | Task 3 (history endpoint), Task 10 |
| Regime sub-tab (inline SPY-SMA classifier) | Task 1 (query_regime_breakdown), Task 3 (endpoint), Task 11 |
| Feature importance sub-tab | Task 3 (feature_importance endpoint), Task 12 |
| Per-model drill-down (all 6 sections + signal history) | Task 1 (query_signal_history), Task 13 |
| Daily accuracy snapshot persistence | Task 2 |
| Layout D (sub-tabs + drill-down via Router) | Task 5 (sub-tab scaffold + Router integration) |
| Time-window picker + refresh wiring | Task 5 |
| Scope=None default on all live reads | Task 1 (explicit in SQL via no scope threading; existing base-model reads already default to live) |

**Placeholder scan:** None.

**Type consistency:** All 6 new Database methods use `Optional[datetime]` for `from_dt`/`to_dt`. The `_handle_*` handlers route through `parse_time_window` (added in Plan 1). The `fetchJSON` helper in `models_tab.js` appends the window as URL params. Feature-importance loading degrades to `{available: false, reason}` for unsupported model types.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-17-dashboard-plan-2-models.md`.

**Two execution options:**

1. **Subagent-Driven (recommended)** — fresh subagent per task, two-stage review
2. **Inline Execution** — batch tasks in this session

Which approach?
