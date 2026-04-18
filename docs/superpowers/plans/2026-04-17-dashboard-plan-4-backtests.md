# Dashboard Expansion Plan 4 — Backtests Domain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the Backtests tab into a full run explorer: 3 top-level sub-tabs (List, Compare, Sensitivity), per-run detail page with 7 sub-tabs (Summary, Equity, Folds, Per-symbol, Distributions, Live-vs-backtest, Funnel), plus tags/notes inline editing and a promote-to-live flow for runtime config.

**Architecture:** Migration 019 adds `tags`, `notes` to `backtest_runs` and a new `runtime_config_audit` table. Existing `/api/backtests/{id}/equity`, `/folds`, `/trades` get enhanced with drawdown and fold-metrics fields. Six new `/api/backtests/*` GET endpoints implement Symbols, Distributions, Live-vs-backtest, Funnel, Compare, and Sensitivity. Three new POST endpoints handle tag/note edits and config promotion. The signal-to-trade funnel reads `backtest.predictions` and `backtest.orders` via `BacktestScope(run_id, fold_index=None)` aggregation. Frontend `backtests.js` gains a sub-tab bar at the top plus sub-tabbed detail page; new views use Plan 1's Charts/Leaderboard/DrillDown components.

**Tech Stack:** aiohttp + asyncpg backend, vanilla JS + Chart.js frontend. No new dependencies.

**Spec:** [docs/superpowers/specs/2026-04-17-dashboard-expansion-design.md](../specs/2026-04-17-dashboard-expansion-design.md)

**Depends on:**
- [Plan 1 foundation](2026-04-17-dashboard-foundation.md) — Charts, Leaderboard, DrillDown components.
- [Plan 2 models](2026-04-17-dashboard-plan-2-models.md) — reference pattern for sub-tab structure.
- [Plan 3 trading](2026-04-17-dashboard-plan-3-trading.md) — `query_distributions` is reused inside `query_backtest_distributions`.
- [Backtest scoping](2026-04-17-backtest-run-scoping.md) — funnel uses `BacktestScope`.
- Existing `backtests_api.py`, `backtests.js`, `backtests.css` — extended, not replaced.

---

## Pre-Task: Worktree & Dev Iteration

Worktree `.worktrees/dashboard-plan-4-backtests` branched from master. Suggested branch: `feat/dashboard-backtests`. Paths relative to that worktree.

Dev iteration mount as in Plans 1-3 (gitignored `docker-compose.override.yml` pointing at `./services/dashboard_service/static`).

---

## File Structure

### New files

```
bullforge/migrations/
  019_backtest_tags_notes.sql           # ALTER backtest_runs + CREATE runtime_config_audit

bullforge/services/dashboard_service/
  backtests_extra_api.py                # new handlers: symbols, distributions, live-overlay, funnel, compare, sensitivity, tags, notes, promote

bullforge/services/dashboard_service/static/
  backtests_tab.js                      # sub-tab controller layered on top of existing backtests.js

bullforge/tests/
  test_dashboard_backtests_extra_api.py # aiohttp tests for new endpoints
  test_dashboard_backtests_extra_db.py  # integration tests for new DB methods (tests/integration/)
```

### Modified files

```
bullforge/services/dashboard_service/api.py              # register_backtests_extra_routes(app)
bullforge/services/dashboard_service/backtests_api.py    # /list includes tags+notes; /equity adds drawdown+fold_boundaries; /folds embeds metrics_json
bullforge/services/dashboard_service/stubs_api.py        # remove remaining 8 backtest stubs
bullforge/services/dashboard_service/static/index.html   # add sub-tab bar to Backtests tab; load backtests_tab.js
bullforge/services/dashboard_service/static/backtests.js # expose render hooks; add detail sub-tab routing
bullforge/services/dashboard_service/static/backtests.css # new styles (tags, notes editor, modal)
bullforge/shared/db.py                                   # add 6 new query methods + list_runs extension for tags/notes
bullforge/tests/test_dashboard_stubs.py                  # expected_prefixes becomes empty set (all stubs now implemented)
```

---

## Task 1: Migration 019 — tags, notes, runtime_config_audit

**Files:**
- Create: `bullforge/migrations/019_backtest_tags_notes.sql`

- [ ] **Step 1: Write the migration SQL**

Create `bullforge/migrations/019_backtest_tags_notes.sql`:

```sql
-- 019_backtest_tags_notes.sql
--
-- Adds tags (text array) and notes (free text) to backtest_runs for
-- dashboard filtering + annotation. Adds runtime_config_audit for tracking
-- which backtest runs' configs have been promoted to live runtime_config.

ALTER TABLE backtest_runs
    ADD COLUMN IF NOT EXISTS tags TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS notes TEXT;

CREATE INDEX IF NOT EXISTS idx_backtest_runs_tags ON backtest_runs USING GIN (tags);

CREATE TABLE IF NOT EXISTS runtime_config_audit (
    id                     BIGSERIAL PRIMARY KEY,
    key                    TEXT        NOT NULL,
    old_value              TEXT,
    new_value              TEXT        NOT NULL,
    changed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_backtest_run_id BIGINT      REFERENCES backtest_runs(id) ON DELETE SET NULL,
    note                   TEXT
);

CREATE INDEX IF NOT EXISTS idx_runtime_config_audit_key
    ON runtime_config_audit (key, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_runtime_config_audit_source
    ON runtime_config_audit (source_backtest_run_id)
    WHERE source_backtest_run_id IS NOT NULL;
```

- [ ] **Step 2: Apply and verify**

```bash
cd bullforge && docker compose exec timescaledb psql -U bullforge -d bullforge \
    -f /dev/stdin < migrations/019_backtest_tags_notes.sql
docker compose exec timescaledb psql -U bullforge -d bullforge -c "\d backtest_runs" | grep -E "tags|notes"
docker compose exec timescaledb psql -U bullforge -d bullforge -c "\dt runtime_config_audit"
```

Expected: `tags` and `notes` columns on `backtest_runs`; `runtime_config_audit` table listed.

- [ ] **Step 3: Commit**

```bash
git add bullforge/migrations/019_backtest_tags_notes.sql
git commit -m "feat(db): migration 019 — backtest tags/notes + runtime_config_audit table"
```

---

## Task 2: DB query methods + tags/notes on list_runs

**Files:**
- Modify: `bullforge/shared/db.py`
- Modify: `bullforge/services/backtest_service/results_db.py` (extend `list_runs` + `get_run` to include tags/notes)
- Create: `bullforge/tests/integration/test_dashboard_backtests_extra_db.py`

Methods added to `Database` class (plus DB helper writes for tags/notes updates). Supports 6 new endpoints + tag/note editing + promote-to-live audit write.

- [ ] **Step 1: Write failing tests**

Create `bullforge/tests/integration/test_dashboard_backtests_extra_db.py`:

```python
"""Tests for the /api/backtests/* extra DB query methods.

Uses the real_db fixture from tests/integration/conftest.py.
"""
from datetime import datetime, timedelta, timezone

import pytest

from shared.models import BacktestScope


@pytest.mark.asyncio
async def test_set_backtest_tags(real_db):
    # Insert a minimal run.
    async with real_db.pool.acquire() as conn:
        run_id = await conn.fetchval(
            """
            INSERT INTO backtest_runs (name, symbols, timeframe, horizons,
                range_start, range_end, train_days, test_days, embargo_min,
                mode, config_json, status)
            VALUES ('t', ARRAY['AAPL'], '1m', ARRAY[5],
                NOW(), NOW() + INTERVAL '1 day', 30, 5, 30,
                'rolling', '{}'::jsonb, 'completed')
            RETURNING id
            """
        )
    await real_db.set_backtest_tags(run_id, ["tagA", "tagB"])
    tags = await real_db.get_backtest_tags(run_id)
    assert set(tags) == {"tagA", "tagB"}


@pytest.mark.asyncio
async def test_set_backtest_notes(real_db):
    async with real_db.pool.acquire() as conn:
        run_id = await conn.fetchval(
            """
            INSERT INTO backtest_runs (name, symbols, timeframe, horizons,
                range_start, range_end, train_days, test_days, embargo_min,
                mode, config_json, status)
            VALUES ('n', ARRAY['AAPL'], '1m', ARRAY[5],
                NOW(), NOW() + INTERVAL '1 day', 30, 5, 30,
                'rolling', '{}'::jsonb, 'completed')
            RETURNING id
            """
        )
    await real_db.set_backtest_notes(run_id, "this run is the v2 baseline")
    notes = await real_db.get_backtest_notes(run_id)
    assert notes == "this run is the v2 baseline"


@pytest.mark.asyncio
async def test_query_backtest_symbols_aggregates_by_symbol(real_db):
    async with real_db.pool.acquire() as conn:
        run_id = await conn.fetchval(
            """
            INSERT INTO backtest_runs (name, symbols, timeframe, horizons,
                range_start, range_end, train_days, test_days, embargo_min,
                mode, config_json, status)
            VALUES ('sym', ARRAY['AAPL','MSFT'], '1m', ARRAY[5],
                NOW(), NOW() + INTERVAL '1 day', 30, 5, 30,
                'rolling', '{}'::jsonb, 'completed')
            RETURNING id
            """
        )
        fold_id = await conn.fetchval(
            """
            INSERT INTO backtest_folds (run_id, fold_index,
                train_start, train_end, test_start, test_end)
            VALUES ($1, 0, NOW(), NOW(), NOW(), NOW()) RETURNING id
            """, run_id,
        )
        for (sym, pnl) in [("AAPL", 10.0), ("AAPL", -2.0), ("MSFT", 5.0)]:
            await conn.execute(
                """
                INSERT INTO backtest_trades (run_id, fold_id, symbol, side,
                    entry_time, entry_price, exit_time, exit_price,
                    quantity, pnl, signal_strength)
                VALUES ($1, $2, $3, 'long', NOW(), 100.0,
                    NOW() + INTERVAL '10 minutes', 100.0 + $4,
                    1, $4, 0.7)
                """, run_id, fold_id, sym, pnl,
            )

    rows = await real_db.query_backtest_symbols(run_id=run_id)
    by_sym = {r["symbol"]: r for r in rows}
    assert by_sym["AAPL"]["trades"] == 2
    assert by_sym["AAPL"]["total_pnl"] == pytest.approx(8.0)
    assert by_sym["MSFT"]["trades"] == 1


@pytest.mark.asyncio
async def test_query_backtest_distributions_scoped_to_run(real_db):
    async with real_db.pool.acquire() as conn:
        run_id = await conn.fetchval(
            """
            INSERT INTO backtest_runs (name, symbols, timeframe, horizons,
                range_start, range_end, train_days, test_days, embargo_min,
                mode, config_json, status)
            VALUES ('dist', ARRAY['X'], '1m', ARRAY[5],
                NOW(), NOW() + INTERVAL '1 day', 30, 5, 30,
                'rolling', '{}'::jsonb, 'completed')
            RETURNING id
            """
        )
        fold_id = await conn.fetchval(
            """
            INSERT INTO backtest_folds (run_id, fold_index,
                train_start, train_end, test_start, test_end)
            VALUES ($1, 0, NOW(), NOW(), NOW(), NOW()) RETURNING id
            """, run_id,
        )
        for pnl in [5.0, -2.0, 3.0, -1.0, 7.0]:
            await conn.execute(
                """
                INSERT INTO backtest_trades (run_id, fold_id, symbol, side,
                    entry_time, entry_price, exit_time, exit_price,
                    quantity, pnl, signal_strength)
                VALUES ($1, $2, 'X', 'long', NOW(), 100.0,
                    NOW() + INTERVAL '10 minutes', 100.0, 1, $3, 0.7)
                """, run_id, fold_id, pnl,
            )
    dist = await real_db.query_backtest_distributions(run_id=run_id)
    assert dist["trade_pnl_histogram"]["count"] == 5 or \
        sum(dist["trade_pnl_histogram"]["counts"]) == 5


@pytest.mark.asyncio
async def test_query_backtest_live_overlay_returns_both_paths(real_db):
    # Seed one backtest run and a public trade inside its window.
    async with real_db.pool.acquire() as conn:
        base = datetime(2026, 3, 1, tzinfo=timezone.utc)
        run_id = await conn.fetchval(
            """
            INSERT INTO backtest_runs (name, symbols, timeframe, horizons,
                range_start, range_end, train_days, test_days, embargo_min,
                mode, config_json, status)
            VALUES ('lv', ARRAY['OVL'], '1m', ARRAY[5],
                $1, $1 + INTERVAL '5 days', 30, 5, 30,
                'rolling', '{}'::jsonb, 'completed')
            RETURNING id
            """, base,
        )
        fold_id = await conn.fetchval(
            """
            INSERT INTO backtest_folds (run_id, fold_index,
                train_start, train_end, test_start, test_end)
            VALUES ($1, 0, $2, $2, $2, $2 + INTERVAL '1 day') RETURNING id
            """, run_id, base,
        )
        # Backtest trade for symbol OVL
        await conn.execute(
            """
            INSERT INTO backtest_trades (run_id, fold_id, symbol, side,
                entry_time, entry_price, exit_time, exit_price,
                quantity, pnl, signal_strength)
            VALUES ($1, $2, 'OVL', 'long', $3, 100.0, $4, 103.0,
                1, 3.0, 0.7)
            """, run_id, fold_id, base, base + timedelta(hours=1),
        )
        # Live trade for symbol OVL in the same window
        order_id = await conn.fetchval(
            """
            INSERT INTO orders (symbol, side, quantity, order_type,
                submitted_at, signal_strength, alpaca_order_id, status,
                filled_at, filled_price, filled_quantity)
            VALUES ('OVL', 'buy', 1, 'market', $1, 0.7,
                'ovl-1', 'filled', $1, 100.0, 1)
            RETURNING id
            """, base + timedelta(hours=2),
        )
        await conn.execute(
            """
            INSERT INTO trades (symbol, side, entry_time, entry_price,
                quantity, entry_order_id, exit_time, exit_price, pnl,
                hold_duration_seconds)
            VALUES ('OVL', 'long', $1, 100.0, 1, $2, $3, 102.0, 2.0, 3600)
            """, base + timedelta(hours=2), order_id, base + timedelta(hours=3),
        )

    overlay = await real_db.query_backtest_live_overlay(run_id=run_id)
    assert "backtest_series" in overlay
    assert "live_series" in overlay
    assert overlay["has_overlap"] is True


@pytest.mark.asyncio
async def test_query_backtest_funnel_counts_attrition_per_stage(real_db):
    # Seed a run with predictions at varying confidence and a few orders.
    async with real_db.pool.acquire() as conn:
        run_id = await conn.fetchval(
            """
            INSERT INTO backtest_runs (name, symbols, timeframe, horizons,
                range_start, range_end, train_days, test_days, embargo_min,
                mode, config_json, status)
            VALUES ('fn', ARRAY['F'], '1m', ARRAY[5],
                NOW(), NOW() + INTERVAL '1 day', 30, 5, 30,
                'rolling', '{}'::jsonb, 'completed')
            RETURNING id
            """
        )
    # Insert predictions scoped to this run.
    scope = BacktestScope(backtest_run_id=run_id, fold_index=0)
    from shared.models import Prediction
    base = datetime(2026, 3, 1, tzinfo=timezone.utc)
    sandbox = real_db  # live pool also sees backtest.* via ALTER
    # Use the existing scope-aware insert.
    for i, conf in enumerate([0.8, 0.7, 0.65, 0.5, 0.4]):
        await sandbox.insert_prediction(Prediction(
            symbol="F", timestamp=base + timedelta(minutes=i),
            timeframe="1m", horizon_minutes=5, model_id=1,
            predicted_direction=1, confidence=conf,
        ), scope=scope)

    funnel = await real_db.query_backtest_funnel(
        run_id=run_id, min_confidence=0.6,
    )
    assert funnel["signals"] == 5
    assert funnel["confident"] == 3
    # Orders placed / filled / closed columns default to 0 when no
    # backtest.orders rows exist for the run.
    assert funnel["orders_placed"] == 0
    assert funnel["orders_filled"] == 0


@pytest.mark.asyncio
async def test_query_backtest_compare_returns_metrics_for_each_run(real_db):
    async with real_db.pool.acquire() as conn:
        ids = []
        for name in ["a", "b"]:
            rid = await conn.fetchval(
                """
                INSERT INTO backtest_runs (name, symbols, timeframe, horizons,
                    range_start, range_end, train_days, test_days, embargo_min,
                    mode, config_json, status)
                VALUES ($1, ARRAY['C'], '1m', ARRAY[5],
                    NOW(), NOW() + INTERVAL '1 day', 30, 5, 30,
                    'rolling', '{}'::jsonb, 'completed')
                RETURNING id
                """, name,
            )
            ids.append(rid)
            fold_id = await conn.fetchval(
                """
                INSERT INTO backtest_folds (run_id, fold_index,
                    train_start, train_end, test_start, test_end, metrics_json)
                VALUES ($1, 0, NOW(), NOW(), NOW(), NOW(),
                    '{"pnl": 10, "sharpe": 1.2}'::jsonb)
                RETURNING id
                """, rid,
            )
            await conn.execute(
                """
                INSERT INTO backtest_equity (run_id, fold_id, timestamp, equity, cash)
                VALUES ($1, $2, NOW(), 100100.0, 90000.0)
                """, rid, fold_id,
            )

    compare = await real_db.query_backtest_compare(run_ids=ids)
    assert len(compare["runs"]) == 2
    for r in compare["runs"]:
        assert "equity_series" in r
        assert "metrics" in r


@pytest.mark.asyncio
async def test_query_backtest_sensitivity_groups_by_swept_param(real_db):
    async with real_db.pool.acquire() as conn:
        # Three runs with same grouping key (symbols + timeframe + horizons)
        # but different `min_confidence` (the swept parameter).
        for conf in [0.5, 0.6, 0.7]:
            await conn.execute(
                """
                INSERT INTO backtest_runs (name, symbols, timeframe, horizons,
                    range_start, range_end, train_days, test_days, embargo_min,
                    mode, config_json, status)
                VALUES ('sens-' || $1::TEXT, ARRAY['S'], '1m', ARRAY[5],
                    NOW(), NOW() + INTERVAL '1 day', 30, 5, 30, 'rolling',
                    jsonb_build_object('min_confidence', $1::DOUBLE PRECISION),
                    'completed')
                """, conf,
            )
    sens = await real_db.query_backtest_sensitivity(
        group_by=["symbols", "timeframe"],
        swept_param="min_confidence",
    )
    assert len(sens["points"]) >= 3
    for p in sens["points"]:
        assert "param_value" in p
        assert "metrics" in p
```

- [ ] **Step 2: Run to verify failure**

Run: `cd bullforge && python -m pytest tests/integration/test_dashboard_backtests_extra_db.py -v`

Expected: FAIL — methods don't exist. If Postgres isn't running: `ConnectionRefusedError` (OK; aiohttp tests in Task 3 use mocks).

- [ ] **Step 3: Implement `set_backtest_tags`, `get_backtest_tags`**

Append to `Database` class in `bullforge/shared/db.py`:

```python
    async def set_backtest_tags(self, run_id: int, tags: list[str]) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE backtest_runs SET tags = $1 WHERE id = $2",
                tags, run_id,
            )

    async def get_backtest_tags(self, run_id: int) -> list[str]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchval(
                "SELECT tags FROM backtest_runs WHERE id = $1", run_id,
            )
        return list(row) if row else []

    async def set_backtest_notes(self, run_id: int, notes: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE backtest_runs SET notes = $1 WHERE id = $2",
                notes, run_id,
            )

    async def get_backtest_notes(self, run_id: int) -> Optional[str]:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT notes FROM backtest_runs WHERE id = $1", run_id,
            )

    async def insert_runtime_config_audit(
        self, key: str, old_value: Optional[str], new_value: str,
        source_backtest_run_id: Optional[int] = None,
        note: Optional[str] = None,
    ) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                """
                INSERT INTO runtime_config_audit
                    (key, old_value, new_value, source_backtest_run_id, note)
                VALUES ($1, $2, $3, $4, $5) RETURNING id
                """,
                key, old_value, new_value, source_backtest_run_id, note,
            )
```

- [ ] **Step 4: Implement `query_backtest_symbols`**

Append to `db.py`:

```python
    async def query_backtest_symbols(self, run_id: int) -> list[dict]:
        """Per-symbol aggregation within a backtest run.

        Returns [{symbol, trades, wins, win_rate, total_pnl, avg_pnl}].
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    symbol,
                    COUNT(*) AS trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END)::INT AS wins,
                    SUM(pnl)::DOUBLE PRECISION AS total_pnl
                FROM backtest_trades
                WHERE run_id = $1 AND exit_time IS NOT NULL AND pnl IS NOT NULL
                GROUP BY symbol
                ORDER BY total_pnl DESC
                """,
                run_id,
            )
        return [
            {
                "symbol": r["symbol"],
                "trades": r["trades"],
                "wins": r["wins"],
                "win_rate": (r["wins"] / r["trades"]) if r["trades"] > 0 else 0.0,
                "total_pnl": float(r["total_pnl"] or 0.0),
                "avg_pnl": (float(r["total_pnl"] or 0.0) / r["trades"])
                           if r["trades"] > 0 else 0.0,
            }
            for r in rows
        ]
```

- [ ] **Step 5: Implement `query_backtest_distributions`**

Append to `db.py`:

```python
    async def query_backtest_distributions(self, run_id: int) -> dict:
        """Same shape as Plan 3's query_distributions but scoped to one backtest run."""
        async with self.pool.acquire() as conn:
            all_trades = await conn.fetch(
                """
                SELECT pnl, side,
                       EXTRACT(EPOCH FROM (exit_time - entry_time))::INT AS hold_seconds
                FROM backtest_trades
                WHERE run_id = $1 AND exit_time IS NOT NULL AND pnl IS NOT NULL
                """,
                run_id,
            )
            daily = await conn.fetch(
                """
                SELECT DATE_TRUNC('day', exit_time) AS date,
                       SUM(pnl) AS pnl, COUNT(*) AS trades
                FROM backtest_trades
                WHERE run_id = $1 AND exit_time IS NOT NULL AND pnl IS NOT NULL
                GROUP BY DATE_TRUNC('day', exit_time)
                ORDER BY date ASC
                """,
                run_id,
            )

        pnls = [float(t["pnl"]) for t in all_trades]
        holds = [int(t["hold_seconds"]) for t in all_trades
                 if t["hold_seconds"] is not None]

        # 20-bin histogram
        labels: list = []
        counts: list = []
        if pnls:
            lo, hi = min(pnls), max(pnls)
            if lo == hi: lo -= 1.0; hi += 1.0
            bins = 20
            width = (hi - lo) / bins
            counts = [0] * bins
            for v in pnls:
                i = min(bins - 1, int((v - lo) / width))
                counts[i] += 1
            labels = [round(lo + i * width, 2) for i in range(bins)]

        # Hold-time buckets (same 7 as Plan 3)
        hold_buckets_def = [
            ("<5m", 0, 300),
            ("5-30m", 300, 1800),
            ("30m-2h", 1800, 7200),
            ("2h-8h", 7200, 28800),
            ("8h-1d", 28800, 86400),
            ("overnight", 86400, 172800),
            ("multi-day", 172800, 10**9),
        ]
        hold_table = []
        for name, lo_s, hi_s in hold_buckets_def:
            bucket = [t for t in all_trades
                      if t["hold_seconds"] is not None
                      and lo_s <= t["hold_seconds"] < hi_s]
            n = len(bucket)
            avg = sum(float(t["pnl"]) for t in bucket) / n if n > 0 else 0.0
            hold_table.append({"bucket": name, "trades": n, "avg_pnl": avg})

        def side_stats(side):
            rows = [t for t in all_trades if t["side"] == side]
            n = len(rows)
            wins = sum(1 for t in rows if float(t["pnl"]) > 0)
            total = sum(float(t["pnl"]) for t in rows)
            return {
                "side": side, "trades": n, "wins": wins,
                "win_rate": (wins / n) if n > 0 else 0.0,
                "total_pnl": total,
                "avg_pnl": (total / n) if n > 0 else 0.0,
            }

        return {
            "daily_pnl": [
                {"date": r["date"], "pnl": float(r["pnl"]), "trades": r["trades"]}
                for r in daily
            ],
            "trade_pnl_histogram": {
                "labels": labels, "counts": counts,
                "count": len(pnls),
                "median": sorted(pnls)[len(pnls) // 2] if pnls else 0.0,
                "mean": (sum(pnls) / len(pnls)) if pnls else 0.0,
            },
            "hold_time_buckets": hold_table,
            "side_breakdown": [side_stats("long"), side_stats("short")],
        }
```

- [ ] **Step 6: Implement `query_backtest_live_overlay`**

Append to `db.py`:

```python
    async def query_backtest_live_overlay(self, run_id: int) -> dict:
        """Overlay: live vs backtest equity for the run's symbols/window.

        Returns {has_overlap, backtest_series, live_series, symbols,
                 range_start, range_end}.
        """
        async with self.pool.acquire() as conn:
            run = await conn.fetchrow(
                """
                SELECT symbols, range_start, range_end
                FROM backtest_runs WHERE id = $1
                """,
                run_id,
            )
            if run is None:
                return {"has_overlap": False, "backtest_series": [],
                        "live_series": [], "symbols": [],
                        "range_start": None, "range_end": None}

            # Backtest series: concatenate equity across folds.
            bt_rows = await conn.fetch(
                """
                SELECT timestamp, equity
                FROM backtest_equity
                WHERE run_id = $1
                ORDER BY timestamp ASC
                """,
                run_id,
            )
            # Live series: walk closed live trades for the same symbols within
            # the backtest's time window. scope=None (live-only): public.trades
            # has no backtest_run_id column, so no filter needed there.
            live_rows = await conn.fetch(
                """
                SELECT exit_time, pnl
                FROM trades
                WHERE symbol = ANY($1::TEXT[])
                    AND exit_time IS NOT NULL
                    AND pnl IS NOT NULL
                    AND exit_time >= $2
                    AND exit_time < $3
                ORDER BY exit_time ASC
                """,
                list(run["symbols"]), run["range_start"], run["range_end"],
            )

        bt_series = [
            {"timestamp": r["timestamp"], "equity": float(r["equity"])}
            for r in bt_rows
        ]
        # Live equity walked from 0 (symbol-scoped; compare shape not level).
        live_series = []
        cum = 0.0
        for r in live_rows:
            cum += float(r["pnl"])
            live_series.append({
                "timestamp": r["exit_time"], "equity": cum,
            })
        return {
            "has_overlap": len(live_series) > 0,
            "backtest_series": bt_series,
            "live_series": live_series,
            "symbols": list(run["symbols"]),
            "range_start": run["range_start"],
            "range_end": run["range_end"],
        }
```

- [ ] **Step 7: Implement `query_backtest_funnel`**

Append to `db.py`:

```python
    async def query_backtest_funnel(
        self, run_id: int, min_confidence: float = 0.6,
    ) -> dict:
        """Signal-to-trade funnel for a backtest run.

        Reads predictions + orders scoped to run_id (any fold). Returns
        {signals, confident, orders_placed, orders_filled, orders_closed}.
        """
        async with self.pool.acquire() as conn:
            signals = await conn.fetchval(
                """
                SELECT COUNT(*)::INT FROM predictions
                WHERE backtest_run_id = $1
                """,
                run_id,
            )
            confident = await conn.fetchval(
                """
                SELECT COUNT(*)::INT FROM predictions
                WHERE backtest_run_id = $1 AND confidence >= $2
                """,
                run_id, min_confidence,
            )
            orders_placed = await conn.fetchval(
                """
                SELECT COUNT(*)::INT FROM orders WHERE backtest_run_id = $1
                """,
                run_id,
            )
            orders_filled = await conn.fetchval(
                """
                SELECT COUNT(*)::INT FROM orders
                WHERE backtest_run_id = $1 AND status = 'filled'
                """,
                run_id,
            )
            orders_closed = await conn.fetchval(
                """
                SELECT COUNT(*)::INT FROM backtest_trades
                WHERE run_id = $1 AND exit_time IS NOT NULL
                """,
                run_id,
            )
        return {
            "signals": signals or 0,
            "confident": confident or 0,
            "orders_placed": orders_placed or 0,
            "orders_filled": orders_filled or 0,
            "orders_closed": orders_closed or 0,
            "min_confidence": min_confidence,
        }
```

- [ ] **Step 8: Implement `query_backtest_compare`**

Append to `db.py`:

```python
    async def query_backtest_compare(
        self, run_ids: list[int],
    ) -> dict:
        """Overlay-ready payload for N runs. Returns aggregate equity series
        per run + aggregated metrics (sum/mean across folds).
        """
        if not run_ids:
            return {"runs": []}

        async with self.pool.acquire() as conn:
            runs = await conn.fetch(
                """
                SELECT id, name, symbols, tags, notes
                FROM backtest_runs WHERE id = ANY($1::BIGINT[])
                ORDER BY id
                """,
                run_ids,
            )
            equity = await conn.fetch(
                """
                SELECT run_id, timestamp, equity
                FROM backtest_equity
                WHERE run_id = ANY($1::BIGINT[])
                ORDER BY run_id, timestamp ASC
                """,
                run_ids,
            )
            metrics = await conn.fetch(
                """
                SELECT run_id, SUM((metrics_json->>'pnl')::DOUBLE PRECISION) AS total_pnl,
                       AVG((metrics_json->>'sharpe')::DOUBLE PRECISION) AS avg_sharpe,
                       MIN((metrics_json->>'max_drawdown')::DOUBLE PRECISION) AS min_max_dd,
                       SUM((metrics_json->>'trades')::INT) AS total_trades,
                       AVG((metrics_json->>'hit_rate')::DOUBLE PRECISION) AS avg_hit_rate
                FROM backtest_folds
                WHERE run_id = ANY($1::BIGINT[])
                GROUP BY run_id
                """,
                run_ids,
            )

        eq_by_run: dict[int, list[dict]] = {rid: [] for rid in run_ids}
        for e in equity:
            eq_by_run[e["run_id"]].append({
                "timestamp": e["timestamp"], "equity": float(e["equity"]),
            })
        mx_by_run = {m["run_id"]: m for m in metrics}

        result = []
        for r in runs:
            m = mx_by_run.get(r["id"], {})
            result.append({
                "run_id": r["id"], "name": r["name"],
                "symbols": list(r["symbols"]),
                "tags": list(r["tags"] or []),
                "notes": r["notes"],
                "equity_series": eq_by_run.get(r["id"], []),
                "metrics": {
                    "total_pnl": float(m.get("total_pnl") or 0.0),
                    "avg_sharpe": float(m.get("avg_sharpe") or 0.0),
                    "min_max_dd": float(m.get("min_max_dd") or 0.0),
                    "total_trades": int(m.get("total_trades") or 0),
                    "avg_hit_rate": float(m.get("avg_hit_rate") or 0.0),
                },
            })
        return {"runs": result}
```

- [ ] **Step 9: Implement `query_backtest_sensitivity`**

Append to `db.py`:

```python
    async def query_backtest_sensitivity(
        self,
        group_by: list[str],
        swept_param: str,
    ) -> dict:
        """Runs sharing group_by dims but differing on swept_param.

        For each run, extracts swept_param's value from config_json and
        aggregates test-fold metrics. Returns {swept_param, group_by,
        points: [{param_value, metrics}]}.
        """
        # Build a canonical group key from (symbols, timeframe, horizons).
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT r.id, r.symbols, r.timeframe, r.horizons, r.config_json,
                       SUM((f.metrics_json->>'pnl')::DOUBLE PRECISION) AS total_pnl,
                       AVG((f.metrics_json->>'sharpe')::DOUBLE PRECISION) AS avg_sharpe,
                       MIN((f.metrics_json->>'max_drawdown')::DOUBLE PRECISION) AS min_max_dd
                FROM backtest_runs r
                LEFT JOIN backtest_folds f ON f.run_id = r.id
                WHERE r.status = 'completed'
                GROUP BY r.id, r.symbols, r.timeframe, r.horizons, r.config_json
                """
            )

        def group_key(r):
            parts = []
            if "symbols" in group_by:
                parts.append(tuple(r["symbols"]))
            if "timeframe" in group_by:
                parts.append(r["timeframe"])
            if "horizons" in group_by:
                parts.append(tuple(r["horizons"]))
            return tuple(parts)

        # Keep only runs whose config_json has the swept_param.
        points = []
        for r in rows:
            cfg = r["config_json"] if isinstance(r["config_json"], dict) \
                else (json.loads(r["config_json"]) if r["config_json"] else {})
            if swept_param not in cfg:
                continue
            points.append({
                "run_id": r["id"],
                "group_key": list(group_key(r)),
                "param_value": cfg[swept_param],
                "metrics": {
                    "total_pnl": float(r["total_pnl"] or 0.0),
                    "avg_sharpe": float(r["avg_sharpe"] or 0.0),
                    "min_max_dd": float(r["min_max_dd"] or 0.0),
                },
            })
        points.sort(key=lambda p: (p["group_key"], p["param_value"]))
        return {
            "swept_param": swept_param,
            "group_by": group_by,
            "points": points,
        }
```

- [ ] **Step 10: Extend `results_db.list_runs` to include tags + notes**

In `bullforge/services/backtest_service/results_db.py`, find `list_runs` and `get_run` methods. Add `tags` and `notes` to the SELECT column list and to the returned dict. (The file is ~200 lines; locate and adjust both SELECT statements.)

- [ ] **Step 11: Run tests**

Run: `cd bullforge && python -m pytest tests/integration/test_dashboard_backtests_extra_db.py -v`

Expected: all PASS (or `ConnectionRefusedError` if no DB).

- [ ] **Step 12: Commit**

```bash
git add bullforge/shared/db.py \
        bullforge/services/backtest_service/results_db.py \
        bullforge/tests/integration/test_dashboard_backtests_extra_db.py
git commit -m "feat(db): add backtest symbols/distributions/live-overlay/funnel/compare/sensitivity + tags/notes"
```

---

## Task 3: New `backtests_extra_api.py` with GET endpoints

**Files:**
- Create: `bullforge/services/dashboard_service/backtests_extra_api.py`
- Create: `bullforge/tests/test_dashboard_backtests_extra_api.py`
- Modify: `bullforge/services/dashboard_service/api.py`
- Modify: `bullforge/services/dashboard_service/stubs_api.py`
- Modify: `bullforge/tests/test_dashboard_stubs.py`

Six new GET endpoints + scaffolding for three POST endpoints (actual POST handlers implemented in Task 4).

- [ ] **Step 1: Write failing tests**

Create `bullforge/tests/test_dashboard_backtests_extra_api.py`:

```python
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from services.dashboard_service.backtests_extra_api import register_backtests_extra_routes


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.query_backtest_symbols = AsyncMock(return_value=[
        {"symbol": "AAPL", "trades": 5, "wins": 3, "win_rate": 0.6,
         "total_pnl": 12.0, "avg_pnl": 2.4},
    ])
    db.query_backtest_distributions = AsyncMock(return_value={
        "daily_pnl": [], "trade_pnl_histogram":
            {"labels": [], "counts": [], "count": 0, "median": 0.0, "mean": 0.0},
        "hold_time_buckets": [], "side_breakdown": [],
    })
    db.query_backtest_live_overlay = AsyncMock(return_value={
        "has_overlap": True, "backtest_series": [], "live_series": [],
        "symbols": ["AAPL"],
        "range_start": datetime(2026, 3, 1, tzinfo=timezone.utc),
        "range_end": datetime(2026, 3, 5, tzinfo=timezone.utc),
    })
    db.query_backtest_funnel = AsyncMock(return_value={
        "signals": 10, "confident": 6,
        "orders_placed": 5, "orders_filled": 5,
        "orders_closed": 5, "min_confidence": 0.6,
    })
    db.query_backtest_compare = AsyncMock(return_value={
        "runs": [{"run_id": 1, "name": "a", "symbols": ["X"],
                  "tags": [], "notes": None,
                  "equity_series": [],
                  "metrics": {"total_pnl": 10, "avg_sharpe": 1.1,
                               "min_max_dd": -0.05, "total_trades": 20,
                               "avg_hit_rate": 0.6}}],
    })
    db.query_backtest_sensitivity = AsyncMock(return_value={
        "swept_param": "min_confidence", "group_by": ["symbols"],
        "points": [{"run_id": 1, "group_key": [["AAPL"]],
                    "param_value": 0.5,
                    "metrics": {"total_pnl": 5, "avg_sharpe": 0.9,
                                 "min_max_dd": -0.04}}],
    })
    return db


@pytest.fixture
def extras_app(mock_db):
    app = web.Application()
    app["db"] = mock_db
    register_backtests_extra_routes(app)
    return app


@pytest.mark.asyncio
async def test_symbols_endpoint(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/42/symbols")
    assert resp.status == 200
    body = await resp.json()
    assert len(body) == 1
    assert body[0]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_distributions_endpoint(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/42/distributions")
    assert resp.status == 200
    body = await resp.json()
    assert "daily_pnl" in body
    assert "trade_pnl_histogram" in body


@pytest.mark.asyncio
async def test_live_overlay_endpoint(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/42/live-overlay")
    assert resp.status == 200
    body = await resp.json()
    assert body["has_overlap"] is True


@pytest.mark.asyncio
async def test_funnel_endpoint(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/42/funnel")
    assert resp.status == 200
    body = await resp.json()
    assert body["signals"] == 10


@pytest.mark.asyncio
async def test_compare_endpoint_single_id(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/compare?ids=1")
    assert resp.status == 200
    body = await resp.json()
    assert "runs" in body


@pytest.mark.asyncio
async def test_compare_endpoint_missing_ids(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/compare")
    assert resp.status == 400


@pytest.mark.asyncio
async def test_compare_endpoint_malformed_ids(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/compare?ids=abc,def")
    assert resp.status == 400


@pytest.mark.asyncio
async def test_sensitivity_endpoint(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get(
        "/api/backtests/sensitivity?group_by=symbols&swept_param=min_confidence",
    )
    assert resp.status == 200
    body = await resp.json()
    assert body["swept_param"] == "min_confidence"


@pytest.mark.asyncio
async def test_sensitivity_missing_param(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/sensitivity")
    assert resp.status == 400


@pytest.mark.asyncio
async def test_invalid_run_id_returns_400(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/not-a-number/symbols")
    assert resp.status == 400
```

- [ ] **Step 2: Run to verify failure**

Run: `cd bullforge && python -m pytest tests/test_dashboard_backtests_extra_api.py -v`

Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Implement `backtests_extra_api.py`**

Create `bullforge/services/dashboard_service/backtests_extra_api.py`:

```python
from __future__ import annotations

import logging
from datetime import datetime

from aiohttp import web

from shared.db import Database

logger = logging.getLogger(__name__)


def register_backtests_extra_routes(app: web.Application) -> None:
    # IMPORTANT: aiohttp matches routes in registration order. Register the
    # literal-path routes (compare, sensitivity) BEFORE the parameterized
    # /{id}/... routes so "/api/backtests/compare" isn't caught by
    # "/api/backtests/{id}" (which already exists in backtests_api.py but
    # doesn't shadow here — different paths — but keep this convention).
    app.router.add_get("/api/backtests/compare", _handle_compare)
    app.router.add_get("/api/backtests/sensitivity", _handle_sensitivity)
    app.router.add_get("/api/backtests/{id}/symbols", _handle_symbols)
    app.router.add_get("/api/backtests/{id}/distributions", _handle_distributions)
    app.router.add_get("/api/backtests/{id}/live-overlay", _handle_live_overlay)
    app.router.add_get("/api/backtests/{id}/funnel", _handle_funnel)


def _parse_run_id(request: web.Request) -> int:
    raw = request.match_info.get("id", "")
    try:
        return int(raw)
    except ValueError:
        raise web.HTTPBadRequest(reason=f"invalid backtest run id: {raw!r}")


def _iso(dt):
    return dt.isoformat() if isinstance(dt, datetime) else dt


async def _handle_symbols(request: web.Request) -> web.Response:
    run_id = _parse_run_id(request)
    db: Database = request.app["db"]
    rows = await db.query_backtest_symbols(run_id=run_id)
    return web.json_response(rows)


async def _handle_distributions(request: web.Request) -> web.Response:
    run_id = _parse_run_id(request)
    db: Database = request.app["db"]
    data = await db.query_backtest_distributions(run_id=run_id)
    return web.json_response({
        "daily_pnl": [
            {"date": _iso(d["date"]), "pnl": d["pnl"], "trades": d["trades"]}
            for d in data["daily_pnl"]
        ],
        "trade_pnl_histogram": data["trade_pnl_histogram"],
        "hold_time_buckets": data["hold_time_buckets"],
        "side_breakdown": data["side_breakdown"],
    })


async def _handle_live_overlay(request: web.Request) -> web.Response:
    run_id = _parse_run_id(request)
    db: Database = request.app["db"]
    data = await db.query_backtest_live_overlay(run_id=run_id)
    return web.json_response({
        "has_overlap": data["has_overlap"],
        "symbols": data["symbols"],
        "range_start": _iso(data["range_start"]),
        "range_end": _iso(data["range_end"]),
        "backtest_series": [
            {"timestamp": _iso(s["timestamp"]), "equity": s["equity"]}
            for s in data["backtest_series"]
        ],
        "live_series": [
            {"timestamp": _iso(s["timestamp"]), "equity": s["equity"]}
            for s in data["live_series"]
        ],
    })


async def _handle_funnel(request: web.Request) -> web.Response:
    run_id = _parse_run_id(request)
    min_conf = float(request.query.get("min_confidence", "0.6"))
    db: Database = request.app["db"]
    data = await db.query_backtest_funnel(
        run_id=run_id, min_confidence=min_conf,
    )
    return web.json_response(data)


async def _handle_compare(request: web.Request) -> web.Response:
    raw_ids = request.query.get("ids")
    if not raw_ids:
        raise web.HTTPBadRequest(reason="missing 'ids' query parameter")
    try:
        ids = [int(x) for x in raw_ids.split(",") if x.strip()]
    except ValueError:
        raise web.HTTPBadRequest(reason="'ids' must be comma-separated integers")
    if not ids:
        raise web.HTTPBadRequest(reason="'ids' must include at least one run id")
    if len(ids) > 5:
        raise web.HTTPBadRequest(reason="compare supports at most 5 runs")
    db: Database = request.app["db"]
    data = await db.query_backtest_compare(run_ids=ids)
    return web.json_response({
        "runs": [
            {
                **r,
                "equity_series": [
                    {"timestamp": _iso(p["timestamp"]), "equity": p["equity"]}
                    for p in r["equity_series"]
                ],
            }
            for r in data["runs"]
        ],
    })


async def _handle_sensitivity(request: web.Request) -> web.Response:
    swept = request.query.get("swept_param")
    if not swept:
        raise web.HTTPBadRequest(reason="missing 'swept_param' query parameter")
    group_by_raw = request.query.get("group_by", "symbols,timeframe,horizons")
    group_by = [g.strip() for g in group_by_raw.split(",") if g.strip()]
    db: Database = request.app["db"]
    data = await db.query_backtest_sensitivity(
        group_by=group_by, swept_param=swept,
    )
    return web.json_response(data)
```

- [ ] **Step 4: Register in `api.py`**

In `bullforge/services/dashboard_service/api.py`, add import:

```python
from services.dashboard_service.backtests_extra_api import register_backtests_extra_routes
```

And register after the existing `register_backtests_routes(app)` call:

```python
    register_backtests_extra_routes(app)
```

- [ ] **Step 5: Remove all 8 Plan 4 stubs from `stubs_api.py`**

Delete all 8 remaining entries from `STUB_ENDPOINTS`. The dict should now be empty:

```python
STUB_ENDPOINTS: dict[str, dict] = {}
```

- [ ] **Step 6: Update `test_dashboard_stubs.py`**

In `bullforge/tests/test_dashboard_stubs.py`, replace `expected_prefixes` with an empty set:

```python
    expected_prefixes: set = set()
```

The `test_every_stub_returns_501` test still works (trivially — empty iteration). The `test_stubs_include_every_spec_endpoint` test asserts `expected <= STUB_ENDPOINTS` which is trivially true for empty expected.

- [ ] **Step 7: Run tests**

Run: `cd bullforge && python -m pytest tests/test_dashboard_backtests_extra_api.py tests/test_dashboard_stubs.py tests/test_dashboard_api.py tests/test_dashboard_backtests_api.py -v`

Expected: all PASS.

- [ ] **Step 8: Commit**

```bash
git add bullforge/services/dashboard_service/backtests_extra_api.py \
        bullforge/services/dashboard_service/api.py \
        bullforge/services/dashboard_service/stubs_api.py \
        bullforge/tests/test_dashboard_backtests_extra_api.py \
        bullforge/tests/test_dashboard_stubs.py
git commit -m "feat(dashboard): implement /api/backtests/* extra endpoints (symbols/distributions/overlay/funnel/compare/sensitivity, replace Plan 1 stubs)"
```

---

## Task 4: Tags, notes, promote-to-live POST endpoints

**Files:**
- Modify: `bullforge/services/dashboard_service/backtests_extra_api.py`
- Modify: `bullforge/tests/test_dashboard_backtests_extra_api.py`

- [ ] **Step 1: Extend test file with POST tests**

Append to `bullforge/tests/test_dashboard_backtests_extra_api.py`:

```python
@pytest.fixture
def mock_db_with_writes(mock_db):
    mock_db.set_backtest_tags = AsyncMock(return_value=None)
    mock_db.get_backtest_tags = AsyncMock(return_value=["new-tag"])
    mock_db.set_backtest_notes = AsyncMock(return_value=None)
    mock_db.get_backtest_notes = AsyncMock(return_value="a note")
    mock_db.insert_runtime_config_audit = AsyncMock(return_value=1)
    return mock_db


@pytest.fixture
def extras_app_with_writes(mock_db_with_writes):
    app = web.Application()
    app["db"] = mock_db_with_writes
    # runtime_config fixture mock
    rc = AsyncMock()
    rc.get = AsyncMock(return_value=0.6)
    rc.set = AsyncMock(return_value=None)
    app["runtime_config"] = rc
    register_backtests_extra_routes(app)
    return app


@pytest.mark.asyncio
async def test_post_tags_replaces_list(aiohttp_client, extras_app_with_writes, mock_db_with_writes):
    client = await aiohttp_client(extras_app_with_writes)
    resp = await client.post(
        "/api/backtests/42/tags",
        json={"tags": ["tagA", "tagB"]},
    )
    assert resp.status == 200
    body = await resp.json()
    assert "tags" in body
    mock_db_with_writes.set_backtest_tags.assert_awaited_once_with(42, ["tagA", "tagB"])


@pytest.mark.asyncio
async def test_post_tags_rejects_non_list(aiohttp_client, extras_app_with_writes):
    client = await aiohttp_client(extras_app_with_writes)
    resp = await client.post(
        "/api/backtests/42/tags", json={"tags": "not-a-list"},
    )
    assert resp.status == 400


@pytest.mark.asyncio
async def test_post_notes_saves_string(aiohttp_client, extras_app_with_writes, mock_db_with_writes):
    client = await aiohttp_client(extras_app_with_writes)
    resp = await client.post(
        "/api/backtests/42/notes",
        json={"notes": "my notes"},
    )
    assert resp.status == 200
    mock_db_with_writes.set_backtest_notes.assert_awaited_once_with(42, "my notes")


@pytest.mark.asyncio
async def test_post_promote_config_writes_diff_and_audit(
    aiohttp_client, extras_app_with_writes, mock_db_with_writes,
):
    mock_db_with_writes.get_backtest_tags.return_value = []
    # query_backtest_compare is already mocked; use the compare fixture for run metadata.
    # We need the run's config_json to drive the diff — inject via a simple query mock.
    mock_db_with_writes.pool = AsyncMock()
    # Mock: the handler fetches the run's config_json itself via pool.acquire.

    async def fetchrow_mock(query, run_id):
        return {
            "config_json": {"min_confidence": 0.75, "max_position_pct": 0.03},
        }

    conn_cm = AsyncMock()
    conn_cm.__aenter__.return_value.fetchrow = fetchrow_mock
    conn_cm.__aexit__.return_value = False
    mock_db_with_writes.pool.acquire = lambda: conn_cm

    client = await aiohttp_client(extras_app_with_writes)
    resp = await client.post(
        "/api/backtests/42/promote-config",
        json={"keys": ["min_confidence"], "note": "promoted after sensitivity sweep"},
    )
    assert resp.status == 200
    body = await resp.json()
    assert "diff" in body
    # Old value 0.6 → new value 0.75.
    assert any(d["key"] == "execution_min_confidence" and
               d["new_value"] == 0.75 for d in body["diff"])
    # runtime_config.set was called with the execution_ prefix.
    extras_app_with_writes["runtime_config"].set.assert_awaited()
    mock_db_with_writes.insert_runtime_config_audit.assert_awaited()
```

- [ ] **Step 2: Implement POST endpoints**

Append to `bullforge/services/dashboard_service/backtests_extra_api.py` (before the last line):

```python
async def _handle_set_tags(request: web.Request) -> web.Response:
    run_id = _parse_run_id(request)
    try:
        body = await request.json()
    except Exception:
        raise web.HTTPBadRequest(reason="invalid JSON body")
    tags = body.get("tags")
    if not isinstance(tags, list) or not all(isinstance(t, str) for t in tags):
        raise web.HTTPBadRequest(reason="'tags' must be a list of strings")
    db: Database = request.app["db"]
    await db.set_backtest_tags(run_id, tags)
    saved = await db.get_backtest_tags(run_id)
    return web.json_response({"tags": saved})


async def _handle_set_notes(request: web.Request) -> web.Response:
    run_id = _parse_run_id(request)
    try:
        body = await request.json()
    except Exception:
        raise web.HTTPBadRequest(reason="invalid JSON body")
    notes = body.get("notes")
    if notes is not None and not isinstance(notes, str):
        raise web.HTTPBadRequest(reason="'notes' must be a string or null")
    db: Database = request.app["db"]
    await db.set_backtest_notes(run_id, notes or "")
    return web.json_response({"notes": await db.get_backtest_notes(run_id)})


_PROMOTABLE_KEY_PREFIXES = ("execution_",)


async def _handle_promote_config(request: web.Request) -> web.Response:
    """Promote a backtest run's config snapshot to live runtime_config.

    Body: {keys: ["min_confidence", ...], note: "..."}
    Walks the run's config_json for each key; compares to current
    runtime_config['execution_<key>']; applies changes; records audit rows.
    """
    run_id = _parse_run_id(request)
    try:
        body = await request.json()
    except Exception:
        raise web.HTTPBadRequest(reason="invalid JSON body")

    keys = body.get("keys")
    if not isinstance(keys, list) or not keys:
        raise web.HTTPBadRequest(reason="'keys' must be a non-empty list")
    note = body.get("note", "")

    db: Database = request.app["db"]
    rc = request.app.get("runtime_config")
    if rc is None:
        raise web.HTTPServiceUnavailable(reason="runtime_config not available")

    # Fetch the run's config_json.
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT config_json FROM backtest_runs WHERE id = $1", run_id,
        )
    if row is None:
        raise web.HTTPNotFound(reason=f"backtest run {run_id} not found")
    import json as _json
    cfg = row["config_json"] if isinstance(row["config_json"], dict) \
        else _json.loads(row["config_json"])

    diff: list[dict] = []
    for key in keys:
        if not isinstance(key, str) or key not in cfg:
            continue
        rt_key = None
        if f"execution_{key}" in cfg or any(key.startswith(p) for p in ("execution_",)):
            rt_key = key if key.startswith("execution_") else f"execution_{key}"
        else:
            rt_key = f"execution_{key}"
        new_value = cfg[key]
        try:
            old_value = await rc.get(rt_key)
        except Exception:
            old_value = None
        if old_value == new_value:
            continue
        await rc.set(rt_key, new_value)
        await db.insert_runtime_config_audit(
            key=rt_key,
            old_value=str(old_value) if old_value is not None else None,
            new_value=str(new_value),
            source_backtest_run_id=run_id,
            note=note or None,
        )
        diff.append({
            "key": rt_key, "old_value": old_value, "new_value": new_value,
        })
    return web.json_response({"diff": diff, "run_id": run_id})
```

- [ ] **Step 3: Register the 3 new routes**

In `register_backtests_extra_routes(app)`, add:

```python
    app.router.add_post("/api/backtests/{id}/tags", _handle_set_tags)
    app.router.add_post("/api/backtests/{id}/notes", _handle_set_notes)
    app.router.add_post("/api/backtests/{id}/promote-config", _handle_promote_config)
```

- [ ] **Step 4: Run tests**

Run: `cd bullforge && python -m pytest tests/test_dashboard_backtests_extra_api.py -v`

Expected: all PASS including 4 new POST tests.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/dashboard_service/backtests_extra_api.py \
        bullforge/tests/test_dashboard_backtests_extra_api.py
git commit -m "feat(dashboard): tags/notes/promote-config POST endpoints for backtests"
```

---

## Task 5: Enhance existing /api/backtests/{id}/equity and /folds

**Files:**
- Modify: `bullforge/services/dashboard_service/backtests_api.py`
- Modify: `bullforge/tests/test_dashboard_backtests_api.py`

Two enhancements:
1. `/api/backtests/{id}/equity` — add per-point `drawdown` + a top-level `fold_boundaries` list so the chart can draw dividers.
2. `/api/backtests/{id}/folds` — embed `metrics_json` values into each row (pnl, sharpe, max_drawdown, trades, hit_rate) so the Folds sub-tab doesn't need to parse JSON blobs.

- [ ] **Step 1: Read existing endpoints and test coverage**

Read `bullforge/services/dashboard_service/backtests_api.py` `_equity` and `_folds` handlers. Read `bullforge/tests/test_dashboard_backtests_api.py` to see what's currently asserted.

- [ ] **Step 2: Add failing tests for new fields**

Append to `bullforge/tests/test_dashboard_backtests_api.py`:

```python
@pytest.mark.asyncio
async def test_equity_endpoint_includes_drawdown_and_fold_boundaries(
    aiohttp_client, backtests_app,
):
    """Existing /api/backtests/{id}/equity returns a shape; Plan 4 adds
    `drawdown` per point and a top-level `fold_boundaries` array."""
    client = await aiohttp_client(backtests_app)
    resp = await client.get("/api/backtests/1/equity")
    assert resp.status == 200
    body = await resp.json()
    # Shape can be dict {series, fold_boundaries} OR list for back-compat.
    if isinstance(body, dict):
        assert "series" in body
        assert "fold_boundaries" in body
        for p in body["series"]:
            assert "drawdown" in p
    else:
        # Legacy shape — at minimum each point has drawdown.
        for p in body:
            assert "drawdown" in p


@pytest.mark.asyncio
async def test_folds_endpoint_embeds_metrics(aiohttp_client, backtests_app):
    client = await aiohttp_client(backtests_app)
    resp = await client.get("/api/backtests/1/folds")
    assert resp.status == 200
    body = await resp.json()
    # Each fold row has extracted metrics.
    for row in body:
        for key in ("pnl", "sharpe", "max_drawdown", "trades", "hit_rate"):
            assert key in row
```

- [ ] **Step 3: Update the existing handlers**

In `bullforge/services/dashboard_service/backtests_api.py`, replace `_equity` (around the middle of the file):

```python
async def _equity(request: web.Request) -> web.Response:
    run_id = int(request.match_info["id"])
    results = request.app["results_db"]

    async with results.db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp, equity, fold_id
            FROM backtest_equity
            WHERE run_id = $1
            ORDER BY timestamp ASC
            """,
            run_id,
        )

    peak = None
    series = []
    fold_boundaries = []
    prev_fold = None
    for r in rows:
        eq = float(r["equity"])
        if peak is None or eq > peak:
            peak = eq
        dd = (eq - peak) / peak if peak else 0.0
        if prev_fold is not None and r["fold_id"] != prev_fold:
            fold_boundaries.append(_iso(r["timestamp"]))
        prev_fold = r["fold_id"]
        series.append({
            "timestamp": _iso(r["timestamp"]),
            "equity": eq, "drawdown": dd,
            "fold_id": r["fold_id"],
        })
    return web.json_response({
        "series": series,
        "fold_boundaries": fold_boundaries,
    })
```

Replace `_folds`:

```python
async def _folds(request: web.Request) -> web.Response:
    run_id = int(request.match_info["id"])
    results = request.app["results_db"]

    async with results.db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, fold_index, train_start, train_end,
                   test_start, test_end, metrics_json
            FROM backtest_folds
            WHERE run_id = $1
            ORDER BY fold_index ASC
            """,
            run_id,
        )

    import json as _json
    payload = []
    for r in rows:
        m = r["metrics_json"]
        if isinstance(m, str):
            m = _json.loads(m) if m else {}
        m = m or {}
        payload.append({
            "id": r["id"],
            "fold_index": r["fold_index"],
            "train_start": _iso(r["train_start"]),
            "train_end": _iso(r["train_end"]),
            "test_start": _iso(r["test_start"]),
            "test_end": _iso(r["test_end"]),
            "pnl": float(m.get("pnl") or 0),
            "sharpe": float(m.get("sharpe") or 0),
            "max_drawdown": float(m.get("max_drawdown") or 0),
            "trades": int(m.get("trades") or 0),
            "hit_rate": float(m.get("hit_rate") or 0),
        })
    return web.json_response(payload)
```

- [ ] **Step 4: Run tests**

Run: `cd bullforge && python -m pytest tests/test_dashboard_backtests_api.py -v`

Expected: all PASS including 2 new assertions. Existing backtests_api tests may need tiny adjustments if they asserted a specific shape on `/equity` or `/folds` — update those to the new shape.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/dashboard_service/backtests_api.py \
        bullforge/tests/test_dashboard_backtests_api.py
git commit -m "feat(dashboard): enhance /api/backtests/{id}/equity (drawdown + fold boundaries) and /folds (embed metrics)"
```

---

## Task 6: Extend /api/backtests list with tags + notes

**Files:**
- Modify: `bullforge/services/dashboard_service/backtests_api.py` (in `_list`, add `tags` and `notes` fields to each returned row)

- [ ] **Step 1: Add assertion in existing test**

Append to `bullforge/tests/test_dashboard_backtests_api.py`:

```python
@pytest.mark.asyncio
async def test_list_includes_tags_and_notes(aiohttp_client, backtests_app):
    client = await aiohttp_client(backtests_app)
    resp = await client.get("/api/backtests")
    assert resp.status == 200
    body = await resp.json()
    assert "rows" in body
    for row in body["rows"]:
        assert "tags" in row
        assert "notes" in row
```

(If the existing `mock_db_or_results_db` fixture used by this test doesn't already include `tags`/`notes` in its run rows, update the fixture to add `tags=[]` and `notes=None` — check what the fixture returns.)

- [ ] **Step 2: Update `_list` handler in `backtests_api.py`**

Find the `rows.append({...})` block for `kind='run'` and add `tags` and `notes`:

```python
            rows.append({
                "kind": "run",
                "id": r["id"],
                "name": r["name"],
                "custom_name": r.get("custom_name"),
                "symbols": r["symbols"],
                "range_start": _iso(r["range_start"]),
                "range_end": _iso(r["range_end"]),
                "status": r["status"],
                "started_at": _iso(r["started_at"]),
                "finished_at": _iso(r["finished_at"]),
                "metrics": r["metrics"],
                "tags": list(r.get("tags") or []),
                "notes": r.get("notes"),
            })
```

And the `kind='job'` branch gets `tags=[], notes=None` (jobs don't have tags yet — acceptable):

```python
            rows.append({
                "kind": "job",
                ...existing fields...
                "tags": [],
                "notes": None,
            })
```

- [ ] **Step 3: Run tests**

Run: `cd bullforge && python -m pytest tests/test_dashboard_backtests_api.py -v`

Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/dashboard_service/backtests_api.py \
        bullforge/tests/test_dashboard_backtests_api.py
git commit -m "feat(dashboard): /api/backtests list includes tags and notes"
```

---

## Task 7: Backtests tab top-level sub-tab bar

**Files:**
- Modify: `bullforge/services/dashboard_service/static/index.html`
- Create: `bullforge/services/dashboard_service/static/backtests_tab.js`
- Modify: `bullforge/services/dashboard_service/static/backtests.css`

The existing `backtests.js` owns the List/Detail views. Plan 4 adds a sub-tab bar above the List that lets the user switch to Compare or Sensitivity views. The sub-tab bar calls into `backtests.js` for the List view and into a new `backtests_tab.js` for Compare/Sensitivity.

- [ ] **Step 1: Replace the Backtests tab markup in `index.html`**

Find:

```html
    <!-- Backtests Tab -->
    <section id="tab-backtests" class="tab-content">
        <div id="backtests-root"></div>
    </section>
```

Replace with:

```html
    <!-- Backtests Tab -->
    <section id="tab-backtests" class="tab-content">
        <div class="subtab-bar" id="backtests-subtabs">
            <button class="subtab active" data-subtab="list">List</button>
            <button class="subtab" data-subtab="compare">Compare</button>
            <button class="subtab" data-subtab="sensitivity">Sensitivity</button>
        </div>
        <div id="backtests-root"></div>
    </section>
```

Add `<script src="/static/backtests_tab.js"></script>` after the existing `<script src="/static/backtests.js?v=2"></script>`:

```html
    <script src="/static/backtests.js?v=2"></script>
    <script src="/static/backtests_tab.js"></script>
```

- [ ] **Step 2: Create `backtests_tab.js` with sub-tab routing**

Create `bullforge/services/dashboard_service/static/backtests_tab.js`:

```javascript
// backtests_tab.js — adds a sub-tab bar above the existing backtests.js list
// and wires in Compare/Sensitivity views.
//
// The existing backtests.js handles `#backtests` (list) and
// `#backtests/<id>` (detail). This script intercepts `#backtests/compare`
// and `#backtests/sensitivity` to render alternate views.
(function () {
  var activeSubtab = 'list';

  function $(id) { return document.getElementById(id); }

  async function fetchJSON(url) {
    var r = await fetch(url);
    if (!r.ok) {
      var body = await r.json().catch(function () { return {}; });
      throw new Error('HTTP ' + r.status + ' ' + JSON.stringify(body));
    }
    return r.json();
  }

  function renderCompare() {
    var root = $('backtests-root');
    root.innerHTML =
      '<p class="subtitle">Pick 2–5 runs (comma-separated IDs in the URL) to overlay their equity curves and compare metrics.</p>' +
      '<div id="compare-input-row">' +
      '<label>Run IDs: <input id="compare-ids-input" placeholder="e.g. 17,22,34" style="width:200px;"></label> ' +
      '<button id="compare-load">Load</button></div>' +
      '<div id="compare-chart-host" class="chart-host" style="width:100%;height:360px;margin-top:1rem;"></div>' +
      '<div id="compare-table" style="margin-top:1rem;"></div>';

    var hash = window.location.hash || '';
    var m = /ids=([^&]+)/.exec(hash);
    var inputEl = $('compare-ids-input');
    if (m) inputEl.value = decodeURIComponent(m[1]);

    async function load() {
      var ids = inputEl.value.trim();
      if (!ids) return;
      window.location.hash = '#backtests/compare?ids=' + encodeURIComponent(ids);
      var data;
      try {
        data = await fetchJSON('/api/backtests/compare?ids=' + encodeURIComponent(ids));
      } catch (e) {
        $('compare-chart-host').innerHTML = '<p class="error-state">Error: ' + e.message + '</p>';
        return;
      }
      var colors = ['#00b4d8', '#f39c12', '#2ecc71', '#e74c3c', '#9b59b6'];
      var series = data.runs.map(function (r, i) {
        // Normalize each run's equity to start at 100000 for shape comparison.
        var s = r.equity_series.slice();
        if (s.length === 0) return {label: r.name, data: [], color: colors[i % 5]};
        var base = s[0].equity || 1;
        return {
          label: r.name,
          color: colors[i % 5],
          data: s.map(function (p) {
            return {x: p.timestamp, y: p.equity * 100000 / base};
          }),
        };
      });
      window.Charts.lineChart($('compare-chart-host'), {
        series: series, xIsTime: true, yLabel: 'Normalized equity (start = 100k)',
      });

      $('compare-table').innerHTML =
        '<table class="data-table"><thead><tr>' +
        '<th>Run</th><th>Symbols</th><th>Total P&L</th><th>Avg Sharpe</th>' +
        '<th>Min Max DD</th><th>Trades</th><th>Avg Hit Rate</th><th>Tags</th>' +
        '</tr></thead><tbody>' +
        data.runs.map(function (r) {
          var mm = r.metrics;
          return '<tr><td><a href="#backtests/' + r.run_id + '">' + r.name + '</a></td>' +
            '<td>' + r.symbols.join(', ') + '</td>' +
            '<td class="' + (mm.total_pnl >= 0 ? 'positive' : 'negative') + '">$' + mm.total_pnl.toFixed(2) + '</td>' +
            '<td>' + mm.avg_sharpe.toFixed(2) + '</td>' +
            '<td class="negative">' + (mm.min_max_dd * 100).toFixed(2) + '%</td>' +
            '<td>' + mm.total_trades + '</td>' +
            '<td>' + (mm.avg_hit_rate * 100).toFixed(1) + '%</td>' +
            '<td>' + (r.tags || []).join(', ') + '</td></tr>';
        }).join('') + '</tbody></table>';
    }
    $('compare-load').onclick = load;
    if (inputEl.value) load();
  }

  async function renderSensitivity() {
    var root = $('backtests-root');
    root.innerHTML =
      '<p class="subtitle">Pick a config parameter that was swept across multiple runs. View P&L, Sharpe, Max DD as a function of that parameter.</p>' +
      '<label>Swept parameter: <input id="sens-param-input" placeholder="e.g. min_confidence" style="width:200px;"></label> ' +
      '<label>Group by: <input id="sens-group-input" value="symbols,timeframe,horizons" style="width:260px;"></label> ' +
      '<button id="sens-load">Load</button>' +
      '<div id="sens-charts" style="margin-top:1rem;"></div>';

    $('sens-load').onclick = async function () {
      var param = $('sens-param-input').value.trim();
      var grp = $('sens-group-input').value.trim();
      if (!param) return;
      var url = '/api/backtests/sensitivity?swept_param=' +
        encodeURIComponent(param) + '&group_by=' + encodeURIComponent(grp);
      var data;
      try {
        data = await fetchJSON(url);
      } catch (e) {
        $('sens-charts').innerHTML = '<p class="error-state">Error: ' + e.message + '</p>';
        return;
      }
      if (!data.points.length) {
        $('sens-charts').innerHTML = '<p class="empty-state">No runs found with config.' + param + '.</p>';
        return;
      }
      // Group points by group_key.
      var groups = {};
      data.points.forEach(function (p) {
        var key = JSON.stringify(p.group_key);
        if (!groups[key]) groups[key] = [];
        groups[key].push(p);
      });

      var html = '';
      Object.keys(groups).forEach(function (gk, gi) {
        html += '<section class="drill-section"><h3>Group: ' + gk + '</h3>' +
          '<div id="sens-pnl-' + gi + '" class="chart-host" style="width:100%;height:180px;"></div>' +
          '<div id="sens-sharpe-' + gi + '" class="chart-host" style="width:100%;height:180px;"></div>' +
          '<div id="sens-maxdd-' + gi + '" class="chart-host" style="width:100%;height:180px;"></div>' +
          '</section>';
      });
      $('sens-charts').innerHTML = html;

      Object.keys(groups).forEach(function (gk, gi) {
        var pts = groups[gk].slice().sort(function (a, b) {
          return a.param_value - b.param_value;
        });
        window.Charts.lineChart($('sens-pnl-' + gi), {
          series: [{label: 'Total P&L',
            data: pts.map(function (p) { return {x: p.param_value, y: p.metrics.total_pnl}; })}],
          xIsTime: false, yLabel: 'Total P&L ($)',
        });
        window.Charts.lineChart($('sens-sharpe-' + gi), {
          series: [{label: 'Avg Sharpe',
            data: pts.map(function (p) { return {x: p.param_value, y: p.metrics.avg_sharpe}; })}],
          xIsTime: false, yLabel: 'Avg Sharpe',
        });
        window.Charts.lineChart($('sens-maxdd-' + gi), {
          series: [{label: 'Min Max DD',
            data: pts.map(function (p) { return {x: p.param_value, y: p.metrics.min_max_dd * 100}; })}],
          xIsTime: false, yLabel: 'Min Max DD (%)',
        });
      });
    };
  }

  function setActiveSubtab(name) {
    activeSubtab = name;
    document.querySelectorAll('#backtests-subtabs .subtab').forEach(function (b) {
      b.classList.toggle('active', b.dataset.subtab === name);
    });
    if (name === 'list') {
      // Defer to existing backtests.js renderOverview() via hash.
      if (window.location.hash.indexOf('#backtests/compare') >= 0 ||
          window.location.hash.indexOf('#backtests/sensitivity') >= 0) {
        window.location.hash = '#backtests';
      } else if (window.BacktestsTab && window.BacktestsTab.activate) {
        window.BacktestsTab.activate();
      }
    } else if (name === 'compare') {
      renderCompare();
    } else if (name === 'sensitivity') {
      renderSensitivity();
    }
  }

  function onHashChange() {
    var hash = window.location.hash || '';
    if (hash.indexOf('#backtests/compare') >= 0) setActiveSubtab('compare');
    else if (hash.indexOf('#backtests/sensitivity') >= 0) setActiveSubtab('sensitivity');
    else if (hash.indexOf('#backtests/') >= 0) {
      // Detail page — handled by backtests.js. Sub-tab bar follows List.
      document.querySelectorAll('#backtests-subtabs .subtab').forEach(function (b) {
        b.classList.toggle('active', b.dataset.subtab === 'list');
      });
    } else {
      setActiveSubtab('list');
    }
  }

  document.querySelectorAll('#backtests-subtabs .subtab').forEach(function (b) {
    b.onclick = function () {
      var n = b.dataset.subtab;
      if (n === 'list') window.location.hash = '#backtests';
      else if (n === 'compare') window.location.hash = '#backtests/compare';
      else if (n === 'sensitivity') window.location.hash = '#backtests/sensitivity';
    };
  });
  window.addEventListener('hashchange', onHashChange);
  window.addEventListener('DOMContentLoaded', onHashChange);
})();
```

- [ ] **Step 3: Add CSS for the new views**

Append to `bullforge/services/dashboard_service/static/backtests.css`:

```css
#compare-input-row {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 0.5rem;
}
#compare-input-row input,
#sens-param-input,
#sens-group-input {
    background: var(--bg-card);
    color: var(--text);
    border: 1px solid var(--border);
    padding: 0.25rem 0.5rem;
    border-radius: 4px;
    font-family: inherit;
    font-size: 0.85rem;
}
```

- [ ] **Step 4: Reload + verify**

Reload `http://localhost:18084/#backtests`. Expected:
- Sub-tab bar at top with List/Compare/Sensitivity.
- List view unchanged (renders via existing backtests.js).
- Clicking Compare: form to enter run IDs, Load button, placeholder chart/table.
- Clicking Sensitivity: form with swept param + group_by inputs.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/dashboard_service/static/backtests_tab.js \
        bullforge/services/dashboard_service/static/index.html \
        bullforge/services/dashboard_service/static/backtests.css
git commit -m "feat(dashboard): Backtests sub-tab bar (List/Compare/Sensitivity) + Compare/Sensitivity views"
```

---

## Task 8: Detail-page sub-tab scaffold in `backtests.js`

**Files:**
- Modify: `bullforge/services/dashboard_service/static/backtests.js`

Existing `renderDetail(id)` shows the Summary view. Plan 4 adds 6 more detail sub-tabs routed as `#backtests/{id}/{subtab}`:
Summary (existing), Equity, Folds, Symbols, Distributions, Live-vs-backtest, Funnel.

- [ ] **Step 1: Read existing renderDetail**

Open `bullforge/services/dashboard_service/static/backtests.js`. Locate `renderDetail(id)` and `route()`. Understand what `renderDetail` currently puts on the page and how `route()` chooses between `renderOverview` and `renderDetail`.

- [ ] **Step 2: Extend `route()` to handle sub-tab hashes**

Find the existing `route()` function. Modify it to extract the sub-tab after the run id:

```javascript
  function route() {
    const hash = window.location.hash || '#backtests';
    const parts = hash.replace(/^#/, '').split('/');
    if (parts[0] === 'backtests' && parts[1]) {
      // Special sub-tabs at the top level.
      if (parts[1] === 'compare' || parts[1] === 'sensitivity') {
        // Handled by backtests_tab.js.
        return;
      }
      const id = parts[1];
      const subtab = parts[2] || 'summary';
      renderDetail(id, subtab);
    } else {
      renderOverview();
    }
  }
```

- [ ] **Step 3: Modify `renderDetail(id, subtab)` to render a sub-tab bar and dispatch**

Wrap the existing Summary rendering in a sub-tab switcher. Replace the existing `renderDetail` signature/body with:

```javascript
  async function renderDetail(id, subtab) {
    subtab = subtab || 'summary';
    root().innerHTML =
      '<div class="bt-detail">' +
      '<a class="bt-back" href="#backtests">← Back to list</a>' +
      '<h2 id="bt-detail-title">Run ' + id + '</h2>' +
      '<div class="subtab-bar" id="bt-detail-subtabs">' +
        '<button class="subtab" data-subtab="summary">Summary</button>' +
        '<button class="subtab" data-subtab="equity">Equity</button>' +
        '<button class="subtab" data-subtab="folds">Folds</button>' +
        '<button class="subtab" data-subtab="symbols">Per-symbol</button>' +
        '<button class="subtab" data-subtab="distributions">Distributions</button>' +
        '<button class="subtab" data-subtab="live">Live-vs-backtest</button>' +
        '<button class="subtab" data-subtab="funnel">Funnel</button>' +
      '</div>' +
      '<div id="bt-detail-panel"></div>' +
      '</div>';

    document.querySelectorAll('#bt-detail-subtabs .subtab').forEach(function (b) {
      b.classList.toggle('active', b.dataset.subtab === subtab);
      b.onclick = function () {
        window.location.hash = '#backtests/' + id + '/' + b.dataset.subtab;
      };
    });

    var panel = document.getElementById('bt-detail-panel');

    try {
      if (subtab === 'summary')         await renderDetailSummary(id, panel);
      else if (subtab === 'equity')     await renderDetailEquity(id, panel);
      else if (subtab === 'folds')      await renderDetailFolds(id, panel);
      else if (subtab === 'symbols')    await renderDetailSymbols(id, panel);
      else if (subtab === 'distributions') await renderDetailDistributions(id, panel);
      else if (subtab === 'live')       await renderDetailLiveOverlay(id, panel);
      else if (subtab === 'funnel')     await renderDetailFunnel(id, panel);
      else panel.innerHTML = '<p class="empty-state">Unknown sub-tab: ' + subtab + '</p>';
    } catch (e) {
      panel.innerHTML = '<p class="error-state">Error: ' + (e.message || e) + '</p>';
    }
  }

  async function renderDetailSummary(id, panel) {
    var run = await fetchJSON('/api/backtests/' + id);
    // Keep the existing Summary content — move what was in the old
    // renderDetail here (metrics, config_json pretty-print).
    // Minimum: show name, range, status, config JSON.
    panel.innerHTML =
      '<div class="summary-grid">' +
      '<div><strong>Name:</strong> ' + run.name + '</div>' +
      '<div><strong>Status:</strong> ' + run.status + '</div>' +
      '<div><strong>Symbols:</strong> ' + (run.symbols || []).join(', ') + '</div>' +
      '<div><strong>Range:</strong> ' + (run.range_start || '') + ' → ' + (run.range_end || '') + '</div>' +
      '<div><strong>Tags:</strong> <span id="detail-tags"></span></div>' +
      '<div><strong>Notes:</strong> <span id="detail-notes"></span></div>' +
      '</div>' +
      '<h3>Config</h3><pre>' + JSON.stringify(run.config_json || {}, null, 2) + '</pre>' +
      '<button id="bt-promote">Promote config to live</button>' +
      '<div id="bt-promote-modal"></div>';
    // Tags and notes renderer (Task 12 hooks this up).
    if (window.BacktestsTagsNotes) {
      window.BacktestsTagsNotes.mount(id, run.tags || [], run.notes || '');
    }
    // Promote button (Task 13 implements).
    document.getElementById('bt-promote').onclick = function () {
      if (window.BacktestsPromote) window.BacktestsPromote.open(id, run.config_json || {});
    };
  }

  // The specific sub-tab renderers (equity/folds/symbols/distributions/
  // live/funnel) are implemented in Tasks 9-11.
  async function renderDetailEquity(id, panel)        { panel.innerHTML = '<p class="empty-state">Equity sub-tab placeholder (Task 9).</p>'; }
  async function renderDetailFolds(id, panel)         { panel.innerHTML = '<p class="empty-state">Folds sub-tab placeholder (Task 9).</p>'; }
  async function renderDetailSymbols(id, panel)       { panel.innerHTML = '<p class="empty-state">Per-symbol sub-tab placeholder (Task 10).</p>'; }
  async function renderDetailDistributions(id, panel) { panel.innerHTML = '<p class="empty-state">Distributions sub-tab placeholder (Task 10).</p>'; }
  async function renderDetailLiveOverlay(id, panel)   { panel.innerHTML = '<p class="empty-state">Live-vs-backtest sub-tab placeholder (Task 11).</p>'; }
  async function renderDetailFunnel(id, panel)        { panel.innerHTML = '<p class="empty-state">Funnel sub-tab placeholder (Task 11).</p>'; }
```

If the existing code had `renderDetail` doing more (metrics, fold list, equity chart), keep that logic but move it into `renderDetailSummary`. Delete the old renderDetail content.

- [ ] **Step 4: Reload + verify**

Reload `http://localhost:18084/#backtests/17` (use a real run ID). Expected:
- Detail page shows sub-tab bar with 7 buttons.
- Summary is the default sub-tab; shows name/status/symbols/range + config JSON.
- Other sub-tabs show placeholder text.
- Back button returns to list.
- URL updates to `#backtests/17/<subtab>` when clicking.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/dashboard_service/static/backtests.js
git commit -m "feat(dashboard): backtests detail-page sub-tab scaffold (7 sub-tabs + routing)"
```

---

## Task 9: Detail — Equity + Folds sub-tabs

**Files:** Modify `bullforge/services/dashboard_service/static/backtests.js`

- [ ] **Step 1: Replace `renderDetailEquity`**

```javascript
  async function renderDetailEquity(id, panel) {
    panel.innerHTML =
      '<div id="det-eq-host" class="chart-host" style="width:100%;height:360px;"></div>' +
      '<p id="det-eq-summary" style="color:var(--text-muted);"></p>';
    var data = await fetchJSON('/api/backtests/' + id + '/equity');
    var series = (data.series || data || []).map(function (p) {
      return {x: p.timestamp, y: p.equity};
    });
    window.Charts.lineChart(document.getElementById('det-eq-host'), {
      series: [{label: 'Equity', data: series}],
      xIsTime: true, yLabel: 'Equity ($)',
    });
    var fbs = (data.fold_boundaries || []).length;
    document.getElementById('det-eq-summary').textContent =
      (data.series || data).length + ' points · ' + fbs + ' fold boundaries';
  }
```

- [ ] **Step 2: Replace `renderDetailFolds`**

```javascript
  async function renderDetailFolds(id, panel) {
    panel.innerHTML = '<div id="det-folds-host"></div>';
    var folds = await fetchJSON('/api/backtests/' + id + '/folds');
    window.Leaderboard.create({
      mountEl: document.getElementById('det-folds-host'),
      columns: [
        {key: 'fold_index', label: 'Fold', sortable: true},
        {key: 'test_start', label: 'Test Start', sortable: true},
        {key: 'test_end', label: 'Test End', sortable: true},
        {key: 'pnl', label: 'P&L', sortable: true,
         render: function (v) {
           var cls = v >= 0 ? 'positive' : 'negative';
           return '<span class="' + cls + '">$' + v.toFixed(2) + '</span>';
         }},
        {key: 'sharpe', label: 'Sharpe', sortable: true,
         render: function (v) { return v.toFixed(2); }},
        {key: 'max_drawdown', label: 'Max DD', sortable: true,
         render: function (v) { return (v * 100).toFixed(2) + '%'; }},
        {key: 'trades', label: '#Trades', sortable: true},
        {key: 'hit_rate', label: 'Hit Rate', sortable: true,
         render: function (v) { return (v * 100).toFixed(1) + '%'; }},
      ],
      rows: folds,
      initialSort: {key: 'fold_index', order: 'asc'},
    });
  }
```

- [ ] **Step 3: Reload + verify**

Reload `#backtests/17/equity` and `#backtests/17/folds`.

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/dashboard_service/static/backtests.js
git commit -m "feat(dashboard): backtests detail Equity + Folds sub-tabs"
```

---

## Task 10: Detail — Per-symbol + Distributions sub-tabs

**Files:** Modify `bullforge/services/dashboard_service/static/backtests.js`

- [ ] **Step 1: Replace `renderDetailSymbols`**

```javascript
  async function renderDetailSymbols(id, panel) {
    panel.innerHTML =
      '<div id="det-sym-chart" class="chart-host" style="width:100%;height:240px;"></div>' +
      '<div id="det-sym-table"></div>';
    var rows = await fetchJSON('/api/backtests/' + id + '/symbols');
    window.Charts.barChart(document.getElementById('det-sym-chart'), {
      labels: rows.map(function (r) { return r.symbol; }),
      values: rows.map(function (r) { return r.total_pnl; }),
      colorByValue: true, yLabel: 'P&L per symbol ($)',
    });
    window.Leaderboard.create({
      mountEl: document.getElementById('det-sym-table'),
      columns: [
        {key: 'symbol', label: 'Symbol', sortable: true},
        {key: 'trades', label: '#Trades', sortable: true},
        {key: 'wins', label: 'Wins', sortable: true},
        {key: 'win_rate', label: 'Win %', sortable: true,
         render: function (v) { return (v * 100).toFixed(1) + '%'; }},
        {key: 'total_pnl', label: 'Total P&L', sortable: true,
         render: function (v) {
           var cls = v >= 0 ? 'positive' : 'negative';
           return '<span class="' + cls + '">$' + v.toFixed(2) + '</span>';
         }},
        {key: 'avg_pnl', label: 'Avg P&L', sortable: true,
         render: function (v) { return '$' + v.toFixed(2); }},
      ],
      rows: rows,
      initialSort: {key: 'total_pnl', order: 'desc'},
    });
  }
```

- [ ] **Step 2: Replace `renderDetailDistributions`**

```javascript
  async function renderDetailDistributions(id, panel) {
    panel.innerHTML =
      '<h3>Daily P&L</h3><div id="det-dist-daily" class="chart-host" style="width:100%;height:200px;"></div>' +
      '<h3>Trade P&L histogram</h3><div id="det-dist-hist" class="chart-host" style="width:100%;height:220px;"></div>' +
      '<h3>Hold-time buckets</h3><div id="det-dist-hold" class="chart-host" style="width:100%;height:220px;"></div>' +
      '<h3>Side breakdown</h3><div id="det-dist-side"></div>';

    var data = await fetchJSON('/api/backtests/' + id + '/distributions');
    window.Charts.barChart(document.getElementById('det-dist-daily'), {
      labels: data.daily_pnl.map(function (d) {
        return (d.date || '').slice(0, 10);
      }),
      values: data.daily_pnl.map(function (d) { return d.pnl; }),
      colorByValue: true, yLabel: 'P&L ($)',
    });
    var hist = data.trade_pnl_histogram;
    window.Charts.barChart(document.getElementById('det-dist-hist'), {
      labels: hist.labels.map(function (v) { return String(v); }),
      values: hist.counts, yLabel: 'Count',
    });
    window.Charts.barChart(document.getElementById('det-dist-hold'), {
      labels: data.hold_time_buckets.map(function (b) {
        return b.bucket + ' (' + b.trades + ')';
      }),
      values: data.hold_time_buckets.map(function (b) { return b.avg_pnl; }),
      colorByValue: true, yLabel: 'Avg P&L ($)',
    });
    document.getElementById('det-dist-side').innerHTML =
      '<table class="data-table"><thead><tr>' +
      '<th>Side</th><th>Trades</th><th>Wins</th><th>Win %</th><th>Total P&L</th><th>Avg P&L</th>' +
      '</tr></thead><tbody>' +
      data.side_breakdown.map(function (s) {
        var cls = s.total_pnl >= 0 ? 'positive' : 'negative';
        return '<tr><td>' + s.side + '</td><td>' + s.trades + '</td><td>' + s.wins + '</td>' +
          '<td>' + (s.win_rate * 100).toFixed(1) + '%</td>' +
          '<td class="' + cls + '">$' + s.total_pnl.toFixed(2) + '</td>' +
          '<td>$' + s.avg_pnl.toFixed(2) + '</td></tr>';
      }).join('') + '</tbody></table>';
  }
```

- [ ] **Step 3: Reload + verify**

Reload `#backtests/17/symbols` and `#backtests/17/distributions`.

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/dashboard_service/static/backtests.js
git commit -m "feat(dashboard): backtests detail Per-symbol + Distributions sub-tabs"
```

---

## Task 11: Detail — Live-vs-backtest + Funnel sub-tabs

**Files:** Modify `bullforge/services/dashboard_service/static/backtests.js`

- [ ] **Step 1: Replace `renderDetailLiveOverlay`**

```javascript
  async function renderDetailLiveOverlay(id, panel) {
    panel.innerHTML =
      '<div id="det-live-host" class="chart-host" style="width:100%;height:340px;"></div>' +
      '<p id="det-live-note" style="color:var(--text-muted);"></p>';
    var data = await fetchJSON('/api/backtests/' + id + '/live-overlay');
    if (!data.has_overlap) {
      document.getElementById('det-live-note').innerHTML =
        '<strong>No overlap.</strong> No live trades exist for symbols [' +
        (data.symbols || []).join(', ') + '] within the backtest window (' +
        (data.range_start || '') + ' → ' + (data.range_end || '') + ').';
    }
    var series = [
      {label: 'Backtest equity',
       data: data.backtest_series.map(function (p) {
         return {x: p.timestamp, y: p.equity};
       })},
      {label: 'Live (cumulative P&L from 0)',
       data: data.live_series.map(function (p) {
         return {x: p.timestamp, y: p.equity};
       })},
    ];
    window.Charts.lineChart(document.getElementById('det-live-host'), {
      series: series, xIsTime: true, yLabel: 'Equity / cumulative P&L',
    });
  }
```

- [ ] **Step 2: Replace `renderDetailFunnel`**

```javascript
  async function renderDetailFunnel(id, panel) {
    panel.innerHTML =
      '<div id="det-funnel-host" class="chart-host" style="width:100%;height:260px;"></div>' +
      '<p id="det-funnel-note" style="color:var(--text-muted);"></p>';
    var data = await fetchJSON('/api/backtests/' + id + '/funnel');
    var labels = ['Signals', 'Confident (≥' +
                  (data.min_confidence * 100).toFixed(0) + '%)',
                  'Orders placed', 'Orders filled', 'Orders closed'];
    var values = [data.signals, data.confident, data.orders_placed,
                   data.orders_filled, data.orders_closed];
    window.Charts.barChart(document.getElementById('det-funnel-host'), {
      labels: labels, values: values, yLabel: 'Count',
    });
    // Drop-off rates between adjacent stages.
    var dropTexts = [];
    for (var i = 1; i < values.length; i++) {
      var prev = values[i - 1], cur = values[i];
      var dropPct = prev > 0 ? (1 - cur / prev) * 100 : 0;
      dropTexts.push(labels[i - 1] + ' → ' + labels[i] + ': ' + dropPct.toFixed(1) + '% drop');
    }
    document.getElementById('det-funnel-note').textContent = dropTexts.join(' · ');
  }
```

- [ ] **Step 3: Reload + verify**

Reload `#backtests/17/live` and `#backtests/17/funnel`.

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/dashboard_service/static/backtests.js
git commit -m "feat(dashboard): backtests detail Live-vs-backtest + Funnel sub-tabs"
```

---

## Task 12: Tags chips + notes inline editor on List + Summary

**Files:**
- Modify: `bullforge/services/dashboard_service/static/backtests.js`
- Modify: `bullforge/services/dashboard_service/static/backtests.css`

- [ ] **Step 1: Render tags chips in the List table**

Find `renderOverview` → the table rendering code. Add a `Tags` column (or put chips inside the Name column). Each chip is clickable to filter by that tag. Also add a tag-filter input in the toolbar.

In the `renderOverview()` or `bindOverviewEvents()` function, after rendering rows, add a call to insert tag chips into each row. Alternatively, add a column header `<th>Tags</th>` and render chips directly.

Simplest approach: add a Tags column to the existing table. In `renderOverview`, find the `<thead>` row and add `<th>Tags</th>` after the existing `<th>Status</th>` (or wherever fits). In the row-rendering loop, for each row append:

```javascript
'<td>' +
  (row.tags || []).map(function (t) {
    return '<span class="tag-chip">' + t + '</span>';
  }).join(' ') +
'</td>'
```

Add CSS to `backtests.css`:

```css
.tag-chip {
    display: inline-block;
    padding: 2px 6px;
    margin-right: 4px;
    background: var(--accent);
    color: #000;
    font-size: 0.7rem;
    border-radius: 8px;
}
.notes-editor {
    width: 100%;
    background: var(--bg);
    color: var(--text);
    border: 1px solid var(--border);
    padding: 0.5rem;
    border-radius: 4px;
    font-family: inherit;
    font-size: 0.85rem;
    min-height: 3rem;
}
.tag-editor {
    width: 300px;
    padding: 0.25rem;
    background: var(--bg);
    color: var(--text);
    border: 1px solid var(--border);
    border-radius: 4px;
}
```

- [ ] **Step 2: Create `BacktestsTagsNotes` helper for the Summary sub-tab**

At the end of `backtests.js` (still inside the IIFE), expose a helper that mounts tag/notes editors inside the Summary sub-tab:

```javascript
  window.BacktestsTagsNotes = {
    mount: function (runId, tags, notes) {
      var tagsEl = document.getElementById('detail-tags');
      var notesEl = document.getElementById('detail-notes');
      if (!tagsEl || !notesEl) return;

      // Tags: show chips + a "Edit" button that reveals an input.
      function renderTags(current) {
        tagsEl.innerHTML =
          current.map(function (t) {
            return '<span class="tag-chip">' + t + '</span>';
          }).join(' ') +
          ' <a href="#" id="tag-edit-link">Edit</a>';
        document.getElementById('tag-edit-link').onclick = function (e) {
          e.preventDefault();
          tagsEl.innerHTML =
            '<input class="tag-editor" id="tag-edit-input" value="' +
              current.join(', ') + '" />' +
            ' <button id="tag-save">Save</button> <button id="tag-cancel">Cancel</button>';
          document.getElementById('tag-save').onclick = async function () {
            var list = document.getElementById('tag-edit-input').value
                         .split(',').map(function (s) { return s.trim(); })
                         .filter(function (s) { return s.length > 0; });
            var r = await fetch('/api/backtests/' + runId + '/tags', {
              method: 'POST',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify({tags: list}),
            });
            if (r.ok) {
              var body = await r.json();
              renderTags(body.tags || []);
            } else {
              alert('Failed to save tags: HTTP ' + r.status);
            }
          };
          document.getElementById('tag-cancel').onclick = function () {
            renderTags(current);
          };
        };
      }
      renderTags(tags || []);

      // Notes: show text + "Edit" button that reveals textarea.
      function renderNotes(current) {
        notesEl.innerHTML =
          '<span>' + (current ? escapeHtml(current) : '<em>(none)</em>') + '</span>' +
          ' <a href="#" id="notes-edit-link">Edit</a>';
        document.getElementById('notes-edit-link').onclick = function (e) {
          e.preventDefault();
          notesEl.innerHTML =
            '<textarea class="notes-editor" id="notes-edit-input">' +
              escapeHtml(current || '') +
            '</textarea><br>' +
            '<button id="notes-save">Save</button> <button id="notes-cancel">Cancel</button>';
          document.getElementById('notes-save').onclick = async function () {
            var text = document.getElementById('notes-edit-input').value;
            var r = await fetch('/api/backtests/' + runId + '/notes', {
              method: 'POST',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify({notes: text}),
            });
            if (r.ok) {
              var body = await r.json();
              renderNotes(body.notes || '');
            } else {
              alert('Failed to save notes: HTTP ' + r.status);
            }
          };
          document.getElementById('notes-cancel').onclick = function () {
            renderNotes(current);
          };
        };
      }
      renderNotes(notes || '');
    },
  };

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
    });
  }
```

- [ ] **Step 3: Reload + verify**

Reload `#backtests/17/summary`. Expected:
- Tags shows chips (empty initially) + "Edit" link.
- Edit reveals input; save hits POST `/tags` and re-renders chips.
- Notes similar with textarea.

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/dashboard_service/static/backtests.js \
        bullforge/services/dashboard_service/static/backtests.css
git commit -m "feat(dashboard): tag chips on list + inline tag/notes editors on detail Summary"
```

---

## Task 13: Promote-to-live modal

**Files:** Modify `bullforge/services/dashboard_service/static/backtests.js`

- [ ] **Step 1: Expose `BacktestsPromote.open(id, config)`**

At the end of `backtests.js`:

```javascript
  window.BacktestsPromote = {
    open: async function (runId, cfg) {
      var modalHost = document.getElementById('bt-promote-modal');
      if (!modalHost) return;
      // Fetch current runtime_config to show diff.
      var rcResp = await fetch('/api/config');
      var rc = await rcResp.json();
      var current = rc.values || {};
      // Consider only execution_* keys for now.
      var rows = [];
      Object.keys(cfg).forEach(function (k) {
        var rtKey = k.startsWith('execution_') ? k : 'execution_' + k;
        if (!(rtKey in current)) return;
        var cur = current[rtKey];
        var nxt = cfg[k];
        if (JSON.stringify(cur) === JSON.stringify(nxt)) return;
        rows.push({key: k, rtKey: rtKey, current: cur, next: nxt});
      });
      if (rows.length === 0) {
        modalHost.innerHTML = '<div class="bt-modal">' +
          '<p>Backtest config matches current runtime config for all execution_* keys. Nothing to promote.</p>' +
          '<button id="bt-promote-close">Close</button></div>';
        document.getElementById('bt-promote-close').onclick = function () {
          modalHost.innerHTML = '';
        };
        return;
      }
      modalHost.innerHTML =
        '<div class="bt-modal">' +
        '<h3>Promote config to live runtime_config</h3>' +
        '<table class="data-table"><thead><tr><th></th><th>Key</th><th>Current</th><th>From backtest</th></tr></thead><tbody>' +
        rows.map(function (r, i) {
          return '<tr><td><input type="checkbox" checked data-idx="' + i + '"></td>' +
            '<td>' + r.rtKey + '</td>' +
            '<td>' + JSON.stringify(r.current) + '</td>' +
            '<td class="positive">' + JSON.stringify(r.next) + '</td></tr>';
        }).join('') + '</tbody></table>' +
        '<p><label>Note: <input id="bt-promote-note" type="text" style="width:300px;"></label></p>' +
        '<button id="bt-promote-apply">Apply</button> ' +
        '<button id="bt-promote-cancel">Cancel</button>' +
        '</div>';

      document.getElementById('bt-promote-cancel').onclick = function () {
        modalHost.innerHTML = '';
      };
      document.getElementById('bt-promote-apply').onclick = async function () {
        var picked = [];
        modalHost.querySelectorAll('input[type=checkbox]').forEach(function (cb) {
          if (cb.checked) picked.push(rows[parseInt(cb.dataset.idx, 10)].key);
        });
        var note = document.getElementById('bt-promote-note').value || '';
        var r = await fetch('/api/backtests/' + runId + '/promote-config', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({keys: picked, note: note}),
        });
        if (r.ok) {
          var body = await r.json();
          modalHost.innerHTML = '<div class="bt-modal">' +
            '<p>Applied ' + body.diff.length + ' change(s).</p>' +
            '<button id="bt-promote-done">Close</button></div>';
          document.getElementById('bt-promote-done').onclick = function () {
            modalHost.innerHTML = '';
          };
        } else {
          var err = await r.text();
          modalHost.innerHTML = '<div class="bt-modal error-state"><p>Error: ' + err + '</p>' +
            '<button id="bt-promote-err">Close</button></div>';
          document.getElementById('bt-promote-err').onclick = function () {
            modalHost.innerHTML = '';
          };
        }
      };
    },
  };
```

Add CSS for `.bt-modal` to `backtests.css`:

```css
.bt-modal {
    background: var(--bg-card);
    border: 1px solid var(--accent);
    border-radius: 4px;
    padding: 1rem;
    margin-top: 1rem;
}
.bt-modal h3 { margin: 0 0 0.5rem 0; color: var(--accent); }
.bt-modal .data-table { margin: 0.5rem 0; }
```

- [ ] **Step 2: Reload + verify**

Go to `#backtests/17/summary`. Click "Promote config to live". Modal shows diff rows with checkboxes. Applying invokes POST `/promote-config` and shows result.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/backtests.js \
        bullforge/services/dashboard_service/static/backtests.css
git commit -m "feat(dashboard): promote-config modal (diff + audit via /promote-config POST)"
```

---

## Task 14: End-to-end smoke test + stub cleanup verification

**Files:** none modified — verification only.

- [ ] **Step 1: Full backend test suite**

Run: `cd bullforge && python -m pytest tests/test_time_window.py tests/test_dashboard_api.py tests/test_dashboard_stubs.py tests/test_dashboard_models_api.py tests/test_dashboard_trading_api.py tests/test_dashboard_backtests_api.py tests/test_dashboard_backtests_extra_api.py tests/test_accuracy_snapshot.py -v`

Expected: all PASS.

- [ ] **Step 2: Core suite sanity**

Run: `cd bullforge && python -m pytest tests/test_signal_aggregator.py tests/test_risk_manager.py tests/test_trainer.py tests/test_lstm_model.py tests/test_order_executor.py tests/meta/ -v`

Expected: no regressions.

- [ ] **Step 3: Probe all endpoints are non-501**

```bash
for path in \
  "api/models/1/history" "api/models/1/calibration" "api/models/1/simulated_pnl" \
  "api/models/1/feature_importance" "api/models/1/regime_breakdown" "api/models/1/signal_history" \
  "api/trading/equity" "api/trading/distributions" "api/trading/heatmap" \
  "api/trading/streaks" "api/trading/exposure" "api/trading/benchmark" "api/trading/symbols/AAPL" \
  "api/backtests/1/equity" "api/backtests/1/folds" "api/backtests/1/symbols" \
  "api/backtests/1/distributions" "api/backtests/1/live-overlay" "api/backtests/1/funnel" \
  "api/backtests/compare?ids=1" "api/backtests/sensitivity?swept_param=min_confidence"; do
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:18084/$path")
  echo "$code $path"
done
```

Expected: zero `501`s. Most 200s (with empty data for nonexistent ids); some 404/400 are acceptable for invalid inputs.

- [ ] **Step 4: Manual dashboard walkthrough**

Visit in order:
1. `#backtests` — List still renders via existing backtests.js; sub-tab bar above shows Compare/Sensitivity.
2. `#backtests/compare` — form + Load button; enter two run IDs → overlay + metrics table render.
3. `#backtests/sensitivity` — form for `min_confidence` → three line charts per group.
4. `#backtests/17/summary` — name, config, tags editor, notes editor, Promote button.
5. `#backtests/17/equity` — equity line chart.
6. `#backtests/17/folds` — sortable fold table.
7. `#backtests/17/symbols` — bar + table.
8. `#backtests/17/distributions` — 4 charts.
9. `#backtests/17/live` — overlay or "no overlap" message.
10. `#backtests/17/funnel` — attrition bars + drop-off rates.
11. Click Promote → diff modal; apply → see diff count; cancel works.
12. Click Edit on tags → input, Save → POST succeeds, chips update.
13. Other tabs (Overview, Models, Trading, etc.) still work.
14. DevTools console: no errors.

- [ ] **Step 5: Commit smoke-test fixes if any**

If needed: `git commit -m "fix(dashboard): address smoke-test findings in Backtests plan"`. Otherwise skip.

---

## Self-Review Summary

**Spec coverage** (against [2026-04-17-dashboard-expansion-design.md](../specs/2026-04-17-dashboard-expansion-design.md) — Plan 4 scope):

| Spec item | Task |
|-----------|------|
| List sub-tab with tags column + notes-preview column + multi-select | Tasks 6 (backend), 7 (sub-tab bar), 12 (chips) |
| Compare sub-tab (overlay 2-5 runs + metrics table, persisted URL) | Tasks 3 (compare endpoint), 7 |
| Sensitivity sub-tab (1D param sweep line charts) | Tasks 3 (sensitivity endpoint), 7 |
| Detail Summary (existing + tags + notes + Promote button) | Tasks 6, 8, 12, 13 |
| Detail Equity (per-fold concatenated, drawdown shading, fold boundaries) | Tasks 5 (endpoint enhancement), 9 |
| Detail Folds (per-fold metrics table) | Tasks 5, 9 |
| Detail Per-symbol (bar + table) | Tasks 2 (query), 3 (endpoint), 10 |
| Detail Distributions (same shape as live Trading) | Tasks 2, 3, 10 |
| Detail Live-vs-backtest overlay | Tasks 2, 3, 11 |
| Detail Signal-to-trade funnel | Tasks 2, 3, 11 |
| Tags/notes POST endpoints | Task 4 |
| Promote-to-live (diff + audit row) | Tasks 1 (audit table), 4 (endpoint), 13 (modal) |
| Migration 019 (tags, notes, runtime_config_audit) | Task 1 |
| All 8 remaining Plan 4 stubs removed | Task 3 |

**Placeholder scan:** None.

**Type consistency:** All new Database methods take `run_id: int` and return plain dicts. All aiohttp handlers parse `id` → int and raise 400 on invalid input. Scope-aware funnel read explicitly via `backtest_run_id = $1`. All POST handlers validate body types and raise 400. Promote-to-live always writes both runtime_config (via existing `rc.set`) and `runtime_config_audit` (via new DB method).

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-17-dashboard-plan-4-backtests.md`.

**Two execution options:**

1. **Subagent-Driven (recommended)** — fresh subagent per task, two-stage review
2. **Inline Execution** — batch tasks in this session

Which approach?
