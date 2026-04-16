# Execution Thresholds — Configurable via UI

**Date:** 2026-04-16
**Branch:** `exec-config`
**Status:** Approved design, ready for implementation planning

## Goal

Make the seven parameters that gate a trade entry editable without a service restart, and allow backtests to override them per-run for reproducible experiments.

## Parameters in scope

| Parameter | Current source | Used by | Currently hardcoded? |
|---|---|---|---|
| `execution_min_confidence` | env (default 0.6) | `SignalAggregator` | no |
| `execution_min_agreement` | env (default 2) | `SignalAggregator` | no |
| `execution_direction_band` | **nowhere** | `SignalAggregator` | **yes — literal `0.3`** |
| `execution_min_signal_strength` | env (default 0.5) | `RiskManager` | no |
| `execution_max_position_pct` | env (default 0.10) | `RiskManager` | no |
| `execution_max_positions` | env (default 5) | `RiskManager` | no |
| `execution_max_exposure` | env (default 0.5) | `RiskManager` | no |

## Architectural decisions

1. **Authoritative source = `runtime_config` (Redis hash).** All 7 keys move into the existing `CONFIG_SCHEMA` allowlist. Env-var fields on `Settings` are removed to avoid two-config-surface drift, matching the precedent set by commit `3d7fb7f` (trading symbols).

2. **Live reload with no pub/sub.** `SignalAggregator` and `RiskManager` read each parameter from `runtime_config` **on every signal**, via a small `_param(key)` helper. Three extra Redis `HGET`s per `model:predictions` message; negligible cost. A config-page PUT takes effect on the very next prediction.

3. **New default values** (tightening risk defaults vs the env-var era):
   - `execution_max_position_pct`: 0.10 → **0.02**
   - `execution_max_exposure`: 0.5 → **0.2**
   - `execution_min_agreement` max: 10 → **10000** (permits very wide ensembles)
   - Other defaults unchanged.

4. **Backtest overrides are per-run and snapshot-on-creation.** The `/api/backtests` POST accepts seven optional override fields. Any field left blank is filled from `runtime_config` *at job creation time* before being persisted in `backtest_jobs.config_json`. Every job row therefore records the exact thresholds that ran — jobs re-run later reproduce identically regardless of runtime_config drift.

5. **New schema keys carry full metadata.** Each entry in `CONFIG_SCHEMA` includes `label`, `description`, and `reference` (Wikipedia link). The existing `config.html` renders from the schema, so the config page gets human-readable labels and help text with zero frontend changes.

## Flow (live path)

```
model:predictions ─▶ execution_service.on_prediction
                       │
                       ├─▶ aggregator.aggregate(...)            (now async)
                       │    ├─ _param("min_confidence")  ─▶ Redis
                       │    ├─ _param("min_agreement")   ─▶ Redis
                       │    ├─ _param("direction_band")  ─▶ Redis
                       │    └─ (filter + weighted vote + band check)
                       │
                       └─▶ executor.process_signal → risk.check_entry
                                                     ├─ _param("min_signal_strength")
                                                     ├─ _param("max_position_pct")
                                                     ├─ _param("max_positions")
                                                     └─ _param("max_exposure")
```

## Flow (backtest path)

```
Modal submit ─▶ POST /api/backtests
                  │
                  ├─ For each of 7 threshold keys:
                  │    body[key] = body[key] or await rc.get(f"execution_{key}")
                  │    _validate_threshold(key, body[key])
                  │
                  └─ insert_job(config_json=body)   ← all 7 values frozen

Worker picks up job ─▶ _build_config → BacktestConfig(..., min_confidence=..., ...)
                       │
                       └─ engine.run(cfg)
                            overrides = {k: getattr(cfg, k) for k in THRESHOLD_KEYS}
                            aggregator = SignalAggregator(rc, overrides=overrides)
                            risk       = RiskManager(rc,       overrides=overrides)
```

Inside `_param`: if `overrides.get(key) is not None`, return it; else `await rc.get(f"execution_{key}")`. Because the API guarantees all 7 are populated, backtests never fall through to live runtime_config.

## File inventory

### Modified

- **`bullforge/shared/runtime_config.py`** — add 7 schema entries with label/description/reference.
- **`bullforge/shared/config.py`** — remove 6 execution-threshold fields from `Settings` (leave reconciler fields; `execution_direction_band` is new and was never an env var).
- **`bullforge/services/execution_service/signal_aggregator.py`** — drop threshold kwargs, accept `runtime_config` + optional `overrides`; `aggregate()` becomes async; replace literal `0.3` with `band = await self._param("direction_band")`.
- **`bullforge/services/execution_service/risk_manager.py`** — drop threshold kwargs, accept optional `overrides`; `check_entry` reads all 4 thresholds via `_param`.
- **`bullforge/services/execution_service/main.py`** — simplify `SignalAggregator(...)` / `RiskManager(...)` construction; `await aggregator.aggregate(...)` in `on_prediction`.
- **`bullforge/services/backtest_service/engine.py`** — extend `BacktestConfig` with 7 optional fields; pass `overrides` dict into aggregator and risk manager.
- **`bullforge/services/backtest_service/worker.py`** — `_build_config` reads the 7 new fields from job JSON.
- **`bullforge/services/dashboard_service/backtests_api.py`** — accept optional threshold overrides, validate against schema constraints, snapshot-fill blanks from runtime_config before insert.
- **`bullforge/services/dashboard_service/static/backtests.js`** — add collapsed `<details>` "Advanced — Entry thresholds (optional overrides)" section to the new-backtest modal with 7 numeric inputs, placeholders showing current runtime_config values.
- **`.env.example`** — remove the 6 `EXECUTION_*` entries that correspond to removed `Settings` fields.
- **`docker-compose.yml`** — remove the same 6 `EXECUTION_*` entries from any service env blocks that set them.

### Tests modified / new

- **`bullforge/tests/test_signal_aggregator.py`** — rewrite: fake RuntimeConfig (in-memory dict), assert overrides win, assert direction band value controls neutral zone (0.1 vs 0.3 vs 0.5 → different directions on same inputs), `aggregate()` awaited.
- **`bullforge/tests/test_risk_manager.py`** — rewrite: drop kwarg-based construction, drive via fake RuntimeConfig + overrides, cover all 4 thresholds.
- **`bullforge/tests/test_config.py`** — drop `execution_*` assertions (those fields no longer exist on `Settings`).
- **`bullforge/tests/test_runtime_config.py`** — smoke test that the 7 new keys validate correctly (min/max bounds, type coercion).
- **`bullforge/tests/test_backtests_api.py`** — POST with valid overrides → job row contains them in `config_json`; POST with out-of-range override → 400; POST with blanks → snapshot from runtime_config fills them.
- **`bullforge/tests/test_backtest_worker.py`** (or existing equivalent) — `_build_config` copies all 7 overrides into `BacktestConfig`.
- **`bullforge/tests/test_backtest_engine.py`** — run a fold with `min_confidence=0.9` override vs default 0.6, assert fewer trades (override respected end-to-end).

## Non-goals

- Not adding env-var fallback for the removed fields. Clean cutover.
- Not snapshotting *all* runtime_config into each job (only the 7 threshold keys). Horizons, symbols, etc. continue to track their own pattern.
- Not exposing reconciler tuning (`execution_reconciler_*`) via config page — out of scope; those remain env vars.
- Not adding a config-page UI for presets / profiles. Single flat schema-driven form stays.

## Risks and mitigations

- **Per-signal Redis read cost.** 3 HGETs in aggregate + 4 in risk = 7 per entry decision. Redis is co-located; microseconds. Not a concern.
- **runtime_config drift between job creation and job run.** Mitigated by snapshot-on-creation — the job row is the source of truth for its own run.
- **Breaking change to `SignalAggregator`/`RiskManager` constructor signatures.** Caught by the test rewrites; no external callers outside `execution_service/main.py` and `backtest_service/engine.py` which are both updated in this branch.
- **Existing env vars still set in users' `.env`.** After merge, they're ignored with no error (Python `os.getenv` just returns a value nobody reads). Release note should recommend removing them, but no runtime impact.

## Acceptance

- Config page shows 7 new fields under their labels with descriptions; PUT to any field takes effect on the next trade signal without restart.
- New-backtest modal has a collapsible "Advanced" section; blank fields snapshot from runtime_config at submit; filled fields override.
- Existing backtests keep working (old `backtest_jobs` rows with no override columns in `config_json` don't crash — `_build_config` treats missing keys as `None`, which trips the snapshot-fill in the API path for new jobs and will cause old replays to fall back to runtime_config; acceptable since old jobs already ran).
- All test files above pass.
