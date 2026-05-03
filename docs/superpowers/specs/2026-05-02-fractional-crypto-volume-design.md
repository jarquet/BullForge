# Fractional crypto volume — design

**Date:** 2026-05-02
**Owner:** Jarquet
**Status:** Draft

## Summary

Stop truncating fractional crypto bar volumes by widening `Bar.volume` from `int` to `float` and the `bars_1m`/`bars_daily` `volume` column from `BIGINT` to `DOUBLE PRECISION`. The bug entry point is `int(alpaca_bar.volume)` at `bullforge/services/data_pipeline/alpaca_client.py:43`, which rounds a 0.7 BTC minute to 0 BTC and feeds a falsely-zero volume into the backtest fill cap and into volume-weighted features.

Single-field type change with one schema migration. No new abstractions, no behavioural change for equities (whose volumes are integer-valued and cast losslessly).

## Motivation

Crypto bars from Alpaca carry base-asset volume as a fractional `float` (e.g. `v=3.227759152` BTC for a single minute). Equity bars carry integer share counts. The shared `Bar` dataclass currently types `volume: int` and the data-pipeline parser performs `int(alpaca_bar.volume)`, which is harmless for equities but truncates fractional crypto values.

Two known downstream consequences:

1. **Backtest fill cap is too tight.** `services/backtest_service/fill_model.py` rejects fills whose `qty > volume * max_volume_pct`. A 0.7-BTC bar stored as `volume=0` rejects any non-zero fractional fill outright; intermediate values (e.g. `0.85` → `0`) cause spurious rejections that obscure real liquidity-cap behaviour.
2. **Volume features are biased low for crypto.** `feature_engine/calculators/volume.py` already explicitly does `df["volume"].astype(float)`, but the pre-truncation step in `parse_bar` removes information before the feature stage ever sees it.

The pre-existing investigation `docs/research/crypto-bar-volume-units.md` (lines 29 & 64–74) documents this finding and recommends exactly the fix specified here.

## Scope

**In scope:**
- `bullforge/shared/models.py`: `Bar.volume: int` → `Bar.volume: float`.
- `bullforge/services/data_pipeline/alpaca_client.py:43`: `volume=int(alpaca_bar.volume)` → `volume=float(alpaca_bar.volume)`.
- New migration `bullforge/migrations/023_widen_bar_volume_to_double.sql`: two `ALTER TABLE ... ALTER COLUMN ... TYPE DOUBLE PRECISION USING volume::double precision` statements (one for `bars_1m`, one for `bars_daily`).
- TDD tests:
  - `tests/test_alpaca_client.py::test_parse_bar_preserves_fractional_volume` — hermetic, asserts `Bar.volume == 0.7` after parsing a mocked Alpaca bar with `volume=0.7`.
  - `tests/test_db.py::test_insert_and_query_bar_fractional_volume` — integration, asserts a `Bar(volume=0.7)` round-trips through `insert_bar` → `query_bars` as `0.7`.
- Tick the truncation bullet in `TODO.md` (line 51 of master `cdd6549`) with an in-place completion note.

**Out of scope:**
- Changing the fill-model behaviour, the per-asset-class volume cap, or any feature-engine calculation. Those landed earlier; this spec only restores fidelity to their inputs.
- Migrating any `int(...)` casts elsewhere in the test suite (e.g. `tests/test_engine.py:27` uses `int(np.random.randint(...))` in a fixture — semantically still valid for an int-typed integer count, and the dataclass field annotation is non-enforced).
- Refactoring `Bar`, `parse_bar`, the alpaca-py wrapper layer, or the volume calculators beyond what the type change requires.
- `query_latest_close` merge with `query_feature_rows` — explicitly noted as premature in TODO.md.

## Decisions

### Column type: `DOUBLE PRECISION`

Rejected `NUMERIC(20, 8)` (exact decimal, BTC satoshi-precision). Reasons:

- **Consistency.** `open`/`high`/`low`/`close` in the same tables are already `DOUBLE PRECISION`. Introducing exact decimal for `volume` only would be inconsistent with no operational benefit.
- **Downstream math is float anyway.** `feature_engine/calculators/volume.py:11` immediately does `df["volume"].astype(float)` for cumsum, rolling means, OBV, and ratios. NUMERIC's precision would be discarded at the first operation.
- **Range is sufficient.** IEEE-754 double gives ~15–17 significant digits. BTC-USD per-minute volume is ≤ 100 BTC; equity per-minute volume is ≤ ~10M shares. Both are far below the `2^53` ≈ 9×10^15 boundary above which integer-valued doubles lose 1-unit precision.
- **Storage.** 8 bytes vs. 16+ bytes for NUMERIC.

### Cast in `parse_bar`: explicit `float(...)`, not bare assignment

`alpaca_bar.volume` is `int` for equities and `float` for crypto in the alpaca-py SDK. We could drop the cast entirely and let Python pass through, but an explicit `float(...)` pins the runtime type so a future SDK quirk (e.g. `Decimal` or NumPy scalar) cannot surprise downstream consumers.

### TimescaleDB hypertable migration

`ALTER COLUMN ... TYPE DOUBLE PRECISION` on a hypertable rewrites every chunk under an `ACCESS EXCLUSIVE` lock. For our scale (handful of symbols × O(weeks) of minute bars) this is sub-minute. Existing equity rows are integer-valued and the cast is lossless, so `NOT NULL` and the existing PRIMARY KEY are preserved as-is.

The data_pipeline websocket reconnects with backfill on transient errors, so a brief lock window is recoverable. No maintenance window required at current data volume; for future operators with larger row counts, a comment in the SQL file flags that the lock blocks data_pipeline writes for the duration of chunk rewrite.

### Backtest schema

The `backtest` schema does not duplicate `bars_1m`/`bars_daily` — sandbox connections read these tables from `public` via `search_path=backtest,public` (per CLAUDE.md and `shared/db.py::sync_sandbox`). One migration on `public` is sufficient; no `backtest.*` action needed.

## Implementation order (TDD)

1. **Worktree:** `fix/crypto-volume-truncation` via `superpowers:using-git-worktrees`.
2. **Failing tests first.** Write the hermetic `test_parse_bar_preserves_fractional_volume` and the integration `test_insert_and_query_bar_fractional_volume`. Run `bin/test python -m pytest tests/test_alpaca_client.py tests/test_db.py -v -k "fractional_volume"` and confirm both fail (the hermetic on `int(...)` truncation, the DB on `BIGINT` truncation).
3. **Migration `023_widen_bar_volume_to_double.sql`.** With `bin/test_prune` to ensure a fresh DB on the next run, the migration applies as part of `Database.connect()` in the test fixtures.
4. **`Bar.volume: float`** in `shared/models.py`. One-line type-annotation change; no `__post_init__` change (volume isn't validated there).
5. **Drop the `int()` cast** in `alpaca_client.py:43` → `float(alpaca_bar.volume)`.
6. **Verify both new tests pass.** Then `bin/test python -m pytest tests/ -v` — full sweep — to catch regressions in the 11 files that reference volume.
7. **Code review** via `superpowers:requesting-code-review`.
8. **Tick TODO.md, merge with `--no-ff`** per `superpowers:finishing-a-development-branch`. Conventional commit scope `fix(data-pipeline)`. No Co-Authored-By trailer. Push to `origin/master`.

## Risks / watch-outs

- **Frozen-dataclass type annotation is not runtime-enforced.** Pre-existing call sites that pass `int` (e.g. test fixtures using `np.random.randint`) keep working silently. Confirmed via grep that no `isinstance(..., volume)` defensive checks exist.
- **alpaca-py SDK return type for crypto.** Verified by reading `parse_bar` and the existing investigation doc: `alpaca_bar.volume` for crypto is `float`. The fix (`float(alpaca_bar.volume)`) is safe whether the SDK returns `int` or `float`.
- **Equity row migration.** `BIGINT NOT NULL` → `DOUBLE PRECISION USING volume::double precision` casts integer values losslessly. Nothing to backfill.
- **Hypertable lock.** Sub-minute at current scale; documented in the SQL comment for future operators.

## Verification

- `bin/test python -m pytest tests/ -v` — full sweep before claiming done.
- New tests fail before implementation, pass after.
- TODO.md bullet flipped from `- [ ]` to `- [x]` with one-paragraph completion note in-place.
