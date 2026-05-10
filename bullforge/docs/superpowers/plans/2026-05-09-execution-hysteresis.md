# Execution-side Direction Hysteresis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add count-based direction debouncing to `SignalAggregator` so a new aggregated direction must persist for `execution_hysteresis_bars` consecutive bars before being emitted. Suppressed ticks return `None` (silent hold) — the existing "no actionable signal" code path.

**Architecture:** Per-symbol in-memory state (`_HysteresisState` dataclass: `committed_direction`, `candidate_direction`, `candidate_streak`) on the `SignalAggregator` instance. New runtime-config key `execution_hysteresis_bars` (int, default 0 = disabled, opt-in). All gating happens inside the existing `aggregate()` method after the current band/direction computation. No schema change, no Redis change, no touch to `RiskManager` / `OrderExecutor` / pub-sub channels.

**Tech Stack:** asyncio, pytest-asyncio, `unittest.mock.AsyncMock`. The test file `tests/test_signal_aggregator.py` already exercises the aggregator with a fake `RuntimeConfig` — extend that fixture and add new tests there.

**Source spec:** `bullforge/docs/superpowers/specs/2026-05-09-execution-hysteresis-design.md`

**Baseline (master @ `5a7888e`):** symbol-normalization fix already landed in `564dc05`. `bin/test python -m pytest tests/ -k "execution or risk or order_executor or signal_aggregator or broker_relay or polling_sweeper or trade_updates or reconciler"` reports `132 passed`.

**Out of scope:** Per-symbol or per-asset-class tuning, asymmetric entry/exit hysteresis, Redis persistence of state, seeding `committed_direction` from current Alpaca positions on startup, telemetry counters, any change to `meta_hysteresis_bars` (separate path).

---

## Task 1: Add `execution_hysteresis_bars` runtime-config key

Foundation — pure schema addition, no behavior change. With `default=0` the aggregator (after Task 3) bypasses the state machine entirely, so this commit is safe to land before the implementation.

**Files:**
- Modify: `bullforge/shared/runtime_config.py` — append entry to `CONFIG_SCHEMA`
- Modify: `bullforge/tests/test_runtime_config.py` — add `execution_hysteresis_bars` to `EXECUTION_KEYS` + a bounds assertion

- [ ] **Step 1: Add the schema entry**

In `bullforge/shared/runtime_config.py`, find the existing `"execution_max_exposure"` entry (around line 150). Immediately after the closing `},` of that entry, before the `"label_horizons_1m"` entry, insert:

```python
    "execution_hysteresis_bars": {
        "default": 0, "type": "int", "min": 0, "max": 100,
        "label": "Direction Hysteresis (bars)",
        "description": (
            "Number of consecutive bars a new aggregated direction must "
            "persist before becoming the emitted direction. 0 disables "
            "hysteresis (legacy behavior — every bar can flip). With the "
            "value set to 5, a fresh direction must be confirmed by 5 "
            "consecutive ticks before the system acts on it. Suppressed "
            "ticks return no signal, so the existing position simply holds."
        ),
        "reference": "https://en.wikipedia.org/wiki/Hysteresis",
    },
```

- [ ] **Step 2: Extend the runtime-config tests**

In `bullforge/tests/test_runtime_config.py`, find the `EXECUTION_KEYS` tuple (line 114). Append `"execution_hysteresis_bars"`:

```python
EXECUTION_KEYS = (
    "execution_min_confidence",
    "execution_min_agreement",
    "execution_direction_band",
    "execution_min_signal_strength",
    "execution_max_position_pct",
    "execution_max_positions",
    "execution_max_exposure",
    "execution_hysteresis_bars",
)
```

In the same file, find `test_execution_key_defaults_and_bounds` (line 134). Add the new bounds assertion at the end of the function (after the `execution_max_exposure` assertion):

```python
    assert CONFIG_SCHEMA["execution_hysteresis_bars"]["default"] == 0
    assert CONFIG_SCHEMA["execution_hysteresis_bars"]["min"] == 0
    assert CONFIG_SCHEMA["execution_hysteresis_bars"]["max"] == 100
```

- [ ] **Step 3: Run the runtime-config tests**

Run from `bullforge/`:
```bash
bin/test python -m pytest tests/test_runtime_config.py -v
```

Expected: all tests pass, including `test_execution_keys_present_in_schema` and `test_execution_key_defaults_and_bounds` which now also exercise the new key.

- [ ] **Step 4: Commit**

```bash
git add shared/runtime_config.py tests/test_runtime_config.py
git commit -m "$(cat <<'EOF'
feat(execution): add execution_hysteresis_bars runtime-config key

Schema-only addition. Default 0 = disabled, so this is a no-op until
the SignalAggregator is wired up to consult it in a follow-up commit.
Mirrors the existing meta_hysteresis_bars pattern.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: TDD red — extend test fixture and add 7 failing hysteresis tests

Add the new tests *first*, watch them fail, then implement in Task 3. This locks the contract before writing any state-machine code.

**Files:**
- Modify: `bullforge/tests/test_signal_aggregator.py` — extend `_rc()` defaults dict; append 7 new test functions

- [ ] **Step 1: Extend the `_rc()` defaults to include hysteresis_bars**

In `bullforge/tests/test_signal_aggregator.py`, find the `_rc()` helper (line 9). Update the `defaults` dict so existing tests keep getting the legacy behavior (hysteresis disabled) by default:

```python
def _rc(values: dict | None = None):
    """Build a fake RuntimeConfig whose .get returns `values[key]` or a default."""
    defaults = {
        "execution_min_confidence": 0.6,
        "execution_min_agreement": 2,
        "execution_direction_band": 0.3,
        "execution_hysteresis_bars": 0,
    }
    data = {**defaults, **(values or {})}
    rc = AsyncMock()

    async def get(key):
        return data[key]

    rc.get = get
    return rc
```

- [ ] **Step 2: Append the 7 new hysteresis tests at the end of the file**

Append to `bullforge/tests/test_signal_aggregator.py`:

```python
# ----- Hysteresis tests (execution_hysteresis_bars) -----

def _strong_buy_preds():
    """Predictions whose weighted avg comfortably clears band=0.3 → raw=+1."""
    return [
        {"direction": 1, "confidence": 0.80},
        {"direction": 1, "confidence": 0.75},
    ]


def _strong_sell_preds():
    """Predictions whose weighted avg comfortably crosses -band → raw=-1."""
    return [
        {"direction": -1, "confidence": 0.80},
        {"direction": -1, "confidence": 0.75},
    ]


def _flat_preds():
    """Mixed predictions producing weighted avg ~0 → raw=0."""
    return [
        {"direction": 1, "confidence": 0.75},
        {"direction": -1, "confidence": 0.75},
    ]


@pytest.mark.asyncio
async def test_hysteresis_disabled_passes_raw_direction():
    """With hysteresis_bars=0, every raw direction passes through untouched.

    Regression guard for the default — existing deployments must see no
    behavior change after this feature lands.
    """
    agg = SignalAggregator(runtime_config=_rc({"execution_hysteresis_bars": 0}))
    s1 = await agg.aggregate("SPY", _ts(), _strong_buy_preds())
    assert s1 is not None and s1.direction == 1
    s2 = await agg.aggregate("SPY", _ts(), _strong_sell_preds())
    assert s2 is not None and s2.direction == -1
    s3 = await agg.aggregate("SPY", _ts(), _flat_preds())
    assert s3 is not None and s3.direction == 0


@pytest.mark.asyncio
async def test_hysteresis_requires_consecutive_same_direction_to_commit():
    """With hysteresis_bars=5, starting from committed=0, a +1 raw must
    persist for 5 consecutive bars before the aggregator emits +1.

    Bars 1-4 return None (silent hold); bar 5 commits and returns +1.
    """
    agg = SignalAggregator(runtime_config=_rc({"execution_hysteresis_bars": 5}))
    for i in range(1, 5):
        result = await agg.aggregate("SPY", _ts(), _strong_buy_preds())
        assert result is None, f"bar {i} should be suppressed (streak < 5)"
    final = await agg.aggregate("SPY", _ts(), _strong_buy_preds())
    assert final is not None
    assert final.direction == 1


@pytest.mark.asyncio
async def test_hysteresis_resets_streak_on_revert_to_committed():
    """A bar that matches the committed direction resets the candidate streak.

    Sequence: +1, +1, +1, +1, 0, +1, +1, +1, +1 (9 bars). With committed=0
    initially: bars 1-4 build streak to 4 (none commit); bar 5 is raw=0
    which equals committed=0, so the candidate is cleared and streak
    resets; bars 6-9 only build a fresh streak to 4. With bars=5, no
    commit ever happens. Demonstrates the reset path — without bar 5's
    reset, bar 5's input would have committed +1.
    """
    agg = SignalAggregator(runtime_config=_rc({"execution_hysteresis_bars": 5}))
    sequence = [
        _strong_buy_preds(), _strong_buy_preds(),
        _strong_buy_preds(), _strong_buy_preds(),  # streak 1..4, all None
        _flat_preds(),                              # raw=0 matches committed=0 → reset
        _strong_buy_preds(), _strong_buy_preds(),
        _strong_buy_preds(), _strong_buy_preds(),  # new streak 1..4, still None
    ]
    results = []
    for preds in sequence:
        results.append(await agg.aggregate("SPY", _ts(), preds))
    # Bar 5 emits committed=0 (a real signal, direction=0); the rest are None.
    assert results[4] is not None and results[4].direction == 0
    for i, r in enumerate(results):
        if i == 4:
            continue
        assert r is None, f"bar {i + 1} should have been suppressed"


@pytest.mark.asyncio
async def test_hysteresis_resets_streak_on_change_in_candidate():
    """A change in candidate direction restarts the streak from 1.

    With hysteresis_bars=5 and committed=0, feed +1, +1, -1, -1, -1.
    Bars 1-2 build candidate=+1 streak=2. Bar 3 has raw=-1 which
    doesn't match committed (0) and doesn't match candidate (+1), so
    candidate becomes -1, streak=1. Bars 4-5 reach streak=3. Five bars
    in, no commit yet (candidate=-1 needs 5 consecutive to commit).
    """
    agg = SignalAggregator(runtime_config=_rc({"execution_hysteresis_bars": 5}))
    sequence = [
        _strong_buy_preds(), _strong_buy_preds(),
        _strong_sell_preds(), _strong_sell_preds(), _strong_sell_preds(),
    ]
    for i, preds in enumerate(sequence, start=1):
        result = await agg.aggregate("SPY", _ts(), preds)
        assert result is None, f"bar {i} should be suppressed"


@pytest.mark.asyncio
async def test_hysteresis_per_symbol_state_isolated():
    """Hysteresis state is keyed per symbol; one symbol's streak does not
    influence another's.

    With hysteresis_bars=3, feed BTC and ETH alternating buys. After 3
    BTC calls, BTC commits +1. ETH, having only seen 2 calls, is still
    suppressed. After ETH's 3rd call, ETH commits.
    """
    agg = SignalAggregator(runtime_config=_rc({"execution_hysteresis_bars": 3}))

    btc1 = await agg.aggregate("BTC/USD", _ts(), _strong_buy_preds())
    eth1 = await agg.aggregate("ETH/USD", _ts(), _strong_buy_preds())
    btc2 = await agg.aggregate("BTC/USD", _ts(), _strong_buy_preds())
    eth2 = await agg.aggregate("ETH/USD", _ts(), _strong_buy_preds())
    btc3 = await agg.aggregate("BTC/USD", _ts(), _strong_buy_preds())
    # BTC has now hit streak=3; should commit on this call.
    assert btc1 is None and btc2 is None
    assert btc3 is not None and btc3.direction == 1

    # ETH has only seen 2 of its own calls; still suppressed.
    assert eth1 is None and eth2 is None
    eth3 = await agg.aggregate("ETH/USD", _ts(), _strong_buy_preds())
    assert eth3 is not None and eth3.direction == 1


@pytest.mark.asyncio
async def test_hysteresis_suppressed_tick_returns_none_not_flat_signal():
    """During the streak buildup the aggregator returns None, NOT a
    TradingSignal with direction=0.

    OrderExecutor distinguishes 'no signal at all' (silent return,
    nothing happens) from 'flat signal' (direction=0, which triggers
    check_exit -> 'Flat signal' -> close on a held position). Hysteresis
    suppression must use the former.
    """
    agg = SignalAggregator(runtime_config=_rc({"execution_hysteresis_bars": 3}))
    result = await agg.aggregate("SPY", _ts(), _strong_buy_preds())
    assert result is None  # not TradingSignal(direction=0)


@pytest.mark.asyncio
async def test_hysteresis_strength_uses_current_raw_when_committing():
    """When the streak completes, the emitted signal's strength reflects
    the current bar's weighted avg, not a stale value from earlier in
    the streak.

    Build streak=2 with high-conviction inputs (avg ~0.422), then
    commit on bar 3 with weaker inputs (avg ~0.333). Both produce
    raw_direction=+1 (above band=0.3), so the streak progresses. The
    emitted strength on bar 3 must equal bar 3's avg ~0.333, NOT
    bar 1's ~0.422 — the strength must not be cached from when the
    candidate was first seen.

    Mixed direction votes are required because uniform-direction
    predictions always produce |avg|=1.0 regardless of confidences,
    making this test indistinguishable.
    """
    agg = SignalAggregator(runtime_config=_rc({"execution_hysteresis_bars": 3}))
    high_conf = [
        {"direction": 1, "confidence": 0.80},
        {"direction": 1, "confidence": 0.80},
        {"direction": -1, "confidence": 0.65},
    ]
    low_conf = [
        {"direction": 1, "confidence": 0.65},
        {"direction": 1, "confidence": 0.65},
        {"direction": -1, "confidence": 0.65},
    ]
    # Sanity: high_conf avg = (0.80+0.80-0.65)/(0.80+0.80+0.65) ≈ 0.422
    #        low_conf avg  = (0.65+0.65-0.65)/(0.65+0.65+0.65) ≈ 0.333
    assert await agg.aggregate("SPY", _ts(), high_conf) is None  # streak=1
    assert await agg.aggregate("SPY", _ts(), high_conf) is None  # streak=2
    final = await agg.aggregate("SPY", _ts(), low_conf)          # streak=3, commit
    assert final is not None
    assert final.direction == 1
    assert final.strength == pytest.approx(0.65 / 1.95, abs=1e-6)
    # Sanity-guard: strength is the low_conf value, NOT the high_conf one.
    assert final.strength != pytest.approx(0.95 / 2.25, abs=1e-6)
```

- [ ] **Step 3: Run the new tests; verify they FAIL on master**

Run from `bullforge/`:
```bash
bin/test python -m pytest tests/test_signal_aggregator.py -k "hysteresis" -v
```

Expected: all 7 new `test_hysteresis_*` tests FAIL.
- `test_hysteresis_disabled_passes_raw_direction` may pass (the default-0 path doesn't yet exist, but the existing aggregator emits raw direction by default — verify: it should pass because the existing code already emits raw direction with no hysteresis). If it passes, that's fine — it's a regression guard for the future.
- All other hysteresis tests fail with assertions like `assert result is None` failing because the unmodified aggregator returns a `TradingSignal` on every call (no suppression).

- [ ] **Step 4: Run the existing aggregator tests to confirm no incidental breakage**

```bash
bin/test python -m pytest tests/test_signal_aggregator.py -v
```

Expected: the 14 pre-existing tests still pass; the 7 new hysteresis tests fail (per Step 3).

- [ ] **Step 5: Do not commit yet**

The red phase is complete but uncommitted. Task 3 implements the green phase and the two land together as a single TDD commit.

---

## Task 3: TDD green — implement `_HysteresisState` + `_apply_hysteresis` and integrate into `aggregate`

**Files:**
- Modify: `bullforge/services/execution_service/signal_aggregator.py`

- [ ] **Step 1: Replace the entire file with the hysteresis-aware implementation**

Overwrite `bullforge/services/execution_service/signal_aggregator.py` with:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Set

from shared.models import TradingSignal
from shared.runtime_config import RuntimeConfig


@dataclass
class _HysteresisState:
    """Per-symbol debounce state for SignalAggregator.

    Lives in-memory on the SignalAggregator instance. Resets to defaults
    on execution_service restart — see the design doc's 'restart-flap'
    risk note. Allocated lazily on first prediction message per symbol.
    """
    committed_direction: int = 0
    candidate_direction: Optional[int] = None
    candidate_streak: int = 0


class SignalAggregator:
    def __init__(
        self,
        *,
        runtime_config: RuntimeConfig,
        overrides: Optional[dict] = None,
    ):
        self._rc = runtime_config
        self._overrides = overrides or {}
        self._disabled_model_ids: Set[int] = set()
        self._symbol_states: dict[str, _HysteresisState] = {}

    def update_disabled_models(self, disabled_ids: Set[int]) -> None:
        self._disabled_model_ids = disabled_ids

    async def _param(self, key: str):
        v = self._overrides.get(key)
        if v is not None:
            return v
        return await self._rc.get(f"execution_{key}")

    async def _apply_hysteresis(
        self, symbol: str, raw_direction: int,
    ) -> Optional[int]:
        """Debounce raw direction through per-symbol streak state.

        Returns the direction to emit, or None to suppress this tick.
        With hysteresis_bars <= 0, returns raw_direction unchanged and
        does not allocate or touch any per-symbol state.
        """
        hysteresis_bars = await self._param("hysteresis_bars")
        if hysteresis_bars <= 0:
            return raw_direction

        state = self._symbol_states.setdefault(symbol, _HysteresisState())

        if raw_direction == state.committed_direction:
            # Raw matches what we're already emitting — clear any pending
            # change and emit committed. This is the reset path that
            # makes a single contradictory bar followed by a return to
            # the committed direction collapse to a no-op.
            state.candidate_direction = None
            state.candidate_streak = 0
            return state.committed_direction

        if raw_direction == state.candidate_direction:
            state.candidate_streak += 1
        else:
            state.candidate_direction = raw_direction
            state.candidate_streak = 1

        if state.candidate_streak >= hysteresis_bars:
            state.committed_direction = state.candidate_direction
            state.candidate_direction = None
            state.candidate_streak = 0
            return state.committed_direction

        return None  # streak still building — silent hold

    async def aggregate(
        self,
        symbol: str,
        timestamp: datetime,
        predictions: list[dict],
    ) -> Optional[TradingSignal]:
        if not predictions:
            return None

        active_preds = [
            p for p in predictions
            if p.get("model_id") not in self._disabled_model_ids
        ]

        min_confidence = await self._param("min_confidence")
        confident = [
            p for p in active_preds if p["confidence"] >= min_confidence
        ]
        min_agreement = await self._param("min_agreement")
        if len(confident) < min_agreement:
            return None

        weighted_sum = sum(
            p["direction"] * p["confidence"] for p in confident
        )
        total_weight = sum(p["confidence"] for p in confident)
        avg = weighted_sum / total_weight

        band = await self._param("direction_band")
        if avg > band:
            raw_direction = 1
        elif avg < -band:
            raw_direction = -1
        else:
            raw_direction = 0

        committed = await self._apply_hysteresis(symbol, raw_direction)
        if committed is None:
            return None
        direction = committed

        strength = min(abs(avg), 1.0)

        return TradingSignal(
            symbol=symbol,
            timestamp=timestamp,
            direction=direction,
            strength=strength,
            contributing_predictions=len(confident),
        )
```

- [ ] **Step 2: Run the hysteresis tests; verify all 7 PASS**

Run from `bullforge/`:
```bash
bin/test python -m pytest tests/test_signal_aggregator.py -k "hysteresis" -v
```

Expected: all 7 `test_hysteresis_*` tests pass.

- [ ] **Step 3: Run the full signal_aggregator suite; verify no regressions**

```bash
bin/test python -m pytest tests/test_signal_aggregator.py -v
```

Expected: 14 pre-existing + 7 new = 21 tests pass.

- [ ] **Step 4: Commit the red+green TDD pair as one commit**

```bash
git add services/execution_service/signal_aggregator.py tests/test_signal_aggregator.py
git commit -m "$(cat <<'EOF'
feat(execution): add count-based direction hysteresis to SignalAggregator

Per-symbol in-memory state debounces the emitted direction so a new
direction must persist for execution_hysteresis_bars consecutive bars
before the aggregator commits to it. Suppressed ticks return None
(silent hold), reusing the executor's existing 'no actionable signal'
code path. Default 0 = disabled = legacy behavior.

Mirrors the meta-shadow path's meta_hysteresis_bars=5 concept;
addresses NVDA buy/sell churn observed on 2026-05-08.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Final regression sweep across the execution surface

Sanity-check that no downstream consumer of `SignalAggregator` cares about the new state machine or the new config key.

**Files:** none modified.

- [ ] **Step 1: Run the broader execution-side test suite**

Run from `bullforge/`:
```bash
bin/test python -m pytest tests/ -k "execution or risk or order_executor or signal_aggregator or broker_relay or polling_sweeper or trade_updates or reconciler or runtime_config" -v
```

Expected: all tests pass. Baseline before this plan was 132 passed in this filter; the new tests add 8 (1 to runtime_config + 7 to signal_aggregator), so expect 140 passed.

- [ ] **Step 2: Run the full test suite as a final smoke check**

```bash
bin/test python -m pytest tests/ -v 2>&1 | tail -20
```

Expected: same passed-count as baseline + 8 new. Watch the tail for any unexpected failures, particularly in tests that mock `SignalAggregator` or pass it to other components.

- [ ] **Step 3: No commit — this task is verification only**

If everything passes, the implementation is complete. Hand off to the operator with these next steps (paste into the session summary, do NOT execute):

```bash
# Deploy: pull and rebuild only the execution service.
git pull
docker compose up -d --build execution_service

# Turn it on (start with 5 to match the meta-path default; tune after observing).
docker compose exec redis redis-cli HSET bullforge:config execution_hysteresis_bars 5

# Verify the config landed.
docker compose exec redis redis-cli HGET bullforge:config execution_hysteresis_bars

# Watch the trade rate over the next session.
docker compose logs -f execution_service | grep "Executed:"
```
