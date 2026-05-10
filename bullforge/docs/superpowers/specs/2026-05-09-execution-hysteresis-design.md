# Execution-side direction hysteresis — design

**Status:** approved (auto-mode brainstorm)
**Date:** 2026-05-09
**Author:** Claude (Opus 4.7)
**Source:** triage of "BullForge isn't making crypto trades right now" — 2026-05-09 session. The same triage uncovered two compounding bugs: (1) Alpaca's `get_all_positions()` returns crypto symbols without the slash (`BTCUSD` vs `BTC/USD`), making `PositionTracker` blind to crypto positions; and (2) `execution_direction_band=0.4` was set in Redis runtime config, putting essentially every per-minute aggregated direction into the dead zone. Bugs (1) and (2) are addressed separately (`shared/symbol_utils.normalize_alpaca_symbol` plus a runtime-config tweak). This spec covers the structural follow-up the operator asked for: stop the legacy `SignalAggregator` from flip-flopping its emitted direction every bar.

## Motivation

The legacy `SignalAggregator` recomputes a fresh direction (`-1`, `0`, `+1`) for every prediction message. There is no debouncing — each minute's vote stands on its own. In practice that produced the NVDA churn visible in the orders table on 2026-05-08 14:30–19:36 UTC: alternating `buy 9.21` / `sell 9.21` pairs roughly every 6 minutes, each round-trip paying spread + market-order slippage to act on a one-bar signal flip. The meta path already has `meta_hysteresis_bars=5` for exactly this reason; the legacy path needs the same concept.

The operator's stated goals (verbatim from the session): "place/track more crypto/equity trades" and "don't really want them flipping long-short-long-short frequently." Lowering `execution_direction_band` and `execution_min_signal_strength` (also done in the same session) gets the first goal. Hysteresis is the structural fix for the second.

## Non-goals

- Per-symbol or per-asset-class hysteresis tuning. One global `execution_hysteresis_bars` value applies uniformly. Tunable later if real evidence demands it.
- Asymmetric hysteresis (e.g., faster entries than exits). Not requested. YAGNI.
- Persistence of hysteresis state across `execution_service` restarts. State lives in-memory in the aggregator; restart resets every symbol back to `committed_direction=0`. The cost — possibly one extra flip per symbol immediately after restart — is acceptable; restarts are rare.
- Seeding `committed_direction` from current Alpaca positions at startup. Considered, deferred. Ship without it; if the restart-flap actually bites in operation, add it as a small follow-up.
- Telemetry counters for "suppressed flips this hour". Easy to add later; no current operator ask.
- Touching the meta-shadow path. `meta_hysteresis_bars` already exists and works; this spec leaves it alone.

## Design

### Architecture

A single new piece of per-symbol state inside `SignalAggregator`, debouncing the direction output. No change to `RiskManager`, `OrderExecutor`, schemas, pub/sub channels, or any other service. One new runtime-config key.

```
on_prediction(message)            # services/execution_service/main.py
   └─ aggregator.aggregate(symbol, ts, predictions)
        ├─ existing logic computes raw_direction from weighted_avg + band
        ├─ NEW: _apply_hysteresis(symbol, raw_direction) → committed | None
        └─ if committed is None  → return None  (silent hold)
            else                  → return TradingSignal(direction=committed, ...)
```

`committed = None` reuses the existing "no actionable signal" code path — `OrderExecutor.process_signal` is never called, the position simply holds. This is the same shape as today's `len(confident) < min_agreement` early-return.

### Per-symbol state

```python
@dataclass
class _HysteresisState:
    committed_direction: int = 0         # the value last emitted to caller
    candidate_direction: int | None = None
    candidate_streak: int = 0            # consecutive ticks of candidate
```

State lives on `SignalAggregator` instance: `self._symbol_states: dict[str, _HysteresisState]`. Allocated lazily on first prediction per symbol. Sized in bytes; no eviction needed.

### Hysteresis rule

Per `aggregate` call, after the existing logic computes `raw_direction`:

```python
hysteresis_bars = await self._param("hysteresis_bars")  # 0 = disabled

if hysteresis_bars <= 0:
    committed = raw_direction
else:
    state = self._symbol_states.setdefault(symbol, _HysteresisState())
    if raw_direction == state.committed_direction:
        # raw matches what we're already emitting; clear any pending change
        state.candidate_direction = None
        state.candidate_streak = 0
        committed = state.committed_direction
    else:
        if raw_direction == state.candidate_direction:
            state.candidate_streak += 1
        else:
            state.candidate_direction = raw_direction
            state.candidate_streak = 1
        if state.candidate_streak >= hysteresis_bars:
            state.committed_direction = state.candidate_direction
            state.candidate_direction = None
            state.candidate_streak = 0
            committed = state.committed_direction
        else:
            committed = None  # suppress this tick → silent hold
```

Worked-through behavior (assume `hysteresis_bars=5`, initial `committed_direction=0`):

| Bar | raw | candidate after | streak after | committed | emitted |
|---|---|---|---|---|---|
| 1 | +1 | +1 | 1 | 0 | None |
| 2 | +1 | +1 | 2 | 0 | None |
| 3 | +1 | +1 | 3 | 0 | None |
| 4 | +1 | +1 | 4 | 0 | None |
| 5 | +1 | (cleared) | 0 | +1 | +1 |
| 6 | +1 | None | 0 | +1 | +1 |
| 7 | -1 | -1 | 1 | +1 | None |
| 8 | +1 | (cleared, matches committed) | 0 | +1 | +1 |

The bar-7/bar-8 pair shows the anti-flap behavior: a single-bar opposing dip does not commit; reverting to the committed direction resets the candidate.

### Where the new code lives

- `bullforge/services/execution_service/signal_aggregator.py` — the only file with logic changes.
  - Add `_HysteresisState` dataclass at module level (or `_apply_hysteresis` as a small private method on `SignalAggregator`).
  - Add `self._symbol_states: dict[str, _HysteresisState] = {}` to `__init__`.
  - In `aggregate`, after the existing band/direction computation, run the rule above and gate the return on `committed is not None`.
- `bullforge/shared/runtime_config.py` — add the new schema entry below.

### Config schema entry

New entry in `CONFIG_SCHEMA`:

```python
"execution_hysteresis_bars": {
    "default": 0, "type": "int", "min": 0, "max": 100,
    "label": "Direction Hysteresis (bars)",
    "description": (
        "Number of consecutive bars a new aggregated direction must "
        "persist before becoming the emitted direction. 0 disables "
        "hysteresis (legacy behavior — every bar can flip). Example: "
        "with the value set to 5, a fresh direction must be confirmed "
        "by 5 consecutive ticks before the system acts on it. Suppressed "
        "ticks return no signal, so the existing position simply holds."
    ),
    "reference": "https://en.wikipedia.org/wiki/Hysteresis",
},
```

Default `0` means existing deployments see no behavior change after the code lands; the operator opts in via the dashboard's Config tab when ready.

### Edge cases handled

- **`hysteresis_bars=0`** (default): the state machine is bypassed entirely. `_symbol_states` is never populated. Behavior is bit-identical to today.
- **Aggregator early-returns `None` before reaching the state machine** (empty predictions list, fewer than `min_agreement` confident preds, etc.): the state machine is *not* consulted. The next valid bar continues whatever streak was in progress. This preserves the meaning of "missed bar" vs "actively contradicting bar."
- **Symbol removed mid-session via `config:symbols_changed`**: the orphan state stays in the dict (~tens of bytes per symbol). Pruning on the channel handler is plausible but YAGNI — if it ever matters, a single line in `on_symbols_changed` can clear stale keys.
- **`execution_service` restart**: state resets, every symbol starts at `committed_direction=0`. The first bar after restart on a symbol that should be `+1` emits `committed=0`; if a position exists, `RiskManager.check_exit` returns `True` ("Flat signal") and `_execute_close` fires. To avoid a mass close-on-restart, a future change can seed `committed_direction` from current Alpaca positions in `__init__`. Not in scope here. If observed in operation, add then.

### Strength reporting

When the streak completes and `committed` is set, the emitted `TradingSignal` carries the *current* bar's `weighted_avg` as `strength` (`min(abs(weighted_avg), 1.0)` — the existing formula). This is correct because the commit only happens when the current raw direction equals the candidate and the candidate has persisted: the current raw signal is, by construction, the same direction we're committing to, so its magnitude is a faithful measure of conviction.

The suppressed-tick path returns `None` outright, so there is no misleading "weak signal in a wrong direction" case to handle.

## Tests

All in `bullforge/tests/test_signal_aggregator.py` (the file already exercises the existing aggregator with mock `RuntimeConfig`).

1. `test_hysteresis_disabled_passes_raw_direction` — with `hysteresis_bars=0`, feed any sequence of raw directions; assert each emitted signal matches the raw direction (regression guard for the default).
2. `test_hysteresis_requires_consecutive_same_direction_to_commit` — with `hysteresis_bars=5`, starting from `committed=0`, feed five `+1` bars; assert bars 1–4 return `None` and bar 5 returns a `TradingSignal` with `direction=+1`.
3. `test_hysteresis_resets_streak_on_revert_to_committed` — with `hysteresis_bars=5`, starting from `committed=0`, feed `+1, +1, +1, +1, 0, +1, +1, +1, +1` (nine bars). Assert *no commit* through any of the nine bars: bars 1–4 build streak to 4, bar 5 is `raw==committed` and resets the streak, bars 6–9 only build a new streak to 4. Pair with an explicit assertion that without the bar-5 reset the same prefix would have committed at bar 5 (covered by test 2).
4. `test_hysteresis_resets_streak_on_change_in_candidate` — with `hysteresis_bars=5`, feed `+1, +1, -1, -1, -1` (five bars). Assert no commit: the candidate flipped from `+1` (streak=2) to `-1` at bar 3 (streak resets to 1), then only reaches streak=3 by bar 5.
5. `test_hysteresis_per_symbol_state_isolated` — interleave `BTC/USD` and `ETH/USD` calls with `hysteresis_bars=3`; assert one symbol's streak doesn't influence the other (e.g., 3 BTC bars commit BTC without committing ETH).
6. `test_hysteresis_suppressed_tick_returns_none` — explicitly assert the suppressed path returns `None` (not a `TradingSignal` with `direction=0`). Important because the executor distinguishes "no signal at all" from "flat signal".
7. `test_hysteresis_strength_uses_current_raw_when_committing` — when the streak completes, the emitted signal's `strength` reflects the current bar's `weighted_avg`, not a stale value from the start of the streak.

These can all be written against the existing test fixtures (mock `RuntimeConfig` returning the per-key values) — no new infrastructure needed. Hermetic; runs in milliseconds.

## Rollout

1. Land the code change with default `0` → no behavioral change in production.
2. Operator flips `execution_hysteresis_bars` to `5` (matches the meta-path default) via the dashboard's Config tab. Watch a session of `docker compose logs -f execution_service | grep Executed:` to confirm reduced flip rate without entries drying up.
3. If entries feel sluggish, lower to `3`. If flips persist, raise to `8`.

## Risks

- **Slower entries.** A new conviction now requires `hysteresis_bars` consecutive bars before the first order. With `bars=5` and 1m bars, that's a 5-minute delay on entry. Acceptable trade — the operator explicitly preferred fewer flips over faster entries.
- **Slower exits on truly bad reversals.** Same delay applies in reverse: a real regime change takes `hysteresis_bars` bars before the close fires. Concrete worst-case: with `bars=5` on a long position when an opposing signal appears, you eat 5 bars of adverse move before exiting. For 1m bars and the sizes in current use ($2k–$6k positions), this is a small dollar exposure but worth flagging.
- **Restart-flap.** Covered above; deferred to a follow-up if observed.
- **State corruption from a subtle off-by-one in the rule.** Mitigated by tests 2–4 walking the worked-through behavior table explicitly.
