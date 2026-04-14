# Phase 6D — Trading Window Enforcement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enforce a trading window in `RiskManager.check_entry` that allows crypto to trade 24/7 and restricts equity trading to NYSE hours with a 1-hour buffer after open and before close (10:30–15:00 ET). Buffers are runtime-configurable via `RuntimeConfig`.

**Architecture:** A pure `shared/trading_window.py` module with a classification helper (`is_crypto`) and a window check (`is_trading_allowed`). `RiskManager.check_entry` calls the check as its first gate. Buffer minutes come from `RuntimeConfig` (6A).

**Tech Stack:** Python 3.12, `zoneinfo` for US/Eastern conversion (stdlib), pytest.

---

## File Structure

**Create:**
- `bullforge/shared/symbol_utils.py` — `is_crypto(symbol)` helper
- `bullforge/shared/trading_window.py` — `is_trading_allowed(...)` pure function
- `bullforge/tests/test_trading_window.py`

**Modify:**
- `bullforge/services/execution_service/risk_manager.py` — first check is window gate
- `bullforge/shared/runtime_config.py` — add `equity_window_open_buffer_minutes`, `equity_window_close_buffer_minutes` to `CONFIG_SCHEMA`
- `bullforge/tests/test_risk_manager.py` — integration test for rejection
- `bullforge/tests/test_runtime_config.py` — assert new schema keys

**Depends on:** Phase 6A (RuntimeConfig must exist).

---

## Task 1: `is_crypto` symbol classifier

**Files:**
- Create: `bullforge/shared/symbol_utils.py`
- Test: `bullforge/tests/test_symbol_utils.py`

- [ ] **Step 1: Write failing test**

```python
# bullforge/tests/test_symbol_utils.py
from shared.symbol_utils import is_crypto


def test_crypto_pair():
    assert is_crypto("BTC/USD") is True
    assert is_crypto("ETH/USD") is True


def test_equity_symbol():
    assert is_crypto("SPY") is False
    assert is_crypto("QQQ") is False
    assert is_crypto("AAPL") is False


def test_empty_and_edge():
    assert is_crypto("") is False
    assert is_crypto("X/Y/Z") is True  # Any slash counts
```

- [ ] **Step 2: Run to fail**

Run: `cd bullforge && pytest tests/test_symbol_utils.py -v`
Expected: FAIL (module missing)

- [ ] **Step 3: Implement**

```python
# bullforge/shared/symbol_utils.py
def is_crypto(symbol: str) -> bool:
    """Crypto pairs contain a slash (e.g., BTC/USD). Equities never do."""
    return "/" in symbol
```

- [ ] **Step 4: Run, PASS**

Run: `cd bullforge && pytest tests/test_symbol_utils.py -v`
Expected: 3 pass

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/symbol_utils.py bullforge/tests/test_symbol_utils.py
git commit -m "feat: add is_crypto symbol classifier"
```

---

## Task 2: `is_trading_allowed` window check

**Files:**
- Create: `bullforge/shared/trading_window.py`
- Test: `bullforge/tests/test_trading_window.py`

- [ ] **Step 1: Write failing tests**

```python
# bullforge/tests/test_trading_window.py
from datetime import datetime, timezone

import pytest

from shared.trading_window import is_trading_allowed


# UTC offsets: In April (EDT), ET = UTC-4. So 10:30 ET = 14:30 UTC.
# Use dates in April 2026 (all EDT).

def _dt(year, month, day, hour, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def test_crypto_always_allowed_weekend():
    # Sunday 03:00 UTC
    ok, reason = is_trading_allowed(
        "BTC/USD", _dt(2026, 4, 12, 3, 0),
        open_buffer_min=60, close_buffer_min=60,
    )
    assert ok is True
    assert "crypto" in reason.lower()


def test_crypto_always_allowed_weekday_midnight():
    ok, _ = is_trading_allowed(
        "ETH/USD", _dt(2026, 4, 15, 0, 0),
        open_buffer_min=60, close_buffer_min=60,
    )
    assert ok is True


def test_equity_rejected_on_weekend():
    # Saturday 12:00 ET = 16:00 UTC
    ok, reason = is_trading_allowed(
        "SPY", _dt(2026, 4, 11, 16, 0),
        open_buffer_min=60, close_buffer_min=60,
    )
    assert ok is False
    assert "weekend" in reason.lower()


def test_equity_rejected_before_buffer():
    # Monday 10:00 ET = 14:00 UTC (inside open buffer, since 10:30 ET = 14:30 UTC)
    ok, reason = is_trading_allowed(
        "SPY", _dt(2026, 4, 13, 14, 0),
        open_buffer_min=60, close_buffer_min=60,
    )
    assert ok is False
    assert "outside" in reason.lower()


def test_equity_allowed_midday():
    # Monday 12:00 ET = 16:00 UTC
    ok, _ = is_trading_allowed(
        "SPY", _dt(2026, 4, 13, 16, 0),
        open_buffer_min=60, close_buffer_min=60,
    )
    assert ok is True


def test_equity_rejected_inside_close_buffer():
    # Monday 15:30 ET = 19:30 UTC (inside close buffer; window ends 15:00 ET)
    ok, _ = is_trading_allowed(
        "SPY", _dt(2026, 4, 13, 19, 30),
        open_buffer_min=60, close_buffer_min=60,
    )
    assert ok is False


def test_equity_rejected_after_close():
    # Monday 17:00 ET = 21:00 UTC
    ok, _ = is_trading_allowed(
        "SPY", _dt(2026, 4, 13, 21, 0),
        open_buffer_min=60, close_buffer_min=60,
    )
    assert ok is False


def test_equity_allowed_at_window_open_boundary():
    # Exactly 10:30 ET = 14:30 UTC — allowed (inclusive lower bound)
    ok, _ = is_trading_allowed(
        "SPY", _dt(2026, 4, 13, 14, 30),
        open_buffer_min=60, close_buffer_min=60,
    )
    assert ok is True


def test_equity_allowed_with_zero_buffer():
    # 09:30 ET = 13:30 UTC with 0 buffer -> allowed
    ok, _ = is_trading_allowed(
        "SPY", _dt(2026, 4, 13, 13, 30),
        open_buffer_min=0, close_buffer_min=0,
    )
    assert ok is True


def test_equity_naive_datetime_rejected():
    with pytest.raises(ValueError):
        is_trading_allowed(
            "SPY", datetime(2026, 4, 13, 14, 0),  # no tzinfo
            open_buffer_min=60, close_buffer_min=60,
        )
```

- [ ] **Step 2: Run, fail**

Run: `cd bullforge && pytest tests/test_trading_window.py -v`
Expected: FAIL

- [ ] **Step 3: Implement**

```python
# bullforge/shared/trading_window.py
from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from shared.symbol_utils import is_crypto

EASTERN = ZoneInfo("America/New_York")
NYSE_OPEN = time(9, 30)
NYSE_CLOSE = time(16, 0)


def is_trading_allowed(
    symbol: str,
    now_utc: datetime,
    *,
    open_buffer_min: int,
    close_buffer_min: int,
) -> tuple[bool, str]:
    """
    Return (allowed, reason).

    - Crypto (symbol containing '/'): always allowed, 24/7.
    - Equity: allowed only on NYSE weekdays, within
      [09:30 ET + open_buffer_min, 16:00 ET - close_buffer_min).
      Holidays are NOT checked here (broker will reject on holidays).
    """
    if now_utc.tzinfo is None:
        raise ValueError(f"now_utc must be timezone-aware UTC, got {now_utc}")

    if is_crypto(symbol):
        return True, "crypto 24/7"

    et = now_utc.astimezone(EASTERN)
    # Monday=0 ... Sunday=6
    if et.weekday() >= 5:
        return False, f"weekend (ET={et.isoformat()})"

    open_dt = et.replace(
        hour=NYSE_OPEN.hour, minute=NYSE_OPEN.minute, second=0, microsecond=0,
    ) + timedelta(minutes=open_buffer_min)
    close_dt = et.replace(
        hour=NYSE_CLOSE.hour, minute=NYSE_CLOSE.minute, second=0, microsecond=0,
    ) - timedelta(minutes=close_buffer_min)

    if open_dt <= et < close_dt:
        return True, f"within window ({open_dt.time()}–{close_dt.time()} ET)"
    return False, (
        f"outside window (now={et.time()} ET, window={open_dt.time()}–{close_dt.time()} ET)"
    )
```

- [ ] **Step 4: Run, PASS**

Run: `cd bullforge && pytest tests/test_trading_window.py -v`
Expected: 10 pass

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/trading_window.py bullforge/tests/test_trading_window.py
git commit -m "feat: trading window check (crypto 24/7, equity 1hr buffer default)"
```

---

## Task 3: Add buffer keys to CONFIG_SCHEMA

**Files:**
- Modify: `bullforge/shared/runtime_config.py`
- Modify: `bullforge/tests/test_runtime_config.py`

- [ ] **Step 1: Add schema test**

Append to `bullforge/tests/test_runtime_config.py`:

```python
def test_schema_has_trading_window_buffers():
    from shared.runtime_config import CONFIG_SCHEMA
    assert "equity_window_open_buffer_minutes" in CONFIG_SCHEMA
    assert "equity_window_close_buffer_minutes" in CONFIG_SCHEMA
    assert CONFIG_SCHEMA["equity_window_open_buffer_minutes"]["default"] == 60
    assert CONFIG_SCHEMA["equity_window_close_buffer_minutes"]["default"] == 60
```

- [ ] **Step 2: Run, fail**

Run: `cd bullforge && pytest tests/test_runtime_config.py::test_schema_has_trading_window_buffers -v`
Expected: FAIL

- [ ] **Step 3: Extend `CONFIG_SCHEMA` in `shared/runtime_config.py`**

Add these entries to the `CONFIG_SCHEMA` dict:

```python
    "equity_window_open_buffer_minutes": {
        "default": 60, "type": "int", "min": 0, "max": 240,
        "label": "Equity Open Buffer (minutes)",
        "description": "Minutes after NYSE open (9:30 ET) before equity trading begins. 60 = trade from 10:30 ET onward.",
        "reference": "https://en.wikipedia.org/wiki/Opening_Bell",
    },
    "equity_window_close_buffer_minutes": {
        "default": 60, "type": "int", "min": 0, "max": 240,
        "label": "Equity Close Buffer (minutes)",
        "description": "Minutes before NYSE close (16:00 ET) to stop equity trading. 60 = stop at 15:00 ET.",
        "reference": "https://en.wikipedia.org/wiki/Closing_Bell",
    },
```

- [ ] **Step 4: Run, PASS**

Run: `cd bullforge && pytest tests/test_runtime_config.py -v`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/runtime_config.py bullforge/tests/test_runtime_config.py
git commit -m "feat: add equity window buffer keys to CONFIG_SCHEMA"
```

---

## Task 4: `RiskManager.check_entry` gates on trading window

**Files:**
- Modify: `bullforge/services/execution_service/risk_manager.py`
- Modify: `bullforge/tests/test_risk_manager.py`

- [ ] **Step 1: Read current RiskManager signature**

```bash
cd bullforge && grep -n "class RiskManager\|def check_entry\|def __init__" services/execution_service/risk_manager.py
```

Note the current `__init__` signature and `check_entry` return shape; they already return `(bool, str, int)`.

- [ ] **Step 2: Write failing integration test**

Append to `bullforge/tests/test_risk_manager.py`:

```python
from datetime import datetime, timezone
from unittest.mock import AsyncMock
import pytest

from shared.models import TradingSignal
from services.execution_service.risk_manager import RiskManager


@pytest.fixture
def runtime_config():
    rc = AsyncMock()
    async def get(key):
        return {
            "equity_window_open_buffer_minutes": 60,
            "equity_window_close_buffer_minutes": 60,
        }.get(key, 0)
    rc.get = get
    return rc


def _signal(symbol, hour_utc):
    return TradingSignal(
        symbol=symbol,
        timestamp=datetime(2026, 4, 13, hour_utc, 0, tzinfo=timezone.utc),
        direction=1, strength=0.8, contributing_predictions=3,
    )


@pytest.mark.asyncio
async def test_risk_manager_rejects_equity_outside_window(runtime_config):
    rm = RiskManager(
        # ... fill with your existing constructor args — min position, max positions, etc.
        runtime_config=runtime_config,
    )
    # 09:00 ET = 13:00 UTC — outside the 10:30 buffer
    ok, reason, qty = await rm.check_entry(_signal("SPY", 13), current_price=420.0)
    assert ok is False
    assert "outside" in reason.lower() or "window" in reason.lower()


@pytest.mark.asyncio
async def test_risk_manager_allows_crypto_any_time(runtime_config):
    rm = RiskManager(runtime_config=runtime_config)  # fill with other args as needed
    # 03:00 UTC Sunday
    sig = TradingSignal(
        symbol="BTC/USD",
        timestamp=datetime(2026, 4, 12, 3, 0, tzinfo=timezone.utc),
        direction=1, strength=0.8, contributing_predictions=3,
    )
    ok, reason, qty = await rm.check_entry(sig, current_price=67000.0)
    # Shouldn't reject for window reasons (may still reject for other reasons
    # like insufficient equity — that's fine, test only asserts the reason isn't window)
    if not ok:
        assert "window" not in reason.lower()
        assert "weekend" not in reason.lower()
```

Adjust the `RiskManager(...)` instantiation to match the existing constructor. If existing tests already build it with specific mocks, mirror that fixture.

- [ ] **Step 3: Run, fail**

Run: `cd bullforge && pytest tests/test_risk_manager.py -v -k window`
Expected: FAIL (no runtime_config parameter)

- [ ] **Step 4: Modify RiskManager**

In `bullforge/services/execution_service/risk_manager.py`:

1. Accept `runtime_config: RuntimeConfig` in `__init__` (keep it optional for backwards compatibility with existing tests — default `None`).
2. At the top of `check_entry`, call `is_trading_allowed` if `runtime_config` is not None:

```python
from shared.runtime_config import RuntimeConfig
from shared.trading_window import is_trading_allowed

class RiskManager:
    def __init__(self, *existing_args, runtime_config: Optional[RuntimeConfig] = None, **existing_kwargs):
        ...
        self._rc = runtime_config

    async def check_entry(self, signal: TradingSignal, current_price: float) -> tuple[bool, str, int]:
        # Window gate (first)
        if self._rc is not None:
            open_buf = int(await self._rc.get("equity_window_open_buffer_minutes"))
            close_buf = int(await self._rc.get("equity_window_close_buffer_minutes"))
            allowed, reason = is_trading_allowed(
                signal.symbol, signal.timestamp,
                open_buffer_min=open_buf, close_buffer_min=close_buf,
            )
            if not allowed:
                return False, f"trading window: {reason}", 0
        # ... existing body unchanged
```

**Important:** Existing `check_entry` may be `def check_entry(...)` (sync). If so, convert to `async def` and update every caller (grep `check_entry(` under `services/execution_service/` and tests). The `order_executor.py` call site (line ~referenced in test_order_executor.py `check_entry.assert_called_once`) must switch to `await self._risk_manager.check_entry(...)`.

- [ ] **Step 5: Update all call sites**

Run: `grep -rn "check_entry(" bullforge/services bullforge/tests`. For each, if the function is now async, ensure:

- Callers use `await risk_manager.check_entry(...)`
- Mocks in tests use `AsyncMock` (already done in `test_order_executor.py` — `MagicMock(return_value=...)` must change to `AsyncMock(return_value=...)`).

Update `bullforge/tests/test_order_executor.py` fixture `mock_risk`:

```python
@pytest.fixture
def mock_risk():
    risk = MagicMock()
    risk.check_entry = AsyncMock(return_value=(True, "Approved", 10))
    risk.check_exit = MagicMock(return_value=(False, "Signal matches position"))
    return risk
```

(If `check_exit` is also converted, update that too.)

- [ ] **Step 6: Run full test suite**

Run: `cd bullforge && pytest tests/ -v -x`
Expected: all pass. Fix any call-site that still uses the sync signature.

- [ ] **Step 7: Wire `RuntimeConfig` in execution_service/main.py**

In `bullforge/services/execution_service/main.py`, after `redis_client.connect()`:

```python
from shared.runtime_config import RuntimeConfig
runtime_config = RuntimeConfig(redis_client)

risk_manager = RiskManager(
    # ... existing args
    runtime_config=runtime_config,
)
```

- [ ] **Step 8: Commit**

```bash
git add bullforge/services/execution_service/risk_manager.py bullforge/services/execution_service/main.py bullforge/tests/test_risk_manager.py bullforge/tests/test_order_executor.py
git commit -m "feat: RiskManager gates entries on trading window (crypto 24/7, equity 1h buffer)"
```

---

## Final Self-Review Checklist

- [ ] `is_crypto("BTC/USD")` True, `is_crypto("SPY")` False
- [ ] Crypto passes window check at any time including weekends
- [ ] Equity rejected on weekends
- [ ] Equity rejected inside the open buffer (before 10:30 ET with default 60m buffer)
- [ ] Equity rejected inside the close buffer (after 15:00 ET with default 60m buffer)
- [ ] Equity allowed at 10:30 ET exactly (inclusive lower bound)
- [ ] Buffer values are live-tunable via `RuntimeConfig` (setting to 0 restores full NYSE hours)
- [ ] DST is handled correctly via `zoneinfo`
- [ ] `RiskManager` wires `runtime_config` from `execution_service/main.py`
