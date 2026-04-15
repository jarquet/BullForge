# Full-History Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fixed-window backfill with two-phase convergence — a blocking forward gap-fill on startup/symbol-add, plus a background reach-back to a configured floor date — so every (symbol, timeframe) pair eventually holds all available Alpaca history.

**Architecture:** Refactor `Backfiller` around two operations: `forward_fill` (blocking, fast) and `reach_back` (background, slow, idempotent via upsert). `main.py` runs `backfill_all_forward` before streams start, then schedules `backfill_all_reach_back` as a tracked background task. The redis `config:symbols_changed` add path uses the same pattern. Per-symbol failures are logged and never crash the loop.

**Tech Stack:** Python 3.11 asyncio, asyncpg (TimescaleDB), alpaca-py SDK, redis pub/sub, pytest + pytest-asyncio.

**Reference spec:** `docs/superpowers/specs/2026-04-14-full-history-backfill-design.md`

---

## File Structure

| File | Action | Responsibility |
| --- | --- | --- |
| `bullforge/shared/config.py` | Modify | Add `backfill_floor_equity` and `backfill_floor_crypto` `date` settings. |
| `bullforge/shared/db.py` | Modify | Add `get_bar_range(symbol, timeframe)` returning `(earliest, latest)`. |
| `bullforge/services/data_pipeline/backfill.py` | Rewrite | Two-phase API: `forward_fill`, `reach_back`, `backfill_all_forward`, `backfill_all_reach_back`, plus `backfill_symbol`. Drops `backfill_timeframe` / `backfill_all`. |
| `bullforge/services/data_pipeline/main.py` | Modify | Call `backfill_all_forward` blockingly; schedule `backfill_all_reach_back` after streams up; track background tasks for shutdown; per-symbol reach-back from add events. |
| `bullforge/tests/test_backfill.py` | Rewrite | Cover the new two-phase API and failure handling. |
| `bullforge/tests/test_db.py` | Modify | Add `get_bar_range` test coverage. |
| `bullforge/tests/test_config.py` | Modify | Assert defaults for the two new floor settings. |
| `bullforge/tests/test_symbol_hot_reload.py` | Modify | Update existing add-event coverage to reflect forward-then-background reach-back. |

---

## Task 1: Add backfill floor settings

**Files:**
- Modify: `bullforge/shared/config.py` (add fields after `trading_symbols_crypto`)
- Test: `bullforge/tests/test_config.py`

- [ ] **Step 1: Write the failing tests**

Append to `bullforge/tests/test_config.py`:

```python
from datetime import date


def test_backfill_floor_equity_default(monkeypatch):
    monkeypatch.delenv("BACKFILL_FLOOR_EQUITY", raising=False)
    s = Settings()
    assert s.backfill_floor_equity == date(2016, 1, 1)


def test_backfill_floor_crypto_default(monkeypatch):
    monkeypatch.delenv("BACKFILL_FLOOR_CRYPTO", raising=False)
    s = Settings()
    assert s.backfill_floor_crypto == date(2021, 1, 1)


def test_backfill_floor_equity_env_override(monkeypatch):
    monkeypatch.setenv("BACKFILL_FLOOR_EQUITY", "2018-06-15")
    s = Settings()
    assert s.backfill_floor_equity == date(2018, 6, 15)


def test_backfill_floor_crypto_env_override(monkeypatch):
    monkeypatch.setenv("BACKFILL_FLOOR_CRYPTO", "2022-03-01")
    s = Settings()
    assert s.backfill_floor_crypto == date(2022, 3, 1)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest bullforge/tests/test_config.py -v -k backfill_floor`
Expected: FAIL — `Settings` has no attribute `backfill_floor_equity`.

- [ ] **Step 3: Implement the settings**

In `bullforge/shared/config.py`:

Update the `datetime` import line to also import `date`:

```python
from datetime import date, time
```

Add a parser helper near `_parse_time`:

```python
def _parse_date(env_key: str, default: str) -> date:
    raw = os.getenv(env_key, default)
    return date.fromisoformat(raw)
```

Add the two fields directly after `trading_symbols_crypto`:

```python
    # Backfill floors (earliest historical bar dates from Alpaca)
    backfill_floor_equity: date = field(
        default_factory=lambda: _parse_date(
            "BACKFILL_FLOOR_EQUITY", "2016-01-01"
        )
    )
    backfill_floor_crypto: date = field(
        default_factory=lambda: _parse_date(
            "BACKFILL_FLOOR_CRYPTO", "2021-01-01"
        )
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest bullforge/tests/test_config.py -v`
Expected: PASS for all `backfill_floor_*` tests, no regressions.

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/config.py bullforge/tests/test_config.py
git commit -m "feat(config): add backfill_floor_equity/crypto settings"
```

---

## Task 2: Add `Database.get_bar_range`

**Files:**
- Modify: `bullforge/shared/db.py` (add method to `Database` class, near `query_bars`)
- Test: `bullforge/tests/test_db.py`

- [ ] **Step 1: Write the failing test**

Append to `bullforge/tests/test_db.py`:

```python
import pytest
from datetime import datetime, timezone, timedelta
from shared.models import Bar


@pytest.mark.asyncio
async def test_get_bar_range_empty(db_fixture):
    earliest, latest = await db_fixture.get_bar_range("XYZ", "1m")
    assert earliest is None
    assert latest is None


@pytest.mark.asyncio
async def test_get_bar_range_populated(db_fixture):
    base = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    bars = [
        Bar(
            symbol="RNG", timestamp=base + timedelta(minutes=i),
            open=1.0, high=1.0, low=1.0, close=1.0, volume=1,
            timeframe="1m",
        )
        for i in range(3)
    ]
    await db_fixture.insert_bars(bars)
    earliest, latest = await db_fixture.get_bar_range("RNG", "1m")
    assert earliest == base
    assert latest == base + timedelta(minutes=2)


@pytest.mark.asyncio
async def test_get_bar_range_timeframe_isolated(db_fixture):
    base = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    minute_bar = Bar(
        symbol="ISO", timestamp=base, open=1.0, high=1.0,
        low=1.0, close=1.0, volume=1, timeframe="1m",
    )
    daily_bar = Bar(
        symbol="ISO", timestamp=base + timedelta(days=10),
        open=1.0, high=1.0, low=1.0, close=1.0, volume=1,
        timeframe="1d",
    )
    await db_fixture.insert_bars([minute_bar])
    await db_fixture.insert_bars([daily_bar])
    e1, l1 = await db_fixture.get_bar_range("ISO", "1m")
    e2, l2 = await db_fixture.get_bar_range("ISO", "1d")
    assert e1 == l1 == base
    assert e2 == l2 == base + timedelta(days=10)
```

`db_fixture` is the existing async DB fixture used elsewhere in `test_db.py`. If it has a different name there, reuse that name verbatim — do not invent a new one.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest bullforge/tests/test_db.py -v -k get_bar_range`
Expected: FAIL — `Database` has no attribute `get_bar_range`.

- [ ] **Step 3: Implement `get_bar_range`**

In `bullforge/shared/db.py`, add this method to the `Database` class immediately after `query_bars`:

```python
    async def get_bar_range(
        self, symbol: str, timeframe: str
    ) -> tuple[Optional[datetime], Optional[datetime]]:
        """Return (earliest_ts, latest_ts) for (symbol, timeframe), or (None, None)."""
        table = "bars_1m" if timeframe == "1m" else "bars_daily"
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT MIN(timestamp) AS lo, MAX(timestamp) AS hi "
                f"FROM {table} WHERE symbol = $1",
                symbol,
            )
        if row is None or row["lo"] is None:
            return None, None
        return row["lo"], row["hi"]
```

If `datetime` is not already imported at the top of `db.py`, add it. Likewise for `Optional` (the file already uses both based on its method signatures, so this should be a no-op).

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest bullforge/tests/test_db.py -v -k get_bar_range`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/db.py bullforge/tests/test_db.py
git commit -m "feat(db): add get_bar_range for backfill convergence"
```

---

## Task 3: Rewrite `Backfiller` with two-phase API

**Files:**
- Rewrite: `bullforge/services/data_pipeline/backfill.py`
- Rewrite: `bullforge/tests/test_backfill.py`

This task has the largest blast radius. Tests come first to lock in the contract.

- [ ] **Step 1: Write the failing tests**

Replace the entire contents of `bullforge/tests/test_backfill.py` with:

```python
import pytest
from datetime import datetime, timezone, timedelta, date
from unittest.mock import AsyncMock, MagicMock

from shared.config import Settings
from shared.models import Bar
from services.data_pipeline.backfill import Backfiller


@pytest.fixture
def mock_settings(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test_key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test_secret")
    monkeypatch.setenv("TRADING_SYMBOLS_EQUITY", "SPY")
    monkeypatch.setenv("TRADING_SYMBOLS_CRYPTO", "BTC/USD")
    monkeypatch.setenv("BACKFILL_FLOOR_EQUITY", "2016-01-01")
    monkeypatch.setenv("BACKFILL_FLOOR_CRYPTO", "2021-01-01")
    return Settings()


def _bars(symbol: str, count: int, timeframe: str, start: datetime) -> list[Bar]:
    step = timedelta(minutes=1) if timeframe == "1m" else timedelta(days=1)
    return [
        Bar(
            symbol=symbol, timestamp=start + step * i,
            open=1.0, high=1.0, low=1.0, close=1.0, volume=1,
            timeframe=timeframe,
        )
        for i in range(count)
    ]


def _make_backfiller(settings, *, range_map=None, fetch_returns=None,
                     fetch_side_effect=None):
    """Build a Backfiller with mocked db.get_bar_range and alpaca client."""
    db = AsyncMock()
    range_map = range_map or {}

    async def _get_range(symbol, timeframe):
        return range_map.get((symbol, timeframe), (None, None))

    db.get_bar_range.side_effect = _get_range
    db.insert_bars = AsyncMock()

    alpaca = MagicMock()
    if fetch_side_effect is not None:
        alpaca.get_historical_bars.side_effect = fetch_side_effect
    else:
        alpaca.get_historical_bars.return_value = fetch_returns or []

    return Backfiller(settings=settings, db=db, alpaca_client=alpaca), db, alpaca


@pytest.mark.asyncio
async def test_forward_fill_empty_db_pulls_from_floor(mock_settings):
    bars = _bars("SPY", 3, "1m", datetime(2016, 1, 1, tzinfo=timezone.utc))
    backfiller, db, alpaca = _make_backfiller(
        mock_settings, fetch_returns=bars,
    )
    inserted = await backfiller.forward_fill("SPY", "1m")

    assert inserted == 3
    db.insert_bars.assert_awaited_once()
    call_kwargs = alpaca.get_historical_bars.call_args.kwargs
    assert call_kwargs["start"].date() == date(2016, 1, 1)


@pytest.mark.asyncio
async def test_forward_fill_uses_latest_plus_one_when_populated(mock_settings):
    latest = datetime(2026, 4, 14, 15, 0, tzinfo=timezone.utc)
    backfiller, db, alpaca = _make_backfiller(
        mock_settings,
        range_map={("SPY", "1m"): (
            datetime(2016, 1, 1, tzinfo=timezone.utc), latest,
        )},
        fetch_returns=_bars("SPY", 1, "1m", latest + timedelta(minutes=1)),
    )

    await backfiller.forward_fill("SPY", "1m")

    call_kwargs = alpaca.get_historical_bars.call_args.kwargs
    assert call_kwargs["start"] == latest + timedelta(minutes=1)


@pytest.mark.asyncio
async def test_forward_fill_no_bars_skips_insert(mock_settings):
    backfiller, db, _ = _make_backfiller(mock_settings, fetch_returns=[])
    inserted = await backfiller.forward_fill("SPY", "1m")
    assert inserted == 0
    db.insert_bars.assert_not_awaited()


@pytest.mark.asyncio
async def test_reach_back_noop_when_at_floor(mock_settings):
    floor_dt = datetime(2016, 1, 1, tzinfo=timezone.utc)
    backfiller, db, alpaca = _make_backfiller(
        mock_settings,
        range_map={("SPY", "1m"): (floor_dt, floor_dt)},
    )
    inserted = await backfiller.reach_back("SPY", "1m")
    assert inserted == 0
    alpaca.get_historical_bars.assert_not_called()
    db.insert_bars.assert_not_awaited()


@pytest.mark.asyncio
async def test_reach_back_pulls_floor_to_earliest(mock_settings):
    earliest = datetime(2024, 1, 1, tzinfo=timezone.utc)
    bars = _bars("SPY", 5, "1d", datetime(2016, 1, 1, tzinfo=timezone.utc))
    backfiller, _, alpaca = _make_backfiller(
        mock_settings,
        range_map={("SPY", "1d"): (earliest, earliest)},
        fetch_returns=bars,
    )

    inserted = await backfiller.reach_back("SPY", "1d")

    assert inserted == 5
    call_kwargs = alpaca.get_historical_bars.call_args.kwargs
    assert call_kwargs["start"].date() == date(2016, 1, 1)
    assert call_kwargs["end"] == earliest


@pytest.mark.asyncio
async def test_reach_back_noop_when_db_empty(mock_settings):
    """If forward_fill hasn't run yet, reach_back has nothing to extend backward from."""
    backfiller, _, alpaca = _make_backfiller(mock_settings)
    inserted = await backfiller.reach_back("SPY", "1m")
    assert inserted == 0
    alpaca.get_historical_bars.assert_not_called()


@pytest.mark.asyncio
async def test_reach_back_uses_crypto_floor_for_crypto_symbol(mock_settings):
    earliest = datetime(2025, 1, 1, tzinfo=timezone.utc)
    backfiller, _, alpaca = _make_backfiller(
        mock_settings,
        range_map={("BTC/USD", "1d"): (earliest, earliest)},
        fetch_returns=_bars("BTC/USD", 1, "1d",
                            datetime(2021, 1, 1, tzinfo=timezone.utc)),
    )
    await backfiller.reach_back("BTC/USD", "1d")
    call_kwargs = alpaca.get_historical_bars.call_args.kwargs
    assert call_kwargs["start"].date() == date(2021, 1, 1)


@pytest.mark.asyncio
async def test_backfill_all_forward_runs_each_symbol_and_timeframe(mock_settings):
    backfiller, _, alpaca = _make_backfiller(
        mock_settings,
        fetch_returns=_bars("X", 1, "1m",
                            datetime(2016, 1, 1, tzinfo=timezone.utc)),
    )
    await backfiller.backfill_all_forward()
    # 2 symbols * 2 timeframes = 4 fetches
    assert alpaca.get_historical_bars.call_count == 4


@pytest.mark.asyncio
async def test_backfill_all_forward_continues_past_failure(
    mock_settings, caplog,
):
    def _side_effect(*args, **kwargs):
        if kwargs.get("symbol") == "SPY":
            raise RuntimeError("alpaca down")
        return _bars(kwargs["symbol"], 1, kwargs["timeframe"],
                     datetime(2021, 1, 1, tzinfo=timezone.utc))

    backfiller, _, alpaca = _make_backfiller(
        mock_settings, fetch_side_effect=_side_effect,
    )
    with caplog.at_level("WARNING"):
        await backfiller.backfill_all_forward()
    # SPY failed twice (1m + 1d); BTC/USD succeeded twice
    assert alpaca.get_historical_bars.call_count == 4
    assert any("SPY" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_backfill_all_reach_back_runs_each_symbol_and_timeframe(mock_settings):
    earliest = datetime(2024, 1, 1, tzinfo=timezone.utc)
    range_map = {
        ("SPY", "1m"): (earliest, earliest),
        ("SPY", "1d"): (earliest, earliest),
        ("BTC/USD", "1m"): (earliest, earliest),
        ("BTC/USD", "1d"): (earliest, earliest),
    }
    backfiller, _, alpaca = _make_backfiller(
        mock_settings, range_map=range_map,
        fetch_returns=_bars("X", 1, "1m",
                            datetime(2016, 1, 1, tzinfo=timezone.utc)),
    )
    await backfiller.backfill_all_reach_back()
    assert alpaca.get_historical_bars.call_count == 4


@pytest.mark.asyncio
async def test_backfill_symbol_runs_forward_only(mock_settings):
    backfiller, _, alpaca = _make_backfiller(
        mock_settings,
        fetch_returns=_bars("NEW", 1, "1m",
                            datetime(2016, 1, 1, tzinfo=timezone.utc)),
    )
    await backfiller.backfill_symbol("NEW", kind="equity")
    # 1 symbol * 2 timeframes = 2 forward fetches; reach-back is caller's job
    assert alpaca.get_historical_bars.call_count == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest bullforge/tests/test_backfill.py -v`
Expected: FAIL — methods `forward_fill`, `reach_back`, `backfill_all_forward`, `backfill_all_reach_back` do not exist; `backfill_symbol` signature has changed.

- [ ] **Step 3: Replace `Backfiller` implementation**

Replace the entire contents of `bullforge/services/data_pipeline/backfill.py` with:

```python
from __future__ import annotations

import logging
import time
from datetime import date, datetime, time as dtime, timedelta, timezone

from shared.config import Settings
from shared.db import Database
from shared.symbol_utils import is_crypto
from services.data_pipeline.alpaca_client import AlpacaClient

logger = logging.getLogger(__name__)


class Backfiller:
    def __init__(
        self, settings: Settings, db: Database, alpaca_client: AlpacaClient,
    ):
        self._settings = settings
        self._db = db
        self._alpaca = alpaca_client

    # ---- helpers ----------------------------------------------------------

    def _floor_dt(self, symbol: str) -> datetime:
        floor: date = (
            self._settings.backfill_floor_crypto
            if is_crypto(symbol)
            else self._settings.backfill_floor_equity
        )
        return datetime.combine(floor, dtime.min, tzinfo=timezone.utc)

    @staticmethod
    def _step(timeframe: str) -> timedelta:
        return timedelta(minutes=1) if timeframe == "1m" else timedelta(days=1)

    async def _fetch_and_insert(
        self, symbol: str, timeframe: str,
        start: datetime, end: datetime,
    ) -> int:
        if start >= end:
            return 0
        t0 = time.monotonic()
        bars = self._alpaca.get_historical_bars(
            symbol=symbol, timeframe=timeframe, start=start, end=end,
        )
        if not bars:
            logger.info(
                "No bars returned for %s %s [%s → %s]",
                symbol, timeframe, start.isoformat(), end.isoformat(),
            )
            return 0
        await self._db.insert_bars(bars)
        logger.info(
            "Inserted %d %s bars for %s [%s → %s] in %.2fs",
            len(bars), timeframe, symbol,
            start.isoformat(), end.isoformat(), time.monotonic() - t0,
        )
        return len(bars)

    # ---- public API -------------------------------------------------------

    async def forward_fill(self, symbol: str, timeframe: str) -> int:
        _, latest = await self._db.get_bar_range(symbol, timeframe)
        start = (latest + self._step(timeframe)) if latest else self._floor_dt(symbol)
        end = datetime.now(timezone.utc)
        return await self._fetch_and_insert(symbol, timeframe, start, end)

    async def reach_back(self, symbol: str, timeframe: str) -> int:
        earliest, _ = await self._db.get_bar_range(symbol, timeframe)
        if earliest is None:
            return 0
        floor = self._floor_dt(symbol)
        if earliest <= floor:
            return 0
        return await self._fetch_and_insert(symbol, timeframe, floor, earliest)

    async def backfill_all_forward(self) -> None:
        symbols = (
            self._settings.trading_symbols_equity
            + self._settings.trading_symbols_crypto
        )
        failures: list[tuple[str, str]] = []
        for symbol in symbols:
            for timeframe in ("1m", "1d"):
                try:
                    await self.forward_fill(symbol, timeframe)
                except Exception:
                    logger.exception(
                        "forward_fill failed for %s %s", symbol, timeframe,
                    )
                    failures.append((symbol, timeframe))
        if failures:
            logger.warning(
                "Forward backfill completed with %d failures: %s",
                len(failures), failures,
            )
        else:
            logger.info("Forward backfill complete for all symbols")

    async def backfill_all_reach_back(self) -> None:
        symbols = (
            self._settings.trading_symbols_equity
            + self._settings.trading_symbols_crypto
        )
        failures: list[tuple[str, str]] = []
        for symbol in symbols:
            for timeframe in ("1m", "1d"):
                try:
                    await self.reach_back(symbol, timeframe)
                except Exception:
                    logger.exception(
                        "reach_back failed for %s %s", symbol, timeframe,
                    )
                    failures.append((symbol, timeframe))
        if failures:
            logger.warning(
                "Reach-back completed with %d failures: %s",
                len(failures), failures,
            )
        else:
            logger.info("Reach-back complete for all symbols")

    async def backfill_symbol(self, symbol: str, *, kind: str) -> int:
        """Forward-fill a newly added symbol for both timeframes.

        The caller is responsible for scheduling reach_back as a background task.
        """
        total = 0
        for timeframe in ("1m", "1d"):
            try:
                total += await self.forward_fill(symbol, timeframe)
            except Exception:
                logger.exception(
                    "forward_fill failed for %s %s", symbol, timeframe,
                )
        return total
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest bullforge/tests/test_backfill.py -v`
Expected: PASS for all tests listed in Step 1.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/data_pipeline/backfill.py bullforge/tests/test_backfill.py
git commit -m "feat(backfill): two-phase forward+reachback convergence"
```

---

## Task 4: Wire main.py to the new API

**Files:**
- Modify: `bullforge/services/data_pipeline/main.py`
- Test: `bullforge/tests/test_symbol_hot_reload.py` (update existing add-event coverage)

- [ ] **Step 1: Inspect existing add-event test**

Run: `pytest bullforge/tests/test_symbol_hot_reload.py -v --collect-only`

Read the test that exercises `handle_symbol_change_event` for an `add` action. Note the names of the mocks it passes for `stream` and `backfill`. The new contract: on `add`, the handler awaits `backfill.backfill_symbol(symbol, kind=kind)`, then awaits `stream.subscribe_symbol(symbol, kind=kind)`, then schedules `backfill.reach_back(symbol, "1m")` and `backfill.reach_back(symbol, "1d")` as `asyncio.create_task` calls (tracked in a passed-in set so the caller can cancel on shutdown).

- [ ] **Step 2: Update the add-event test**

In `bullforge/tests/test_symbol_hot_reload.py`, modify the test that covers `add` events so that:

- The handler is called with an additional `background_tasks: set[asyncio.Task]` argument.
- After the call, assert that `backfill.backfill_symbol` was awaited once with `(symbol, kind=kind)`.
- Assert that `stream.subscribe_symbol` was awaited.
- Assert that `background_tasks` contains exactly two tasks (one per timeframe) — i.e., `len(background_tasks) == 2`.
- Await `asyncio.gather(*background_tasks)` and then assert that `backfill.reach_back` was awaited twice with timeframes `"1m"` and `"1d"`.

If multiple tests cover this path, update each one.

Run: `pytest bullforge/tests/test_symbol_hot_reload.py -v`
Expected: FAIL — `handle_symbol_change_event` does not yet accept `background_tasks` and does not schedule `reach_back`.

- [ ] **Step 3: Update `handle_symbol_change_event`**

In `bullforge/services/data_pipeline/main.py`, replace the `handle_symbol_change_event` function with:

```python
async def handle_symbol_change_event(
    message: str, *, stream, backfill,
    background_tasks: set[asyncio.Task],
) -> None:
    """Handle a config:symbols_changed event.

    On add: forward-fill (blocking), subscribe, then schedule reach_back
    for both timeframes as background tasks.
    On remove: unsubscribe only (historical data is retained).
    """
    data = json.loads(message)
    action, kind, symbol = data["action"], data["kind"], data["symbol"]
    if action == "add":
        await backfill.backfill_symbol(symbol, kind=kind)
        await stream.subscribe_symbol(symbol, kind=kind)
        for timeframe in ("1m", "1d"):
            task = asyncio.create_task(backfill.reach_back(symbol, timeframe))
            background_tasks.add(task)
            task.add_done_callback(background_tasks.discard)
    elif action == "remove":
        await stream.unsubscribe_symbol(symbol, kind=kind)
```

- [ ] **Step 4: Update `main()` to use the new backfill API and track background tasks**

Still in `bullforge/services/data_pipeline/main.py`:

Replace the lines:

```python
    # Run backfill on startup
    backfiller = Backfiller(settings=settings, db=db, alpaca_client=alpaca_client)
    logger.info("Running initial backfill...")
    await backfiller.backfill_all(days_1m=730, days_daily=365)
```

with:

```python
    # Forward gap-fill blocks startup so live ticks have no gap; the long
    # reach-back to the configured floor runs in the background once
    # streams are up.
    backfiller = Backfiller(settings=settings, db=db, alpaca_client=alpaca_client)
    background_tasks: set[asyncio.Task] = set()
    logger.info("Running forward gap-fill...")
    await backfiller.backfill_all_forward()
```

After the existing `streamer.attach_streams(...)` call and before the existing `_on_symbols_changed` definition, insert:

```python
    reach_back_task = asyncio.create_task(backfiller.backfill_all_reach_back())
    background_tasks.add(reach_back_task)
    reach_back_task.add_done_callback(background_tasks.discard)
    logger.info("Scheduled background reach-back to historical floor")
```

Replace the existing `_on_symbols_changed` definition with:

```python
    async def _on_symbols_changed(msg: str) -> None:
        try:
            await handle_symbol_change_event(
                msg, stream=streamer, backfill=backfiller,
                background_tasks=background_tasks,
            )
        except Exception:
            logger.exception("Failed handling symbols_changed event: %s", msg)
```

In the `finally:` shutdown block, immediately after `consumer_task.cancel()` / its `await`, insert background-task cleanup before `await runner.cleanup()`:

```python
        for task in list(background_tasks):
            task.cancel()
        for task in list(background_tasks):
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Background task raised during shutdown")
```

- [ ] **Step 5: Run all related tests**

Run: `pytest bullforge/tests/test_symbol_hot_reload.py bullforge/tests/test_backfill.py -v`
Expected: PASS.

- [ ] **Step 6: Run the full pipeline test slice**

Run: `pytest bullforge/tests/test_backfill.py bullforge/tests/test_symbol_hot_reload.py bullforge/tests/test_config.py bullforge/tests/test_db.py -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add bullforge/services/data_pipeline/main.py bullforge/tests/test_symbol_hot_reload.py
git commit -m "feat(data_pipeline): wire two-phase backfill with background reach-back"
```

---

## Task 4.5: Publish backfill counters to `monitoring:data_pipeline`

**Files:**
- Modify: `bullforge/services/data_pipeline/backfill.py` (add optional redis client + counter publishing)
- Modify: `bullforge/services/data_pipeline/main.py` (pass `redis_client` into `Backfiller`)
- Modify: `bullforge/tests/test_backfill.py` (add coverage for counter publishing)

The reconciler already follows this pattern in `bullforge/services/execution_service/reconciler_core.py:14-26` — mirror it. Counter payload format (matches reconciler exactly):
`json.dumps({"counter": <name>, "labels": {<k:v>...}, "value": <int>})`

- [ ] **Step 1: Write the failing tests**

Append to `bullforge/tests/test_backfill.py`:

```python
import json


def _make_backfiller_with_redis(settings, *, range_map=None,
                                 fetch_returns=None, fetch_side_effect=None):
    backfiller, db, alpaca = _make_backfiller(
        settings, range_map=range_map,
        fetch_returns=fetch_returns, fetch_side_effect=fetch_side_effect,
    )
    redis = AsyncMock()
    redis.publish = AsyncMock()
    backfiller._redis = redis
    return backfiller, db, alpaca, redis


def _published_counters(redis) -> list[dict]:
    return [
        json.loads(call.args[1])
        for call in redis.publish.await_args_list
        if call.args[0] == "monitoring:data_pipeline"
    ]


@pytest.mark.asyncio
async def test_forward_fill_publishes_bars_inserted_counter(mock_settings):
    bars = _bars("SPY", 7, "1m", datetime(2016, 1, 1, tzinfo=timezone.utc))
    backfiller, _, _, redis = _make_backfiller_with_redis(
        mock_settings, fetch_returns=bars,
    )
    await backfiller.forward_fill("SPY", "1m")
    counters = _published_counters(redis)
    assert any(
        c["counter"] == "backfill.forward.bars_inserted"
        and c["labels"] == {"symbol": "SPY", "timeframe": "1m"}
        and c["value"] == 7
        for c in counters
    )


@pytest.mark.asyncio
async def test_reach_back_publishes_bars_inserted_counter(mock_settings):
    earliest = datetime(2024, 1, 1, tzinfo=timezone.utc)
    bars = _bars("SPY", 4, "1d", datetime(2016, 1, 1, tzinfo=timezone.utc))
    backfiller, _, _, redis = _make_backfiller_with_redis(
        mock_settings,
        range_map={("SPY", "1d"): (earliest, earliest)},
        fetch_returns=bars,
    )
    await backfiller.reach_back("SPY", "1d")
    counters = _published_counters(redis)
    assert any(
        c["counter"] == "backfill.reachback.bars_inserted"
        and c["labels"] == {"symbol": "SPY", "timeframe": "1d"}
        and c["value"] == 4
        for c in counters
    )


@pytest.mark.asyncio
async def test_backfill_all_forward_publishes_failure_counter(mock_settings):
    def _side_effect(*args, **kwargs):
        if kwargs.get("symbol") == "SPY":
            raise RuntimeError("alpaca down")
        return _bars(kwargs["symbol"], 1, kwargs["timeframe"],
                     datetime(2021, 1, 1, tzinfo=timezone.utc))
    backfiller, _, _, redis = _make_backfiller_with_redis(
        mock_settings, fetch_side_effect=_side_effect,
    )
    await backfiller.backfill_all_forward()
    counters = _published_counters(redis)
    failures = [c for c in counters if c["counter"] == "backfill.failures"]
    # SPY fails for both timeframes during forward phase
    assert len(failures) == 2
    assert all(f["labels"]["phase"] == "forward" for f in failures)
    assert all(f["labels"]["symbol"] == "SPY" for f in failures)


@pytest.mark.asyncio
async def test_backfill_all_reach_back_publishes_complete_event(mock_settings):
    backfiller, _, _, redis = _make_backfiller_with_redis(mock_settings)
    await backfiller.backfill_all_reach_back()
    counters = _published_counters(redis)
    assert any(c["counter"] == "backfill.reachback.complete" for c in counters)


@pytest.mark.asyncio
async def test_backfiller_without_redis_does_not_publish(mock_settings):
    bars = _bars("SPY", 1, "1m", datetime(2016, 1, 1, tzinfo=timezone.utc))
    backfiller, _, _ = _make_backfiller(mock_settings, fetch_returns=bars)
    # _redis defaults to None — should not raise
    await backfiller.forward_fill("SPY", "1m")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest bullforge/tests/test_backfill.py -v -k "publishes or without_redis"`
Expected: FAIL — `Backfiller` does not publish counters and `_redis` attribute does not exist.

- [ ] **Step 3: Add counter publishing to `Backfiller`**

In `bullforge/services/data_pipeline/backfill.py`:

Add `import json` near the existing imports. Add the channel constant under `logger`:

```python
_MONITORING_CHANNEL = "monitoring:data_pipeline"
```

Update `__init__` to accept an optional redis client:

```python
    def __init__(
        self, settings: Settings, db: Database, alpaca_client: AlpacaClient,
        redis_client=None,
    ):
        self._settings = settings
        self._db = db
        self._alpaca = alpaca_client
        self._redis = redis_client
```

Add a publishing helper directly under the helpers section (after `_step`):

```python
    async def _publish_counter(self, name: str, value: int = 1, **labels) -> None:
        if self._redis is None:
            return
        payload = json.dumps(
            {"counter": name, "labels": labels, "value": value}
        )
        try:
            await self._redis.publish(_MONITORING_CHANNEL, payload)
        except Exception:
            logger.exception("Failed publishing counter %s", name)
```

In `_fetch_and_insert`, return early as today when no bars; otherwise after the existing `await self._db.insert_bars(bars)` and log line, the *caller* will publish the phase-tagged counter (so `forward_fill` vs `reach_back` get distinct names). Leave `_fetch_and_insert` itself unchanged otherwise — it already returns the inserted count.

Update `forward_fill` to publish on success:

```python
    async def forward_fill(self, symbol: str, timeframe: str) -> int:
        _, latest = await self._db.get_bar_range(symbol, timeframe)
        start = (latest + self._step(timeframe)) if latest else self._floor_dt(symbol)
        end = datetime.now(timezone.utc)
        inserted = await self._fetch_and_insert(symbol, timeframe, start, end)
        if inserted:
            await self._publish_counter(
                "backfill.forward.bars_inserted", value=inserted,
                symbol=symbol, timeframe=timeframe,
            )
        return inserted
```

Update `reach_back` similarly:

```python
    async def reach_back(self, symbol: str, timeframe: str) -> int:
        earliest, _ = await self._db.get_bar_range(symbol, timeframe)
        if earliest is None:
            return 0
        floor = self._floor_dt(symbol)
        if earliest <= floor:
            return 0
        inserted = await self._fetch_and_insert(symbol, timeframe, floor, earliest)
        if inserted:
            await self._publish_counter(
                "backfill.reachback.bars_inserted", value=inserted,
                symbol=symbol, timeframe=timeframe,
            )
        return inserted
```

In `backfill_all_forward`, replace the `except Exception:` block so it also publishes a failure counter:

```python
                except Exception:
                    logger.exception(
                        "forward_fill failed for %s %s", symbol, timeframe,
                    )
                    failures.append((symbol, timeframe))
                    await self._publish_counter(
                        "backfill.failures", phase="forward",
                        symbol=symbol, timeframe=timeframe,
                    )
```

In `backfill_all_reach_back`, mirror the change for the failure path AND publish a `complete` event at the end (after the `if failures: ... else: ...` block):

```python
                except Exception:
                    logger.exception(
                        "reach_back failed for %s %s", symbol, timeframe,
                    )
                    failures.append((symbol, timeframe))
                    await self._publish_counter(
                        "backfill.failures", phase="reachback",
                        symbol=symbol, timeframe=timeframe,
                    )
```

…and at the end of `backfill_all_reach_back`:

```python
        await self._publish_counter(
            "backfill.reachback.complete", failures=len(failures),
        )
```

In `backfill_symbol`, also publish failures (phase `forward`, since this path is forward-only):

```python
            try:
                total += await self.forward_fill(symbol, timeframe)
            except Exception:
                logger.exception(
                    "forward_fill failed for %s %s", symbol, timeframe,
                )
                await self._publish_counter(
                    "backfill.failures", phase="forward",
                    symbol=symbol, timeframe=timeframe,
                )
```

- [ ] **Step 4: Wire `redis_client` through `main.py`**

In `bullforge/services/data_pipeline/main.py`, change the `Backfiller(...)` construction added in Task 4 to pass the existing `redis_client`:

```python
    backfiller = Backfiller(
        settings=settings, db=db,
        alpaca_client=alpaca_client, redis_client=redis_client,
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest bullforge/tests/test_backfill.py -v`
Expected: PASS for all tests (including the new counter ones and the original 11).

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/data_pipeline/backfill.py bullforge/services/data_pipeline/main.py bullforge/tests/test_backfill.py
git commit -m "feat(backfill): publish counters to monitoring:data_pipeline"
```

---

## Task 5: Full test sweep + manual smoke

**Files:** none modified.

- [ ] **Step 1: Run the full test suite**

Run: `pytest bullforge/tests -x -q`
Expected: all green. If any unrelated test broke (e.g., something else patched `Backfiller.backfill_all`), fix it minimally — the symptom is almost certainly a test that referenced the deleted `backfill_all` / `backfill_timeframe` methods. Update the test to call `backfill_all_forward` / `forward_fill` instead.

- [ ] **Step 2: Lint/type check (if configured)**

If the project has a configured linter or type checker, run it on the changed files:
- `bullforge/shared/config.py`
- `bullforge/shared/db.py`
- `bullforge/services/data_pipeline/backfill.py`
- `bullforge/services/data_pipeline/main.py`

If no linter is configured for the project, skip this step.

- [ ] **Step 3: Commit any test fix-ups**

```bash
git add -A
git commit -m "test: align stragglers with two-phase backfill API"
```

(Skip the commit if there are no changes.)

- [ ] **Step 4: Document in TODO.md (only if there is one tracking backfill behavior)**

Run: `grep -i backfill TODO.md || echo "no entry"`

If the grep returns lines mentioning the old fixed-window behavior (`days_1m=730` or `days_daily=365`), edit those lines to reflect the new two-phase behavior. Otherwise skip.

If you edited it: `git add TODO.md && git commit -m "docs(todo): note two-phase backfill landed"`.

---

## Self-Review Notes

- **Spec coverage:** floors → Task 1; `get_bar_range` → Task 2; two-phase `Backfiller` API → Task 3; main.py wiring + background reach-back + shutdown cancellation → Task 4; observability counters → Task 4.5.
- **Type/name consistency:** `forward_fill`, `reach_back`, `backfill_all_forward`, `backfill_all_reach_back`, `backfill_symbol(symbol, *, kind)` used identically across plan, tests, and main.py wiring.
- **No placeholders.** Every step contains the actual code or command.
