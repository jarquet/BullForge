# Telegram Alert Cleanup and Position-Closed Alerts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Silence noisy `model_warned`/`model_disabled` Telegram messages (keep publishing to Redis), add a new `position_closed` Telegram alert carrying trade P&L plus account day stats, and introduce a maintained Redis pub/sub reference doc.

**Architecture:** Filter happens in `TelegramBot._on_alert` via a module-level `frozenset`; `HaltManager` is unchanged. The new alert is published from `ReconcilerCore._handle_fill` after `update_trade_exit` commits, using the same `monitoring:alerts` Redis channel as today. Day stats are computed via a new `day_stats.compute_day_stats(db, broker, now)` helper that combines Alpaca's `equity`/`last_equity` (new field on the account dict) with `query_closed_trades(since=utc_midnight)`.

**Tech Stack:** Python 3.11, asyncio, asyncpg (via `shared.db`), redis-py (via `shared.redis.RedisClient`), alpaca-py, python-telegram-bot, pytest + pytest-asyncio.

**Spec:** [docs/superpowers/specs/2026-04-22-telegram-alert-cleanup-and-close-alerts-design.md](../specs/2026-04-22-telegram-alert-cleanup-and-close-alerts-design.md)

---

## File Map

**Create**
- `bullforge/services/execution_service/day_stats.py` — `compute_day_stats(db, broker, now)` async helper.
- `bullforge/tests/test_day_stats.py` — unit tests for the helper.
- `docs/architecture/redis-pubsub.md` — single source of truth for Redis channels.

**Modify**
- `bullforge/services/execution_service/alpaca_broker.py` — add `last_equity` to `_sync_get_account`.
- `bullforge/services/execution_service/reconciler_core.py` — accept `broker`; publish `position_closed` after closing fills.
- `bullforge/services/execution_service/main.py` — pass `broker=broker` to `ReconcilerCore(...)`.
- `bullforge/services/monitoring_service/telegram_bot.py` — add `TELEGRAM_SUPPRESSED_ALERT_TYPES`; filter in `_on_alert`.
- `bullforge/services/monitoring_service/telegram_alerts.py` — new `position_closed` branch in `format_alert`; add `_format_duration` and `_format_signed_money` helpers.
- `bullforge/tests/test_alpaca_broker.py` — assert `last_equity` is exposed.
- `bullforge/tests/test_reconciler_core.py` — assert `position_closed` is published on closing fills; assert no publish when broker is absent.
- `bullforge/tests/test_telegram_bot.py` — update the existing `test_on_alert_from_redis` case and add suppression + delivery tests.
- `bullforge/tests/test_telegram_alerts.py` — new format tests for `position_closed` (win, loss, with & without day stats).

---

## Task 1: Suppress noisy alerts from Telegram delivery

**Files:**
- Modify: `bullforge/services/monitoring_service/telegram_bot.py`
- Modify (tests): `bullforge/tests/test_telegram_bot.py`

- [ ] **Step 1: Write / update the failing tests**

Open `bullforge/tests/test_telegram_bot.py`.

Replace the existing `test_on_alert_from_redis` (it asserts `model_disabled` delivers; we now assert the opposite). Add three new tests: one each for `model_warned` and `model_disabled` being suppressed, and one for a non-suppressed type (`direction_halt`) still delivering.

Remove the old test (lines 87–93 in the current file) and replace with:

```python
@pytest.mark.asyncio
async def test_on_alert_suppresses_model_warned(bot):
    alert = {"tier": 1, "type": "model_warned", "model_id": 3, "reason": "x"}
    await bot._on_alert(json.dumps(alert))
    bot._bot.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_on_alert_suppresses_model_disabled(bot):
    alert = {"tier": 2, "type": "model_disabled", "model_id": 3, "reason": "x"}
    await bot._on_alert(json.dumps(alert))
    bot._bot.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_on_alert_delivers_non_suppressed_type(bot):
    alert = {"tier": 3, "type": "direction_halt", "reason": "half down"}
    await bot._on_alert(json.dumps(alert))
    bot._bot.send_message.assert_called_once()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest bullforge/tests/test_telegram_bot.py -v -k "on_alert"`
Expected: two suppression tests FAIL (current code calls `send_message`), `delivers_non_suppressed_type` PASSES.

- [ ] **Step 3: Add the suppression set + filter**

Edit `bullforge/services/monitoring_service/telegram_bot.py`.

After the `logger = logging.getLogger(__name__)` line (line 32), add:

```python
# Alert types whose publication on `monitoring:alerts` should NOT result in
# a Telegram message. They remain on the Redis channel for other consumers.
TELEGRAM_SUPPRESSED_ALERT_TYPES = frozenset({"model_warned", "model_disabled"})
```

Replace the existing `_on_alert` method (lines 113–118) with:

```python
    async def _on_alert(self, message: str) -> None:
        try:
            alert = json.loads(message)
            alert_type = alert.get("type")
            if alert_type in TELEGRAM_SUPPRESSED_ALERT_TYPES:
                logger.debug(
                    "Suppressed Telegram delivery for alert type %s",
                    alert_type,
                )
                return
            await self.send_alert(alert)
        except Exception:
            logger.exception("Failed to process alert for Telegram")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest bullforge/tests/test_telegram_bot.py -v`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/monitoring_service/telegram_bot.py bullforge/tests/test_telegram_bot.py
git commit -m "feat(telegram): suppress model_warned and model_disabled from Telegram delivery"
```

---

## Task 2: Expose `last_equity` from AlpacaBroker

**Files:**
- Modify: `bullforge/services/execution_service/alpaca_broker.py:54-61`
- Modify (tests): `bullforge/tests/test_alpaca_broker.py`

- [ ] **Step 1: Write the failing test**

Open `bullforge/tests/test_alpaca_broker.py` and read the existing `get_account` test (find the block that mocks `account.equity`, `account.cash`, etc.). Extend it — or add a new test adjacent to it — that also asserts `last_equity` is present.

Append this test to the file:

```python
def test_sync_get_account_exposes_last_equity():
    from services.execution_service.alpaca_broker import AlpacaBroker
    from unittest.mock import MagicMock

    broker = AlpacaBroker.__new__(AlpacaBroker)
    fake_account = MagicMock()
    fake_account.equity = "103500.00"
    fake_account.last_equity = "103000.00"
    fake_account.buying_power = "200000.00"
    fake_account.cash = "50000.00"
    fake_account.portfolio_value = "103500.00"

    fake_client = MagicMock()
    fake_client.get_account.return_value = fake_account
    broker._client = fake_client

    result = broker._sync_get_account()
    assert result["equity"] == 103500.00
    assert result["last_equity"] == 103000.00
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest bullforge/tests/test_alpaca_broker.py::test_sync_get_account_exposes_last_equity -v`
Expected: FAIL with `KeyError: 'last_equity'`.

- [ ] **Step 3: Add the field**

Edit `bullforge/services/execution_service/alpaca_broker.py`. Replace the body of `_sync_get_account` (lines 54–61) with:

```python
    def _sync_get_account(self) -> dict:
        account = self._client.get_account()
        return {
            "equity": float(account.equity),
            "last_equity": float(account.last_equity),
            "buying_power": float(account.buying_power),
            "cash": float(account.cash),
            "portfolio_value": float(account.portfolio_value),
        }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest bullforge/tests/test_alpaca_broker.py -v`
Expected: all tests PASS (including existing ones, since adding a key is backward-compatible).

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/execution_service/alpaca_broker.py bullforge/tests/test_alpaca_broker.py
git commit -m "feat(execution): expose Alpaca last_equity in account dict"
```

---

## Task 3: `compute_day_stats` helper

**Files:**
- Create: `bullforge/services/execution_service/day_stats.py`
- Create (tests): `bullforge/tests/test_day_stats.py`

- [ ] **Step 1: Write the failing tests**

Create `bullforge/tests/test_day_stats.py`:

```python
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from services.execution_service.day_stats import compute_day_stats


def _trade(pnl):
    t = MagicMock()
    t.pnl = pnl
    return t


@pytest.mark.asyncio
async def test_compute_day_stats_mixed_wins_losses_breakeven():
    db = AsyncMock()
    db.query_closed_trades = AsyncMock(return_value=[
        _trade(150.0), _trade(-50.0), _trade(0.0), _trade(25.0),
    ])
    broker = AsyncMock()
    broker.get_account = AsyncMock(return_value={
        "equity": 101_025.0, "last_equity": 100_900.0,
    })
    now = datetime(2026, 4, 22, 15, 0, tzinfo=timezone.utc)

    stats = await compute_day_stats(db, broker, now)

    assert stats["equity"] == 101_025.0
    assert stats["last_equity"] == 100_900.0
    assert stats["day_pnl"] == pytest.approx(125.0)
    assert stats["day_pnl_pct"] == pytest.approx(125.0 / 100_900.0)
    assert stats["day_trades"] == 4
    assert stats["day_wins"] == 2
    assert stats["day_losses"] == 1
    assert stats["day_win_rate"] == pytest.approx(2 / 3)
    assert stats["day_realized_pnl"] == pytest.approx(125.0)

    call = db.query_closed_trades.await_args
    assert call.kwargs["since"] == datetime(2026, 4, 22, 0, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_compute_day_stats_handles_zero_last_equity():
    db = AsyncMock()
    db.query_closed_trades = AsyncMock(return_value=[])
    broker = AsyncMock()
    broker.get_account = AsyncMock(return_value={
        "equity": 100.0, "last_equity": 0.0,
    })
    now = datetime(2026, 4, 22, 0, 30, tzinfo=timezone.utc)

    stats = await compute_day_stats(db, broker, now)
    assert stats["day_pnl"] == pytest.approx(100.0)
    assert stats["day_pnl_pct"] == 0.0
    assert stats["day_trades"] == 0
    assert stats["day_wins"] == 0
    assert stats["day_losses"] == 0
    assert stats["day_win_rate"] == 0.0


@pytest.mark.asyncio
async def test_compute_day_stats_no_losses_gives_full_win_rate():
    db = AsyncMock()
    db.query_closed_trades = AsyncMock(return_value=[_trade(10.0), _trade(20.0)])
    broker = AsyncMock()
    broker.get_account = AsyncMock(return_value={
        "equity": 1_000.0, "last_equity": 970.0,
    })
    now = datetime(2026, 4, 22, 15, 0, tzinfo=timezone.utc)

    stats = await compute_day_stats(db, broker, now)
    assert stats["day_win_rate"] == pytest.approx(1.0)
    assert stats["day_wins"] == 2
    assert stats["day_losses"] == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest bullforge/tests/test_day_stats.py -v`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Write the helper**

Create `bullforge/services/execution_service/day_stats.py`:

```python
from __future__ import annotations

from datetime import datetime, timezone

from shared.db import Database
from services.execution_service.alpaca_broker import AlpacaBroker


async def compute_day_stats(
    db: Database, broker: AlpacaBroker, now: datetime,
) -> dict:
    """Return a snapshot of account and trading activity for the current UTC day.

    The 'day' boundary is UTC midnight, matching how closed-trade exit_time is
    stored. This diverges from a regular-hours equities session but keeps the
    query aligned with the TIMESTAMPTZ column.
    """
    account = await broker.get_account()
    equity = float(account.get("equity", 0.0))
    last_equity = float(account.get("last_equity", 0.0))
    day_pnl = equity - last_equity
    day_pnl_pct = (day_pnl / last_equity) if last_equity else 0.0

    day_start = datetime(
        now.year, now.month, now.day, tzinfo=timezone.utc,
    )
    trades = await db.query_closed_trades(since=day_start, limit=1000)

    wins = sum(1 for t in trades if float(t.pnl or 0.0) > 0)
    losses = sum(1 for t in trades if float(t.pnl or 0.0) < 0)
    denom = wins + losses
    win_rate = (wins / denom) if denom else 0.0
    realized = sum(float(t.pnl or 0.0) for t in trades)

    return {
        "equity": equity,
        "last_equity": last_equity,
        "day_pnl": day_pnl,
        "day_pnl_pct": day_pnl_pct,
        "day_trades": len(trades),
        "day_wins": wins,
        "day_losses": losses,
        "day_win_rate": win_rate,
        "day_realized_pnl": realized,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest bullforge/tests/test_day_stats.py -v`
Expected: all three tests PASS.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/execution_service/day_stats.py bullforge/tests/test_day_stats.py
git commit -m "feat(execution): add compute_day_stats helper for account + trade day metrics"
```

---

## Task 4: Publish `position_closed` from ReconcilerCore

**Files:**
- Modify: `bullforge/services/execution_service/reconciler_core.py`
- Modify: `bullforge/services/execution_service/main.py:55`
- Modify (tests): `bullforge/tests/test_reconciler_core.py`

- [ ] **Step 1: Write the failing tests**

Open `bullforge/tests/test_reconciler_core.py`. Add a new fixture and three new tests at the bottom of the file:

```python
# ---------------------------------------------------------------------------
# position_closed alert tests
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_broker():
    b = AsyncMock()
    b.get_account = AsyncMock(return_value={
        "equity": 101_000.0, "last_equity": 100_000.0,
    })
    return b


def _alert_payloads(mock_redis, channel):
    return [
        json.loads(call.args[1])
        for call in mock_redis.publish.await_args_list
        if call.args[0] == channel
    ]


@pytest.mark.asyncio
async def test_closing_fill_publishes_position_closed(
    mock_db, mock_redis, mock_broker,
):
    core = ReconcilerCore(
        db=mock_db, redis_client=mock_redis, broker=mock_broker,
    )
    open_trade = Trade(
        id=42, symbol="SPY", side="long",
        entry_time=datetime(2026, 4, 22, 13, 0, tzinfo=timezone.utc),
        entry_price=100.0, quantity=2.0, entry_order_id=1,
    )
    mock_db.get_open_trade.return_value = open_trade
    mock_db.query_closed_trades.return_value = []
    mock_db.upsert_order_from_alpaca.return_value = UpsertResult(
        order=_order(status="filled", side="sell", filled_price=105.0),
        old_status="submitted", new_status="filled", inserted=False,
    )
    snap = _snapshot(status="filled", side="sell")
    snap["filled_avg_price"] = 105.0

    await core.apply_alpaca_order(snap, source="stream")

    alerts = _alert_payloads(mock_redis, "monitoring:alerts")
    assert len(alerts) == 1
    a = alerts[0]
    assert a["type"] == "position_closed"
    assert a["symbol"] == "SPY"
    assert a["side"] == "long"
    assert a["quantity"] == 2.0
    assert a["entry_price"] == 100.0
    assert a["exit_price"] == 105.0
    assert a["pnl"] == pytest.approx(10.0)
    assert a["pnl_pct"] == pytest.approx(10.0 / (100.0 * 2.0))
    assert a["trade_id"] == 42
    assert a["account"]["equity"] == 101_000.0
    assert a["account"]["day_pnl"] == pytest.approx(1_000.0)


@pytest.mark.asyncio
async def test_opening_fill_does_not_publish_position_closed(
    mock_db, mock_redis, mock_broker,
):
    core = ReconcilerCore(
        db=mock_db, redis_client=mock_redis, broker=mock_broker,
    )
    mock_db.get_open_trade.return_value = None
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="submitted", new_status="filled",
    )
    await core.apply_alpaca_order(_snapshot(status="filled"), source="stream")
    assert _alert_payloads(mock_redis, "monitoring:alerts") == []


@pytest.mark.asyncio
async def test_closing_fill_without_broker_skips_alert(mock_db, mock_redis):
    core = ReconcilerCore(db=mock_db, redis_client=mock_redis, broker=None)
    open_trade = Trade(
        id=1, symbol="SPY", side="long",
        entry_time=datetime(2026, 4, 22, 13, 0, tzinfo=timezone.utc),
        entry_price=100.0, quantity=1.0, entry_order_id=1,
    )
    mock_db.get_open_trade.return_value = open_trade
    mock_db.upsert_order_from_alpaca.return_value = UpsertResult(
        order=_order(status="filled", side="sell", filled_price=101.0),
        old_status="submitted", new_status="filled", inserted=False,
    )
    snap = _snapshot(status="filled", side="sell")
    snap["filled_avg_price"] = 101.0

    await core.apply_alpaca_order(snap, source="stream")
    assert _alert_payloads(mock_redis, "monitoring:alerts") == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest bullforge/tests/test_reconciler_core.py -v -k position_closed`
Expected: all three new tests FAIL — `ReconcilerCore.__init__` rejects `broker=` kwarg, and no `monitoring:alerts` publish happens.

- [ ] **Step 3: Modify ReconcilerCore**

Edit `bullforge/services/execution_service/reconciler_core.py`.

Update the imports at the top:

```python
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from shared.db import Database
from shared.models import Order, Trade
from services.execution_service.day_stats import compute_day_stats

logger = logging.getLogger(__name__)

TERMINAL_STATUSES = {"filled", "cancelled", "rejected", "expired"}

_MONITORING_CHANNEL = "monitoring:reconciler"
_ALERTS_CHANNEL = "monitoring:alerts"
```

Update `__init__`:

```python
    def __init__(self, db: Database, redis_client=None, broker=None):
        self._db = db
        self._redis = redis_client
        self._broker = broker
```

Replace the body of `_handle_fill` (lines 83–123) — keep the existing close/open branches, but capture the closing-branch locals and publish after the DB write:

```python
    async def _handle_fill(self, order: Order) -> None:
        if order.filled_price is None or order.filled_at is None:
            logger.warning(
                "fill event for %s missing price/time; skipping trade bookkeeping",
                order.alpaca_order_id,
            )
            return

        open_trade = await self._db.get_open_trade(order.symbol)

        is_closing = open_trade is not None and (
            (open_trade.side == "long" and order.side == "sell")
            or (open_trade.side == "short" and order.side == "buy")
        )

        if is_closing:
            pnl = (order.filled_price - open_trade.entry_price) * open_trade.quantity
            if open_trade.side == "short":
                pnl = -pnl
            hold_secs = int(
                (order.filled_at - open_trade.entry_time).total_seconds()
            )
            await self._db.update_trade_exit(
                trade_id=open_trade.id,
                exit_time=order.filled_at,
                exit_price=order.filled_price,
                exit_order_id=order.id,
                pnl=pnl,
                hold_duration_seconds=hold_secs,
            )
            await self._publish_position_closed(
                open_trade=open_trade,
                order=order,
                pnl=pnl,
                hold_duration_seconds=hold_secs,
            )
        else:
            trade_side = "long" if order.side == "buy" else "short"
            trade = Trade(
                symbol=order.symbol,
                side=trade_side,
                entry_time=order.filled_at,
                entry_price=order.filled_price,
                quantity=order.filled_quantity or order.quantity,
                entry_order_id=order.id,
            )
            await self._db.insert_trade(trade)
```

Add a new method at the bottom of the class, before `_publish`:

```python
    async def _publish_position_closed(
        self,
        *,
        open_trade: Trade,
        order: Order,
        pnl: float,
        hold_duration_seconds: int,
    ) -> None:
        if self._redis is None or self._broker is None:
            return
        try:
            day = await compute_day_stats(
                self._db, self._broker, datetime.now(timezone.utc),
            )
            notional = open_trade.entry_price * open_trade.quantity
            pnl_pct = (pnl / notional) if notional else 0.0
            payload = {
                "type": "position_closed",
                "tier": 0,
                "trade_id": open_trade.id,
                "symbol": open_trade.symbol,
                "side": open_trade.side,
                "quantity": float(open_trade.quantity),
                "entry_time": _iso(open_trade.entry_time),
                "entry_price": float(open_trade.entry_price),
                "exit_time": _iso(order.filled_at),
                "exit_price": float(order.filled_price),
                "pnl": float(pnl),
                "pnl_pct": float(pnl_pct),
                "hold_duration_seconds": int(hold_duration_seconds),
                "account": day,
            }
            await self._redis.publish(_ALERTS_CHANNEL, json.dumps(payload))
        except Exception:
            logger.exception("Failed to publish position_closed alert")
```

Add this module-level helper at the bottom of the file (after the class definition):

```python
def _iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")
```

- [ ] **Step 4: Wire the broker into ReconcilerCore at the service entrypoint**

Edit `bullforge/services/execution_service/main.py` line 55. Change:

```python
    core = ReconcilerCore(db=db, redis_client=redis_client)
```

to:

```python
    core = ReconcilerCore(db=db, redis_client=redis_client, broker=broker)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest bullforge/tests/test_reconciler_core.py -v`
Expected: all tests PASS (new and existing). The pre-existing closing-fill test still passes because `broker` defaults to `None` in that fixture's `core`.

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/execution_service/reconciler_core.py bullforge/services/execution_service/main.py bullforge/tests/test_reconciler_core.py
git commit -m "feat(execution): publish position_closed alert on closing fills"
```

---

## Task 5: Format `position_closed` alerts in Telegram

**Files:**
- Modify: `bullforge/services/monitoring_service/telegram_alerts.py`
- Modify (tests): `bullforge/tests/test_telegram_alerts.py`

- [ ] **Step 1: Write the failing tests**

Append to `bullforge/tests/test_telegram_alerts.py`:

```python
def _close_alert(**overrides):
    base = {
        "type": "position_closed",
        "tier": 0,
        "trade_id": 12,
        "symbol": "AAPL",
        "side": "long",
        "quantity": 100.0,
        "entry_time": "2026-04-22 13:31:15",
        "entry_price": 187.42,
        "exit_time":  "2026-04-22 15:45:03",
        "exit_price": 189.18,
        "pnl": 176.00,
        "pnl_pct": 0.0094,
        "hold_duration_seconds": 8028,
        "account": {
            "equity": 104_326.12,
            "last_equity": 103_900.00,
            "day_pnl": 426.12,
            "day_pnl_pct": 0.0041,
            "day_trades": 4,
            "day_wins": 3,
            "day_losses": 1,
            "day_win_rate": 0.75,
            "day_realized_pnl": 352.00,
        },
    }
    base.update(overrides)
    return base


def test_format_position_closed_winning_trade():
    text, buttons = format_alert(_close_alert())
    assert "TRADE CLOSED" in text
    assert "AAPL long" in text
    assert "+$176.00" in text
    assert "+0.94%" in text
    assert "2026-04-22 13:31:15" in text
    assert "2026-04-22 15:45:03" in text
    assert "2h 13m 48s" in text
    assert "Equity:" in text
    assert "$104,326.12" in text
    assert "+$426.12" in text
    assert "Realized:" in text
    assert "+$352.00" in text
    assert "4 trades" in text
    assert "Win rate:" in text
    assert "75.0%" in text
    assert "3W / 1L" in text
    assert buttons == []


def test_format_position_closed_losing_trade():
    text, _ = format_alert(_close_alert(pnl=-45.50, pnl_pct=-0.0024))
    assert "-$45.50" in text
    assert "-0.24%" in text


def test_format_position_closed_no_day_trades_yet():
    text, _ = format_alert(_close_alert(account={
        "equity": 100_000.0, "last_equity": 100_000.0,
        "day_pnl": 0.0, "day_pnl_pct": 0.0,
        "day_trades": 0, "day_wins": 0, "day_losses": 0,
        "day_win_rate": 0.0, "day_realized_pnl": 0.0,
    }))
    # With 0 day trades, realized and win-rate lines are omitted.
    assert "Realized:" not in text
    assert "Win rate:" not in text
    # Equity line still present.
    assert "Equity:" in text


def test_format_position_closed_omits_win_rate_when_only_breakevens():
    text, _ = format_alert(_close_alert(account={
        "equity": 100_000.0, "last_equity": 100_000.0,
        "day_pnl": 0.0, "day_pnl_pct": 0.0,
        "day_trades": 1, "day_wins": 0, "day_losses": 0,
        "day_win_rate": 0.0, "day_realized_pnl": 0.0,
    }))
    assert "Realized:" in text     # 1 trade — show realized line
    assert "Win rate:" not in text  # 0W + 0L denominator — hide win-rate line


def test_format_position_closed_short_duration():
    text, _ = format_alert(_close_alert(hold_duration_seconds=42))
    assert "42s" in text
    assert "m " not in text.split("Hold:")[1].splitlines()[0]  # no minutes segment
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest bullforge/tests/test_telegram_alerts.py -v -k position_closed`
Expected: FAIL — current `format_alert` routes the new type to the `[INFO]` default branch.

- [ ] **Step 3: Implement the formatter**

Edit `bullforge/services/monitoring_service/telegram_alerts.py`.

Move the existing `from datetime import datetime, timezone` import (currently at line 114) up to the top of the file with the other imports. This matches the top-of-file import convention and avoids re-importing when we add helpers.

Add these two module-level helpers near the top, below the `ButtonLayout` type alias:

```python
def _format_signed_money(value: float) -> str:
    sign = "+" if value >= 0 else "-"
    return f"{sign}${abs(value):,.2f}"


def _format_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"
```

Add a `position_closed` branch in `format_alert`. Insert it **before** the `else` branch:

```python
    elif alert_type == "position_closed":
        symbol = alert.get("symbol", "?")
        side = alert.get("side", "?")
        qty = float(alert.get("quantity") or 0.0)
        entry_time = alert.get("entry_time") or "--"
        exit_time = alert.get("exit_time") or "--"
        entry_price = float(alert.get("entry_price") or 0.0)
        exit_price = float(alert.get("exit_price") or 0.0)
        pnl = float(alert.get("pnl") or 0.0)
        pnl_pct = float(alert.get("pnl_pct") or 0.0) * 100.0
        hold_s = int(alert.get("hold_duration_seconds") or 0)
        account = alert.get("account") or {}

        lines = [
            f"[TRADE CLOSED] {symbol} {side}",
            f"Entry: {entry_time} UTC @ ${entry_price:,.2f}",
            f"Exit:  {exit_time} UTC @ ${exit_price:,.2f}",
            f"Qty:   {qty:g}",
            f"P&L:   {_format_signed_money(pnl)} ({pnl_pct:+.2f}%)",
            f"Hold:  {_format_duration(hold_s)}",
        ]

        if account:
            equity = float(account.get("equity") or 0.0)
            day_pnl = float(account.get("day_pnl") or 0.0)
            day_pnl_pct = float(account.get("day_pnl_pct") or 0.0) * 100.0
            day_trades = int(account.get("day_trades") or 0)
            day_wins = int(account.get("day_wins") or 0)
            day_losses = int(account.get("day_losses") or 0)
            day_realized = float(account.get("day_realized_pnl") or 0.0)

            lines.append("")
            lines.append("Account (today, UTC)")
            lines.append(
                f"Equity:     ${equity:,.2f}   "
                f"({_format_signed_money(day_pnl)}, {day_pnl_pct:+.2f}%)"
            )
            if day_trades > 0:
                plural = "trade" if day_trades == 1 else "trades"
                lines.append(
                    f"Realized:   {_format_signed_money(day_realized)} "
                    f"on {day_trades} {plural}"
                )
            if day_wins + day_losses > 0:
                rate = float(account.get("day_win_rate") or 0.0) * 100.0
                lines.append(
                    f"Win rate:   {rate:.1f}% ({day_wins}W / {day_losses}L)"
                )

        text = "\n".join(lines)
        buttons = []
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest bullforge/tests/test_telegram_alerts.py -v`
Expected: all tests PASS. Existing alerts' tests must still pass.

- [ ] **Step 5: Verify `TelegramBot.send_alert` handles empty buttons**

Read `bullforge/services/monitoring_service/telegram_bot.py` lines 53–62 (`_build_keyboard`) and 103–110 (`send_alert`). Confirm that `InlineKeyboardMarkup([])` is accepted by `bot.send_message(reply_markup=...)`. It is — `InlineKeyboardMarkup` accepts an empty list. No change needed here.

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/monitoring_service/telegram_alerts.py bullforge/tests/test_telegram_alerts.py
git commit -m "feat(telegram): format position_closed alerts with trade P&L and day stats"
```

---

## Task 6: Redis pub/sub reference doc

**Files:**
- Create: `docs/architecture/redis-pubsub.md`

- [ ] **Step 1: Write the doc**

Create `docs/architecture/redis-pubsub.md` with the content below. The channel list was built from a full-repo scan of `.publish(` and `.subscribe(` calls; if any new channel shows up while implementing the plan, add it here before committing.

```markdown
# Redis Pub/Sub Channels

**This document is the source of truth.** Any PR that adds, removes, renames, or changes the payload of a Redis `publish`/`subscribe` call must update this table in the same commit.

All channels flow through the shared `RedisClient` in [bullforge/shared/redis.py](../../bullforge/shared/redis.py). Messages are JSON-encoded strings unless otherwise noted.

## Channels

| Channel | Publisher(s) | Subscriber(s) | Payload | Purpose |
|---|---|---|---|---|
| `model:predictions` | `services/model_service/main.py` | `services/execution_service/main.py` (`on_prediction`) | `{symbol, timeframe, predictions: [...]}` | Per-model predictions fan out to execution for signal aggregation. |
| `models:status` | `services/monitoring_service/halt_manager.py`; `services/execution_service/main.py` | `services/execution_service/main.py` (`on_model_status`) | `{model_id: "active"|"warned"|"disabled", ...}` | Model enable/disable state; execution filters signals from disabled models. |
| `market:ticks` | `services/data_pipeline/stream.py` | `services/feature_engine/main.py` | `{symbol, timeframe, timestamp, bar}` | Live bar updates trigger reactive feature computation. |
| `feature:updates` | `services/feature_engine/engine.py` | `services/model_service/main.py` | `{symbol, timeframe, timestamp}` | Feature write completion triggers inference. |
| `execution:orders` | `services/execution_service/reconciler_core.py` | *(none currently)* | `{symbol, side, quantity, status, filled_price, alpaca_order_id, origin}` | Order status transitions for logging/monitoring. |
| `execution:orders:anomaly` | `services/execution_service/reconciler_core.py` | *(none currently)* | Same shape as `execution:orders`. | Orders that transitioned `filled → cancelled/rejected/expired`. |
| `monitoring:reconciler` | `services/execution_service/reconciler_core.py` | *(none currently)* | `{counter: str, labels: {...}, value: int}` | Reconciler counter metrics. |
| `monitoring:data_pipeline` | `services/data_pipeline/backfill.py` | *(none currently)* | `{counter: str, labels: {...}, value: int}` | Data pipeline counter metrics. |
| `monitoring:alerts` | `services/monitoring_service/main.py` (halt/fidelity alerts); `services/execution_service/reconciler_core.py` (`position_closed`) | `services/monitoring_service/telegram_bot.py` (`_on_alert`) | See "Alert types" below. | Operator-facing alerts. The Telegram bot filters a subset (see `TELEGRAM_SUPPRESSED_ALERT_TYPES`). |
| `request:broker` | `services/monitoring_service/broker_client.py` | `services/execution_service/broker_relay.py` | `{correlation_id, method, args}` | RPC request: broker queries from monitoring → execution. |
| `response:broker:{correlation_id}` | `services/execution_service/broker_relay.py` | `services/monitoring_service/broker_client.py` (one-shot per request) | `{result}` or `{error}` | RPC response matched by correlation id. |
| `config:symbols_changed` | `services/dashboard_service/symbols_api.py` | `services/data_pipeline/main.py` | `{equity: [...], crypto: [...]}` | Live trading-symbol list change from the dashboard UI. |

## Alert types on `monitoring:alerts`

| Type | Source | Telegram delivery | Shape |
|---|---|---|---|
| `model_warned` | `HaltManager.evaluate` | **Suppressed** | `{type, tier: 1, model_id, reason}` |
| `model_disabled` | `HaltManager.evaluate` | **Suppressed** | `{type, tier: 2, model_id, reason}` |
| `direction_halt` | `HaltManager.evaluate` | Delivered | `{type, tier: 3, reason}` |
| `drawdown_halt` | `HaltManager.evaluate` | Delivered | `{type, tier: 4, reason}` |
| `majority_halt` | `HaltManager.evaluate` | Delivered | `{type, tier: 4, reason}` |
| `position_closed` | `ReconcilerCore._handle_fill` | Delivered | `{type, tier: 0, trade_id, symbol, side, quantity, entry_time, entry_price, exit_time, exit_price, pnl, pnl_pct, hold_duration_seconds, account: {equity, last_equity, day_pnl, day_pnl_pct, day_trades, day_wins, day_losses, day_win_rate, day_realized_pnl}}` |

Suppression is configured at the bot level: `TELEGRAM_SUPPRESSED_ALERT_TYPES` in [bullforge/services/monitoring_service/telegram_bot.py](../../bullforge/services/monitoring_service/telegram_bot.py). Suppressed alerts still flow on Redis for other consumers.
```

- [ ] **Step 2: Verify by grep that every `.publish(` call has a row**

Run: `grep -rn "\.publish(" bullforge/services/ bullforge/shared/ | grep -v "test_" | grep -v "\.publish_status\|\.publish_counter\|publish_ohlc"`
Expected: every channel string that appears as the first argument to `.publish(` is listed in the table above.

Also: `grep -rn "\.subscribe(" bullforge/services/ | grep -v "test_"`
Expected: every channel listed on the subscribe side appears somewhere in the table.

If any channel is missing from the table, add a row before committing.

- [ ] **Step 3: Commit**

```bash
git add docs/architecture/redis-pubsub.md
git commit -m "docs(architecture): add Redis pub/sub channel reference"
```

---

## Task 7: Final verification

**Files:** none — verification only.

- [ ] **Step 1: Run the full affected test suite**

Run:
```bash
pytest bullforge/tests/test_telegram_bot.py bullforge/tests/test_telegram_alerts.py bullforge/tests/test_reconciler_core.py bullforge/tests/test_alpaca_broker.py bullforge/tests/test_day_stats.py -v
```
Expected: all PASS.

- [ ] **Step 2: Smoke-check unrelated tests did not regress**

Run: `pytest bullforge/tests/ -x --ignore=bullforge/tests/integration -q`
Expected: all PASS (or the same set that was passing before on the `master` branch tip).

If any previously-passing test fails, investigate before concluding the task is done.

- [ ] **Step 3: Confirm no stray `model_warned`/`model_disabled` handling in UI copy**

Run: `grep -rn "model_warned\|model_disabled" bullforge/ docs/ | grep -v "test_\|\.git\|\.pyc"`
Expected: references are confined to `halt_manager.py` (emission), `telegram_alerts.py` (format, left intact for non-Telegram consumers), `telegram_bot.py` (suppression set), `day_stats.py`/`reconciler_core.py` (none), `docs/architecture/redis-pubsub.md` (table rows). No orphan references.

- [ ] **Step 4: Final commit if any fix-ups were needed**

If Steps 1–3 surfaced small adjustments, commit them as a follow-up:

```bash
git add -A
git commit -m "fix: address verification findings from telegram alert work"
```

Otherwise, done.
