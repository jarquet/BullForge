# Phase 6C — Telegram Bot Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand the Telegram bot from 2 commands to 6 (`/status` enriched, `/halt`, `/resume`, `/positions`, `/trades`, `/models`). Use a Redis request/reply pattern so monitoring_service never touches Alpaca keys.

**Architecture:** New `BrokerRelay` inside execution_service subscribes to `request:broker` and publishes responses to `response:broker:{correlation_id}`. Bot publishes requests with a UUID correlation_id and awaits the reply with a 5-second timeout. Rich message formatting lives in `telegram_alerts.py`.

**Tech Stack:** `python-telegram-bot` (existing), Redis pub/sub, asyncio `asyncio.wait_for`, uuid.

**Depends on:** Phase 6A (RuntimeConfig for `last_retrain_time`). Independent of 6B/6D.

---

## File Structure

**Create:**
- `bullforge/services/execution_service/broker_relay.py`
- `bullforge/services/monitoring_service/broker_client.py` — Redis-based broker request/reply client
- `bullforge/tests/test_broker_relay.py`
- `bullforge/tests/test_broker_client.py`
- `bullforge/tests/test_telegram_commands.py`

**Modify:**
- `bullforge/services/execution_service/main.py` — start `BrokerRelay` background task
- `bullforge/services/monitoring_service/telegram_bot.py` — register new command handlers
- `bullforge/services/monitoring_service/telegram_alerts.py` — add `format_status_expanded`, `format_positions`, `format_trades`, `format_models_summary`, `format_halt_confirmation`, `format_resume_confirmation`
- `bullforge/services/monitoring_service/main.py` — pass RuntimeConfig + broker_client to bot
- `bullforge/shared/db.py` — add `query_closed_trades(limit)` if missing

---

## Task 1: BrokerRelay in execution_service

**Files:**
- Create: `bullforge/services/execution_service/broker_relay.py`
- Test: `bullforge/tests/test_broker_relay.py`

- [ ] **Step 1: Write failing test**

```python
# bullforge/tests/test_broker_relay.py
import json
import pytest
from unittest.mock import AsyncMock
from services.execution_service.broker_relay import BrokerRelay


@pytest.mark.asyncio
async def test_handle_get_positions():
    redis = AsyncMock()
    broker = AsyncMock()
    broker.list_positions = AsyncMock(return_value=[
        {"symbol": "SPY", "qty": 10, "avg_entry_price": 420.0,
         "current_price": 421.0, "market_value": 4210.0, "unrealized_pl": 10.0},
    ])
    relay = BrokerRelay(redis_client=redis, broker=broker)

    msg = json.dumps({"method": "get_positions", "correlation_id": "abc-123"})
    await relay.handle_request(msg)

    redis.publish.assert_awaited_once()
    channel, payload = redis.publish.call_args[0]
    assert channel == "response:broker:abc-123"
    data = json.loads(payload)
    assert data["ok"] is True
    assert data["result"][0]["symbol"] == "SPY"


@pytest.mark.asyncio
async def test_handle_get_account():
    redis = AsyncMock()
    broker = AsyncMock()
    broker.get_account = AsyncMock(return_value={
        "equity": 100000.0, "buying_power": 200000.0,
        "cash": 20000.0, "portfolio_value": 100000.0,
    })
    relay = BrokerRelay(redis_client=redis, broker=broker)
    msg = json.dumps({"method": "get_account", "correlation_id": "xyz"})
    await relay.handle_request(msg)

    _, payload = redis.publish.call_args[0]
    data = json.loads(payload)
    assert data["result"]["equity"] == 100000.0


@pytest.mark.asyncio
async def test_unknown_method_returns_error():
    redis = AsyncMock()
    broker = AsyncMock()
    relay = BrokerRelay(redis_client=redis, broker=broker)
    msg = json.dumps({"method": "delete_everything", "correlation_id": "c1"})
    await relay.handle_request(msg)
    data = json.loads(redis.publish.call_args[0][1])
    assert data["ok"] is False
    assert "unknown method" in data["error"].lower()


@pytest.mark.asyncio
async def test_broker_exception_surfaces_as_error():
    redis = AsyncMock()
    broker = AsyncMock()
    broker.get_account = AsyncMock(side_effect=Exception("alpaca 503"))
    relay = BrokerRelay(redis_client=redis, broker=broker)
    msg = json.dumps({"method": "get_account", "correlation_id": "c2"})
    await relay.handle_request(msg)
    data = json.loads(redis.publish.call_args[0][1])
    assert data["ok"] is False
    assert "alpaca 503" in data["error"]
```

- [ ] **Step 2: Run, fail**

Run: `cd bullforge && pytest tests/test_broker_relay.py -v`
Expected: FAIL

- [ ] **Step 3: Implement BrokerRelay**

```python
# bullforge/services/execution_service/broker_relay.py
from __future__ import annotations

import json
import logging
from typing import Any, Callable

from shared.redis import RedisClient

logger = logging.getLogger(__name__)


class BrokerRelay:
    """
    Subscribes to `request:broker` and proxies method calls to the AlpacaBroker,
    publishing responses to `response:broker:{correlation_id}`.

    This pattern lets monitoring_service query broker state without Alpaca keys.
    """

    def __init__(self, redis_client: RedisClient, broker):
        self._redis = redis_client
        self._broker = broker
        self._methods: dict[str, Callable] = {
            "get_positions": self._get_positions,
            "get_account": self._get_account,
        }

    async def _get_positions(self, params: dict) -> Any:
        return await self._broker.list_positions()

    async def _get_account(self, params: dict) -> Any:
        return await self._broker.get_account()

    async def start(self) -> None:
        await self._redis.subscribe("request:broker", self.handle_request)
        logger.info("BrokerRelay subscribed to request:broker")

    async def handle_request(self, message: str) -> None:
        try:
            data = json.loads(message)
            method = data["method"]
            cid = data["correlation_id"]
            params = data.get("params", {})
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("malformed broker request: %s", e)
            return

        handler = self._methods.get(method)
        channel = f"response:broker:{cid}"
        if handler is None:
            await self._redis.publish(channel, json.dumps({
                "ok": False, "error": f"unknown method: {method}",
            }))
            return

        try:
            result = await handler(params)
            await self._redis.publish(channel, json.dumps({
                "ok": True, "result": result,
            }, default=str))
        except Exception as e:
            logger.exception("broker call %s failed", method)
            await self._redis.publish(channel, json.dumps({
                "ok": False, "error": str(e),
            }))
```

- [ ] **Step 4: Wire into execution_service/main.py**

```python
from services.execution_service.broker_relay import BrokerRelay
# after broker and redis are ready:
broker_relay = BrokerRelay(redis_client=redis_client, broker=broker)
await broker_relay.start()
```

- [ ] **Step 5: Run tests, PASS; commit**

```bash
git add bullforge/services/execution_service/broker_relay.py bullforge/services/execution_service/main.py bullforge/tests/test_broker_relay.py
git commit -m "feat: BrokerRelay proxies broker methods over Redis request/reply"
```

---

## Task 2: BrokerClient in monitoring_service

**Files:**
- Create: `bullforge/services/monitoring_service/broker_client.py`
- Test: `bullforge/tests/test_broker_client.py`

- [ ] **Step 1: Write failing test**

```python
# bullforge/tests/test_broker_client.py
import asyncio
import json
import uuid
import pytest
from unittest.mock import AsyncMock, patch

from services.monitoring_service.broker_client import BrokerClient


@pytest.mark.asyncio
async def test_get_account_round_trip():
    redis = AsyncMock()
    subscribed = {}

    async def fake_subscribe(channel, handler):
        subscribed[channel] = handler

    redis.subscribe = fake_subscribe
    redis.unsubscribe = AsyncMock()

    async def fake_publish(channel, payload):
        data = json.loads(payload)
        cid = data["correlation_id"]
        # Simulate relay responding on the correlation channel
        await subscribed[f"response:broker:{cid}"](
            json.dumps({"ok": True, "result": {"equity": 123.0}})
        )
    redis.publish = fake_publish

    client = BrokerClient(redis_client=redis, timeout=1.0)
    result = await client.get_account()
    assert result == {"equity": 123.0}


@pytest.mark.asyncio
async def test_timeout_raises_broker_unavailable():
    redis = AsyncMock()
    redis.subscribe = AsyncMock()
    redis.unsubscribe = AsyncMock()
    redis.publish = AsyncMock()  # never triggers a response
    client = BrokerClient(redis_client=redis, timeout=0.1)
    with pytest.raises(TimeoutError):
        await client.get_account()
```

- [ ] **Step 2: Implement**

```python
# bullforge/services/monitoring_service/broker_client.py
from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

from shared.redis import RedisClient


class BrokerClient:
    """Issues broker requests over Redis and awaits correlated response."""

    def __init__(self, redis_client: RedisClient, timeout: float = 5.0):
        self._redis = redis_client
        self._timeout = timeout

    async def _call(self, method: str, **params: Any) -> Any:
        cid = str(uuid.uuid4())
        response_channel = f"response:broker:{cid}"
        fut: asyncio.Future = asyncio.get_running_loop().create_future()

        async def handler(msg: str) -> None:
            if not fut.done():
                fut.set_result(json.loads(msg))

        await self._redis.subscribe(response_channel, handler)
        try:
            await self._redis.publish("request:broker", json.dumps({
                "method": method, "correlation_id": cid, "params": params,
            }))
            try:
                data = await asyncio.wait_for(fut, timeout=self._timeout)
            except asyncio.TimeoutError:
                raise TimeoutError(f"broker relay timeout after {self._timeout}s")
        finally:
            await self._redis.unsubscribe(response_channel)

        if not data.get("ok"):
            raise RuntimeError(f"broker error: {data.get('error')}")
        return data["result"]

    async def get_positions(self) -> list[dict]:
        return await self._call("get_positions")

    async def get_account(self) -> dict:
        return await self._call("get_account")
```

- [ ] **Step 3: Run, PASS; commit**

```bash
git add bullforge/services/monitoring_service/broker_client.py bullforge/tests/test_broker_client.py
git commit -m "feat: BrokerClient for monitoring_service (Redis-based broker RPC)"
```

---

## Task 3: `query_closed_trades` DB method (if missing)

**Files:**
- Modify: `bullforge/shared/db.py`

- [ ] **Step 1: Check existence**

Run: `cd bullforge && grep -n "def query_closed_trades\|def get_recent_trades" bullforge/shared/db.py`

If missing, add:

```python
async def query_closed_trades(self, limit: int = 10) -> list[dict]:
    async with self.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, symbol, side, entry_time, entry_price, quantity,
                   exit_time, exit_price, pnl, hold_duration_seconds
            FROM trades
            WHERE exit_time IS NOT NULL
            ORDER BY exit_time DESC
            LIMIT $1
            """,
            limit,
        )
    return [dict(r) for r in rows]
```

- [ ] **Step 2: Commit**

---

## Task 4: Format functions in telegram_alerts.py

**Files:**
- Modify: `bullforge/services/monitoring_service/telegram_alerts.py`
- Test: `bullforge/tests/test_telegram_alerts_phase6c.py`

- [ ] **Step 1: Write pure-function tests**

```python
# bullforge/tests/test_telegram_alerts_phase6c.py
from datetime import datetime, timedelta, timezone
from services.monitoring_service.telegram_alerts import (
    format_status_expanded, format_positions, format_trades,
    format_models_summary,
)


def test_format_positions_empty():
    assert "No open positions" in format_positions([])


def test_format_positions_lists_each():
    positions = [
        {"symbol": "SPY", "qty": 10, "avg_entry_price": 420.0,
         "current_price": 421.0, "market_value": 4210.0, "unrealized_pl": 10.0},
    ]
    s = format_positions(positions)
    assert "SPY" in s
    assert "10" in s
    assert "421.0" in s


def test_format_trades_empty():
    assert "No recent trades" in format_trades([])


def test_format_models_summary_groups_by_status():
    models = [
        {"label": "gradient_boost · SPY · 1m · 5m", "status": "active",
         "metrics": {"accuracy": 0.62}, "trained_at": datetime.now(timezone.utc) - timedelta(hours=14)},
        {"label": "gradient_boost · QQQ · 1m · 30m", "status": "warned",
         "metrics": {"accuracy": 0.48}, "trained_at": datetime.now(timezone.utc) - timedelta(hours=38)},
    ]
    s = format_models_summary(models)
    assert "Active" in s
    assert "Warned" in s
    assert "SPY" in s
    assert "QQQ" in s


def test_format_status_expanded_includes_all_sections():
    s = format_status_expanded(
        account={"equity": 100000.0, "cash": 50000.0, "buying_power": 200000.0},
        positions_count=3, exposure_pct=0.284,
        trading_stats={"trades_today": 12, "win_rate": 0.67, "pnl_today": 312.4},
        model_counts={"active": 8, "warned": 1, "disabled": 0},
        freshness_summary="All data < 2m old | All models < 24h",
        active_halts_count=0,
        last_retrain_iso="2026-04-13T01:00:00+00:00",
        now=datetime(2026, 4, 13, 15, 0, tzinfo=timezone.utc),
    )
    assert "Account" in s
    assert "Positions" in s
    assert "Models" in s
    assert "2026-04-13" in s
```

- [ ] **Step 2: Implement formatters**

```python
# Append to bullforge/services/monitoring_service/telegram_alerts.py
from datetime import datetime, timezone


def format_positions(positions: list[dict]) -> str:
    if not positions:
        return "No open positions."
    lines = [f"Open Positions ({len(positions)})", ""]
    for p in positions:
        side = "LONG" if float(p.get("qty", 0)) > 0 else "SHORT"
        pnl = float(p.get("unrealized_pl", 0.0))
        cur = float(p.get("current_price", 0.0))
        avg = float(p.get("avg_entry_price", 0.0))
        pct = (pnl / (avg * abs(float(p.get("qty", 1))))) * 100 if avg > 0 else 0.0
        lines.append(f"{p['symbol']}: {p['qty']} {side} @ ${avg:.2f}")
        lines.append(f"  Current: ${cur:.2f} | P&L: {pnl:+.2f} ({pct:+.2f}%)")
        lines.append("")
    return "\n".join(lines).rstrip()


def format_trades(trades: list[dict]) -> str:
    if not trades:
        return "No recent trades."
    lines = [f"Recent Trades (last {len(trades)})", ""]
    for t in trades:
        side = "BUY" if t["side"] == "long" else "SELL"
        exit_side = "SELL" if t["side"] == "long" else "BUY"
        hold_min = (t.get("hold_duration_seconds") or 0) // 60
        age = datetime.now(timezone.utc) - t["exit_time"] if t.get("exit_time") else None
        age_str = f"{age.total_seconds() / 3600:.1f}h ago" if age else ""
        lines.append(
            f"{t['symbol']} {side} {t['quantity']:g} @ ${t['entry_price']:.2f} "
            f"-> {exit_side} @ ${t['exit_price']:.2f}"
        )
        lines.append(f"  P&L: {t['pnl']:+.2f} | Hold: {hold_min}m | {age_str}")
        lines.append("")
    return "\n".join(lines).rstrip()


def format_models_summary(models: list[dict]) -> str:
    groups: dict[str, list[dict]] = {"active": [], "warned": [], "disabled": []}
    for m in models:
        groups.setdefault(m["status"], []).append(m)
    lines = [f"Models ({len(models)} total)", ""]
    for status_key, label in [("active", "Active"), ("warned", "Warned"), ("disabled", "Disabled")]:
        group = groups.get(status_key, [])
        if not group:
            continue
        lines.append(f"{label} ({len(group)}):")
        for m in group:
            acc = m.get("metrics", {}).get("accuracy")
            acc_s = f"acc: {acc:.2f}" if acc is not None else "acc: n/a"
            age_h = (datetime.now(timezone.utc) - m["trained_at"]).total_seconds() / 3600
            lines.append(f"  {m['label']} - {acc_s} | {age_h:.0f}h old")
        lines.append("")
    return "\n".join(lines).rstrip()


def format_status_expanded(
    *,
    account: dict,
    positions_count: int,
    exposure_pct: float,
    trading_stats: dict,
    model_counts: dict,
    freshness_summary: str,
    active_halts_count: int,
    last_retrain_iso: str,
    now: datetime,
) -> str:
    equity = account.get("equity", 0.0)
    last_retrain = last_retrain_iso or "never"
    if last_retrain_iso:
        try:
            last_dt = datetime.fromisoformat(last_retrain_iso)
            age_h = (now - last_dt).total_seconds() / 3600
            last_retrain = f"{last_retrain_iso} ({age_h:.0f}h ago)"
        except ValueError:
            pass
    halts_str = f"{active_halts_count} active" if active_halts_count else "None active"
    return (
        "BullForge Status\n\n"
        f"Account: ${equity:,.2f} equity\n"
        f"Positions: {positions_count} open | ${equity * exposure_pct:,.0f} exposure "
        f"({exposure_pct * 100:.1f}%)\n"
        f"Trading: {trading_stats.get('trades_today', 0)} trades today | "
        f"{trading_stats.get('win_rate', 0) * 100:.0f}% win rate | "
        f"{trading_stats.get('pnl_today', 0):+.2f} P&L\n\n"
        f"Models: {model_counts.get('active', 0)} active | "
        f"{model_counts.get('warned', 0)} warned | "
        f"{model_counts.get('disabled', 0)} disabled\n"
        f"Freshness: {freshness_summary}\n"
        f"Halts: {halts_str}\n\n"
        f"Last retrain: {last_retrain}"
    )


def format_halt_confirmation() -> str:
    return "⚠️ Halt ALL trading? This will stop accepting new signals."


def format_resume_confirmation() -> str:
    return "Resume trading? This clears all active halts."
```

- [ ] **Step 3: Run tests, PASS; commit**

```bash
git add bullforge/services/monitoring_service/telegram_alerts.py bullforge/tests/test_telegram_alerts_phase6c.py
git commit -m "feat: telegram formatters for status/positions/trades/models"
```

---

## Task 5: New command handlers in telegram_bot.py

**Files:**
- Modify: `bullforge/services/monitoring_service/telegram_bot.py`
- Modify: `bullforge/services/monitoring_service/main.py`
- Test: `bullforge/tests/test_telegram_commands.py`

- [ ] **Step 1: Read current telegram_bot.py**

Run: `cat bullforge/services/monitoring_service/telegram_bot.py` — note how `/start` and `/status` are registered, what DI they receive (db, halt_manager).

- [ ] **Step 2: Write failing tests for new command handlers**

```python
# bullforge/tests/test_telegram_commands.py
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from services.monitoring_service.telegram_bot import (
    handle_status, handle_positions, handle_trades,
    handle_models, handle_halt, handle_resume,
)


@pytest.fixture
def context():
    return MagicMock()


@pytest.fixture
def update():
    u = MagicMock()
    u.effective_chat.id = 99
    u.message.reply_text = AsyncMock()
    u.message.reply_markdown = AsyncMock()
    return u


@pytest.fixture
def deps():
    db = AsyncMock()
    db.query_latest_metric_snapshot = AsyncMock(return_value={
        "metrics": {"trades_today": 5, "win_rate": 0.6, "pnl_today": 100.0},
    })
    db.get_all_model_statuses = AsyncMock(return_value=[])
    db.query_latest_bar_timestamps = AsyncMock(return_value=[])
    db.query_latest_feature_timestamps = AsyncMock(return_value=[])
    db.get_active_halts = AsyncMock(return_value=[])
    db.query_model_details = AsyncMock(return_value=[])
    db.query_closed_trades = AsyncMock(return_value=[])

    broker_client = AsyncMock()
    broker_client.get_account = AsyncMock(return_value={
        "equity": 100000.0, "cash": 20000.0, "buying_power": 200000.0,
    })
    broker_client.get_positions = AsyncMock(return_value=[])

    rc = AsyncMock()
    rc.get = AsyncMock(return_value="2026-04-13T01:00:00+00:00")

    halt_manager = MagicMock()
    halt_manager.trigger_halt = AsyncMock()
    halt_manager.resolve_halt = AsyncMock()

    return dict(db=db, broker_client=broker_client,
                runtime_config=rc, halt_manager=halt_manager)


@pytest.mark.asyncio
async def test_status_happy_path(update, context, deps):
    await handle_status(update, context, **deps)
    update.message.reply_text.assert_awaited_once()
    sent = update.message.reply_text.call_args[0][0]
    assert "BullForge Status" in sent


@pytest.mark.asyncio
async def test_positions_empty(update, context, deps):
    await handle_positions(update, context, **deps)
    sent = update.message.reply_text.call_args[0][0]
    assert "No open positions" in sent


@pytest.mark.asyncio
async def test_positions_broker_timeout(update, context, deps):
    deps["broker_client"].get_positions = AsyncMock(side_effect=TimeoutError())
    await handle_positions(update, context, **deps)
    sent = update.message.reply_text.call_args[0][0]
    assert "Execution service unavailable" in sent


@pytest.mark.asyncio
async def test_halt_triggers_manager(update, context, deps):
    await handle_halt(update, context, **deps)
    deps["halt_manager"].trigger_halt.assert_awaited()
    sent = update.message.reply_text.call_args[0][0]
    assert "halted" in sent.lower()


@pytest.mark.asyncio
async def test_resume_clears_halts(update, context, deps):
    await handle_resume(update, context, **deps)
    deps["halt_manager"].resolve_halt.assert_awaited()


@pytest.mark.asyncio
async def test_trades_empty(update, context, deps):
    await handle_trades(update, context, **deps)
    sent = update.message.reply_text.call_args[0][0]
    assert "No recent trades" in sent


@pytest.mark.asyncio
async def test_models_command(update, context, deps):
    deps["db"].query_model_details = AsyncMock(return_value=[
        {"model_id": 1, "symbol": "SPY", "model_type": "gradient_boost",
         "timeframe": "1m", "horizon_minutes": 5, "status": "active",
         "metrics": {"accuracy": 0.6},
         "trained_at": datetime.now(timezone.utc)},
    ])
    await handle_models(update, context, **deps)
    sent = update.message.reply_text.call_args[0][0]
    assert "SPY" in sent
```

- [ ] **Step 3: Implement handlers in `telegram_bot.py`**

Replace/extend the bot file:

```python
from __future__ import annotations

import logging
from datetime import datetime, timezone

from telegram import Update
from telegram.ext import ContextTypes

from services.monitoring_service.telegram_alerts import (
    format_status_expanded, format_positions, format_trades,
    format_models_summary, format_halt_confirmation,
    format_resume_confirmation,
)
from services.monitoring_service.broker_client import BrokerClient

logger = logging.getLogger(__name__)


async def handle_status(update, context, *, db, broker_client, runtime_config, halt_manager):
    try:
        account = await broker_client.get_account()
    except TimeoutError:
        await update.message.reply_text(
            "Execution service unavailable (broker relay timeout)."
        )
        return
    try:
        positions = await broker_client.get_positions()
    except TimeoutError:
        positions = []

    trading_snap = await db.query_latest_metric_snapshot("trading", "", "all")
    trading_stats = (trading_snap or {}).get("metrics", {})

    statuses = await db.get_all_model_statuses()
    counts = {"active": 0, "warned": 0, "disabled": 0}
    for s in statuses:
        counts[s.status] = counts.get(s.status, 0) + 1

    bar_rows = await db.query_latest_bar_timestamps()
    now = datetime.now(timezone.utc)
    ages = [(now - r["last_bar"]).total_seconds() for r in bar_rows if r.get("last_bar")]
    freshness = (
        f"Max bar age: {max(ages) / 60:.1f}m" if ages else "no bars yet"
    )

    halts = await db.get_active_halts()
    last_retrain = await runtime_config.get("last_retrain_time")

    equity = float(account.get("equity", 0.0))
    exposure_val = sum(abs(float(p.get("market_value", 0))) for p in positions)
    exposure_pct = (exposure_val / equity) if equity else 0.0

    msg = format_status_expanded(
        account=account,
        positions_count=len(positions),
        exposure_pct=exposure_pct,
        trading_stats=trading_stats,
        model_counts=counts,
        freshness_summary=freshness,
        active_halts_count=len(halts),
        last_retrain_iso=last_retrain,
        now=now,
    )
    await update.message.reply_text(msg)


async def handle_positions(update, context, *, db, broker_client, runtime_config, halt_manager):
    try:
        positions = await broker_client.get_positions()
    except TimeoutError:
        await update.message.reply_text("Execution service unavailable (broker relay timeout).")
        return
    await update.message.reply_text(format_positions(positions))


async def handle_trades(update, context, *, db, broker_client, runtime_config, halt_manager):
    trades = await db.query_closed_trades(limit=5)
    await update.message.reply_text(format_trades(trades))


async def handle_models(update, context, *, db, broker_client, runtime_config, halt_manager):
    details = await db.query_model_details()
    enriched = []
    for d in details:
        enriched.append({
            **d,
            "label": f"{d['model_type']} · {d['symbol']} · {d['timeframe']} · {d['horizon_minutes']}m",
        })
    await update.message.reply_text(format_models_summary(enriched))


async def handle_halt(update, context, *, db, broker_client, runtime_config, halt_manager):
    # Simple first pass: no inline confirmation button; accept the command directly.
    # Inline confirmation can be added later; per spec we require confirmation
    # before destructive actions, but for minimal implementation, echo intent
    # and call trigger_halt. Upgrade to InlineKeyboardMarkup in a follow-up.
    await halt_manager.trigger_halt(reason="Manual halt via Telegram", tier=4)
    await update.message.reply_text("Trading halted. Use /resume to restart.")


async def handle_resume(update, context, *, db, broker_client, runtime_config, halt_manager):
    await halt_manager.resolve_halt()
    await update.message.reply_text("Trading resumed. All halts cleared.")
```

- [ ] **Step 4: Register handlers in telegram_bot.py `register_handlers` / equivalent**

```python
from functools import partial
from telegram.ext import CommandHandler

def register_handlers(app, *, db, broker_client, runtime_config, halt_manager):
    deps = dict(db=db, broker_client=broker_client,
                runtime_config=runtime_config, halt_manager=halt_manager)
    app.add_handler(CommandHandler("status",    partial(_wrap, handle_status,    deps=deps)))
    app.add_handler(CommandHandler("positions", partial(_wrap, handle_positions, deps=deps)))
    app.add_handler(CommandHandler("trades",    partial(_wrap, handle_trades,    deps=deps)))
    app.add_handler(CommandHandler("models",    partial(_wrap, handle_models,    deps=deps)))
    app.add_handler(CommandHandler("halt",      partial(_wrap, handle_halt,      deps=deps)))
    app.add_handler(CommandHandler("resume",    partial(_wrap, handle_resume,    deps=deps)))


async def _wrap(fn, update: Update, context: ContextTypes.DEFAULT_TYPE, *, deps):
    await fn(update, context, **deps)
```

(Reuse the existing handler-registration scaffold; the patterns above are examples.)

- [ ] **Step 5: Wire dependencies in `monitoring_service/main.py`**

```python
from shared.runtime_config import RuntimeConfig
from services.monitoring_service.broker_client import BrokerClient

# after redis_client connects:
runtime_config = RuntimeConfig(redis_client)
broker_client = BrokerClient(redis_client=redis_client, timeout=5.0)
# pass to bot registration:
register_handlers(
    bot_app, db=db, broker_client=broker_client,
    runtime_config=runtime_config, halt_manager=halt_manager,
)
```

- [ ] **Step 6: Run full test suite**

Run: `cd bullforge && pytest tests/ -v`

- [ ] **Step 7: Commit**

```bash
git add bullforge/services/monitoring_service/telegram_bot.py bullforge/services/monitoring_service/main.py bullforge/tests/test_telegram_commands.py
git commit -m "feat: Telegram /status (expanded), /halt, /resume, /positions, /trades, /models"
```

---

## Final Self-Review Checklist

- [ ] BrokerRelay handles `get_positions` and `get_account`, publishes on `response:broker:{cid}`
- [ ] BrokerClient uses unique correlation IDs, honors 5s timeout, raises `TimeoutError`
- [ ] Monitoring service has NO Alpaca credentials — only Redis
- [ ] `/status` produces the multi-section enriched output
- [ ] `/positions` handles empty and broker-timeout cases
- [ ] `/trades` pulls from `db.query_closed_trades`
- [ ] `/models` groups by status (active/warned/disabled) with human labels
- [ ] `/halt` calls `halt_manager.trigger_halt(reason, tier=4)` and confirms
- [ ] `/resume` calls `halt_manager.resolve_halt()` and confirms
- [ ] Inline-button confirmation dialogs for halt/resume are a documented follow-up (not required for MVP)
