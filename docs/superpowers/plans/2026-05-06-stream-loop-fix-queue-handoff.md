# Trade-Updates Stream — Loop-Boundary Fix (Queue Handoff) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the cross-event-loop crash in `TradeUpdatesStream` by handing trade-update events from Alpaca's worker thread to the main service loop via a thread-safe queue, so all asyncpg work runs on the loop the pool was created on.

**Architecture:** `_handle_event` (alpaca worker loop B) does pure-CPU normalization, then `loop_a.call_soon_threadsafe(_enqueue, snap)`. A new `_consume` task on loop A pulls from a bounded `asyncio.Queue` and awaits `apply_alpaca_order`. Bounded queue + counter for backpressure; `stop()` drains with a configurable timeout. Two new Settings fields. Counter exposed on `/health`.

**Tech Stack:** Python 3.11 asyncio, `alpaca-py`'s `TradingStream`, asyncpg via `shared/db.py`, aiohttp `/health` app, pytest with `asyncio_mode=auto`.

**Spec:** `docs/superpowers/specs/2026-05-06-stream-loop-fix-queue-handoff-design.md`

**Working directory for all commands:** `bullforge/` (relative paths in this plan are relative to the `bullforge/` package root).

**Test runner:** `bin/test python -m pytest <path> -v` from `bullforge/`. `bin/test` builds the test image and runs against the ephemeral test compose stack.

**Commit convention:** Conventional Commits. **No `Co-Authored-By` trailer.**

---

## File map

| File | Role | Status |
|------|------|--------|
| `shared/config.py` | `Settings` dataclass with `execution_*` fields | Modify (add 2 fields) |
| `services/execution_service/trade_updates_stream.py` | Trade updates stream — primary edit target | Modify |
| `services/execution_service/health.py` | `/health` aiohttp app | Modify (accept optional `stream`) |
| `services/execution_service/main.py` | Service entrypoint | Modify (pass `stream` to health) |
| `tests/test_trade_updates_stream.py` | Hermetic stream tests | Modify (adapt 1, add 5 tests) |
| `tests/test_health_endpoint.py` (or existing) | Health-endpoint test | Modify or create |

`reconciler_core.py`, `polling_sweeper.py`, `shared/db.py` are read-only references; no edits.

---

## Task 0: Locate health-endpoint test file

Before writing the new health test, confirm whether a test file for `health.py` exists.

- [ ] **Step 1: Look for existing health tests**

Run from `bullforge/`:

```bash
grep -rl "create_health_app\|health_handler" tests/ || echo "no existing health tests"
ls tests/ | grep -i health || echo "no health test file"
```

Expected: either an existing test file path, or "no existing health tests / no health test file".

- [ ] **Step 2: Decide target file**

If an existing health test file is reported, the new health test in Task 6 lands there. If none exists, create `tests/test_health_endpoint.py` in Task 6.

Record the decision inline in the Task 6 step body before writing the test.

No commit for this task.

---

## Task 1: Add Settings fields for queue size and shutdown drain timeout

**Files:**
- Modify: `shared/config.py` (Execution service section, after `execution_reconciler_stream_enabled`)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_config.py` if it exists, otherwise create `tests/test_config_stream_fields.py`. First check:

```bash
ls tests/test_config*.py 2>/dev/null || echo "no config test"
```

If `tests/test_config.py` exists, append to it. Otherwise create `tests/test_config_stream_fields.py` with:

```python
import os
from unittest.mock import patch

from shared.config import Settings


def test_execution_stream_queue_max_default():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("EXECUTION_STREAM_QUEUE_MAX", None)
        s = Settings()
    assert s.execution_stream_queue_max == 1000


def test_execution_stream_queue_max_env_override():
    with patch.dict(os.environ, {"EXECUTION_STREAM_QUEUE_MAX": "250"}):
        s = Settings()
    assert s.execution_stream_queue_max == 250


def test_execution_stream_shutdown_drain_s_default():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("EXECUTION_STREAM_SHUTDOWN_DRAIN_S", None)
        s = Settings()
    assert s.execution_stream_shutdown_drain_s == 5.0


def test_execution_stream_shutdown_drain_s_env_override():
    with patch.dict(os.environ, {"EXECUTION_STREAM_SHUTDOWN_DRAIN_S": "2.5"}):
        s = Settings()
    assert s.execution_stream_shutdown_drain_s == 2.5
```

- [ ] **Step 2: Run test to verify it fails**

```bash
bin/test python -m pytest tests/test_config_stream_fields.py -v
```

Expected: FAIL with `AttributeError: 'Settings' object has no attribute 'execution_stream_queue_max'` (or similar).

- [ ] **Step 3: Add the Settings fields**

In `shared/config.py`, find the line:

```python
    execution_reconciler_stream_enabled: bool = field(
        default_factory=lambda: os.getenv(
            "EXECUTION_RECONCILER_STREAM_ENABLED", "true"
        ).lower() == "true"
    )
```

Append immediately after that field, **inside the `Settings` dataclass**:

```python
    execution_stream_queue_max: int = field(
        default_factory=lambda: int(
            os.getenv("EXECUTION_STREAM_QUEUE_MAX", "1000")
        )
    )
    execution_stream_shutdown_drain_s: float = field(
        default_factory=lambda: float(
            os.getenv("EXECUTION_STREAM_SHUTDOWN_DRAIN_S", "5")
        )
    )
```

- [ ] **Step 4: Run test to verify it passes**

```bash
bin/test python -m pytest tests/test_config_stream_fields.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add shared/config.py tests/test_config_stream_fields.py
git commit -m "feat(config): add execution_stream_queue_max and shutdown_drain_s Settings"
```

(If you appended to an existing `tests/test_config.py`, adjust the `git add` accordingly.)

---

## Task 2: Loop-binding regression test (failing first)

This is the spec's required regression test: the cross-thread/cross-loop test that **fails on current code** and **passes after the fix**.

**Files:**
- Modify: `tests/test_trade_updates_stream.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_trade_updates_stream.py`:

```python
import threading
from unittest.mock import AsyncMock, MagicMock


@pytest.mark.asyncio
async def test_apply_alpaca_order_runs_on_main_loop_when_event_arrives_from_worker_thread():
    """
    Regression: when _handle_event is invoked from a worker thread running its
    own asyncio loop (loop B'), apply_alpaca_order must execute on the main
    service loop (loop A) where the asyncpg pool was created.

    On the pre-fix code, _handle_event awaits apply_alpaca_order directly on
    loop B' -> the captured loop is B' -> the assertion fails.
    """
    main_loop = asyncio.get_running_loop()
    captured_loops: list = []

    async def _capture(*args, **kwargs):
        captured_loops.append(asyncio.get_running_loop())

    core = MagicMock()
    core.apply_alpaca_order = AsyncMock(side_effect=_capture)
    core.record_stream_reconnect = AsyncMock()

    settings = MagicMock(
        alpaca_api_key="k",
        alpaca_secret_key="s",
        alpaca_base_url="https://paper-api.alpaca.markets",
        execution_stream_queue_max=1000,
        execution_stream_shutdown_drain_s=2.0,
    )

    stream = TradeUpdatesStream(
        settings=settings,
        core=core,
        on_reconnect=AsyncMock(),
    )
    await stream.start()
    try:
        event = MagicMock()
        event.order = MagicMock(
            id="alp-loop-1", symbol="SPY",
            side=MagicMock(value="buy"), qty="1",
            order_type=MagicMock(value="market"),
            status=MagicMock(value="filled"),
            submitted_at=datetime.now(timezone.utc),
            filled_qty="1", filled_avg_price="100",
            filled_at=datetime.now(timezone.utc),
        )

        # Drive _handle_event from a worker thread that runs its own loop -
        # this models how alpaca-py's TradingStream.run() invokes the callback.
        def _drive_in_worker_loop():
            worker_loop = asyncio.new_event_loop()
            try:
                worker_loop.run_until_complete(stream._handle_event(event))
            finally:
                worker_loop.close()

        t = threading.Thread(target=_drive_in_worker_loop)
        t.start()
        t.join(timeout=2.0)
        assert not t.is_alive(), "worker thread did not finish"

        # Wait for main-loop consumer to drain the queue.
        for _ in range(200):
            await asyncio.sleep(0.01)
            if captured_loops:
                break
    finally:
        await stream.stop()

    assert captured_loops, "apply_alpaca_order was never called"
    assert captured_loops[0] is main_loop, (
        f"apply_alpaca_order ran on {captured_loops[0]!r}, "
        f"expected main_loop {main_loop!r}"
    )
```

The `datetime, timezone` imports already exist at the top of the file.

- [ ] **Step 2: Run test to verify it fails on current code**

```bash
bin/test python -m pytest tests/test_trade_updates_stream.py::test_apply_alpaca_order_runs_on_main_loop_when_event_arrives_from_worker_thread -v
```

Expected: FAIL — either an `AttributeError` (no `_loop` / `_queue` / `_consumer_task` yet, plus `start()` does not depend on `execution_stream_queue_max`), or the captured-loop assertion. Both are valid failure modes proving the test is meaningful.

- [ ] **Step 3: Commit the failing test**

```bash
git add tests/test_trade_updates_stream.py
git commit -m "test(stream): regression test for cross-loop apply_alpaca_order"
```

(Yes, commit a failing test. Tasks 3 and 4 implement the fix that makes it pass. Frequent commits are the spec's intent — and this is the regression-test commit that anchors the fix.)

---

## Task 3: Refactor `TradeUpdatesStream` — queue + consumer + threadsafe enqueue

This is the core implementation. After this task the regression test from Task 2 passes.

**Files:**
- Modify: `services/execution_service/trade_updates_stream.py`

- [ ] **Step 1: Replace the file body**

Open `services/execution_service/trade_updates_stream.py`. Replace its entire contents (from `class TradeUpdatesStream` onward — keep the `normalize_trade_update` function and all imports) with the version below. The `normalize_trade_update` function and the imports stay byte-identical.

Replace lines 48–127 (the `class TradeUpdatesStream` block) with:

```python
class TradeUpdatesStream:
    def __init__(
        self,
        settings: Settings,
        core: ReconcilerCore,
        on_reconnect: Callable[[], Awaitable[None]],
        max_backoff_s: int = 30,
    ):
        self._settings = settings
        self._core = core
        self._on_reconnect = on_reconnect
        self._max_backoff_s = max_backoff_s
        self._task: Optional[asyncio.Task] = None
        self._consumer_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._queue: Optional[asyncio.Queue] = None
        self.queue_full_dropped: int = 0

    def _backoff_delay(self, attempt: int) -> int:
        return min(2 ** attempt, self._max_backoff_s)

    async def _handle_event(self, event) -> None:
        # Runs on alpaca's worker loop. Must NOT touch the asyncpg pool
        # and must NOT await anything bound to the main service loop.
        try:
            snap = normalize_trade_update(event)
        except Exception:
            logger.exception("malformed trade update event")
            return
        loop = self._loop
        if loop is None or self._queue is None:
            logger.error("trade_stream_dropped_pre_start id=%s", snap.get("id"))
            return
        loop.call_soon_threadsafe(self._enqueue, snap)

    def _enqueue(self, snap: dict) -> None:
        # Runs on the main service loop via call_soon_threadsafe.
        assert self._queue is not None  # set in start()
        try:
            self._queue.put_nowait(snap)
        except asyncio.QueueFull:
            self.queue_full_dropped += 1
            logger.error(
                "trade_stream_queue_full id=%s dropped_total=%d",
                snap.get("id"), self.queue_full_dropped,
            )

    async def _consume(self) -> None:
        assert self._queue is not None
        while True:
            snap = await self._queue.get()
            try:
                await self._core.apply_alpaca_order(snap, source="stream")
            except Exception:
                logger.exception("stream: apply_alpaca_order failed")
            finally:
                self._queue.task_done()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop_event.clear()
        self._loop = asyncio.get_running_loop()
        self._queue = asyncio.Queue(
            maxsize=self._settings.execution_stream_queue_max,
        )
        self._consumer_task = asyncio.create_task(self._consume())
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop_event.set()
        # Stop accepting new events first.
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        # Drain whatever is already queued, bounded by configured timeout.
        if self._queue is not None and self._consumer_task is not None:
            try:
                await asyncio.wait_for(
                    self._queue.join(),
                    timeout=self._settings.execution_stream_shutdown_drain_s,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "trade_stream_drain_timeout pending=%d",
                    self._queue.qsize(),
                )
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
            self._consumer_task = None
        self._queue = None
        self._loop = None

    async def _run(self) -> None:
        attempt = 0
        while not self._stop_event.is_set():
            client = TradingStream(
                api_key=self._settings.alpaca_api_key,
                secret_key=self._settings.alpaca_secret_key,
                paper=True,
            )
            client.subscribe_trade_updates(self._handle_event)
            try:
                if attempt > 0:
                    try:
                        await self._core.record_stream_reconnect()
                    except Exception:
                        logger.exception("record_stream_reconnect failed")
                    try:
                        await self._on_reconnect()
                    except Exception:
                        logger.exception("on_reconnect callback failed")
                attempt = 0
                await asyncio.to_thread(client.run)
            except Exception:
                logger.exception("trade updates stream dropped")
            if self._stop_event.is_set():
                return
            delay = self._backoff_delay(attempt)
            attempt += 1
            logger.info("reconnecting trade stream in %ds", delay)
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=delay,
                )
            except asyncio.TimeoutError:
                pass
```

Key behavioral changes vs. the previous version:

- `start()` captures `self._loop` (loop A), creates the bounded `_queue`, and spawns `_consume`.
- `_handle_event` no longer awaits `apply_alpaca_order` itself — it normalizes and `call_soon_threadsafe(self._enqueue, snap)`.
- `_consume` is the sole place that awaits `apply_alpaca_order`, on loop A.
- `stop()` cancels the producer (`_task`), drains `_queue` for `execution_stream_shutdown_drain_s` seconds, then cancels the consumer.
- `_run` body is **unchanged** — same reconnect/backoff flow, same `record_stream_reconnect` + `on_reconnect` ordering. Existing tests for those paths must still pass.

- [ ] **Step 2: Run the regression test from Task 2**

```bash
bin/test python -m pytest tests/test_trade_updates_stream.py::test_apply_alpaca_order_runs_on_main_loop_when_event_arrives_from_worker_thread -v
```

Expected: PASS.

- [ ] **Step 3: Run the full existing test file to catch regressions in adapted tests**

```bash
bin/test python -m pytest tests/test_trade_updates_stream.py -v
```

Expected: most tests PASS. `test_handler_forwards_to_core` may fail or hang because it calls `_handle_event` directly without starting the stream — Task 4 fixes that. Note any other failures and stop if anything beyond `test_handler_forwards_to_core` is broken; otherwise proceed.

- [ ] **Step 4: Commit**

```bash
git add services/execution_service/trade_updates_stream.py
git commit -m "fix(execution_service): hand trade-update events from alpaca thread to main loop via queue"
```

This commit is the actual bug fix. The commit message should be searchable later.

---

## Task 4: Adapt `test_handler_forwards_to_core` to the consumer path

The existing test calls `await stream._handle_event(event)` then `core.apply_alpaca_order.assert_awaited_once()`. With the new architecture, `_handle_event` only enqueues — the consumer awaits `apply_alpaca_order`. The test has to start the stream and wait for the queue to drain.

**Files:**
- Modify: `tests/test_trade_updates_stream.py`

- [ ] **Step 1: Rewrite the test**

In `tests/test_trade_updates_stream.py`, replace the existing `test_handler_forwards_to_core` (lines ~33–55) with:

```python
@pytest.mark.asyncio
async def test_handler_forwards_to_core():
    core = AsyncMock()
    stream = TradeUpdatesStream(
        settings=MagicMock(
            alpaca_api_key="k", alpaca_secret_key="s",
            alpaca_base_url="https://paper-api.alpaca.markets",
            execution_stream_queue_max=1000,
            execution_stream_shutdown_drain_s=2.0,
        ),
        core=core,
        on_reconnect=AsyncMock(),
    )
    await stream.start()
    try:
        event = MagicMock()
        event.order = MagicMock(
            id="a", symbol="SPY", side=MagicMock(value="buy"), qty="1",
            order_type=MagicMock(value="market"),
            status=MagicMock(value="filled"),
            submitted_at=datetime.now(timezone.utc),
            filled_qty="1", filled_avg_price="100",
            filled_at=datetime.now(timezone.utc),
        )
        await stream._handle_event(event)
        # Consumer drains on the main loop; wait for it.
        await asyncio.wait_for(stream._queue.join(), timeout=2.0)
    finally:
        await stream.stop()
    core.apply_alpaca_order.assert_awaited_once()
    args, kwargs = core.apply_alpaca_order.await_args
    assert (kwargs.get("source") or args[1]) == "stream"
```

Note: `_handle_event` here is called from the same loop, so `call_soon_threadsafe` schedules onto the loop currently running the test. That is correct behavior — `call_soon_threadsafe` works whether or not it's called cross-thread.

Also rewrite `test_handler_swallows_malformed_event` (lines ~58–72) to match the new shape:

```python
@pytest.mark.asyncio
async def test_handler_swallows_malformed_event():
    core = AsyncMock()
    stream = TradeUpdatesStream(
        settings=MagicMock(
            alpaca_api_key="k", alpaca_secret_key="s",
            alpaca_base_url="https://paper-api.alpaca.markets",
            execution_stream_queue_max=1000,
            execution_stream_shutdown_drain_s=2.0,
        ),
        core=core,
        on_reconnect=AsyncMock(),
    )
    await stream.start()
    try:
        bad = MagicMock()
        bad.order = None
        await stream._handle_event(bad)
        # Give the (empty) queue a tick.
        await asyncio.sleep(0.05)
    finally:
        await stream.stop()
    core.apply_alpaca_order.assert_not_called()
```

The reconnect-counter tests (`test_reconnect_records_stream_counter`, `test_no_reconnect_counter_on_first_connect`) already build `MagicMock` settings — they need `execution_stream_queue_max` and `execution_stream_shutdown_drain_s` added to `_make_stream_settings`. Update `_make_stream_settings` (around line 97):

```python
def _make_stream_settings():
    s = MagicMock()
    s.alpaca_api_key = "key"
    s.alpaca_secret_key = "secret"
    s.alpaca_base_url = "https://paper-api.alpaca.markets"
    s.execution_stream_queue_max = 1000
    s.execution_stream_shutdown_drain_s = 2.0
    return s
```

Also update the loop-regression test (Task 2) and `test_reconnect_backoff_schedule` `MagicMock(...)` settings if they don't already have the new fields. (Task 2's test already includes them; `test_reconnect_backoff_schedule` builds `MagicMock(alpaca_api_key="k", ...)` — `MagicMock` auto-creates missing attributes, so it tolerates the new fields without changes, but they're never read in that test.)

- [ ] **Step 2: Run the full test file**

```bash
bin/test python -m pytest tests/test_trade_updates_stream.py -v
```

Expected: ALL tests pass (existing 6 + regression test from Task 2 = 7 passing).

- [ ] **Step 3: Commit**

```bash
git add tests/test_trade_updates_stream.py
git commit -m "test(stream): adapt handler tests to queue-handoff consumer path"
```

---

## Task 5: Backpressure, shutdown drain, and error containment tests

Three new tests covering edge cases.

**Files:**
- Modify: `tests/test_trade_updates_stream.py`

- [ ] **Step 1: Append the three new tests**

Append to the bottom of `tests/test_trade_updates_stream.py`:

```python
@pytest.mark.asyncio
async def test_queue_full_increments_counter_and_does_not_block(caplog):
    """Producer does not block when queue is full; counter and log line fire."""
    core = AsyncMock()
    # Block apply_alpaca_order so the consumer cannot drain.
    consume_gate = asyncio.Event()

    async def _blocked(*args, **kwargs):
        await consume_gate.wait()

    core.apply_alpaca_order = AsyncMock(side_effect=_blocked)

    stream = TradeUpdatesStream(
        settings=MagicMock(
            alpaca_api_key="k", alpaca_secret_key="s",
            alpaca_base_url="https://paper-api.alpaca.markets",
            execution_stream_queue_max=2,
            execution_stream_shutdown_drain_s=0.5,
        ),
        core=core,
        on_reconnect=AsyncMock(),
    )
    await stream.start()
    try:
        snap_template = {
            "id": "x", "symbol": "SPY", "side": "buy", "qty": 1.0,
            "order_type": "market", "status": "filled",
            "submitted_at": datetime.now(timezone.utc),
            "filled_qty": 1.0, "filled_avg_price": 100.0,
            "filled_at": datetime.now(timezone.utc),
        }

        with caplog.at_level("ERROR"):
            # Consumer pulls one item immediately and blocks on consume_gate;
            # queue capacity is 2 so the next 2 items fit, the 4th must drop.
            for i in range(4):
                snap = dict(snap_template, id=f"x-{i}")
                stream._enqueue(snap)
            # _enqueue is sync; if it ever blocked, this assertion path
            # would never be reached. Counter must reflect the drop.
            assert stream.queue_full_dropped >= 1
            assert any(
                "trade_stream_queue_full" in rec.getMessage()
                for rec in caplog.records
            )
    finally:
        consume_gate.set()
        await stream.stop()


@pytest.mark.asyncio
async def test_shutdown_drains_queued_items_within_timeout():
    """stop() waits for queued items up to drain timeout."""
    core = AsyncMock()
    processed: list = []

    async def _slow(snap, *args, **kwargs):
        await asyncio.sleep(0.05)
        processed.append(snap["id"])

    core.apply_alpaca_order = AsyncMock(side_effect=_slow)

    stream = TradeUpdatesStream(
        settings=MagicMock(
            alpaca_api_key="k", alpaca_secret_key="s",
            alpaca_base_url="https://paper-api.alpaca.markets",
            execution_stream_queue_max=1000,
            execution_stream_shutdown_drain_s=2.0,
        ),
        core=core,
        on_reconnect=AsyncMock(),
    )
    await stream.start()
    snap_template = {
        "id": "x", "symbol": "SPY", "side": "buy", "qty": 1.0,
        "order_type": "market", "status": "filled",
        "submitted_at": datetime.now(timezone.utc),
        "filled_qty": 1.0, "filled_avg_price": 100.0,
        "filled_at": datetime.now(timezone.utc),
    }
    for i in range(5):
        stream._enqueue(dict(snap_template, id=f"s-{i}"))

    await stream.stop()
    assert sorted(processed) == [f"s-{i}" for i in range(5)]


@pytest.mark.asyncio
async def test_shutdown_timeout_logs_pending_and_cancels_consumer(caplog):
    """stop() logs a drain-timeout when work exceeds drain budget."""
    core = AsyncMock()

    async def _very_slow(*args, **kwargs):
        await asyncio.sleep(10)

    core.apply_alpaca_order = AsyncMock(side_effect=_very_slow)

    stream = TradeUpdatesStream(
        settings=MagicMock(
            alpaca_api_key="k", alpaca_secret_key="s",
            alpaca_base_url="https://paper-api.alpaca.markets",
            execution_stream_queue_max=1000,
            execution_stream_shutdown_drain_s=0.1,
        ),
        core=core,
        on_reconnect=AsyncMock(),
    )
    await stream.start()
    snap_template = {
        "id": "x", "symbol": "SPY", "side": "buy", "qty": 1.0,
        "order_type": "market", "status": "filled",
        "submitted_at": datetime.now(timezone.utc),
        "filled_qty": 1.0, "filled_avg_price": 100.0,
        "filled_at": datetime.now(timezone.utc),
    }
    for i in range(3):
        stream._enqueue(dict(snap_template, id=f"t-{i}"))

    with caplog.at_level("WARNING"):
        await stream.stop()
        assert any(
            "trade_stream_drain_timeout" in rec.getMessage()
            for rec in caplog.records
        )


@pytest.mark.asyncio
async def test_consumer_continues_after_apply_raises(caplog):
    """One bad event must not stop the consumer."""
    core = AsyncMock()
    seen: list = []
    counter = {"n": 0}

    async def _maybe_fail(snap, *args, **kwargs):
        counter["n"] += 1
        if counter["n"] == 3:
            raise RuntimeError("boom")
        seen.append(snap["id"])

    core.apply_alpaca_order = AsyncMock(side_effect=_maybe_fail)

    stream = TradeUpdatesStream(
        settings=MagicMock(
            alpaca_api_key="k", alpaca_secret_key="s",
            alpaca_base_url="https://paper-api.alpaca.markets",
            execution_stream_queue_max=1000,
            execution_stream_shutdown_drain_s=2.0,
        ),
        core=core,
        on_reconnect=AsyncMock(),
    )
    await stream.start()
    snap_template = {
        "id": "x", "symbol": "SPY", "side": "buy", "qty": 1.0,
        "order_type": "market", "status": "filled",
        "submitted_at": datetime.now(timezone.utc),
        "filled_qty": 1.0, "filled_avg_price": 100.0,
        "filled_at": datetime.now(timezone.utc),
    }
    with caplog.at_level("ERROR"):
        for i in range(5):
            stream._enqueue(dict(snap_template, id=f"e-{i}"))
        await asyncio.wait_for(stream._queue.join(), timeout=2.0)
    await stream.stop()

    assert sorted(seen) == ["e-0", "e-1", "e-3", "e-4"]
    assert any(
        "stream: apply_alpaca_order failed" in rec.getMessage()
        for rec in caplog.records
    )
```

- [ ] **Step 2: Run the new tests**

```bash
bin/test python -m pytest tests/test_trade_updates_stream.py -v
```

Expected: ALL tests pass (the 7 from Task 4 + 4 new = 11 passing).

If `test_queue_full_increments_counter_and_does_not_block` is flaky because the consumer hasn't picked up the first item before the producer enqueues the second, the consume_gate pattern ensures: consumer gets item 1 from queue (queue free slots: 2 of 2), consumer awaits gate, then 4 producer enqueues fill slots 1+2 and the 3rd and 4th hit `QueueFull`. Counter `>= 1` covers either timing.

- [ ] **Step 3: Commit**

```bash
git add tests/test_trade_updates_stream.py
git commit -m "test(stream): cover backpressure, shutdown drain, and error containment"
```

---

## Task 6: Expose `queue_full_dropped` on `/health`

**Files:**
- Modify: `services/execution_service/health.py`
- Modify: `services/execution_service/main.py`
- Create or modify: `tests/test_health_endpoint.py` (per Task 0 decision)

- [ ] **Step 1: Write the failing test**

Per Task 0's decision, this lands in `tests/test_health_endpoint.py` (create) or whichever existing file Task 0 found. The test below assumes a new file:

```python
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiohttp.test_utils import TestClient, TestServer

from services.execution_service.health import create_health_app


def _ok_db():
    db = MagicMock()
    conn = MagicMock()
    conn.execute = AsyncMock(return_value=None)

    class _AcquireCtx:
        async def __aenter__(self):
            return conn

        async def __aexit__(self, *a):
            return False

    db.pool = MagicMock()
    db.pool.acquire = MagicMock(return_value=_AcquireCtx())
    return db


def _ok_redis():
    r = MagicMock()
    r.client = MagicMock()
    r.client.ping = AsyncMock(return_value=True)
    return r


def _ok_broker():
    b = MagicMock()
    b.get_account = AsyncMock(return_value={})
    return b


@pytest.mark.asyncio
async def test_health_includes_stream_queue_dropped_when_stream_provided():
    stream = MagicMock()
    stream.queue_full_dropped = 7

    app = create_health_app(_ok_db(), _ok_redis(), _ok_broker(), stream=stream)
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/health")
        body = await resp.json()
    assert body.get("stream_queue_dropped") == 7


@pytest.mark.asyncio
async def test_health_omits_stream_queue_dropped_when_no_stream():
    app = create_health_app(_ok_db(), _ok_redis(), _ok_broker())
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/health")
        body = await resp.json()
    assert "stream_queue_dropped" not in body
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
bin/test python -m pytest tests/test_health_endpoint.py -v
```

Expected: FAIL with `TypeError: create_health_app() got an unexpected keyword argument 'stream'`.

- [ ] **Step 3: Update `health.py`**

Replace `services/execution_service/health.py` with:

```python
from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from aiohttp import web

from shared.db import Database
from shared.redis import RedisClient
from services.execution_service.alpaca_broker import AlpacaBroker

if TYPE_CHECKING:
    from services.execution_service.trade_updates_stream import TradeUpdatesStream


def create_health_app(
    db: Database,
    redis_client: RedisClient,
    broker: AlpacaBroker,
    stream: Optional["TradeUpdatesStream"] = None,
) -> web.Application:
    app = web.Application()
    app["db"] = db
    app["redis"] = redis_client
    app["broker"] = broker
    app["stream"] = stream
    app.router.add_get("/health", health_handler)
    return app


async def health_handler(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    redis_client: RedisClient = request.app["redis"]
    broker: AlpacaBroker = request.app["broker"]
    stream = request.app.get("stream")

    checks: dict = {}

    try:
        async with db.pool.acquire() as conn:
            await conn.execute("SELECT 1")
        checks["db"] = "ok"
    except Exception as e:
        checks["db"] = f"error: {e}"

    try:
        await redis_client.client.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"error: {e}"

    try:
        await broker.get_account()
        checks["alpaca"] = "ok"
    except Exception as e:
        checks["alpaca"] = f"error: {e}"

    if stream is not None:
        checks["stream_queue_dropped"] = int(getattr(stream, "queue_full_dropped", 0))

    all_ok = all(
        v == "ok"
        for k, v in checks.items()
        if k != "stream_queue_dropped"
    )
    status = 200 if all_ok else 503
    return web.json_response(checks, status=status)
```

The `stream_queue_dropped` field is informational; it does **not** flip `/health` to 503 just because a drop happened. (A drop is already paged via the structured log.)

- [ ] **Step 4: Update `main.py` to pass `stream`**

In `services/execution_service/main.py` find the line:

```python
    health_app = create_health_app(db, redis_client, broker)
```

It is around line 160. Replace with:

```python
    health_app = create_health_app(db, redis_client, broker, stream=stream)
```

`stream` is already in scope (line 84+); when the stream is disabled by config it is `None`, which the new `health.py` handles.

- [ ] **Step 5: Run the health test plus the stream tests**

```bash
bin/test python -m pytest tests/test_health_endpoint.py tests/test_trade_updates_stream.py -v
```

Expected: ALL pass.

- [ ] **Step 6: Commit**

```bash
git add services/execution_service/health.py services/execution_service/main.py tests/test_health_endpoint.py
git commit -m "feat(execution_service): expose stream_queue_dropped counter on /health"
```

---

## Task 7: Full execution-service test sweep

**Files:** none (verification only).

- [ ] **Step 1: Run the broader execution-service test set**

```bash
bin/test python -m pytest tests/test_trade_updates_stream.py tests/test_health_endpoint.py tests/test_config_stream_fields.py tests/test_reconciler_core.py tests/test_polling_sweeper.py tests/test_execution_service.py -v 2>&1 | tail -60
```

(Some of those test files may not exist verbatim — adjust to what `ls tests/test_*.py | grep -E "reconcil|execution|polling|trade"` reports.)

Expected: all pass. If any unrelated test breaks, stop and investigate — likely a missed `Settings` field where a test instantiates `Settings()` and a downstream consumer reads one of the new fields. Add the missing field to that test's `Settings()` mock.

- [ ] **Step 2: Run the unit-level smoke for the whole tree**

```bash
bin/test python -m pytest tests/ --ignore=tests/integration --ignore=tests/meta -q 2>&1 | tail -40
```

Expected: green. Failures here that touch `execution_*` Settings are within scope; failures in unrelated modules are not (stop and investigate before proceeding).

- [ ] **Step 3: Commit (only if test fixes were needed)**

If Step 1 or 2 surfaced a broken unrelated test that needed a fix:

```bash
git add <touched test files>
git commit -m "test: stabilize unrelated tests after Settings additions"
```

Otherwise no commit.

---

## Self-review (informational; no checkbox)

- All spec sections covered: Settings (Task 1), regression test (Task 2), refactor (Task 3), adapted tests (Task 4), edge tests (Task 5), `/health` counter (Task 6).
- No "TBD" / placeholder steps; every code-modifying step shows the code.
- Type/name consistency: `queue_full_dropped` (counter), `_loop`, `_queue`, `_consumer_task`, `_consume`, `_enqueue`, `execution_stream_queue_max`, `execution_stream_shutdown_drain_s` — used identically in plan body, settings, tests, and `/health`.
- Frequent commits: 6 commits across the plan (config, regression test, fix, adapted tests, edge tests, /health).
- Out-of-scope guards: `_run`, `record_stream_reconnect`, `on_reconnect`, `normalize_trade_update`, `PollingSweeper`, `ReconcilerCore` — all explicitly preserved.
