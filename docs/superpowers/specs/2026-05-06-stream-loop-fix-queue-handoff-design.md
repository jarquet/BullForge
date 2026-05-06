# Trade-Updates Stream — Loop-Boundary Fix (Queue Handoff)

**Date:** 2026-05-06
**Branch:** `fix/stream-loop-boundary`
**Service:** `execution_service`
**Primary file:** `bullforge/services/execution_service/trade_updates_stream.py`

## Problem

`TradeUpdatesStream` runs Alpaca's `TradingStream.run()` via `await asyncio.to_thread(client.run)` (line ~113). `TradingStream.run()` internally calls `asyncio.run(self._run_forever())` — i.e. it creates its **own** event loop inside that worker thread.

When Alpaca delivers a trade update, `_handle_event` runs on that worker loop (loop B). The handler then awaits `self._core.apply_alpaca_order(...)`, which calls `await self.pool.acquire()` on our `asyncpg` pool. The pool was created on the main service loop (loop A), so its internal `Future`s are bound to loop A. Loop B cannot await them, producing:

```
RuntimeError: Task <Task-NN ...> got Future <Future ...> attached to a different loop
ConnectionDoesNotExistError: connection was closed in the middle of operation
InterfaceError: cannot perform operation: another operation is in progress
```

These errors fire in bursts roughly every hour, around `:40-:41` — when Alpaca's websocket drops on a server-side keepalive, our outer reconnect loop builds a fresh `TradingStream` and a fresh worker loop, and the next several fills hit the boundary error before things stabilize.

Each error is a **dropped trade-update from the websocket stream**. `PollingSweeper` (~60s) covers most cases via `query_non_terminal_orders`, but for orders we did not submit ourselves (manual liquidations, untracked bracket children) the stream is the only path into our DB. This is the upstream root cause of the 2026-05-04 incident that left BTC/USD trade rows 150–153 stranded as `open`.

## Goal

Implement **Option B — queue handoff**: the alpaca worker loop hands events to the main service loop via a thread-safe enqueue. All asyncpg work moves to the loop the pool was created on, eliminating the boundary error.

## Non-goals

- Switching to `client._run_forever()` (Option A) — rejected, too tightly coupled to alpaca-py private API.
- Reworking `PollingSweeper` or `ReconcilerCore` — drain semantics + replay CLI from the previous session are correct and stay.
- Fixing the same loop-boundary pattern elsewhere — not known to exist; out of scope unless `grep` shows it.
- Refactoring `_handle_event`'s `normalize_trade_update` path — pure dict translation, runs fine on either loop.
- Changing Alpaca client construction args — credentials and `paper=True` stay as-is.
- Prometheus / StatsD instrumentation — a counter via `/health` plus a known structured-log line is enough.

## Design

### Loop ownership

- `start()` runs on **loop A** (the main service loop). It captures `self._loop = asyncio.get_running_loop()` and creates the bounded queue and the consumer task on loop A.
- `_run` (existing reconnect manager) runs on **loop A**. It calls `await asyncio.to_thread(client.run)` — `client.run` then drives **loop B** in the worker thread.
- `_handle_event` is invoked by Alpaca on **loop B**. It must not touch the asyncpg pool and must not await loop A primitives. It does pure-CPU `normalize_trade_update`, then hands the snap to loop A via `loop_a.call_soon_threadsafe(...)`.
- `_consume` runs on **loop A**. It awaits the queue, then awaits `apply_alpaca_order`. The pool acquire now happens on the same loop the pool was created on.

### Queue + consumer

```python
# fields set in start():
self._loop = asyncio.get_running_loop()
self._queue: asyncio.Queue = asyncio.Queue(
    maxsize=self._settings.execution_stream_queue_max,
)
self._consumer_task = asyncio.create_task(self._consume())
self._task = asyncio.create_task(self._run())  # existing
```

`_handle_event` on loop B becomes:

```python
async def _handle_event(self, event) -> None:
    try:
        snap = normalize_trade_update(event)
    except Exception:
        logger.exception("malformed trade update event")
        return
    self._loop.call_soon_threadsafe(self._enqueue, snap)
```

`_enqueue` runs on loop A:

```python
def _enqueue(self, snap: dict) -> None:
    try:
        self._queue.put_nowait(snap)
    except asyncio.QueueFull:
        self.queue_full_dropped += 1
        logger.error(
            "trade_stream_queue_full id=%s dropped_total=%d",
            snap.get("id"), self.queue_full_dropped,
        )
```

`_consume` runs on loop A:

```python
async def _consume(self) -> None:
    while True:
        snap = await self._queue.get()
        try:
            await self._core.apply_alpaca_order(snap, source="stream")
        except Exception:
            logger.exception("stream: apply_alpaca_order failed")
        finally:
            self._queue.task_done()
```

One bad event logs and the consumer keeps draining — failure containment matches the existing direct-call behavior.

### Shutdown

`stop()` becomes:

1. Set `self._stop_event` (existing).
2. Cancel `self._task` (the `_run` task) and await its termination — stops new events from arriving.
3. Drain remaining items: `await asyncio.wait_for(self._queue.join(), timeout=settings.execution_stream_shutdown_drain_s)`. On timeout, log a warning with the residual count.
4. Cancel `self._consumer_task` and await its cancellation.

This guarantees the consumer processes everything Alpaca already delivered (up to the bound), then exits cleanly even if `apply_alpaca_order` is wedged.

### QueueFull handling

- Counter on the instance: `self.queue_full_dropped: int = 0`.
- On full: log `"trade_stream_queue_full id=<id> dropped_total=<n>"` (greppable string), increment counter, drop the snap. Do NOT block — blocking the producer would block the alpaca worker thread, which would block the websocket and cause a disconnect.
- Counter exposed via `/health`. `create_health_app` accepts an optional `stream: TradeUpdatesStream | None` arg; `health_handler` adds `stream_queue_dropped` (or omits it when stream is `None` / disabled).

### Settings

Two new fields in `bullforge/shared/config.py`, following the existing `execution_*` / `EXECUTION_*` convention, placed in the "Execution service" section:

```python
execution_stream_queue_max: int = field(
    default_factory=lambda: int(os.getenv("EXECUTION_STREAM_QUEUE_MAX", "1000"))
)
execution_stream_shutdown_drain_s: float = field(
    default_factory=lambda: float(os.getenv("EXECUTION_STREAM_SHUTDOWN_DRAIN_S", "5"))
)
```

**Defaults rationale.** Realistic peak fills/sec during a fast move: ~10–20 across the symbol set. Consumer recovery time after a transient asyncpg blip: ~30s. So worst-case buffered ≈ 600. `1000` gives ~50–100s of headroom at peak; cheap memory; comfortable margin without enabling minutes-long backlogs that would mask a real outage. `5s` shutdown drain is enough to clear normal-load backlogs (which clear in well under 1s) without making restarts feel sluggish.

## Test plan

All tests hermetic; mock `TradingStream` and `ReconcilerCore`; live in `bullforge/tests/test_trade_updates_stream.py`.

### Adapted existing tests

- `test_handler_forwards_to_core` — handler now enqueues; assert the consumer (running on loop A) calls `apply_alpaca_order`. Drive by calling `_handle_event` directly, then await `queue.join()`.
- `test_handler_swallows_malformed_event` — unchanged in spirit; assert nothing is enqueued.
- `test_reconnect_backoff_schedule`, `test_reconnect_records_stream_counter`, `test_no_reconnect_counter_on_first_connect` — unchanged. They exercise `_run`, which is unaffected.

### New tests

1. **Loop-binding regression test** — drives `_handle_event` from a worker thread with its own dedicated event loop (loop B'), simulating Alpaca's behavior. The `apply_alpaca_order` mock has an async `side_effect` that captures `asyncio.get_running_loop()` into a shared variable. The stream and pool-mock are constructed on loop A (the test's main loop). After driving one event from the worker thread and awaiting `queue.join()` on loop A, assert the captured loop **is** loop A.

   On **current** code (`to_thread(client.run)` path simulated by running `_handle_event` inside `asyncio.new_event_loop().run_until_complete(...)` in a `threading.Thread`): the `await self._core.apply_alpaca_order(...)` resolves on loop B', captured loop ≠ loop A → **test fails**.

   After the **fix**: `_handle_event` only does `call_soon_threadsafe`; the consumer running on loop A awaits `apply_alpaca_order`, captured loop == loop A → **test passes**.
2. **Backpressure test** — set `maxsize=2` via settings, fill the queue, call `_enqueue` a third time, assert `queue_full_dropped == 1`, assert a log record contains `trade_stream_queue_full`, assert no exception escapes and the call returns synchronously.
3. **Shutdown drain test** — enqueue 5 items, make `apply_alpaca_order` `await asyncio.sleep(0.05)`, call `stop()` with `execution_stream_shutdown_drain_s=2.0`. Assert all 5 calls completed.
4. **Shutdown timeout test** — enqueue 3 items, make `apply_alpaca_order` `await asyncio.sleep(10)`, call `stop()` with drain `0.1`. Assert at most 1 completes (whatever was in flight) and a timeout warning is logged. Consumer is cancelled cleanly (no leaked task).
5. **Error containment test** — make `apply_alpaca_order` raise on the 3rd call. Enqueue 5 items, await `queue.join()`. Assert calls 1, 2, 4, 5 all happen and the exception is logged.
6. **`/health` exposes counter** — small test on `create_health_app`: pass a stream stub with `queue_full_dropped=7`, GET `/health`, assert response includes `stream_queue_dropped: 7`. Pass `stream=None`, assert the field is absent.

### Run command

```bash
cd bullforge
bin/test python -m pytest tests/test_trade_updates_stream.py tests/test_health_endpoint.py -v
```

(The `test_health_endpoint.py` filename is the working assumption; if existing health tests live elsewhere, the new test slots into that file.)

## Files touched

| File | Change |
|------|--------|
| `bullforge/services/execution_service/trade_updates_stream.py` | Add queue + consumer; rewrite `_handle_event` to enqueue on loop A; rework `start`/`stop`; add `queue_full_dropped` counter. |
| `bullforge/services/execution_service/health.py` | Optional `stream` arg on `create_health_app`; expose `stream_queue_dropped` on `/health`. |
| `bullforge/services/execution_service/main.py` | Pass `stream` into `create_health_app` after the stream is constructed. |
| `bullforge/shared/config.py` | Add `execution_stream_queue_max` and `execution_stream_shutdown_drain_s` Settings fields. |
| `bullforge/tests/test_trade_updates_stream.py` | Adapt 1 existing test; add 5 new tests covering the queue path. |
| `bullforge/tests/test_health_endpoint.py` (or existing) | Add `/health` counter test. |

## Verification (production)

After deploy on the DGX (`git pull && docker compose up -d --build execution_service`), wait ~1h to cross at least one Alpaca reconnect cycle, then:

```bash
docker compose logs --since 1h execution_service 2>&1 \
  | grep -E "different loop|ConnectionDoesNotExistError"
```

Should return zero matches. Also check `/health` for `stream_queue_dropped` to confirm the counter wires up; expected value is `0` under normal load.
