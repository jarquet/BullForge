import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from services.execution_service.trade_updates_stream import (
    TradeUpdatesStream, normalize_trade_update,
)


def test_normalize_trade_update_matches_broker_snapshot_shape():
    event = MagicMock()
    event.order = MagicMock(
        id="alp-42",
        symbol="SPY",
        side=MagicMock(value="buy"),
        qty="2",
        order_type=MagicMock(value="market"),
        status=MagicMock(value="filled"),
        submitted_at=datetime(2026, 4, 14, tzinfo=timezone.utc),
        filled_qty="2",
        filled_avg_price="420.5",
        filled_at=datetime(2026, 4, 14, 14, 0, 1, tzinfo=timezone.utc),
    )
    snap = normalize_trade_update(event)
    assert snap["id"] == "alp-42"
    assert snap["status"] == "filled"
    assert snap["filled_qty"] == 2.0
    assert snap["filled_avg_price"] == 420.5
    assert snap["filled_at"].tzinfo == timezone.utc


def _patch_trading_stream(stream):
    """
    Helper: returns a `patch` context manager that replaces TradingStream
    with a fake whose `client.run` is a sync no-op blocking until the
    stream's stop_event fires. Mirrors the existing reconnect-test pattern;
    avoids real Alpaca network IO when start() spins up _run().
    """
    def _make_client(*args, **kwargs):
        client = MagicMock()

        def _run():
            import time
            while not stream._stop_event.is_set():
                time.sleep(0.05)

        client.run = _run
        client.subscribe_trade_updates = MagicMock()
        return client

    return patch(
        "services.execution_service.trade_updates_stream.TradingStream",
        side_effect=_make_client,
    )


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
    with _patch_trading_stream(stream):
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
    with _patch_trading_stream(stream):
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


@pytest.mark.asyncio
async def test_reconnect_backoff_schedule():
    stream = TradeUpdatesStream(
        settings=MagicMock(
            alpaca_api_key="k", alpaca_secret_key="s",
            alpaca_base_url="https://paper-api.alpaca.markets",
        ),
        core=AsyncMock(),
        on_reconnect=AsyncMock(),
    )
    delays = [stream._backoff_delay(n) for n in range(7)]
    assert delays[0] == 1
    assert delays[1] == 2
    assert delays[2] == 4
    assert delays[-1] <= 30
    assert delays == sorted(delays)


# ---------------------------------------------------------------------------
# Counter: record_stream_reconnect
# ---------------------------------------------------------------------------

def _make_stream_settings():
    s = MagicMock()
    s.alpaca_api_key = "key"
    s.alpaca_secret_key = "secret"
    s.alpaca_base_url = "https://paper-api.alpaca.markets"
    s.execution_stream_queue_max = 1000
    s.execution_stream_shutdown_drain_s = 2.0
    return s


@pytest.mark.asyncio
async def test_reconnect_records_stream_counter():
    """
    record_stream_reconnect is awaited before on_reconnect on attempt > 0.
    """
    call_order = []

    core = MagicMock()

    async def _record():
        call_order.append("record_stream_reconnect")

    async def _on_reconnect():
        call_order.append("on_reconnect")

    core.record_stream_reconnect = AsyncMock(side_effect=_record)
    core.apply_alpaca_order = AsyncMock()
    on_reconnect_mock = AsyncMock(side_effect=_on_reconnect)

    stream = TradeUpdatesStream(
        settings=_make_stream_settings(),
        core=core,
        on_reconnect=on_reconnect_mock,
        max_backoff_s=0,
    )

    attempts = 0

    def _make_client(*args, **kwargs):
        nonlocal attempts
        client = MagicMock()

        def _run():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise ConnectionError("stream dropped")
            stream._stop_event.set()

        client.run = _run
        client.subscribe_trade_updates = MagicMock()
        return client

    with patch(
        "services.execution_service.trade_updates_stream.TradingStream",
        side_effect=_make_client,
    ):
        await stream.start()
        for _ in range(100):
            await asyncio.sleep(0.01)
            if stream._stop_event.is_set():
                break
        await stream.stop()

    core.record_stream_reconnect.assert_awaited()
    on_reconnect_mock.assert_awaited()
    assert "record_stream_reconnect" in call_order
    assert "on_reconnect" in call_order
    assert call_order.index("record_stream_reconnect") < call_order.index("on_reconnect")


@pytest.mark.asyncio
async def test_no_reconnect_counter_on_first_connect():
    """record_stream_reconnect must NOT be called on the very first connection."""
    core = MagicMock()
    core.record_stream_reconnect = AsyncMock()
    core.apply_alpaca_order = AsyncMock()
    on_reconnect = AsyncMock()

    stream = TradeUpdatesStream(
        settings=_make_stream_settings(),
        core=core,
        on_reconnect=on_reconnect,
        max_backoff_s=0,
    )

    def _make_client(*args, **kwargs):
        client = MagicMock()

        def _run():
            stream._stop_event.set()

        client.run = _run
        client.subscribe_trade_updates = MagicMock()
        return client

    with patch(
        "services.execution_service.trade_updates_stream.TradingStream",
        side_effect=_make_client,
    ):
        await stream.start()
        for _ in range(50):
            await asyncio.sleep(0.01)
            if stream._stop_event.is_set():
                break
        await stream.stop()

    core.record_stream_reconnect.assert_not_awaited()
    on_reconnect.assert_not_awaited()


import threading


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

    # Fake TradingStream: client.run() is a sync no-op that blocks until
    # the stream's stop_event is set, mirroring the pattern in the
    # existing reconnect-counter tests. This avoids real Alpaca network IO.
    def _make_client(*args, **kwargs):
        client = MagicMock()

        def _run():
            # Block in the worker thread until stop_event fires.
            import time
            while not stream._stop_event.is_set():
                time.sleep(0.05)

        client.run = _run
        client.subscribe_trade_updates = MagicMock()
        return client

    with patch(
        "services.execution_service.trade_updates_stream.TradingStream",
        side_effect=_make_client,
    ):
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
            # this models how alpaca-py's TradingStream.run() invokes the
            # callback.
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
    with _patch_trading_stream(stream):
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
    with _patch_trading_stream(stream):
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
    with _patch_trading_stream(stream):
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
    with _patch_trading_stream(stream):
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
