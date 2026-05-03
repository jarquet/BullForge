from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiohttp import web

from services.dashboard_service.models_api import register_models_routes


@pytest.fixture
def mock_db_models():
    db = AsyncMock()
    db.query_accuracy_history = AsyncMock(return_value=[
        {
            "date": datetime(2026, 4, 14, 0, 0, 0, tzinfo=timezone.utc),
            "total": 10, "correct": 7, "accuracy": 0.7,
        },
        {
            "date": datetime(2026, 4, 15, 0, 0, 0, tzinfo=timezone.utc),
            "total": 12, "correct": 9, "accuracy": 0.75,
        },
    ])
    db.query_calibration_bins = AsyncMock(return_value=[
        {"bin_low": 0.0, "bin_high": 0.1, "count": 0, "correct": 0,
         "avg_confidence": 0.0, "hit_rate": 0.0},
        {"bin_low": 0.5, "bin_high": 0.6, "count": 5, "correct": 3,
         "avg_confidence": 0.55, "hit_rate": 0.6},
    ])
    db.query_simulated_pnl = AsyncMock(return_value={
        "signal_count": 42, "cumulative_pnl_pct": 0.08,
        "avg_pnl_pct": 0.002, "win_rate": 0.65,
        "daily_series": [
            {"date": datetime(2026, 4, 15, 0, 0, 0, tzinfo=timezone.utc),
             "pnl_pct": 0.04, "count": 10},
        ],
    })
    db.query_regime_breakdown = AsyncMock(return_value=[
        {"regime": "bull", "count": 30, "correct": 22,
         "accuracy": 0.73, "cumulative_pnl_pct": 0.06},
        {"regime": "bear", "count": 12, "correct": 5,
         "accuracy": 0.42, "cumulative_pnl_pct": -0.01},
    ])
    db.query_signal_history = AsyncMock(return_value=[
        {
            "timestamp": datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc),
            "symbol": "AAPL", "timeframe": "1m", "horizon_minutes": 5,
            "predicted_direction": 1, "confidence": 0.78,
            "entry_price": 180.5, "exit_price": 181.2,
            "is_correct": True, "pnl_pct": 0.00388,
        },
    ])
    return db


@pytest.fixture
def models_app(mock_db_models):
    app = web.Application()
    app["db"] = mock_db_models
    register_models_routes(app)
    return app


@pytest.mark.asyncio
async def test_history_endpoint(aiohttp_client, models_app, mock_db_models):
    client = await aiohttp_client(models_app)
    resp = await client.get(
        "/api/models/42/history?from=2026-04-14T00:00:00Z&to=2026-04-16T00:00:00Z",
    )
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)
    assert len(body) == 2
    assert body[0]["accuracy"] == 0.7
    # Verify DB was called with int model_id and parsed datetimes.
    call = mock_db_models.query_accuracy_history.call_args
    assert call.args[0] == 42 or call.kwargs.get("model_id") == 42
    assert call.kwargs.get("from_dt") is not None
    assert call.kwargs.get("to_dt") is not None


@pytest.mark.asyncio
async def test_calibration_endpoint(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/42/calibration")
    assert resp.status == 200
    body = await resp.json()
    assert "bins" in body
    assert len(body["bins"]) == 2
    # Aggregate calibration error computed over bins with count > 0.
    assert "calibration_error" in body


@pytest.mark.asyncio
async def test_simulated_pnl_endpoint(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/42/simulated_pnl")
    assert resp.status == 200
    body = await resp.json()
    assert body["signal_count"] == 42
    assert "daily_series" in body


@pytest.mark.asyncio
async def test_regime_breakdown_endpoint(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/42/regime_breakdown")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)
    regimes = [r["regime"] for r in body]
    assert "bull" in regimes


@pytest.mark.asyncio
async def test_signal_history_endpoint(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/42/signal_history?limit=50")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)
    assert body[0]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_invalid_model_id_returns_400(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/not-a-number/history")
    assert resp.status == 400


@pytest.mark.asyncio
async def test_history_rejects_malformed_from(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/42/history?from=garbage")
    assert resp.status == 400


# --- Meta shadow health endpoint ---------------------------------------------
#
# Endpoint sums three Redis sliding-window counters (15 minute-buckets each)
# and reports skip + drop rates over the inference denominator. Tile turns red
# when (skip + drop) / inference > 1%.

def _make_redis_with_buckets(skips, drops, inferences):
    """Mock RedisClient whose .client.mget returns canned per-key lists.

    Each `*_buckets` arg is a list of length 15 (one entry per minute),
    matching the 15 keys the handler asks for. Use None to simulate a
    missing/expired bucket.
    """
    expected = {"skip": skips, "drop": drops, "inf": inferences}

    async def _mget(*keys):
        # Distinguish series by key prefix.
        if not keys:
            return []
        prefix = keys[0].rsplit(":", 1)[0]
        if "meta_shadow_skips" in prefix:
            series = expected["skip"]
        elif "meta_publisher_finite_drops" in prefix:
            series = expected["drop"]
        elif "meta_inferences" in prefix:
            series = expected["inf"]
        else:
            return [None] * len(keys)
        # Pad/truncate to the actual key count.
        return list(series)[: len(keys)]

    redis = MagicMock()
    redis.client = MagicMock()
    redis.client.mget = AsyncMock(side_effect=_mget)
    return redis


@pytest.fixture
def models_app_with_redis(mock_db_models):
    app = web.Application()
    app["db"] = mock_db_models
    # The meta_shadow_health handler reads the red-threshold from
    # runtime_config; default to the schema value (0.01) so existing tests
    # exercise the same boundary they were written against. Tests that need
    # a tuned threshold can overwrite app["runtime_config"] in-place.
    runtime_config = MagicMock()
    runtime_config.get = AsyncMock(return_value=0.01)
    app["runtime_config"] = runtime_config
    register_models_routes(app)
    return app


@pytest.mark.asyncio
async def test_meta_shadow_health_healthy(aiohttp_client, models_app_with_redis):
    # 200 inferences, 0 skips, 0 drops → rate 0.0 → not red.
    redis = _make_redis_with_buckets(
        skips=[None] * 15,
        drops=[None] * 15,
        inferences=["20"] * 10 + [None] * 5,  # 200 over 10 buckets
    )
    models_app_with_redis["redis_client"] = redis
    client = await aiohttp_client(models_app_with_redis)
    resp = await client.get("/api/models/meta_shadow_health")
    assert resp.status == 200
    body = await resp.json()
    assert body["window_minutes"] == 15
    assert body["skip_count"] == 0
    assert body["drop_count"] == 0
    assert body["inference_count"] == 200
    assert body["skip_plus_drop_rate"] == 0.0
    assert body["threshold"] == 0.01
    assert body["is_red"] is False
    # Successful mget round-trip: tile should render real counts.
    assert body["redis_ok"] is True


@pytest.mark.asyncio
async def test_meta_shadow_health_red_when_rate_above_threshold(
    aiohttp_client, models_app_with_redis,
):
    # 100 inferences, 1 skip, 1 drop → rate 0.02 → red (above 1%).
    redis = _make_redis_with_buckets(
        skips=["1"] + [None] * 14,
        drops=["1"] + [None] * 14,
        inferences=["10"] * 10 + [None] * 5,
    )
    models_app_with_redis["redis_client"] = redis
    client = await aiohttp_client(models_app_with_redis)
    resp = await client.get("/api/models/meta_shadow_health")
    assert resp.status == 200
    body = await resp.json()
    assert body["skip_count"] == 1
    assert body["drop_count"] == 1
    assert body["inference_count"] == 100
    assert body["skip_plus_drop_rate"] == pytest.approx(0.02)
    assert body["is_red"] is True


@pytest.mark.asyncio
async def test_meta_shadow_health_zero_denominator_safe(
    aiohttp_client, models_app_with_redis,
):
    """When the inference denominator is 0 (e.g., service just started), we
    must not divide by zero — rate should be 0.0 and tile not red."""
    redis = _make_redis_with_buckets(
        skips=[None] * 15, drops=[None] * 15, inferences=[None] * 15,
    )
    models_app_with_redis["redis_client"] = redis
    client = await aiohttp_client(models_app_with_redis)
    resp = await client.get("/api/models/meta_shadow_health")
    assert resp.status == 200
    body = await resp.json()
    assert body["inference_count"] == 0
    assert body["skip_plus_drop_rate"] == 0.0
    assert body["is_red"] is False


@pytest.mark.asyncio
async def test_meta_shadow_health_just_below_threshold_not_red(
    aiohttp_client, models_app_with_redis,
):
    # Boundary: rate exactly == threshold (1%) is NOT red. The handler uses
    # strict > comparison so a steady-state 1% baseline does not trigger.
    redis = _make_redis_with_buckets(
        skips=["1"] + [None] * 14,
        drops=[None] * 15,
        inferences=["100"] + [None] * 14,  # 1/100 = 0.01 exactly
    )
    models_app_with_redis["redis_client"] = redis
    client = await aiohttp_client(models_app_with_redis)
    resp = await client.get("/api/models/meta_shadow_health")
    body = await resp.json()
    assert body["skip_plus_drop_rate"] == pytest.approx(0.01)
    assert body["is_red"] is False


@pytest.mark.asyncio
async def test_meta_shadow_health_queries_15_buckets(
    aiohttp_client, models_app_with_redis,
):
    """Verify the bucket math: 15 minute-bucket keys per series, 3 mget calls
    total (one per series). Locks the window contract used by the tile."""
    captured: dict[str, list[str]] = {}

    async def _mget(*keys):
        prefix = keys[0].rsplit(":", 1)[0]
        captured.setdefault(prefix, []).extend(keys)
        return [None] * len(keys)

    redis = MagicMock()
    redis.client = MagicMock()
    redis.client.mget = AsyncMock(side_effect=_mget)
    models_app_with_redis["redis_client"] = redis

    client = await aiohttp_client(models_app_with_redis)
    resp = await client.get("/api/models/meta_shadow_health")
    assert resp.status == 200
    # Three series, each queried as one mget over 15 keys.
    assert len(captured) == 3
    for prefix, keys in captured.items():
        assert len(keys) == 15, (prefix, keys)


@pytest.mark.asyncio
async def test_meta_shadow_health_redis_client_none_returns_redis_ok_false(
    aiohttp_client, models_app_with_redis,
):
    """When the dashboard came up before Redis was wired (redis_client is
    None), the endpoint must return zero counts AND `redis_ok: false` so
    the SPA can render a distinct "Redis unavailable" state instead of a
    misleading healthy zero baseline."""
    # Note: we deliberately do NOT set models_app_with_redis["redis_client"].
    client = await aiohttp_client(models_app_with_redis)
    resp = await client.get("/api/models/meta_shadow_health")
    assert resp.status == 200
    body = await resp.json()
    assert body["redis_ok"] is False
    assert body["skip_count"] == 0
    assert body["drop_count"] == 0
    assert body["inference_count"] == 0
    assert body["skip_plus_drop_rate"] == 0.0
    assert body["is_red"] is False


@pytest.mark.asyncio
async def test_meta_shadow_health_mget_raises_returns_redis_ok_false(
    aiohttp_client, models_app_with_redis, caplog,
):
    """When mget raises (connection refused, timeout, or any other
    Redis-side failure) the endpoint must catch the exception, log it, and
    return zero counts + `redis_ok: false` rather than 500ing the dashboard."""
    redis = MagicMock()
    redis.client = MagicMock()
    redis.client.mget = AsyncMock(
        side_effect=ConnectionError("redis: connection refused"),
    )
    models_app_with_redis["redis_client"] = redis

    client = await aiohttp_client(models_app_with_redis)
    import logging as _logging
    with caplog.at_level(_logging.ERROR):
        resp = await client.get("/api/models/meta_shadow_health")
    assert resp.status == 200
    body = await resp.json()
    assert body["redis_ok"] is False
    assert body["skip_count"] == 0
    assert body["drop_count"] == 0
    assert body["inference_count"] == 0
    assert body["skip_plus_drop_rate"] == 0.0
    assert body["is_red"] is False
    # Verify the exception was logged (caught + logged, not silently swallowed).
    assert any(
        "meta_shadow_health" in r.message and "redis" in r.message.lower()
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_meta_shadow_health_default_threshold(
    aiohttp_client, models_app_with_redis,
):
    """With the schema default (0.01) wired through runtime_config, the
    boundary semantics match the legacy hardcoded constant: rate exactly at
    the threshold is NOT red (strict-> comparison), rate above flips red.
    """
    # Case 1: rate == threshold (1/100 = 0.01). Boundary case, NOT red.
    redis = _make_redis_with_buckets(
        skips=["1"] + [None] * 14,
        drops=[None] * 15,
        inferences=["100"] + [None] * 14,
    )
    models_app_with_redis["redis_client"] = redis
    # runtime_config.get already returns 0.01 from the fixture default.
    client = await aiohttp_client(models_app_with_redis)
    resp = await client.get("/api/models/meta_shadow_health")
    assert resp.status == 200
    body = await resp.json()
    assert body["threshold"] == 0.01
    assert body["skip_plus_drop_rate"] == pytest.approx(0.01)
    assert body["is_red"] is False

    # Case 2: rate just above (2/100 = 0.02). Above threshold => red.
    redis2 = _make_redis_with_buckets(
        skips=["2"] + [None] * 14,
        drops=[None] * 15,
        inferences=["100"] + [None] * 14,
    )
    models_app_with_redis["redis_client"] = redis2
    resp2 = await client.get("/api/models/meta_shadow_health")
    body2 = await resp2.json()
    assert body2["threshold"] == 0.01
    assert body2["skip_plus_drop_rate"] == pytest.approx(0.02)
    assert body2["is_red"] is True


@pytest.mark.asyncio
async def test_meta_shadow_health_runtime_config_raises_falls_back_to_schema_default(
    aiohttp_client, models_app_with_redis, caplog,
):
    """When runtime_config.get raises (it shares Redis with the metrics mget,
    so a real Redis outage breaks both), the handler must catch the
    exception, log it, fall back to the schema default (0.01) for the
    threshold, and still serve the tile rather than 500ing the dashboard.

    Realistic scenario: same Redis outage also breaks mget, so the response
    should be the redis_ok=False payload with the schema-default threshold.
    """
    # runtime_config.get raises — same root cause we expect mget to hit.
    runtime_config = MagicMock()
    runtime_config.get = AsyncMock(
        side_effect=ConnectionError("redis: connection refused"),
    )
    models_app_with_redis["runtime_config"] = runtime_config

    # mget also raises (same Redis outage).
    redis = MagicMock()
    redis.client = MagicMock()
    redis.client.mget = AsyncMock(
        side_effect=ConnectionError("redis: connection refused"),
    )
    models_app_with_redis["redis_client"] = redis

    client = await aiohttp_client(models_app_with_redis)
    import logging as _logging
    with caplog.at_level(_logging.ERROR):
        resp = await client.get("/api/models/meta_shadow_health")
    # Must NOT 500 — the tile is observability-only and falls back gracefully.
    assert resp.status == 200
    body = await resp.json()
    # Schema default surfaces by reference (not a hardcoded literal in the
    # handler). If someone bumps the schema default, this test moves with it.
    assert body["threshold"] == 0.01
    # Sensible response shape for the redis-error path.
    assert body["redis_ok"] is False
    assert body["skip_count"] == 0
    assert body["drop_count"] == 0
    assert body["inference_count"] == 0
    assert body["skip_plus_drop_rate"] == 0.0
    assert body["is_red"] is False
    # Verify the runtime_config failure was logged (not silently swallowed).
    assert any(
        "meta_shadow_health" in r.message and "runtime_config" in r.message
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_meta_shadow_health_operator_tuned_threshold(
    aiohttp_client, models_app_with_redis,
):
    """When an operator raises the red threshold via runtime_config, the
    handler must (a) surface the new value as `threshold` in the response and
    (b) compute `is_red` against it — proving no hardcoded 0.01 remains.
    """
    # Operator-tuned threshold: 5%.
    runtime_config = MagicMock()
    runtime_config.get = AsyncMock(return_value=0.05)
    models_app_with_redis["runtime_config"] = runtime_config

    # Case 1: rate 0.02 — was red under the default 0.01, now NOT red at 0.05.
    redis = _make_redis_with_buckets(
        skips=["2"] + [None] * 14,
        drops=[None] * 15,
        inferences=["100"] + [None] * 14,
    )
    models_app_with_redis["redis_client"] = redis
    client = await aiohttp_client(models_app_with_redis)
    resp = await client.get("/api/models/meta_shadow_health")
    assert resp.status == 200
    body = await resp.json()
    assert body["threshold"] == 0.05
    assert body["skip_plus_drop_rate"] == pytest.approx(0.02)
    assert body["is_red"] is False

    # Case 2: rate 0.06 — above the tuned 5% threshold, flips red.
    redis2 = _make_redis_with_buckets(
        skips=["6"] + [None] * 14,
        drops=[None] * 15,
        inferences=["100"] + [None] * 14,
    )
    models_app_with_redis["redis_client"] = redis2
    resp2 = await client.get("/api/models/meta_shadow_health")
    body2 = await resp2.json()
    assert body2["threshold"] == 0.05
    assert body2["skip_plus_drop_rate"] == pytest.approx(0.06)
    assert body2["is_red"] is True


# --- Feature importance fallback: equity exclusion buffers --------------------
#
# `_handle_feature_importance` falls back to permutation importance for
# classifiers without `feature_importances_`. Inside that fallback it rebuilds
# the training dataset via DatasetBuilder, which (post-A2, 2026-05-03) accepts
# `open_buffer_min` / `close_buffer_min` to filter out rows whose forward-return
# target lands in the equity exclusion zone. The dashboard fallback must thread
# these through from RuntimeConfig so it sees the same population the live
# trainer would see today.


def _make_model_registry_row(model_type: str = "hist_gradient_boost"):
    """Mimic asyncpg's Record for the model_registry SELECT in
    _handle_feature_importance."""
    from datetime import datetime, timezone
    return {
        "artifact_path": "/tmp/does-not-matter/model.pkl",
        "feature_columns": '["f1","f2","f3"]',
        "model_type": model_type,
        "symbol": "AAPL",
        "timeframe": "1m",
        "horizon_minutes": 5,
        "trained_at": datetime(2026, 4, 30, 14, 0, 0, tzinfo=timezone.utc),
    }


def _wire_model_row_on(db, row):
    """Attach a `db.pool.acquire()` -> conn.fetchrow context-manager mock to
    the AsyncMock from the existing `mock_db_models` fixture."""
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=row)
    conn_cm = AsyncMock()
    conn_cm.__aenter__ = AsyncMock(return_value=conn)
    conn_cm.__aexit__ = AsyncMock(return_value=False)
    db.pool = MagicMock()
    db.pool.acquire = MagicMock(return_value=conn_cm)


@pytest.mark.asyncio
async def test_feature_importance_fallback_passes_runtime_buffers_to_builder(
    aiohttp_client, models_app_with_redis, mock_db_models,
):
    """The permutation-importance fallback must read both equity-window
    buffer keys from RuntimeConfig and thread them into DatasetBuilder.build,
    so the rebuilt training set respects the same exclusion-zone filter
    Trainer.train uses today.
    """
    from unittest.mock import patch
    from services.model_service.dataset import DatasetBuilder

    # Wire a model row whose model_type lacks feature_importances_ so the
    # handler hits the permutation branch.
    _wire_model_row_on(mock_db_models, _make_model_registry_row("hist_gradient_boost"))

    # RuntimeConfig surfaces distinct values so a key swap (open<->close) fails
    # the assertion rather than slipping through.
    async def _rc_get(key):
        return {
            "equity_window_open_buffer_minutes": 60,
            "equity_window_close_buffer_minutes": 45,
        }[key]

    runtime_config = MagicMock()
    runtime_config.get = AsyncMock(side_effect=_rc_get)
    models_app_with_redis["runtime_config"] = runtime_config

    # Fake pipeline: no `feature_importances_` attribute → hits permutation branch.
    fake_pipeline = MagicMock(spec=[])  # spec=[] → no attrs by default
    # The handler inspects pipeline.steps[-1][1] via hasattr(pipeline, "steps").
    # With spec=[] hasattr returns False, so `classifier` == pipeline itself,
    # which also lacks feature_importances_. That routes us to the fallback.

    captured = {}

    async def spy_build(self, *args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return None  # short-circuit to the available=False return path

    from services.model_service.model_store import ModelStore

    with patch.object(ModelStore, "load", return_value=fake_pipeline), \
         patch.object(DatasetBuilder, "build", spy_build):
        client = await aiohttp_client(models_app_with_redis)
        resp = await client.get("/api/models/42/feature_importance")

    # Even though build returned None (handler responds with available=False),
    # the kwargs must have reached it.
    assert resp.status == 200
    assert captured["kwargs"].get("open_buffer_min") == 60
    assert captured["kwargs"].get("close_buffer_min") == 45
