from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
import json
import pytest

from services.model_service.main import (
    on_feature_update_factory, on_model_retrained_factory,
    on_market_tick_factory,
)


def _ts():
    return datetime(2026, 4, 30, 14, 0, tzinfo=timezone.utc)


def _make_inference(predict_meta_return):
    """predict_meta_return may be either a (signals, feature_vec) tuple
    (new contract) or a bare signals list (legacy — wrapped as
    (signals, {})). The handler wiring expects predict_meta to surface
    both the signals AND the assembled feature vector so that the live
    model:meta_signals payload carries the actual dict the meta
    estimator was scored against, not an empty placeholder."""
    inference = AsyncMock()
    inference.predict.return_value = [
        MagicMock(model_id=1, predicted_direction=1, confidence=0.7,
                  symbol="AAPL", timestamp=_ts(),
                  horizon_minutes=5)
    ]
    if isinstance(predict_meta_return, tuple):
        inference.predict_meta.return_value = predict_meta_return
    else:
        inference.predict_meta.return_value = (predict_meta_return, {})
    return inference


def _make_db():
    db = AsyncMock()
    db.query_feature_rows.return_value = [MagicMock(
        timestamp=_ts(),
        features={
            "atr_14": 0.5, "sma_20": 100.0, "realized_vol": 0.02,
            "dist_sma_50": 0.0, "dist_sma_200": 0.0, "dist_sma_200d": 0.0,
        },
    )]
    # Default latest close > 0 so meta inference fires; tests that care
    # about the close override this explicitly.
    db.query_latest_close.return_value = 100.0
    return db


def _make_rb():
    rb = MagicMock()
    rb.regime_at = MagicMock(return_value={
        "spy_trend_1m": 0.0, "spy_trend_1h": 0.0, "spy_trend_1d": 0.0,
        "spy_momentum": 0.0, "vol_regime": 1.0,
    })
    return rb


@pytest.mark.asyncio
async def test_handler_publishes_meta_signals_when_enabled():
    rc = AsyncMock()
    rc.get.side_effect = lambda k: {
        "meta_inference_enabled": True,
        "meta_pair_grid": [[2, 1], [3, 1.5], [4, 2]],
        "meta_rolling_stats_ttl_seconds": 600,
    }.get(k)

    redis = AsyncMock()
    inference = _make_inference([{
        "direction": 1, "estimator_type": "lightgbm",
        "entry_probability": 0.6, "kelly_fraction": 0.05,
        "pair_index": 1, "target_pct": 0.01, "stop_pct": 0.005,
        "model_id": 11,
    }])
    db = _make_db()

    handler = on_feature_update_factory(
        inference=inference, redis=redis, runtime_config=rc, db=db,
        regime_builder=_make_rb(),
    )
    await handler(json.dumps({"symbol": "AAPL", "timeframe": "1m"}))

    channels = [c.args[0] for c in redis.publish.call_args_list]
    assert "model:predictions" in channels
    assert "model:meta_signals" in channels


@pytest.mark.asyncio
async def test_handler_skips_meta_publish_when_disabled():
    rc = AsyncMock()
    rc.get.side_effect = lambda k: {"meta_inference_enabled": False}.get(k)
    redis = AsyncMock()
    inference = _make_inference([])
    db = _make_db()
    handler = on_feature_update_factory(
        inference=inference, redis=redis, runtime_config=rc, db=db,
        regime_builder=_make_rb(),
    )
    await handler(json.dumps({"symbol": "AAPL", "timeframe": "1m"}))
    channels = [c.args[0] for c in redis.publish.call_args_list]
    assert "model:meta_signals" not in channels


@pytest.mark.asyncio
async def test_on_model_retrained_refreshes_in_memory_state():
    """retrain_now.py runs in a SEPARATE process and creates its own
    throwaway InferenceEngine. The live model_service container's actual
    InferenceEngine never gets refreshed, leaving in-memory _models /
    _meta_models pointing at the previous retrain's artifacts. Symptom:
    inference fires LightGBM shape errors after a successful manual
    retrain because the in-memory model has 194 features but the new
    feature_vector has 134. Fix: have the live model_service subscribe
    to model:retrained — published unconditionally at the end of every
    run_once — and reload both base and meta models on receipt."""
    inference = AsyncMock()
    handler = on_model_retrained_factory(inference=inference)
    await handler(json.dumps({"as_of": _ts().isoformat(), "ok": 180}))

    inference.load_models.assert_awaited_once()
    inference.load_meta_models.assert_awaited_once()


@pytest.mark.asyncio
async def test_on_model_retrained_swallows_loader_exceptions():
    """A retrain notification must never bring down the redis subscriber.
    If load_meta_models raises (corrupt artifact, transient DB error),
    the handler logs and continues so future feature:updates still flow."""
    inference = AsyncMock()
    inference.load_models.side_effect = RuntimeError("boom")
    handler = on_model_retrained_factory(inference=inference)
    # Must not raise.
    await handler(json.dumps({"as_of": _ts().isoformat()}))


@pytest.mark.asyncio
async def test_handler_skips_meta_publish_when_no_signals_returned():
    rc = AsyncMock()
    rc.get.side_effect = lambda k: {
        "meta_inference_enabled": True,
        "meta_pair_grid": [[2, 1]],
        "meta_rolling_stats_ttl_seconds": 600,
    }.get(k)
    redis = AsyncMock()
    inference = _make_inference([])  # predict_meta returns no signals
    db = _make_db()
    handler = on_feature_update_factory(
        inference=inference, redis=redis, runtime_config=rc, db=db,
        regime_builder=_make_rb(),
    )
    await handler(json.dumps({"symbol": "AAPL", "timeframe": "1m"}))
    channels = [c.args[0] for c in redis.publish.call_args_list]
    assert "model:meta_signals" not in channels


@pytest.mark.asyncio
async def test_handler_publishes_real_feature_vector_in_meta_signals():
    """The model:meta_signals payload must carry the *actual* assembled
    feature vector (the dict the meta estimator was scored against), not
    an empty placeholder. Downstream MetaShadowWriter persists this dict
    verbatim into meta_features.feature_vector — leaving it empty meant
    every live row was a useless {"_": 0.0} stub.
    """
    rc = AsyncMock()
    rc.get.side_effect = lambda k: {
        "meta_inference_enabled": True,
        "meta_pair_grid": [[2, 1], [3, 1.5], [4, 2]],
        "meta_rolling_stats_ttl_seconds": 600,
    }.get(k)
    redis = AsyncMock()

    real_vec = {
        "base_1_dir": 1.0, "base_1_conf": 0.7,
        "base_1_rolling_acc": 0.55, "base_1_cal_err": 0.05,
        "base_1_disabled": 0.0,
        "regime_spy_trend_1m": 0.0, "regime_spy_trend_1h": 0.0,
        "regime_spy_trend_1d": 0.0, "regime_spy_momentum": 0.0,
        "regime_vol": 1.0,
        "time_bucket": 56.0, "minutes_since_open": 30.0,
        "minutes_to_close": 360.0, "weekday": 3.0,
        "sym_realized_vol": 0.02, "sym_atr_price_ratio": 0.005,
        "sym_dist_sma_50": 0.0, "sym_dist_sma_200bar": 0.0,
        "sym_dist_sma_200day": 0.0,
    }
    signals = [{
        "direction": 1, "estimator_type": "lightgbm",
        "entry_probability": 0.6, "kelly_fraction": 0.05,
        "pair_index": 1, "target_pct": 0.01, "stop_pct": 0.005,
        "model_id": 11,
    }]
    inference = _make_inference((signals, real_vec))
    db = _make_db()
    handler = on_feature_update_factory(
        inference=inference, redis=redis, runtime_config=rc, db=db,
        regime_builder=_make_rb(),
    )
    await handler(json.dumps({"symbol": "AAPL", "timeframe": "1m"}))

    meta_publishes = [
        c for c in redis.publish.call_args_list
        if c.args[0] == "model:meta_signals"
    ]
    assert len(meta_publishes) == 1
    payload = json.loads(meta_publishes[0].args[1])
    # Must round-trip the full dict, not be {} or {"_": 0.0}.
    assert payload["feature_vector"] == real_vec
    assert payload["feature_vector"]  # non-empty
    assert "_" not in payload["feature_vector"]
    # All values JSON-friendly floats.
    for k, v in payload["feature_vector"].items():
        assert isinstance(k, str)
        assert isinstance(v, float)


@pytest.mark.asyncio
async def test_atr_pct_uses_real_close_not_sma_20():
    """The meta-shadow path scales target/stop multipliers by atr_pct =
    atr_14 / close. Earlier code used sma_20 as a close stand-in because
    the feature row didn't carry the raw close — that biases atr_pct away
    from the true ratio during trends (sma_20 lags the actual close).

    Fix: source the latest bars_1m close at-or-before the feature row
    timestamp via Database.query_latest_close and divide by that. Here
    we set sma_20=100.0 but the *real* close=120.0 — atr_price_ratio
    must come out as atr_14/close=0.5/120.0, not the old
    atr_14/sma_20=0.5/100.0.
    """
    rc = AsyncMock()
    rc.get.side_effect = lambda k: {
        "meta_inference_enabled": True,
        "meta_pair_grid": [[2, 1], [3, 1.5], [4, 2]],
        "meta_rolling_stats_ttl_seconds": 600,
    }.get(k)
    redis = AsyncMock()
    inference = _make_inference([{
        "direction": 1, "estimator_type": "lightgbm",
        "entry_probability": 0.6, "kelly_fraction": 0.05,
        "pair_index": 1, "target_pct": 0.01, "stop_pct": 0.005,
        "model_id": 11,
    }])
    db = _make_db()
    db.query_latest_close.return_value = 120.0

    handler = on_feature_update_factory(
        inference=inference, redis=redis, runtime_config=rc, db=db,
        regime_builder=_make_rb(),
    )
    await handler(json.dumps({"symbol": "AAPL", "timeframe": "1m"}))

    # Inspect the symbol_state that was actually handed to predict_meta.
    inference.predict_meta.assert_awaited_once()
    call_kwargs = inference.predict_meta.await_args.kwargs
    symbol_state = call_kwargs["symbol_state"]
    expected = 0.5 / 120.0
    legacy_wrong = 0.5 / 100.0
    assert symbol_state["atr_price_ratio"] == pytest.approx(expected)
    assert symbol_state["atr_price_ratio"] != pytest.approx(legacy_wrong)
    # And the latest-close lookup happened with the feature row's UTC ts.
    db.query_latest_close.assert_awaited_once()
    args, kwargs = (db.query_latest_close.await_args.args,
                    db.query_latest_close.await_args.kwargs)
    # Symbol & timestamp positional or kw — accept either binding style.
    bound = list(args) + list(kwargs.values())
    assert "AAPL" in bound
    assert _ts() in bound


@pytest.mark.asyncio
async def test_atr_pct_missing_close_propagates_nan():
    """When bars_1m has no close at-or-before the feature timestamp
    (cold start, gapped symbol), query_latest_close returns None.
    The handler must propagate NaN into atr_price_ratio rather than
    silently falling back to sma_20 — predict_meta's NaN filter (Task 3)
    will then drop sym_atr_price_ratio from the published feature
    vector instead of poisoning it with a biased value.
    """
    import math
    rc = AsyncMock()
    rc.get.side_effect = lambda k: {
        "meta_inference_enabled": True,
        "meta_pair_grid": [[2, 1]],
        "meta_rolling_stats_ttl_seconds": 600,
    }.get(k)
    redis = AsyncMock()
    inference = _make_inference([])  # predict_meta returns no signals (atr<=0)
    db = _make_db()
    db.query_latest_close.return_value = None

    handler = on_feature_update_factory(
        inference=inference, redis=redis, runtime_config=rc, db=db,
        regime_builder=_make_rb(),
    )
    await handler(json.dumps({"symbol": "AAPL", "timeframe": "1m"}))

    inference.predict_meta.assert_awaited_once()
    symbol_state = inference.predict_meta.await_args.kwargs["symbol_state"]
    # NaN — not 0.0, not the legacy sma_20 fallback.
    assert math.isnan(symbol_state["atr_price_ratio"])


@pytest.mark.asyncio
async def test_on_market_tick_appends_spy_bar_to_regime_builder():
    """SPY market:ticks events must route into RegimeBuilder.append_bar
    so the regime context stays fresh past meta_regime_max_staleness_hours.
    Without this subscription, model_service's RegimeBuilder freezes at
    startup state and silently zeros every regime feature after 48h."""
    rb = MagicMock()
    rb.append_bar = MagicMock()
    handler = on_market_tick_factory(regime_builder=rb)
    ts = datetime(2026, 4, 30, 18, 30, tzinfo=timezone.utc)
    await handler(json.dumps({
        "symbol": "SPY",
        "timestamp": ts.isoformat(),
        "open": 400.0, "high": 401.0, "low": 399.5,
        "close": 400.5, "volume": 12345.0,
    }))
    # Handler signature is fixed-positional: append_bar(ts, close).
    rb.append_bar.assert_called_once_with(ts, 400.5)


@pytest.mark.asyncio
async def test_on_market_tick_ignores_non_spy_symbols():
    """Only SPY drives the regime builder. AAPL ticks must no-op."""
    rb = MagicMock()
    rb.append_bar = MagicMock()
    handler = on_market_tick_factory(regime_builder=rb)
    await handler(json.dumps({
        "symbol": "AAPL",
        "timestamp": _ts().isoformat(),
        "open": 100.0, "high": 100.5, "low": 99.5,
        "close": 100.2, "volume": 1000.0,
    }))
    rb.append_bar.assert_not_called()


@pytest.mark.asyncio
async def test_on_market_tick_swallows_malformed_message():
    """Mirror the on_model_retrained defensiveness — a malformed payload
    must never bring down the redis subscriber."""
    rb = MagicMock()
    rb.append_bar = MagicMock()
    handler = on_market_tick_factory(regime_builder=rb)

    # Garbage JSON: parse fails before append_bar is ever reached.
    await handler("not json at all")
    rb.append_bar.assert_not_called()
    rb.append_bar.reset_mock()

    # Missing required fields: validation rejects before append_bar.
    await handler(json.dumps({"symbol": "SPY"}))
    rb.append_bar.assert_not_called()
    rb.append_bar.reset_mock()

    # append_bar that itself raises must also be swallowed. Here the call
    # IS attempted (payload is valid), so assert call happened — not that
    # it didn't.
    rb.append_bar.side_effect = RuntimeError("boom")
    await handler(json.dumps({
        "symbol": "SPY",
        "timestamp": _ts().isoformat(),
        "close": 400.0,
    }))
    assert rb.append_bar.call_count >= 1


@pytest.mark.asyncio
async def test_meta_signals_round_trip_into_meta_features_writer():
    """End-to-end round-trip: publisher's feature_vector ≡ what
    MetaShadowWriter writes into meta_features.feature_vector."""
    from services.execution_service.meta_shadow_writer import MetaShadowWriter

    rc = AsyncMock()
    rc.get.side_effect = lambda k: {
        "meta_inference_enabled": True,
        "meta_pair_grid": [[2, 1], [3, 1.5], [4, 2]],
        "meta_rolling_stats_ttl_seconds": 600,
    }.get(k)

    captured: dict = {}

    async def _capture_publish(channel, message):
        if channel == "model:meta_signals":
            captured["msg"] = message

    redis = AsyncMock()
    redis.publish.side_effect = _capture_publish

    real_vec = {
        "base_1_dir": 1.0, "base_1_conf": 0.7,
        "base_1_rolling_acc": 0.5, "base_1_cal_err": 0.04,
        "base_1_disabled": 0.0,
        "regime_spy_trend_1m": 0.1, "regime_spy_trend_1h": 0.0,
        "regime_spy_trend_1d": 0.0, "regime_spy_momentum": 0.0,
        "regime_vol": 1.0,
        "time_bucket": 56.0, "minutes_since_open": 30.0,
        "minutes_to_close": 360.0, "weekday": 3.0,
        "sym_realized_vol": 0.02, "sym_atr_price_ratio": 0.005,
        "sym_dist_sma_50": 0.0, "sym_dist_sma_200bar": 0.0,
        "sym_dist_sma_200day": 0.0,
    }
    signals = [{
        "direction": 1, "estimator_type": "lightgbm",
        "entry_probability": 0.6, "kelly_fraction": 0.05,
        "pair_index": 1, "target_pct": 0.01, "stop_pct": 0.005,
        "model_id": 11,
    }]
    inference = _make_inference((signals, real_vec))
    db_pub = _make_db()
    handler = on_feature_update_factory(
        inference=inference, redis=redis, runtime_config=rc, db=db_pub,
        regime_builder=_make_rb(),
    )
    await handler(json.dumps({"symbol": "AAPL", "timeframe": "1m"}))

    assert "msg" in captured, "publisher never sent model:meta_signals"

    db_writer = AsyncMock()
    writer = MetaShadowWriter(db=db_writer)
    await writer.handle_message(captured["msg"])

    assert db_writer.insert_meta_feature.await_count == 1
    written_row = db_writer.insert_meta_feature.await_args.args[0]
    # The full dict round-trips to the writer's MetaFeatureRow.
    assert written_row.feature_vector == real_vec
    # And no placeholder fallback.
    assert written_row.feature_vector != {"_": 0.0}
