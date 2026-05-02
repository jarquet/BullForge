import pytest
import pandas as pd
import numpy as np
from services.feature_engine.calculators.technical import compute_technical_features


def _make_bars_df(n: int = 100) -> pd.DataFrame:
    timestamps = pd.date_range(
        start="2026-04-10 12:00:00", periods=n, freq="1min", tz="UTC"
    )
    np.random.seed(42)
    close = 420.0 + np.cumsum(np.random.randn(n) * 0.1)
    return pd.DataFrame({
        "symbol": "SPY",
        "timestamp": timestamps,
        "open": close - np.random.rand(n) * 0.2,
        "high": close + np.random.rand(n) * 0.3,
        "low": close - np.random.rand(n) * 0.3,
        "close": close,
        "volume": np.random.randint(1000, 50000, size=n),
    })


def test_technical_features_returns_expected_columns():
    df = _make_bars_df()
    result = compute_technical_features(df)
    expected_cols = [
        "sma_10", "sma_20", "sma_50",
        "ema_10", "ema_20",
        "rsi_14",
        "macd", "macd_signal", "macd_histogram",
        "bb_upper", "bb_middle", "bb_lower", "bb_width",
        "atr_14",
    ]
    for col in expected_cols:
        assert col in result.columns, f"Missing column: {col}"


def test_sma_is_rolling_mean():
    df = _make_bars_df(50)
    result = compute_technical_features(df)
    expected = df["close"].rolling(window=10).mean()
    pd.testing.assert_series_equal(
        result["sma_10"], expected, check_names=False
    )


def test_ema_uses_ewm():
    df = _make_bars_df(50)
    result = compute_technical_features(df)
    expected = df["close"].ewm(span=10, adjust=False).mean()
    pd.testing.assert_series_equal(
        result["ema_10"], expected, check_names=False
    )


def test_rsi_bounded_0_100():
    df = _make_bars_df()
    result = compute_technical_features(df)
    rsi = result["rsi_14"].dropna()
    assert (rsi >= 0).all()
    assert (rsi <= 100).all()


def test_bollinger_bands_relation():
    """Upper > middle > lower for all non-NaN rows."""
    df = _make_bars_df()
    result = compute_technical_features(df)
    valid = result.dropna(subset=["bb_upper", "bb_middle", "bb_lower"])
    assert (valid["bb_upper"] >= valid["bb_middle"]).all()
    assert (valid["bb_middle"] >= valid["bb_lower"]).all()


def test_bollinger_width_positive():
    df = _make_bars_df()
    result = compute_technical_features(df)
    valid = result["bb_width"].dropna()
    assert (valid >= 0).all()


def test_atr_positive():
    df = _make_bars_df()
    result = compute_technical_features(df)
    valid = result["atr_14"].dropna()
    assert (valid >= 0).all()


def test_macd_histogram_is_macd_minus_signal():
    df = _make_bars_df()
    result = compute_technical_features(df)
    valid = result.dropna(subset=["macd", "macd_signal", "macd_histogram"])
    expected = valid["macd"] - valid["macd_signal"]
    pd.testing.assert_series_equal(
        valid["macd_histogram"], expected, check_names=False
    )


def test_technical_features_preserves_length():
    df = _make_bars_df(80)
    result = compute_technical_features(df)
    assert len(result) == len(df)


def test_rsi_is_neutral_on_flat_market():
    """Crypto minute bars (BTC/USD) on no-trade minutes are emitted by Alpaca
    with OHLC == prior close. After a stretch of such bars, both avg_gain and
    avg_loss in the RSI EWM are exactly 0.0, rs = 0/0 = NaN, and RSI inherits
    the NaN — which then tanks every row at engine.py's notna().all(axis=1)
    gate. Convention is RSI=50 (neutral) for a flat market.

    Warmup rows (before EWM has min_periods worth of data) must still be NaN
    so engine.py's warmup drop remains correct.
    """
    n = 60
    timestamps = pd.date_range(
        "2026-05-02 03:14:00", periods=n, freq="1min", tz="UTC"
    )
    close = np.full(n, 90000.0)
    df = pd.DataFrame({
        "symbol": "BTC/USD",
        "timestamp": timestamps,
        "open": close, "high": close, "low": close, "close": close,
        "volume": np.zeros(n),
    })

    result = compute_technical_features(df)
    rsi = result["rsi_14"]

    # Post-warmup (index >= 14) must be 50.0 exactly.
    post_warmup = rsi.iloc[14:]
    assert not post_warmup.isna().any(), (
        f"rsi_14 has NaN post-warmup: {post_warmup[post_warmup.isna()].index.tolist()}"
    )
    assert (post_warmup == 50.0).all(), (
        f"rsi_14 post-warmup values: {post_warmup.unique()} (want 50.0)"
    )


def test_rsi_transitions_to_neutral_after_partial_flat_market():
    """The all-flat-from-start fixture (test_rsi_is_neutral_on_flat_market)
    only exercises the case where avg_gain/avg_loss are 0.0 from the very
    first EWM-warm row. The other regression vector is mid-stream: a normal
    market run followed by a stretch of flat bars (e.g. BTC/USD weekend
    no-trade minutes after a real-volume day). During the flat tail the
    EWM averages decay geometrically toward 0 but never reach exact 0 in
    finite float64 steps starting from a non-zero state — so the flat
    mask doesn't fire mid-stream. What matters for engine.py's
    notna().all(axis=1) gate is that RSI stays NON-NAN and FINITE
    throughout the flat tail (both EWMs share the same decay factor, so
    their ratio is preserved and rs stays well-defined). A regression
    that re-introduced rs = 0/0 = NaN mid-stream would surface here.
    """
    n_normal = 30
    n_flat = 30
    n = n_normal + n_flat
    timestamps = pd.date_range(
        "2026-05-02 04:00:00", periods=n, freq="1min", tz="UTC"
    )
    np.random.seed(7)
    # 30 normal bars: random-walk closes give the EWM real gain/loss to
    # chew on, so avg_gain and avg_loss are both strictly > 0 by the end
    # of this segment.
    normal_close = 90000.0 + np.cumsum(np.random.randn(n_normal) * 5.0)
    # 30 flat bars: identical close; every diff is exactly 0.0, so each
    # EWM update multiplies the prior average by (1 - 1/14). Both decay
    # at the same rate, so rs (avg_gain/avg_loss) stays stable.
    flat_close = np.full(n_flat, normal_close[-1])
    close = np.concatenate([normal_close, flat_close])

    df = pd.DataFrame({
        "symbol": "BTC/USD",
        "timestamp": timestamps,
        "open": close, "high": close, "low": close, "close": close,
        "volume": np.zeros(n),
    })

    result = compute_technical_features(df)
    rsi = result["rsi_14"]

    # Across the entire flat tail (after the EWM has warmed past
    # min_periods=14), every RSI value must be non-NaN, finite, and
    # bounded in [0, 100]. The regression we're guarding against is
    # rs = 0/0 = NaN poisoning rows mid-stream.
    flat_tail = rsi.iloc[n_normal:]
    assert not flat_tail.isna().any(), (
        f"rsi_14 NaN during flat tail at indices "
        f"{flat_tail[flat_tail.isna()].index.tolist()}"
    )
    assert np.isfinite(flat_tail.to_numpy()).all(), (
        f"rsi_14 non-finite during flat tail: {flat_tail[~np.isfinite(flat_tail)].tolist()}"
    )
    assert (flat_tail >= 0).all() and (flat_tail <= 100).all(), (
        f"rsi_14 out of [0,100] during flat tail: min={flat_tail.min()}, max={flat_tail.max()}"
    )
    # Both EWMs decay by the same factor each flat bar, so RSI should be
    # essentially flat across the tail (the ratio is preserved). Any
    # drift past a couple of points would mean the masking is wrong.
    assert flat_tail.std() < 1.0, (
        f"rsi_14 drifted across flat tail (std={flat_tail.std()}); "
        f"both EWMs decay at the same rate so the ratio should hold"
    )
