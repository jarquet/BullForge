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
