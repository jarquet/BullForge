from datetime import datetime, timezone
from services.backtest_service.equity_tracker import EquityTracker


UTC = timezone.utc


def test_records_equity_points_in_order():
    et = EquityTracker()
    et.record(datetime(2025, 1, 2, 15, 0, tzinfo=UTC), equity=100_000.0, cash=100_000.0)
    et.record(datetime(2025, 1, 2, 15, 1, tzinfo=UTC), equity=100_500.0, cash=90_000.0)
    points = et.points()
    assert len(points) == 2
    assert points[0].equity == 100_000.0
    assert points[1].equity == 100_500.0


def test_returns_series_for_metrics():
    et = EquityTracker()
    et.record(datetime(2025, 1, 2, 15, 0, tzinfo=UTC), equity=100_000.0, cash=100_000.0)
    et.record(datetime(2025, 1, 2, 15, 1, tzinfo=UTC), equity=101_000.0, cash=100_000.0)
    et.record(datetime(2025, 1, 2, 15, 2, tzinfo=UTC), equity=100_500.0, cash=100_000.0)
    rets = et.returns()
    assert len(rets) == 2
    assert abs(rets[0] - 0.01) < 1e-9
    assert abs(rets[1] - (-0.00495)) < 1e-4
