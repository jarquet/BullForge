from datetime import datetime, timedelta, timezone

import pytest

from services.backtest_service.splitter import (
    Fold,
    WalkForwardSplitter,
)


UTC = timezone.utc


def _dt(y, m, d):
    return datetime(y, m, d, tzinfo=UTC)


def test_rolling_splits_basic():
    splitter = WalkForwardSplitter(
        train_days=10, test_days=5, embargo_minutes=30, mode="rolling",
    )
    folds = list(splitter.split(_dt(2025, 1, 1), _dt(2025, 2, 1)))

    assert len(folds) > 0
    first = folds[0]
    assert isinstance(first, Fold)
    assert first.train_start == _dt(2025, 1, 1)
    assert first.train_end == _dt(2025, 1, 11)
    assert first.test_start == first.train_end + timedelta(minutes=30)
    assert first.test_end == first.test_start + timedelta(days=5)


def test_rolling_step_equals_test_days():
    splitter = WalkForwardSplitter(
        train_days=10, test_days=5, embargo_minutes=30, mode="rolling",
    )
    folds = list(splitter.split(_dt(2025, 1, 1), _dt(2025, 2, 1)))

    deltas = [
        (folds[i + 1].train_start - folds[i].train_start).days
        for i in range(len(folds) - 1)
    ]
    assert all(d == 5 for d in deltas)


def test_anchored_train_start_is_constant():
    splitter = WalkForwardSplitter(
        train_days=10, test_days=5, embargo_minutes=30, mode="anchored",
    )
    folds = list(splitter.split(_dt(2025, 1, 1), _dt(2025, 2, 15)))

    assert len(folds) >= 2
    assert all(f.train_start == _dt(2025, 1, 1) for f in folds)
    deltas = [
        (folds[i + 1].train_end - folds[i].train_end).days
        for i in range(len(folds) - 1)
    ]
    assert all(d == 5 for d in deltas)


def test_no_folds_when_range_too_short():
    splitter = WalkForwardSplitter(
        train_days=10, test_days=5, embargo_minutes=30, mode="rolling",
    )
    folds = list(splitter.split(_dt(2025, 1, 1), _dt(2025, 1, 10)))
    assert folds == []


def test_invalid_mode_raises():
    with pytest.raises(ValueError):
        WalkForwardSplitter(
            train_days=10, test_days=5, embargo_minutes=30, mode="bogus",
        )


def test_invalid_window_sizes_raise():
    with pytest.raises(ValueError):
        WalkForwardSplitter(
            train_days=0, test_days=5, embargo_minutes=30, mode="rolling",
        )
    with pytest.raises(ValueError):
        WalkForwardSplitter(
            train_days=10, test_days=0, embargo_minutes=30, mode="rolling",
        )
    with pytest.raises(ValueError):
        WalkForwardSplitter(
            train_days=10, test_days=5, embargo_minutes=-1, mode="rolling",
        )
