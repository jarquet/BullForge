import pytest

from shared.models import BacktestScope


def test_backtest_scope_is_frozen():
    s = BacktestScope(backtest_run_id=1, fold_index=0)
    with pytest.raises((AttributeError, Exception)):
        s.backtest_run_id = 2  # type: ignore[misc]


def test_backtest_scope_equality_and_hash():
    a = BacktestScope(backtest_run_id=1, fold_index=0)
    b = BacktestScope(backtest_run_id=1, fold_index=0)
    c = BacktestScope(backtest_run_id=1, fold_index=1)
    assert a == b
    assert a != c
    assert hash(a) == hash(b)
    assert {a, b, c} == {a, c}
