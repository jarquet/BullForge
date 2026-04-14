from shared.symbol_utils import is_crypto


def test_crypto_pair():
    assert is_crypto("BTC/USD") is True
    assert is_crypto("ETH/USD") is True


def test_equity_symbol():
    assert is_crypto("SPY") is False
    assert is_crypto("QQQ") is False
    assert is_crypto("AAPL") is False


def test_empty_and_edge():
    assert is_crypto("") is False
    assert is_crypto("X/Y/Z") is True
