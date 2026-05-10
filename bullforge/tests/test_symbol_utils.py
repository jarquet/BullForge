from shared.symbol_utils import is_crypto, normalize_alpaca_symbol


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


def test_normalize_alpaca_symbol_crypto_no_slash():
    assert normalize_alpaca_symbol("BTCUSD") == "BTC/USD"
    assert normalize_alpaca_symbol("ETHUSD") == "ETH/USD"


def test_normalize_alpaca_symbol_crypto_stablecoin_quotes():
    assert normalize_alpaca_symbol("BTCUSDT") == "BTC/USDT"
    assert normalize_alpaca_symbol("ETHUSDC") == "ETH/USDC"


def test_normalize_alpaca_symbol_already_canonical():
    assert normalize_alpaca_symbol("BTC/USD") == "BTC/USD"
    assert normalize_alpaca_symbol("ETH/USDT") == "ETH/USDT"


def test_normalize_alpaca_symbol_equity_unchanged():
    assert normalize_alpaca_symbol("SPY") == "SPY"
    assert normalize_alpaca_symbol("AAPL") == "AAPL"
    assert normalize_alpaca_symbol("NVDA") == "NVDA"


def test_normalize_alpaca_symbol_quote_only_unchanged():
    # Bare 'USD' shouldn't be split into '/USD'.
    assert normalize_alpaca_symbol("USD") == "USD"
    assert normalize_alpaca_symbol("USDT") == "USDT"
