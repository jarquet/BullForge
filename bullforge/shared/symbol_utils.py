def is_crypto(symbol: str) -> bool:
    """Crypto pairs contain a slash (e.g., BTC/USD). Equities never do."""
    return "/" in symbol
