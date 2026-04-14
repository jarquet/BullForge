from __future__ import annotations


def is_crypto(symbol: str) -> bool:
    """Crypto pairs use BASE/QUOTE (e.g. BTC/USD); equity tickers have no slash."""
    return "/" in symbol


def symbol_kind(symbol: str) -> str:
    return "crypto" if is_crypto(symbol) else "equity"
