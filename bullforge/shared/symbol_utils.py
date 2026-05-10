from __future__ import annotations


# Alpaca's positions endpoint returns crypto pairs without the slash
# ('BTCUSD'), but submit_order/close_position accept and round-trip the
# canonical 'BASE/QUOTE' form. Listed longest-first so 'USDT' matches
# before 'USD' on a 'BTCUSDT' input.
_ALPACA_CRYPTO_QUOTES = ("USDT", "USDC", "USD")


def is_crypto(symbol: str) -> bool:
    """Crypto pairs use BASE/QUOTE (e.g. BTC/USD); equity tickers have no slash."""
    return "/" in symbol


def symbol_kind(symbol: str) -> str:
    return "crypto" if is_crypto(symbol) else "equity"


def normalize_alpaca_symbol(symbol: str) -> str:
    """Re-insert the slash on Alpaca-returned crypto symbols.

    Alpaca's get_all_positions / order snapshots strip the slash on crypto
    pairs ('BTCUSD' instead of 'BTC/USD') even though every other Alpaca
    endpoint we use accepts and emits the slash form. Normalizing on
    inbound keeps PositionTracker's symbol-keyed dict aligned with the
    signal symbols flowing through the rest of the pipeline.
    """
    if "/" in symbol:
        return symbol
    for quote in _ALPACA_CRYPTO_QUOTES:
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return f"{symbol[:-len(quote)]}/{quote}"
    return symbol
