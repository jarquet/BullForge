# Crypto bar `volume` units on Alpaca 1m bars

**Status:** investigation only. No production code change.
**Date:** 2026-05-02.
**Question:** What does the `volume` field on a 1-minute crypto bar from Alpaca represent — base-asset quantity (BTC), quote-asset value (USD), or trade count?

## Answer

**Base-asset quantity.** For a `BTC/USD` 1m bar, `volume` is the number of BTC traded during that minute (a `float`, typically fractional). It is *not* USD notional and it is *not* a trade count.

## Evidence

### 1. The bar comes straight from Alpaca's `v` field, with `n` (trade count) tracked separately

`bullforge/services/data_pipeline/alpaca_client.py:60-67` — historical pull uses `CryptoBarsRequest` against `CryptoHistoricalDataClient.get_crypto_bars` for any symbol containing `/`. The websocket equivalent (`bullforge/services/data_pipeline/stream.py:62-66`, `…/alpaca_client.py:92-97`) uses `CryptoDataStream`. Both feed into the same `parse_bar` helper:

```python
# bullforge/services/data_pipeline/alpaca_client.py:30-45
def parse_bar(self, symbol: str, alpaca_bar, timeframe: str) -> Bar:
    ...
    return Bar(
        symbol=symbol,
        ...
        volume=int(alpaca_bar.volume),   # SDK Bar.volume → JSON "v" field
        timeframe=timeframe,
    )
```

The SDK's `alpaca/data/models/bars.py::Bar` is populated via `BAR_MAPPING = {'t': 'timestamp', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume', 'n': 'trade_count', 'vw': 'vwap'}`. So `Bar.volume` is the wire `"v"` field, and `Bar.trade_count` (which we **do not** persist — `parse_bar` drops it) is `"n"`. That alone rules out trade-count: it is a separate, distinct field on the same bar. Note also that we cast `volume` to `int`, which is harmless for equities but lossy for crypto: `int(3.227759152)` becomes `3`. Worth flagging for Task 2 alongside the unit-aware cap work.

### 2. Alpaca's example payload values are dimensionally only consistent with base-asset units

The Alpaca crypto-streaming docs publish example bar payloads with values like `"v": 3.227759152, "n": 93` for `BTC/USD` (search hit on `docs.alpaca.markets`). With BTC trading near $70K and 93 trades in a minute:

- **Trade count interpretation:** ruled out by the schema — `n` already carries trade count, and `n=93` while `v=3.22…` directly contradicts that reading.
- **Quote-value (USD) interpretation:** $3.23 of total USD across 93 trades implies an average trade size of ~3.5 cents on a $70K asset. Not credible.
- **Base-asset (BTC) interpretation:** 3.227 BTC across 93 trades implies an average trade size of ~0.035 BTC ≈ $2,400 notional. Plausible and consistent with a typical BTC/USD minute.

The streaming-docs example payload for an empty bar shows `"v": 0, "n": 0, "vw": 0` with prices held at the quote midpoint — the docs prose explicitly states: *"if in a bar no trade happens, the volume will be 0, but the prices will be determined by the quote prices."* This phrasing also reads naturally as "no trades → 0 base-asset units traded."

### 3. Doc citations

- Historical bars endpoint reference: <https://docs.alpaca.markets/reference/cryptobars-1>. Lists the response schema fields including `v` (volume) and `n` (number of trades) as separate items.
- Historical crypto data overview: <https://docs.alpaca.markets/docs/historical-crypto-data-1>. Quote: *"if in a bar no trade happens, the volume will be 0, but the prices will be determined by the quote prices."*
- Real-time crypto data: <https://docs.alpaca.markets/docs/real-time-crypto-pricing-data>. Carries the example bar payload above.
- alpaca-py SDK Bar model: `alpaca/data/models/bars.py::Bar` and `alpaca/data/mappings.py::BAR_MAPPING` (locally inspected) — confirms wire `"v"` ↔ Python `volume`, wire `"n"` ↔ Python `trade_count`.

> Caveat: the Alpaca docs do not contain a one-line sentence stating "volume is in base-asset units" verbatim. The conclusion above rests on (a) the schema separating `v` from `n`, (b) the magnitude of the published example values being numerically incompatible with USD-value or trade-count readings at BTC's price level, and (c) industry convention across every other major crypto market-data API (Coinbase, Kraken, Binance) returning bar volume in base-asset units. If a future Alpaca doc revision contradicts this — extremely unlikely — the conclusion would need to be revisited.

## Implications for `BacktestConfig.max_volume_pct = 0.10`

The fill model in `bullforge/services/backtest_service/fill_model.py:35-46` does:

```python
max_qty = next_bar["volume"] * self._max_vol_pct
if req.qty > max_qty:
    return FillResult(filled=False, ..., reason=f"qty {req.qty} > volume cap {max_qty:.0f}")
```

Order qty is in the same unit as `volume`: shares for equities, base-asset coins for crypto. So the cap is dimensionally consistent — 10% of a 2.0-BTC-volume minute is 0.2 BTC, which is the right interpretation. Good news: the unit semantics are sane. The TODO line 142 hypothesis "a unit mismatch would make the cap nonsensical for crypto" is not what is biting us.

What *is* biting us is that BTC/USD 1m bars routinely have very small base-asset volumes — many minutes carry well under 1 BTC of trading on Alpaca's own venue (a single venue, not aggregated). The Phase-1 BTC stall investigation already documented that ~77% of BTC/USD minutes have *zero* volume on Alpaca (TODO line 155). On the non-zero minutes, a fractional position size of 0.1+ BTC easily exceeds 10% of, say, a 0.5-BTC-volume minute. So the rejected fills are real liquidity-cap rejections, not unit confusion — but the `0.10` default is still too tight for a single-venue thin-tape asset.

Under-cast also: `parse_bar` does `volume=int(alpaca_bar.volume)`. This was harmless for equities (whole-share volume) but truncates fractional crypto volume. A bar with `v=3.227` becomes stored `volume=3`. For a heavy minute this is rounding; for the marginal-liquidity minutes this is a meaningful 5-25% underestimate of bar volume, which makes the 10% cap *even tighter* than it should be. Worth fixing alongside Task 2 — change the `Bar.volume` dataclass field to `float` and drop the `int(...)` coercion.

## Recommendation for Task 2's per-asset-class default

**Equities: keep `max_volume_pct = 0.10`** (current value, well-established for liquid stock minutes).

**Crypto: `max_volume_pct = 0.25`** as the per-asset-class default. Rationale: Alpaca's crypto venue is a single internal venue with thinner per-minute base-asset volume than US-listed equities have in shares; raising the cap to 25% materially reduces rejection rate on the medium-liquidity minutes (the only ones we can transact on anyway, since zero-volume minutes already reject for `volume=0`) while still preventing any single backtested fill from claiming to absorb the bulk of a minute's trading. We are not market-making — we are taking liquidity — so a backtest fill larger than ~25% of the minute's prints would be implausible regardless of cash on hand. If this still produces too many rejections in BTC/USD walk-forwards, the next step is a per-symbol override surfaced through `BacktestConfig`, not pushing the cap higher than 0.25.

Pair the per-asset-class default with the rejection-counter from TODO line 141 so any tuning decision is data-driven on the next backtest run.

(Suggested ordering: Task 2 should also drop the `int(volume)` cast in `parse_bar` so the cap is computed off honest fractional values. That is one extra line touched but it materially improves cap fidelity for crypto minutes between 0 and 1 BTC.)
