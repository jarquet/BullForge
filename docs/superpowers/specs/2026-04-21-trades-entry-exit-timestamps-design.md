# Trades Tab: Entry and Exit Timestamp Columns — Design

**Date:** 2026-04-21
**Status:** Approved (pending user spec review)
**Scope:** `bullforge/services/dashboard_service/static/` only — no backend, API, or DB changes.

## Goal

Add Entry Time and Exit Time columns to the Trades tab table. Crypto trades (symbol containing `/`) render timestamps in UTC; equity trades render in US Eastern time (EST/EDT handled automatically). Zone information is surfaced on hover, not in the cell text.

## Motivation

The Trades table currently shows entry and exit **prices** under headers titled "Entry" and "Exit", but never surfaces the **times** at which positions were opened and closed. Traders need the session timestamp — particularly for equities — to interpret performance and correlate with external events.

Project convention is ISO 8601 `YYYY-MM-DD HH:MM:SS` with no locale strings, slashes, or AM/PM. Zone awareness differs by asset class: crypto trades 24/7 and is conventionally logged in UTC; US equities align with Eastern time sessions. The existing `fmtTime()` helper renders in browser-local, which is wrong for both asset classes when the viewer is outside those zones.

## Non-Goals

- Backend/API changes. `/api/trades` already returns `entry_time` and `exit_time` as UTC ISO 8601 strings (backed by `TIMESTAMPTZ` columns in the `trades` table).
- Changing timestamp rendering in other tables (Orders' Submitted, Halts' Activated/Resolved, Overview's Model Updated, Freshness' Last Bar). They continue to use `fmtTime()` (browser-local).
- Adding a zone selector or user timezone override.
- Visual CSS changes. The existing `.data-table` styling handles additional columns without modification.

## Design

### 1. Column layout

Before (8 columns):

```
ID | Symbol | Side | Entry | Exit | Qty | P&L | Duration
```

After (10 columns):

```
ID | Symbol | Side | Entry Price | Entry Time | Exit Price | Exit Time | Qty | P&L | Duration
```

The existing "Entry" and "Exit" headers have always shown prices — renaming them to "Entry Price" and "Exit Price" corrects a long-standing mislabel while making room for the new time columns.

### 2. New helper: `fmtTradeTime(iso, symbol)`

Lives in [dashboard.js](../../../bullforge/services/dashboard_service/static/dashboard.js), next to the existing `fmtTime()`.

```js
function fmtTradeTime(iso, symbol) {
    if (!iso) return '--';
    var hasTz = /[zZ]|[+-]\d\d:?\d\d$/.test(iso);
    var d = new Date(hasTz ? iso : iso + 'Z');
    if (isNaN(d.getTime())) return '--';
    var pad = function (n) { return String(n).padStart(2, '0'); };
    var utc = d.toISOString().replace(/\.\d+Z$/, 'Z');

    if (symbol && symbol.indexOf('/') !== -1) {
        // Crypto — render in UTC
        var text = d.getUTCFullYear() + '-' + pad(d.getUTCMonth() + 1) + '-' + pad(d.getUTCDate()) +
                   ' ' + pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes()) + ':' + pad(d.getUTCSeconds());
        return '<time datetime="' + utc + '" title="UTC">' + text + '</time>';
    }

    // Equity — render in America/New_York
    var parts = new Intl.DateTimeFormat('en-CA', {
        timeZone: 'America/New_York',
        year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit', second: '2-digit',
        hour12: false
    }).formatToParts(d).reduce(function (acc, p) { acc[p.type] = p.value; return acc; }, {});
    var text = parts.year + '-' + parts.month + '-' + parts.day +
               ' ' + parts.hour + ':' + parts.minute + ':' + parts.second;
    return '<time datetime="' + utc + '" title="ET (UTC: ' + utc + ')">' + text + '</time>';
}
```

Behavior notes:

- **Null / missing:** `iso` is null or empty → `--`. Handles open trades with no `exit_time`.
- **Defensive TZ normalization:** if the ISO string has no `Z` and no `±HH:MM` suffix, append `Z`. Mirrors `fmtTime`'s behavior. The backend emits `+00:00` today, so this is a safety net.
- **Asset class branch:** `symbol.indexOf('/') !== -1` matches the backend convention in `bullforge/shared/symbol_utils.py` (`is_crypto` returns true when the symbol contains `/`). Any symbol without a slash is treated as equity.
- **`en-CA` locale for equity:** its default date format is `YYYY-MM-DD`, which avoids component reordering surprises. `hour12: false` suppresses AM/PM. `Intl.DateTimeFormat` with `timeZone: 'America/New_York'` handles EST↔EDT transitions automatically based on the instant being formatted.
- **`Intl.DateTimeFormat` midnight edge case:** `en-CA` with `hour12: false` can emit `"24"` for midnight in some environments. The existing dashboard baseline (modern Chrome/Edge/Firefox) emits `"00"`. If this becomes a real problem in practice, the fix is a post-format normalization — not worth the code today.
- **Hover:**
  - Crypto: `title="UTC"`.
  - Equity: `title="ET (UTC: 2026-04-21T13:45:30Z)"` — includes UTC equivalent for cross-referencing between currencies and crypto rows.
- **`datetime` attribute:** always the UTC ISO 8601 instant, per the `<time>` element spec — supports assistive tech and hypothetical sorting/parsing.

### 3. Integration in `loadTrades()`

Current renderer in `dashboard.js`:

```js
tbody.innerHTML = trades.map(function(t) {
    var pnlClass = t.pnl > 0 ? 'positive' : t.pnl < 0 ? 'negative' : '';
    return '<tr>' +
        '<td>' + t.id + '</td>' +
        '<td>' + t.symbol + '</td>' +
        '<td>' + t.side + '</td>' +
        '<td>' + fmtMoney(t.entry_price) + '</td>' +
        '<td>' + fmtMoney(t.exit_price) + '</td>' +
        '<td>' + fmt(t.quantity, 1) + '</td>' +
        '<td class="' + pnlClass + '">' + fmtMoney(t.pnl) + '</td>' +
        '<td>' + fmtDuration(t.hold_duration_seconds) + '</td>' +
        '</tr>';
}).join('');
```

Updated renderer inserts an Entry Time cell after Entry Price and an Exit Time cell after Exit Price:

```js
tbody.innerHTML = trades.map(function(t) {
    var pnlClass = t.pnl > 0 ? 'positive' : t.pnl < 0 ? 'negative' : '';
    return '<tr>' +
        '<td>' + t.id + '</td>' +
        '<td>' + t.symbol + '</td>' +
        '<td>' + t.side + '</td>' +
        '<td>' + fmtMoney(t.entry_price) + '</td>' +
        '<td>' + fmtTradeTime(t.entry_time, t.symbol) + '</td>' +
        '<td>' + fmtMoney(t.exit_price) + '</td>' +
        '<td>' + fmtTradeTime(t.exit_time, t.symbol) + '</td>' +
        '<td>' + fmt(t.quantity, 1) + '</td>' +
        '<td class="' + pnlClass + '">' + fmtMoney(t.pnl) + '</td>' +
        '<td>' + fmtDuration(t.hold_duration_seconds) + '</td>' +
        '</tr>';
}).join('');
```

### 4. HTML header update

In [index.html](../../../bullforge/services/dashboard_service/static/index.html), the Trades tab `<thead>`:

```html
<tr>
    <th>ID</th>
    <th>Symbol</th>
    <th>Side</th>
    <th>Entry Price</th>
    <th>Entry Time</th>
    <th>Exit Price</th>
    <th>Exit Time</th>
    <th>Qty</th>
    <th>P&amp;L</th>
    <th>Duration</th>
</tr>
```

## Error handling

- `iso == null` (open trade, no exit yet) → `--`.
- Malformed ISO → `Date` parses to `Invalid Date`; `isNaN(d.getTime())` check returns `--`. Matches `fmtTime` behavior.
- Unknown symbol convention: anything without `/` is equity. Matches the backend's existing classification used throughout the trading stack.

## Testing

Manual browser verification only — this is a pure frontend formatting change with no behavioral branching that isn't visually observable.

1. Bring up the dashboard via `docker compose`.
2. Trades tab:
   - **Crypto row (e.g. `BTC/USD`):** Entry Time and Exit Time show `YYYY-MM-DD HH:MM:SS`; hover tooltip reads `UTC`; value matches raw `entry_time`/`exit_time` from `/api/trades`.
   - **Equity row (e.g. `AAPL`):** Entry Time and Exit Time show `YYYY-MM-DD HH:MM:SS`; hover tooltip reads `ET (UTC: …)`; value is offset from UTC by 4 hours (EDT, Mar–Nov) or 5 hours (EST, Nov–Mar).
   - **Open trade (null `exit_time`):** Exit Time cell shows `--`.
3. Cross-reference: inspect the `<time>` element's `datetime` attribute in DevTools — it should always be the UTC ISO equivalent.

No new unit/integration tests. The project has no JS test harness for dashboard static files today; adding one for this change would be disproportionate.

## Rollout

Non-breaking. Static assets are served with standard freshness; users may need a hard refresh if the browser caches `dashboard.js` aggressively. No cache-buster bump is required because `dashboard.js` is not served with a `?v=` parameter today.
