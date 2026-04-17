# Dashboard Timezone & ISO Timestamp Rendering — Design

**Date:** 2026-04-16
**Status:** Approved (pending user spec review)
**Scope:** `bullforge/services/dashboard_service/static/` only — no backend changes.

## Goal

Display every timestamp in the dashboard as an ISO 8601 string in the browser's local timezone, with the original UTC value available on hover, and surface the detected timezone once in the header.

## Motivation

Two correctness issues exist today:

1. **Naive ISO strings parsed as local time.** Backend serializers call `datetime.isoformat()`. Some `datetime` objects are timezone-aware (emit `+00:00`); others are naive (emit no suffix). `new Date("2026-04-16T19:46:35")` (naive) is parsed as **local** time by JS, so a UTC timestamp is shown shifted by the user's UTC offset. Aware timestamps render correctly. The bug is silent and inconsistent across tabs.
2. **Inconsistent formatting.** `dashboard.js` routes most fields through `fmtTime()` → `toLocaleString()`. `backtests.js` has its own ad-hoc formatter. The header clock uses yet another format.

Additionally, the user prefers ISO 8601 over locale strings (sortable, unambiguous, no AM/PM, no slashes). Locale formats like `"4/16/2026, 3:46:35 PM"` are not acceptable.

## Non-Goals

- Backend changes. Backend already emits UTC; the helper handles both aware and naive forms.
- Per-row timezone selectors or user TZ overrides — browser's detected zone is the single source of truth.
- A custom styled tooltip library. Native `title` attribute is sufficient.
- Touching `fmtLocal()` in `backtests.js` (used to populate `<input type="datetime-local">`, which requires a specific non-ISO-Z format).

## Design

### 1. Single helper

Rewrite `fmtTime(iso)` in [dashboard.js](../../../bullforge/services/dashboard_service/static/dashboard.js):

```js
function fmtTime(iso) {
  if (!iso) return '--';
  const hasTz = /[zZ]|[+-]\d\d:?\d\d$/.test(iso);
  const d = new Date(hasTz ? iso : iso + 'Z');
  if (isNaN(d.getTime())) return '--';
  const pad = n => String(n).padStart(2, '0');
  const local = `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ` +
                `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  const utc = d.toISOString().replace(/\.\d+Z$/, 'Z');
  return `<time datetime="${utc}" title="UTC: ${utc}">${local}</time>`;
}
```

Behavior:
- **Normalization:** if no `Z` and no `±HH:MM` suffix, append `Z` and treat as UTC. This matches the backend's actual behavior.
- **Output format:** `YYYY-MM-DD HH:MM:SS` (space separator, browser local time). Sortable, ISO-conformant.
- **Hover:** native `title` attribute shows `UTC: 2026-04-16T19:46:35Z`.
- **Returns HTML.** All current call sites already build HTML via template literals and assign with `innerHTML`, so this is compatible.

### 2. Call-site updates

| File | Line | Change |
|------|------|--------|
| [dashboard.js](../../../bullforge/services/dashboard_service/static/dashboard.js) | 85-89 | Replace `fmtTime` body per above |
| [backtests.js](../../../bullforge/services/dashboard_service/static/backtests.js) | 347 | Delete custom `YYYY-MM-DD HH:MM` formatter; call `fmtTime(row.finished_at)` |
| [index.html](../../../bullforge/services/dashboard_service/static/index.html) | header "Updated:" clock | Pass `new Date().toISOString()` through `fmtTime()` and assign via `innerHTML`. One helper, one format. |
| any other `toLocaleString` / `toLocaleDateString` / `toLocaleTimeString` calls in `static/` | sweep | Replace with `fmtTime` |

Sweep step: grep `static/` for `toLocale` and resolve every hit. Catches drift introduced after the original exploration.

### 3. Timezone badge in header

In [index.html](../../../bullforge/services/dashboard_service/static/index.html), add a small badge element next to the existing "Updated:" slot:

```html
<span id="tz-badge" class="tz-badge"></span>
```

In `dashboard.js` initialization:

```js
document.getElementById('tz-badge').textContent =
  'TZ: ' + Intl.DateTimeFormat().resolvedOptions().timeZone;
```

Set once on load. No reactive updates — if the user changes their system TZ they refresh.

Style: subtle, inline with the header. Reuse existing header text styling; minor `color` / `opacity` for de-emphasis only if needed.

## Data Flow

```
backend ISO (aware or naive)
  → fmtTime() normalizes to UTC
  → emits <time datetime="UTC-iso" title="UTC: ...">LOCAL-iso</time>
  → assigned via innerHTML in existing table builders
```

```
page load
  → Intl.DateTimeFormat().resolvedOptions().timeZone
  → header #tz-badge.textContent
```

## Error Handling

- `null` / `undefined` / `''` input → `'--'` (current behavior preserved).
- Unparseable string → `isNaN(d.getTime())` → `'--'`.
- No try/catch needed; `new Date()` does not throw.

## Testing

Manual verification (no UI test infrastructure exists for the dashboard):

1. Load each tab: Overview, Models, Orders, Halts, Backtests, Freshness.
2. Confirm every visible timestamp is `YYYY-MM-DD HH:MM:SS`.
3. Hover any timestamp → tooltip shows `UTC: YYYY-MM-DDTHH:MM:SSZ`.
4. Header shows `TZ: <IANA-zone>` (e.g., `TZ: America/New_York`).
5. Cross-check one timestamp against the backend's raw response in DevTools — local value should equal UTC value shifted by the displayed zone's offset.
6. `grep -r "toLocale" bullforge/services/dashboard_service/static/` returns no matches (except inside `fmtLocal` if it remains for the datetime-local input).

## Risks & Trade-offs

- **Innerhtml assumption.** The helper returns markup, not text. If any future call site uses `.textContent`, the `<time>` tag will render as literal text. Acceptable: today's call sites are all `innerHTML`. Document it inline above the function.
- **Native tooltip styling.** `title` attribute styling cannot be customized cross-browser. Acceptable for an internal tool.
- **No backend fix.** Naive ISO strings remain a latent footgun for any future consumer. Out of scope here, but worth a follow-up to make the backend always emit `Z`.

## Out of Scope (Follow-ups)

- Backend: enforce timezone-aware UTC datetimes everywhere so `.isoformat()` always emits `+00:00`. Tracked separately.
- Add a tiny unit test for `fmtTime()` if/when frontend test infrastructure is added.
