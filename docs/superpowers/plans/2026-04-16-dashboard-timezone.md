# Dashboard Timezone & ISO Timestamp Rendering — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Render every dashboard timestamp as ISO 8601 in the browser's local timezone, with the original UTC value visible on hover, and surface the detected IANA timezone once in the header.

**Architecture:** Pure frontend change in `bullforge/services/dashboard_service/static/`. Replace the existing `fmtTime()` helper with one that (a) normalizes naive ISO strings as UTC, (b) emits a `<time>` element formatted `YYYY-MM-DD HH:MM:SS` (local) with a `title` tooltip showing the UTC original, (c) is the only timestamp formatter in the codebase. Add a small `#tz-badge` element in the header, populated once on load via `Intl.DateTimeFormat().resolvedOptions().timeZone`.

**Tech Stack:** Vanilla JS, no bundler, no JS test framework. A tiny browser-based assertion page (no dependencies) provides automated verification of the helper.

**Spec:** [docs/superpowers/specs/2026-04-16-dashboard-timezone-design.md](../specs/2026-04-16-dashboard-timezone-design.md)

---

## Pre-Task: Worktree & Dev-Iteration Setup

**Worktree.** The user requires implementation work to happen in an isolated git worktree (other sessions may modify the project concurrently). The execution skill (`superpowers:subagent-driven-development` or `superpowers:executing-plans`) should create one before Task 1. Suggested branch: `feat/dashboard-timezone`.

**Static-file dev iteration.** The dashboard service ships static assets baked into the Docker image (see [Dockerfile](../../bullforge/services/dashboard_service/Dockerfile) — `COPY services/dashboard_service/`). Edits to `static/*.js`/`*.html`/`*.css` will NOT appear in the running container without action. Choose one approach for the duration of this work:

- **Option A (recommended): bind-mount static for development.** Create `bullforge/docker-compose.override.yml` (gitignore-aware — DO NOT commit) with:

  ```yaml
  services:
    dashboard_service:
      volumes:
        - ./services/dashboard_service/static:/app/services/dashboard_service/static
  ```

  Then `cd bullforge && docker-compose up -d dashboard_service`. After this, browser hard-reload (Ctrl+Shift+R) is enough to see edits. Remove the override file before finishing.

- **Option B: rebuild after each change.** Run `cd bullforge && docker-compose up -d --build dashboard_service` after every edit. Slower but no infra change.

Throughout this plan, "reload the test page" / "reload the dashboard" assumes you have one of these set up.

---

## Task 1: Write the failing test harness

A tiny standalone HTML page that loads `dashboard.js`, calls `fmtTime()` against several inputs, and reports pass/fail in the page body and `console.assert`. Open it in a browser to run.

**Files:**
- Create: `bullforge/services/dashboard_service/static/test/fmttime.html`

- [ ] **Step 1: Write the test page**

Create `bullforge/services/dashboard_service/static/test/fmttime.html` with the following exact contents:

```html
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>fmtTime tests</title>
<style>body{font-family:monospace}.pass{color:green}.fail{color:red}</style>
</head>
<body>
<h1>fmtTime() tests</h1>
<div id="results"></div>
<script src="/static/dashboard.js"></script>
<script>
(function () {
  const results = document.getElementById('results');
  let passed = 0, failed = 0;

  function assert(name, actual, expected) {
    const ok = actual === expected;
    const div = document.createElement('div');
    div.className = ok ? 'pass' : 'fail';
    div.textContent = (ok ? 'PASS' : 'FAIL') + ' — ' + name +
      (ok ? '' : '\n  expected: ' + expected + '\n  actual:   ' + actual);
    div.style.whiteSpace = 'pre';
    results.appendChild(div);
    if (ok) passed++; else failed++;
    console.assert(ok, name, {expected, actual});
  }

  function assertMatch(name, actual, regex) {
    const ok = regex.test(actual);
    const div = document.createElement('div');
    div.className = ok ? 'pass' : 'fail';
    div.textContent = (ok ? 'PASS' : 'FAIL') + ' — ' + name +
      (ok ? '' : '\n  pattern: ' + regex + '\n  actual:  ' + actual);
    div.style.whiteSpace = 'pre';
    results.appendChild(div);
    if (ok) passed++; else failed++;
    console.assert(ok, name, {pattern: regex.source, actual});
  }

  // Null / empty
  assert('null returns --', fmtTime(null), '--');
  assert('undefined returns --', fmtTime(undefined), '--');
  assert('empty string returns --', fmtTime(''), '--');

  // Garbage input
  assert('garbage returns --', fmtTime('not-a-date'), '--');

  // Aware ISO with Z — same instant regardless of browser TZ
  // 2026-04-16T19:46:35Z  → UTC tooltip must contain that exact string
  assertMatch('aware Z: tooltip has UTC',
    fmtTime('2026-04-16T19:46:35Z'),
    /title="UTC: 2026-04-16T19:46:35Z"/);
  assertMatch('aware Z: returns <time> tag',
    fmtTime('2026-04-16T19:46:35Z'),
    /^<time datetime="2026-04-16T19:46:35Z">.*<\/time>$/);
  assertMatch('aware Z: local body is YYYY-MM-DD HH:MM:SS',
    fmtTime('2026-04-16T19:46:35Z'),
    />\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}</);

  // Naive ISO (no Z, no offset) MUST be treated as UTC, same as aware case above
  assert('naive == aware Z (same instant)',
    fmtTime('2026-04-16T19:46:35'),
    fmtTime('2026-04-16T19:46:35Z'));

  // Aware with explicit offset
  assertMatch('aware +00:00: tooltip is UTC Z form',
    fmtTime('2026-04-16T19:46:35+00:00'),
    /title="UTC: 2026-04-16T19:46:35Z"/);

  // Milliseconds stripped from UTC tooltip
  assertMatch('millis stripped',
    fmtTime('2026-04-16T19:46:35.123Z'),
    /title="UTC: 2026-04-16T19:46:35Z"/);

  const summary = document.createElement('h2');
  summary.textContent = passed + ' passed, ' + failed + ' failed';
  summary.className = failed === 0 ? 'pass' : 'fail';
  results.appendChild(summary);
})();
</script>
</body>
</html>
```

- [ ] **Step 2: Run the test page (it must fail)**

The dashboard service runs in docker-compose, mapped to host port `18084` (see [bullforge/docker-compose.yml](../../bullforge/docker-compose.yml)). If it isn't already running:

```bash
cd <repo>/bullforge && docker-compose up -d dashboard_service
```

Open `http://localhost:18084/static/test/fmttime.html` in a browser.

**Expected:** Most assertions FAIL because today's `fmtTime()` returns `toLocaleString()` output (e.g., `"4/16/2026, 3:46:35 PM"`), not a `<time>` tag. Specifically:
- "aware Z: tooltip has UTC" — FAIL (no `title=` attr)
- "aware Z: returns <time> tag" — FAIL
- "naive == aware Z" — FAIL (current code parses naive as local, so the strings differ if browser TZ ≠ UTC)
- The `--` cases pass already.

If every assertion passes already, stop and re-read the helper — the test page may be loading a stale cached `dashboard.js`. Hard-reload (Ctrl+Shift+R).

- [ ] **Step 3: Commit the failing test**

```bash
git add bullforge/services/dashboard_service/static/test/fmttime.html
git commit -m "test(dashboard): add fmtTime browser test harness"
```

---

## Task 2: Implement the new fmtTime helper

**Files:**
- Modify: `bullforge/services/dashboard_service/static/dashboard.js:85-89`

- [ ] **Step 1: Replace fmtTime**

Replace the existing function (currently lines 85-89):

```js
function fmtTime(iso) {
    if (!iso) return '--';
    var d = new Date(iso);
    return d.toLocaleString();
}
```

with:

```js
// Renders an ISO timestamp as `<time datetime="UTC-iso" title="UTC: ...">YYYY-MM-DD HH:MM:SS</time>`
// in the browser's local timezone. Naive ISO strings (no Z, no ±HH:MM) are treated as UTC,
// matching how the BullForge backend serializes datetimes. Returns HTML — call sites must
// use .innerHTML, not .textContent.
function fmtTime(iso) {
    if (!iso) return '--';
    var hasTz = /[zZ]|[+-]\d\d:?\d\d$/.test(iso);
    var d = new Date(hasTz ? iso : iso + 'Z');
    if (isNaN(d.getTime())) return '--';
    var pad = function (n) { return String(n).padStart(2, '0'); };
    var local = d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()) +
                ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
    var utc = d.toISOString().replace(/\.\d+Z$/, 'Z');
    return '<time datetime="' + utc + '" title="UTC: ' + utc + '">' + local + '</time>';
}
```

- [ ] **Step 2: Reload the test page**

Hard-reload `http://localhost:18084/static/test/fmttime.html` (Ctrl+Shift+R to bypass cache).

**Expected:** All assertions PASS. The summary at the bottom reads `9 passed, 0 failed` (in green).

If anything fails, do not proceed — fix `fmtTime()` until the page is fully green.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/dashboard.js
git commit -m "feat(dashboard): fmtTime renders ISO 8601 in local TZ with UTC tooltip"
```

---

## Task 3: Add timezone badge to the header

**Files:**
- Modify: `bullforge/services/dashboard_service/static/index.html:13`
- Modify: `bullforge/services/dashboard_service/static/dashboard.js` (add init)
- Modify: `bullforge/services/dashboard_service/static/style.css` (small style)

- [ ] **Step 1: Add the badge element to the header**

In `index.html`, change line 13:

```html
        <div id="last-updated" class="last-updated"></div>
```

to:

```html
        <div id="last-updated" class="last-updated"></div>
        <div id="tz-badge" class="tz-badge"></div>
```

- [ ] **Step 2: Populate the badge on page load**

In `dashboard.js`, locate the `DOMContentLoaded` handler near the top (currently around line 37). Add a TZ-badge initialization line at the top of that handler:

```js
window.addEventListener('DOMContentLoaded', () => {
    var tzEl = document.getElementById('tz-badge');
    if (tzEl) {
        tzEl.textContent = 'TZ: ' + Intl.DateTimeFormat().resolvedOptions().timeZone;
    }
    const hash = window.location.hash || '';
    if (hash.startsWith('#backtests')) {
        const btn = document.querySelector('[data-tab="backtests"]');
        if (btn) btn.click();
    }
});
```

(Only the first `if (tzEl)` block is new. Leave everything else in the handler untouched.)

- [ ] **Step 3: Add minimal styling**

Append to `bullforge/services/dashboard_service/static/style.css`:

```css
.tz-badge {
    font-family: monospace;
    font-size: 0.85em;
    opacity: 0.7;
    margin-left: 0.5em;
}
```

- [ ] **Step 4: Verify in browser**

Hard-reload the dashboard at `http://localhost:18084/`.

**Expected:** Header shows `TZ: <your-IANA-zone>` (e.g., `TZ: America/New_York`) next to the existing "Updated:" element. Should be slightly faded to avoid visual noise.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/dashboard_service/static/index.html bullforge/services/dashboard_service/static/dashboard.js bullforge/services/dashboard_service/static/style.css
git commit -m "feat(dashboard): add TZ badge to header"
```

---

## Task 4: Route the header clock through fmtTime

The `last-updated` element in the header is currently set via `.textContent = 'Updated: ' + new Date().toLocaleTimeString()` at [dashboard.js:178](../../bullforge/services/dashboard_service/static/dashboard.js#L178). Switch it to use `fmtTime` so the header is consistent with every other timestamp.

**Files:**
- Modify: `bullforge/services/dashboard_service/static/dashboard.js:178`

- [ ] **Step 1: Replace the line**

Find this line (currently around line 178):

```js
    $('last-updated').textContent = 'Updated: ' + new Date().toLocaleTimeString();
```

Replace with:

```js
    $('last-updated').innerHTML = 'Updated: ' + fmtTime(new Date().toISOString());
```

(Note: `.textContent` → `.innerHTML` because `fmtTime` returns markup.)

- [ ] **Step 2: Verify in browser**

Hard-reload the dashboard. Watch the "Updated:" element in the header.

**Expected:**
- Header shows `Updated: 2026-04-16 15:46:35` (or whatever the current local time is, ISO-formatted).
- Hovering shows `UTC: 2026-04-16T19:46:35Z`.
- The clock keeps refreshing every 30 seconds (REFRESH_INTERVAL).

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/dashboard.js
git commit -m "feat(dashboard): header clock uses fmtTime for ISO display"
```

---

## Task 5: Replace the custom backtests formatter

The backtests row renderer hand-rolls its own format at [backtests.js:347](../../bullforge/services/dashboard_service/static/backtests.js#L347). Route it through `fmtTime`.

**Files:**
- Modify: `bullforge/services/dashboard_service/static/backtests.js:347`

- [ ] **Step 1: Replace the cell**

Find line 347:

```js
        <td>${r.finished_at ? r.finished_at.replace('T',' ').slice(0,16) : '—'}</td>
```

Replace with:

```js
        <td>${fmtTime(r.finished_at)}</td>
```

(`fmtTime` already handles the null/empty case, returning `'--'` instead of `'—'`. This is fine — that em-dash was a one-off; the rest of the dashboard uses `--`.)

- [ ] **Step 2: Verify in browser**

Open the Backtests tab. Look at the "Finished" column.

**Expected:**
- Each finished row shows `YYYY-MM-DD HH:MM:SS` in local time.
- Hover shows `UTC: ...Z`.
- Unfinished rows show `--`.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/backtests.js
git commit -m "feat(dashboard): backtests finished_at uses fmtTime"
```

---

## Task 6: Sweep for any remaining toLocale date calls

The original exploration only found three `toLocale*` call sites in `static/`. Re-grep to be sure none crept in or were missed.

**Files:**
- All of `bullforge/services/dashboard_service/static/`

- [ ] **Step 1: Grep**

Run from repo root:

```bash
grep -rn "toLocale" bullforge/services/dashboard_service/static/
```

**Expected matches (all acceptable):**
- `dashboard.js:70` inside `fmtMoney` — `Math.abs(n).toLocaleString('en-US', ...)` — formats numbers with commas, NOT a timestamp. **Leave it.**
- (Any other matches must be evaluated case-by-case.)

**Anything else** (e.g., `toLocaleDateString`, `toLocaleTimeString`, `toLocaleString` on a `Date`) must be replaced with `fmtTime(<iso-or-Date.toISOString()>)`.

- [ ] **Step 2: If replacements were made, commit**

```bash
git add bullforge/services/dashboard_service/static/
git commit -m "refactor(dashboard): route remaining timestamps through fmtTime"
```

If no replacements were needed, skip the commit.

---

## Task 7: Full manual verification

**Files:** none modified — verification only.

- [ ] **Step 1: Walk every tab**

With the dashboard running, click through each tab in order: Overview, Trading, Models, Trades, Orders, Halts, Symbols, Freshness, Config, Backtests.

For each tab that contains timestamps, check:
- [ ] Every visible timestamp is `YYYY-MM-DD HH:MM:SS` (no `/`, no AM/PM).
- [ ] Hovering any timestamp shows a tooltip beginning with `UTC: ` and ending in `Z`.

Specific timestamp fields to verify (from spec):
- Overview: Active Halts `activated_at`, Model Status `updated_at`
- Models: `status_updated_at`
- Orders: `submitted_at`
- Halts: active `activated_at`, history `activated_at` and `resolved_at`
- Backtests: `finished_at`
- Freshness: `last_bar`, `last_feature`, `trained_at`
- Header: `Updated: ...` clock and `TZ: ...` badge

- [ ] **Step 2: Spot-check correctness against the API**

Open DevTools → Network tab. Pick one row with a timestamp (e.g., a recent halt). Find the corresponding API response (e.g., `/api/halts/active`). Read the raw ISO value.

**Expected:** The displayed local time equals the raw UTC value shifted by your timezone's UTC offset. Example: raw `2026-04-16T19:46:35Z`, browser TZ `America/New_York` (UTC−4 in April due to DST), displayed `2026-04-16 15:46:35`.

If a timestamp displays as the same string as the raw UTC (no shift), the bug from the spec is back — re-check Task 2.

- [ ] **Step 3: Re-run the test page**

Open `http://localhost:18084/static/test/fmttime.html` once more.

**Expected:** `9 passed, 0 failed`.

- [ ] **Step 4: Final grep sweep**

```bash
grep -rn "toLocale" bullforge/services/dashboard_service/static/
```

**Expected:** Only `dashboard.js:70` (the `fmtMoney` number formatter) remains.

---

## Task 8: Wrap up

- [ ] **Step 1: Confirm clean tree**

```bash
git status
```

**Expected:** Working tree clean, all task commits present, branch ahead of `master`.

- [ ] **Step 2: Hand off**

Report back with:
- The branch name (`feat/dashboard-timezone` or whichever was used)
- The list of commits added
- Any anomalies found during manual verification
- Confirmation that `9 passed, 0 failed` was observed on the test page

The user will decide whether to merge, open a PR, or iterate further.
