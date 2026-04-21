# Trades Tab: Entry/Exit Timestamp Columns Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Entry Time and Exit Time columns to the Trades tab table, rendered in UTC for crypto trades (symbol contains `/`) and US Eastern for equity trades. Zone info is shown in a hover tooltip only.

**Architecture:** Pure frontend change in the dashboard service's static assets. A new `fmtTradeTime(iso, symbol)` helper in `dashboard.js` branches on asset class and uses `getUTC*` methods for crypto and `Intl.DateTimeFormat` with `timeZone: 'America/New_York'` for equity. The existing `loadTrades()` renderer is wired to emit two additional cells, and `index.html` gains two new `<th>` elements with the price headers renamed for clarity.

**Tech Stack:** Vanilla ES5-style JavaScript (project uses `var`, no bundler), HTML, browser-native `Intl.DateTimeFormat` API.

**Spec:** [2026-04-21-trades-entry-exit-timestamps-design.md](../specs/2026-04-21-trades-entry-exit-timestamps-design.md)

---

## File Structure

- Modify: `bullforge/services/dashboard_service/static/dashboard.js`
  - Add `fmtTradeTime(iso, symbol)` helper immediately after the existing `fmtTime` helper (currently ends at line 99).
  - Update `loadTrades()` (currently lines 194-210) to render two new `<td>` cells using the helper.
- Modify: `bullforge/services/dashboard_service/static/index.html`
  - Trades tab `<thead>` (currently lines 129-140): rename "Entry" → "Entry Price", "Exit" → "Exit Price", insert "Entry Time" after "Entry Price" and "Exit Time" after "Exit Price".

No new files. No CSS changes. No backend changes.

---

## Task 1: Create isolated git worktree

**Files:** None yet — this sets up the environment.

**Context:** User memory requires implementation work to happen in a git worktree because other sessions may edit the project concurrently. The main checkout lives at `<repo>`.

- [ ] **Step 1: Invoke the `superpowers:using-git-worktrees` skill**

Pass these parameters when the skill asks:
- Source repo: `<repo>`
- Branch name: `feat/trades-entry-exit-timestamps`
- Base branch: `master`
- Worktree location: sibling of main checkout, e.g. `<repo>`

- [ ] **Step 2: Verify the worktree is clean and on the correct branch**

Run from the new worktree directory:

```bash
git status
git branch --show-current
```

Expected output:
- `git status` → `On branch feat/trades-entry-exit-timestamps` and `nothing to commit, working tree clean`
- `git branch --show-current` → `feat/trades-entry-exit-timestamps`

- [ ] **Step 3: All subsequent tasks execute inside this worktree**

No commit at this step — the worktree creation produces its own baseline.

---

## Task 2: Add `fmtTradeTime(iso, symbol)` helper to `dashboard.js`

**Files:**
- Modify: `bullforge/services/dashboard_service/static/dashboard.js` (insert new function after the existing `fmtTime` function, which ends at line 99)

- [ ] **Step 1: Read the current `fmtTime` function and confirm its location**

Run (from the worktree root):

```bash
grep -n "^function fmtTime\|^function fmt" bullforge/services/dashboard_service/static/dashboard.js | head -5
```

Expected: `fmtTime` is defined on line ~89 and ends on line 99. If line numbers differ slightly, anchor on the string `return '<time datetime="' + utc + '" title="UTC: ' + utc + '">' + local + '</time>';` which ends the function, followed by `}`.

- [ ] **Step 2: Insert the new helper immediately after `fmtTime`**

Find the line immediately after the closing `}` of `fmtTime` (there should be a blank line separating it from the next function). Insert the following function there:

```javascript
function fmtTradeTime(iso, symbol) {
    if (!iso) return '--';
    var hasTz = /[zZ]|[+-]\d\d:?\d\d$/.test(iso);
    var d = new Date(hasTz ? iso : iso + 'Z');
    if (isNaN(d.getTime())) return '--';
    var pad = function (n) { return String(n).padStart(2, '0'); };
    var utc = d.toISOString().replace(/\.\d+Z$/, 'Z');
    var text, title;

    if (symbol && symbol.indexOf('/') !== -1) {
        // Crypto — render in UTC
        text = d.getUTCFullYear() + '-' + pad(d.getUTCMonth() + 1) + '-' + pad(d.getUTCDate()) +
               ' ' + pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes()) + ':' + pad(d.getUTCSeconds());
        title = 'UTC';
    } else {
        // Equity — render in America/New_York (handles EST/EDT automatically)
        var parts = new Intl.DateTimeFormat('en-CA', {
            timeZone: 'America/New_York',
            year: 'numeric', month: '2-digit', day: '2-digit',
            hour: '2-digit', minute: '2-digit', second: '2-digit',
            hour12: false
        }).formatToParts(d).reduce(function (acc, p) { acc[p.type] = p.value; return acc; }, {});
        text = parts.year + '-' + parts.month + '-' + parts.day +
               ' ' + parts.hour + ':' + parts.minute + ':' + parts.second;
        title = 'ET (UTC: ' + utc + ')';
    }
    return '<time datetime="' + utc + '" title="' + title + '">' + text + '</time>';
}
```

- [ ] **Step 3: Sanity-check the helper in a browser console**

The helper is not yet wired into the renderer, but you can paste-test it. Start the dashboard (see Task 4 for how) or open any page that loads `dashboard.js`. In the browser devtools console, run:

```javascript
fmtTradeTime('2026-04-21T13:45:30+00:00', 'BTC/USD')
fmtTradeTime('2026-04-21T13:45:30+00:00', 'AAPL')
fmtTradeTime(null, 'AAPL')
fmtTradeTime('not-a-date', 'AAPL')
```

Expected:
- First: `<time datetime="2026-04-21T13:45:30Z" title="UTC">2026-04-21 13:45:30</time>`
- Second: `<time datetime="2026-04-21T13:45:30Z" title="ET (UTC: 2026-04-21T13:45:30Z)">2026-04-21 09:45:30</time>` (EDT; in winter `08:45:30` for EST)
- Third: `--`
- Fourth: `--`

If the dashboard is not running yet, skip this sanity check and rely on Task 4 verification.

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/dashboard_service/static/dashboard.js
git commit -m "$(cat <<'EOF'
feat(dashboard): add fmtTradeTime helper for per-asset-class timestamp rendering

Crypto symbols (containing '/') render in UTC; equity symbols render in
US Eastern via Intl.DateTimeFormat with timeZone America/New_York. Zone
info exposed in the hover tooltip only. Not yet wired into any caller.
EOF
)"
```

---

## Task 3: Update Trades table headers and renderer

**Files:**
- Modify: `bullforge/services/dashboard_service/static/index.html` (Trades tab `<thead>`, currently lines 129-140)
- Modify: `bullforge/services/dashboard_service/static/dashboard.js` (`loadTrades()` function, currently lines 194-210)

Both files must change together — a mismatch between headers and cell count would render a broken table. Keep them in one commit.

- [ ] **Step 1: Update the Trades `<thead>` in `index.html`**

Locate the Trades tab section (anchored on `<!-- Trades Tab -->` or `<section id="tab-trades"`). Replace the entire `<tr>` inside the `<thead>` with:

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

Changes vs current markup:
- `<th>Entry</th>` → `<th>Entry Price</th>`
- `<th>Exit</th>` → `<th>Exit Price</th>`
- Inserted `<th>Entry Time</th>` between Entry Price and Exit Price.
- Inserted `<th>Exit Time</th>` between Exit Price and Qty.
- All other `<th>` elements unchanged and in the same relative order.

- [ ] **Step 2: Update `loadTrades()` in `dashboard.js`**

Locate the `loadTrades` function (`async function loadTrades()`). Replace the entire `tbody.innerHTML = trades.map(...)` block with:

```javascript
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

Changes vs current renderer:
- Inserted `<td>' + fmtTradeTime(t.entry_time, t.symbol) + '</td>'` after the entry_price cell.
- Inserted `<td>' + fmtTradeTime(t.exit_time, t.symbol) + '</td>'` after the exit_price cell.
- Nothing else changed; `pnlClass` logic and all other cells are identical.

- [ ] **Step 3: Verify header count matches cell count**

Run:

```bash
grep -c "<th>" bullforge/services/dashboard_service/static/index.html | head -1
grep -c "'<td>'" bullforge/services/dashboard_service/static/dashboard.js
```

The first count is a global `<th>` count (many other tables contribute), so it's only a sanity check — don't rely on it for correctness. Instead, visually confirm that the Trades `<thead>` has exactly 10 `<th>` entries and `loadTrades()` emits exactly 10 `<td>` entries per row.

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/dashboard_service/static/index.html bullforge/services/dashboard_service/static/dashboard.js
git commit -m "$(cat <<'EOF'
feat(dashboard): add Entry Time / Exit Time columns to Trades tab

Crypto trades render timestamps in UTC; equity trades render in US
Eastern. Zone info shown in hover tooltip only. Existing "Entry" and
"Exit" headers renamed to "Entry Price" and "Exit Price" for clarity.
EOF
)"
```

---

## Task 4: Manual browser verification

**Files:** None — verification only.

**Context:** The project has no JS test harness for dashboard static files. The spec deliberately chose manual verification only; adding a harness for this one formatter would be disproportionate. Verification is done against the live dashboard running via docker compose.

- [ ] **Step 1: Start (or restart) the dashboard service**

From the worktree root:

```bash
docker compose up -d dashboard_service
```

Then confirm it's up:

```bash
docker compose ps dashboard_service
```

Expected: status `running` or `healthy`.

If you need to point the running containers at the worktree's static files rather than the original checkout, confirm the compose file mounts the worktree's `bullforge/services/dashboard_service/static/` directory — this should "just work" because docker compose uses the working directory as the build/mount context, and you are running the command from the worktree.

- [ ] **Step 2: Open the dashboard and hard-refresh**

Open the dashboard URL in a browser (typically http://localhost:8080 or as configured). Hard-refresh to avoid the cached `dashboard.js`:

- Windows: `Ctrl+Shift+R` or `Ctrl+F5`
- Mac: `Cmd+Shift+R`

Navigate to the Trades tab.

- [ ] **Step 3: Verify table structure**

- Column headers are: `ID | Symbol | Side | Entry Price | Entry Time | Exit Price | Exit Time | Qty | P&L | Duration` (exactly 10 headers).
- Every data row has exactly 10 cells, aligned with the headers.
- Table renders without JavaScript errors (check the devtools console for red errors).

- [ ] **Step 4: Verify crypto timestamp rendering**

Find a row with a symbol containing `/` (e.g. `BTC/USD`, `ETH/USD`).

- Entry Time and Exit Time cells display `YYYY-MM-DD HH:MM:SS` (e.g. `2026-04-21 13:45:30`).
- Hover over either cell → tooltip reads exactly `UTC`.
- Open devtools, inspect the `<time>` element: the `datetime` attribute should match the cell text (plus `T` and `Z`), and should match the raw `entry_time` or `exit_time` from `/api/trades?limit=100` (fetch it in devtools Network tab or a new browser tab).

- [ ] **Step 5: Verify equity timestamp rendering**

Find a row with a symbol without `/` (e.g. `AAPL`, `SPY`, `NVDA`).

- Entry Time and Exit Time cells display `YYYY-MM-DD HH:MM:SS`.
- Hover → tooltip reads `ET (UTC: 2026-04-21T13:45:30Z)` with the actual UTC equivalent.
- The displayed cell time should differ from the UTC equivalent in the tooltip by 4 hours (EDT, ~Mar–Nov) or 5 hours (EST, ~Nov–Mar). Confirm the direction: equity display time is **earlier** than UTC (e.g. UTC `13:45:30` → ET `09:45:30` during EDT).

- [ ] **Step 6: Verify open-trade fallback**

Identify a row where `exit_time` is null (an open trade) — check the `/api/trades` JSON response for entries where `exit_time: null`. If no such row exists in the current dataset, skip this step and note it in the sign-off.

- The Exit Time cell should display `--`.
- The Exit Price cell should also show its existing null-handling (unchanged from current behavior).

- [ ] **Step 7: Sign off or iterate**

If all checks pass, this task has no commit — verification only. Proceed to Task 5 for branch finalization.

If any check fails, identify whether the issue is in the helper (Task 2) or the renderer (Task 3), fix it with a new commit (do not amend), and re-run Steps 3-6.

---

## Task 5: Finalize the development branch

**Files:** None — git workflow only.

- [ ] **Step 1: Invoke the `superpowers:finishing-a-development-branch` skill**

The skill presents options for merging the worktree back to master, opening a PR, or cleaning up. Follow its guidance. Typical choice for a small UI change is "merge to master directly" since there are no reviewers configured.

- [ ] **Step 2: Confirm the final state**

After the skill completes, confirm:

```bash
git log --oneline master | head -5
```

Expected: the two commits from Tasks 2 and 3 are present on `master`.

---

## Out of Scope (deliberate)

These were considered during brainstorming and left out of this plan. Do not expand the scope of these tasks to include them:

- Changes to any other dashboard table (Orders, Halts, Overview Model Status, Freshness). They continue to use the existing `fmtTime` helper (browser-local rendering).
- Changes to the `/api/trades` backend or the `trades` DB schema. The API already returns UTC ISO 8601 strings.
- CSS adjustments. Existing `.data-table` styling handles 10 columns without modification.
- A JS unit test harness for `fmtTradeTime`. The project has no dashboard-JS test framework; manual verification is sufficient per the spec.
- Freshness tab color-coding changes. Initially requested but retracted by the user after confirming a stale browser cache.
