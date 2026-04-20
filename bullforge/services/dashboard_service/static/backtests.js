(function () {
  const root = () => document.getElementById('backtests-root');
  let pollTimer = null;

  async function fetchJSON(url, opts) {
    const res = await fetch(url, opts);
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      throw Object.assign(new Error(`HTTP ${res.status}`),
                          { status: res.status, body });
    }
    return res.json();
  }

  async function cancelBacktest(row) {
    if (!confirm('Cancel backtest "' + (row.name || row.id) + '"?')) return;
    const url = row.kind === 'run'
      ? '/api/backtests/' + row.id + '/cancel'
      : '/api/backtest-jobs/' + row.id + '/cancel';
    try {
      await fetchJSON(url, { method: 'POST' });
      reload();
    } catch (e) {
      alert('Cancel failed: ' + ((e.body && e.body.error) || e.message || 'unknown'));
    }
  }

  function route() {
    const hash = window.location.hash || '#backtests';
    const parts = hash.replace(/^#/, '').split('/');
    if (parts[0] === 'backtests' && parts[1]) {
      // Special sub-tabs at the top level.
      if (parts[1] === 'compare' || parts[1] === 'sensitivity') {
        // Handled by backtests_tab.js.
        return;
      }
      const id = parts[1];
      const subtab = parts[2] || 'summary';
      renderDetail(id, subtab);
    } else {
      renderOverview();
    }
  }

  const state = { status: 'all', q: '', sort: 'finished_at', order: 'desc' };

  // Column count for the Backtests list table. Keep in sync with the
  // <thead> in renderOverview. Used for loading/empty/error placeholder rows.
  const BT_TABLE_COLSPAN = 13;

  // Multi-select state for the delete feature.
  // Keys are of the form "run:{id}". Jobs are never added (disabled).
  const selection = new Set();
  let lastCheckedKey = null;  // most recently clicked selKey (for Shift-range, Task 5). String survives re-renders where DOM nodes do not.

  function selectionKey(kind, id) { return kind + ':' + id; }

  function updateActionBar() {
    const bar = document.getElementById('bt-action-bar');
    const count = document.getElementById('bt-sel-count');
    if (!bar || !count) return;  // list view not mounted
    const prev = parseInt(count.textContent, 10) || 0;
    const n = selection.size;
    count.textContent = n;
    bar.hidden = n === 0;
    // When selection goes from 0 to >0, clear any stale post-delete summary.
    // Using a count transition (prev===0 → n>0) rather than bar.hidden means
    // this fires even when the bar was left visible to display the delete summary.
    if (prev === 0 && n > 0) {
      const summary = document.getElementById('bt-action-summary');
      if (summary) summary.textContent = '';
    }
  }

  function fmtPnL(v) {
    if (v === null || v === undefined) return '—';
    const cls = v >= 0 ? 'bt-pos' : 'bt-neg';
    const sign = v >= 0 ? '+' : '';
    return `<span class="${cls}">${sign}$${v.toFixed(2)}</span>`;
  }
  function fmtPct(v) {
    if (v === null || v === undefined) return '—';
    return `${(v * 100).toFixed(1)}%`;
  }
  function fmtBtRuntime(r) {
    if (!r.started_at) return '—';
    const start = new Date(r.started_at).getTime();
    const end = r.finished_at ? new Date(r.finished_at).getTime() : Date.now();
    if (!isFinite(start) || !isFinite(end) || end < start) return '—';
    let seconds = Math.floor((end - start) / 1000);
    const cls = r.finished_at ? '' : ' class="bt-running-runtime"';
    let s;
    if (seconds < 60) s = seconds + 's';
    else if (seconds < 3600) s = Math.floor(seconds / 60) + 'm ' + (seconds % 60) + 's';
    else {
      const h = Math.floor(seconds / 3600);
      const m = Math.floor((seconds % 3600) / 60);
      s = h + 'h ' + m + 'm';
    }
    return `<span${cls}>${s}</span>`;
  }

  function renderOverview() {
    root().innerHTML = `
      <div class="bt-overview">
        <div class="bt-toolbar">
          <select id="bt-status">
            <option value="all">all</option>
            <option value="queued">queued</option>
            <option value="running">running</option>
            <option value="completed">completed</option>
            <option value="failed">failed</option>
            <option value="cancelled">Cancelled</option>
          </select>
          <input id="bt-search" placeholder="search name…" />
          <button id="bt-new">New Backtest</button>
          <button id="bt-refresh">Refresh</button>
        </div>
        <div id="bt-action-bar" class="bt-action-bar" hidden>
          <button id="bt-delete-selected" class="bt-delete-btn" type="button">Delete selected (<span id="bt-sel-count">0</span>)</button>
          <a id="bt-clear-selection" class="bt-clear-link" href="#">Clear</a>
          <span id="bt-action-summary" class="bt-action-summary"></span>
        </div>
        <table class="bt-table">
          <thead><tr>
            <th class="bt-sel-col"><input type="checkbox" id="bt-select-all" title="Select all on this page"></th>
            <th data-sort="name">Name</th>
            <th>Symbols</th>
            <th>Range</th>
            <th data-sort="total_pnl">P&amp;L</th>
            <th data-sort="sharpe">Sharpe</th>
            <th data-sort="max_dd">Max DD</th>
            <th>#Trades</th>
            <th>Hit %</th>
            <th>Status</th>
            <th>Tags</th>
            <th data-sort="finished_at">Finished</th>
            <th>Runtime</th>
          </tr></thead>
          <tbody id="bt-rows"><tr><td colspan="${BT_TABLE_COLSPAN}">Loading…</td></tr></tbody>
        </table>
      </div>
    `;
    bindOverviewEvents();
    reload();
    clearInterval(pollTimer);
    pollTimer = setInterval(reload, 10000);
  }

  function bindOverviewEvents() {
    document.getElementById('bt-status').value = state.status;
    document.getElementById('bt-search').value = state.q;
    document.getElementById('bt-status').onchange = (e) => {
      state.status = e.target.value; reload();
    };
    let t = null;
    document.getElementById('bt-search').oninput = (e) => {
      clearTimeout(t);
      t = setTimeout(() => { state.q = e.target.value; reload(); }, 300);
    };
    document.getElementById('bt-refresh').onclick = () => reload();
    document.getElementById('bt-new').onclick = () => openNewModal();
    document.querySelectorAll('.bt-table th[data-sort]').forEach(th => {
      th.onclick = () => {
        const key = th.getAttribute('data-sort');
        if (state.sort === key) {
          state.order = state.order === 'desc' ? 'asc' : 'desc';
        } else {
          state.sort = key; state.order = 'desc';
        }
        reload();
      };
    });
    const selectAll = document.getElementById('bt-select-all');
    if (selectAll) {
      selectAll.onclick = function (ev) {
        ev.stopPropagation();
        const checked = selectAll.checked;
        const boxes = document.querySelectorAll(
          '#bt-rows .bt-row-select:not(:disabled)'
        );
        boxes.forEach(b => {
          b.checked = checked;
          const key = b.dataset.selKey;
          if (checked) selection.add(key);
          else selection.delete(key);
        });
        lastCheckedKey = null;
        updateActionBar();
      };
    }
    const clearLink = document.getElementById('bt-clear-selection');
    if (clearLink) {
      clearLink.onclick = function (ev) {
        ev.preventDefault();
        clearSelection();
      };
    }
    const deleteBtn = document.getElementById('bt-delete-selected');
    if (deleteBtn) {
      deleteBtn.onclick = function () { deleteSelected(); };
    }
  }

  function clearSelection() {
    selection.clear();
    lastCheckedKey = null;
    document.querySelectorAll('#bt-rows .bt-row-select').forEach(b => {
      b.checked = false;
    });
    const header = document.getElementById('bt-select-all');
    if (header) { header.checked = false; header.indeterminate = false; }
    const summary = document.getElementById('bt-action-summary');
    if (summary) summary.textContent = '';
    updateActionBar();
  }

  function selectedRunIds() {
    // selection keys are "run:{id}"; jobs never enter the set.
    return Array.from(selection)
      .filter(k => k.startsWith('run:'))
      .map(k => parseInt(k.slice(4), 10))
      .filter(Number.isFinite);
  }

  const BULK_DELETE_THRESHOLD = 10;

  async function deleteSelected() {
    const ids = selectedRunIds();
    if (ids.length === 0) return;
    if (!confirm('Delete ' + ids.length + ' backtest(s)? This cannot be undone.')) return;

    let deleted = 0;
    let notFound = 0;
    let stillRunning = 0;
    let errors = 0;

    if (ids.length <= BULK_DELETE_THRESHOLD) {
      const results = await Promise.allSettled(
        ids.map(id => fetch('/api/backtests/' + id, {method: 'DELETE'}))
      );
      for (const r of results) {
        if (r.status !== 'fulfilled') { errors++; continue; }
        const res = r.value;
        if (res.status === 200) deleted++;
        else if (res.status === 404) notFound++;
        else if (res.status === 409) stillRunning++;
        else errors++;
      }
    } else {
      try {
        const res = await fetch('/api/backtests/delete', {
          method: 'POST',
          headers: {'content-type': 'application/json'},
          body: JSON.stringify({ids: ids}),
        });
        if (res.ok) {
          const body = await res.json();
          deleted = (body.deleted || []).length;
          notFound = (body.not_found || []).length;
          stillRunning = (body.still_running || []).length;
        } else {
          alert('Bulk delete failed: HTTP ' + res.status);
          return;
        }
      } catch (e) {
        alert('Bulk delete failed: ' + (e.message || 'network error'));
        return;
      }
    }

    clearSelection();
    reload();
    const summary = document.getElementById('bt-action-summary');
    const bar = document.getElementById('bt-action-bar');
    if (summary && bar) {
      const parts = ['Deleted ' + deleted];
      if (stillRunning) parts.push(stillRunning + ' still running');
      if (notFound)     parts.push(notFound + ' not found');
      if (errors)       parts.push(errors + ' failed');
      summary.textContent = parts.join(' · ');
      // Force-show the bar so the user sees the summary. clearSelection() hid
      // it (selection is now empty). updateActionBar will re-hide it and clear
      // the stale summary on the user's next 0→N selection transition.
      bar.hidden = false;
    }
  }

  async function reload() {
    const params = new URLSearchParams({
      status: state.status, sort: state.sort, order: state.order,
    });
    if (state.q) params.set('q', state.q);
    try {
      const data = await fetchJSON(`/api/backtests?${params}`);
      renderRows(data.rows);
    } catch (e) {
      document.getElementById('bt-rows').innerHTML =
        `<tr><td colspan="${BT_TABLE_COLSPAN}" class="bt-error">Failed to load: ${e.message}</td></tr>`;
    }
  }

  const HORIZON_FALLBACK_1M = [1,2,3,5,8,10,20,40,60,80,100,120];
  const HORIZON_FALLBACK_1D = [1,2,3,5,12,15];
  const TIMEFRAME_MINUTES = { '1m': 1, '1d': 1440 };

  async function openNewModal() {
    let symbolsList = [];
    try {
      const sym = await fetchJSON('/api/symbols');
      symbolsList = [...(sym.equity || []), ...(sym.crypto || [])];
    } catch (_) { symbolsList = []; }

    const horizonsByTf = { '1m': HORIZON_FALLBACK_1M, '1d': HORIZON_FALLBACK_1D };
    let liveThresholds = {};
    try {
      const cfg = await fetchJSON('/api/config');
      const v = cfg.values || {};
      if (Array.isArray(v.label_horizons_1m) && v.label_horizons_1m.length)
        horizonsByTf['1m'] = v.label_horizons_1m;
      if (Array.isArray(v.label_horizons_1d) && v.label_horizons_1d.length)
        horizonsByTf['1d'] = v.label_horizons_1d;
      liveThresholds = {
        min_confidence: v.execution_min_confidence,
        min_agreement: v.execution_min_agreement,
        direction_band: v.execution_direction_band,
        min_signal_strength: v.execution_min_signal_strength,
        max_position_pct: v.execution_max_position_pct,
        max_positions: v.execution_max_positions,
        max_exposure: v.execution_max_exposure,
      };
    } catch (_) { /* keep fallbacks */ }

    // Defaults: start = Jan 1 of (current year - 1), end = yesterday 00:00 UTC.
    const now = new Date();
    const defaultStart = new Date(Date.UTC(now.getUTCFullYear() - 1, 0, 1, 0, 0));
    const defaultEnd = new Date(Date.UTC(
      now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1, 0, 0,
    ));
    const fmtLocal = (d) => d.toISOString().slice(0, 16);

    const backdrop = document.createElement('div');
    backdrop.className = 'bt-modal-backdrop';
    backdrop.innerHTML = `
      <div class="bt-modal">
        <h3>New Backtest</h3>
        <div class="bt-form-row">
          <label>Custom name (optional)
            <input id="f-name" maxlength="32"
                   placeholder="leave blank for random 8-char tag">
          </label>
        </div>
        <div class="bt-form-row">
          <label>Symbols<br>
            <select id="f-symbols" multiple size="6">
              ${symbolsList.map(s => `<option value="${s}">${s}</option>`).join('')}
            </select>
          </label>
          <label><span id="f-horizons-label">Horizons (bars)</span><br>
            <select id="f-horizons" multiple size="6"></select>
          </label>
        </div>
        <div class="bt-form-row">
          <label>Timeframe
            <select id="f-timeframe">
              <option value="1m">1m</option>
              <option value="1d">1d</option>
            </select>
          </label>
          <label>Mode
            <select id="f-mode"><option>rolling</option><option>anchored</option></select>
          </label>
        </div>
        <div class="bt-form-row">
          <label>Start (UTC)<input id="f-start" type="datetime-local" value="${fmtLocal(defaultStart)}"></label>
          <label>End (UTC)<input id="f-end" type="datetime-local" value="${fmtLocal(defaultEnd)}"></label>
        </div>
        <div class="bt-form-row">
          <label>Train days<input id="f-train" type="number" value="60" min="1"></label>
          <label>Test days<input id="f-test" type="number" value="5" min="1"></label>
          <label>Embargo (min)<input id="f-embargo" type="number" value="120" min="1"></label>
        </div>
        <details class="bt-advanced">
          <summary>Advanced — Entry thresholds (optional overrides)</summary>
          <p class="bt-advanced-help">
            Blank fields snapshot the current live values at submit time, so
            the backtest reproduces those exact thresholds even if live config
            changes later.
          </p>
          <div class="bt-form-row">
            <label>Min Prediction Confidence (0-1)
              <input id="f-min_confidence" type="number" step="0.01" min="0" max="1">
            </label>
            <label>Min Agreeing Models
              <input id="f-min_agreement" type="number" step="1" min="1" max="10000">
            </label>
          </div>
          <div class="bt-form-row">
            <label>Direction Neutral Band (0-1)
              <input id="f-direction_band" type="number" step="0.01" min="0" max="1">
            </label>
            <label>Min Signal Strength (0-1)
              <input id="f-min_signal_strength" type="number" step="0.01" min="0" max="1">
            </label>
          </div>
          <div class="bt-form-row">
            <label>Max Position Size (fraction of equity)
              <input id="f-max_position_pct" type="number" step="0.01" min="0" max="1">
            </label>
            <label>Max Concurrent Positions
              <input id="f-max_positions" type="number" step="1" min="1" max="100">
            </label>
            <label>Max Portfolio Exposure (fraction of equity)
              <input id="f-max_exposure" type="number" step="0.01" min="0" max="1">
            </label>
          </div>
        </details>
        <div id="f-errors" class="bt-error"></div>
        <div style="display:flex;justify-content:flex-end;gap:8px">
          <button id="f-cancel">Cancel</button>
          <button id="f-submit">Queue Run</button>
        </div>
      </div>`;
    document.body.appendChild(backdrop);

    const THRESHOLD_KEYS = [
      'min_confidence', 'min_agreement', 'direction_band',
      'min_signal_strength', 'max_position_pct', 'max_positions',
      'max_exposure',
    ];
    THRESHOLD_KEYS.forEach(k => {
      const el = backdrop.querySelector(`#f-${k}`);
      if (el && liveThresholds[k] !== undefined && liveThresholds[k] !== null) {
        el.placeholder = `live: ${liveThresholds[k]}`;
      }
    });

    const close = () => backdrop.remove();
    backdrop.querySelector('#f-cancel').onclick = close;
    backdrop.onclick = (e) => { if (e.target === backdrop) close(); };

    const horizonsSel = backdrop.querySelector('#f-horizons');
    const embargoIn = backdrop.querySelector('#f-embargo');
    const tfSel = backdrop.querySelector('#f-timeframe');
    const horizonsLabel = backdrop.querySelector('#f-horizons-label');

    const updateEmbargoCap = () => {
      const chosen = [...horizonsSel.selectedOptions].map(o => Number(o.value));
      const tfMin = TIMEFRAME_MINUTES[tfSel.value] || 1;
      if (chosen.length) {
        embargoIn.max = Math.max(...chosen) * tfMin;
        if (Number(embargoIn.value) > embargoIn.max) embargoIn.value = embargoIn.max;
      } else {
        embargoIn.removeAttribute('max');
      }
    };

    const populateHorizons = () => {
      const tf = tfSel.value;
      const choices = horizonsByTf[tf] || HORIZON_FALLBACK_1M;
      horizonsLabel.textContent = tf === '1d' ? 'Horizons (days)' : 'Horizons (bars)';
      horizonsSel.innerHTML = choices
        .map(h => `<option value="${h}" selected>${h}</option>`).join('');
      updateEmbargoCap();
    };

    horizonsSel.addEventListener('change', updateEmbargoCap);
    tfSel.addEventListener('change', populateHorizons);
    populateHorizons();

    backdrop.querySelector('#f-submit').onclick = async () => {
      const body = {
        name: backdrop.querySelector('#f-name').value.trim(),
        symbols: [...backdrop.querySelector('#f-symbols').selectedOptions].map(o => o.value),
        timeframe: backdrop.querySelector('#f-timeframe').value,
        horizons: [...horizonsSel.selectedOptions].map(o => Number(o.value)),
        range_start: new Date(backdrop.querySelector('#f-start').value + 'Z').toISOString(),
        range_end:   new Date(backdrop.querySelector('#f-end').value + 'Z').toISOString(),
        train_days: Number(backdrop.querySelector('#f-train').value),
        test_days:  Number(backdrop.querySelector('#f-test').value),
        embargo_min: Number(embargoIn.value),
        mode: backdrop.querySelector('#f-mode').value,
      };
      THRESHOLD_KEYS.forEach(k => {
        const el = backdrop.querySelector(`#f-${k}`);
        const raw = el && el.value.trim();
        if (raw !== undefined && raw !== '') {
          const n = Number(raw);
          if (Number.isFinite(n)) {
            body[k] = n;
          }
        }
      });
      try {
        await fetchJSON('/api/backtests', {
          method: 'POST',
          headers: {'content-type': 'application/json'},
          body: JSON.stringify(body),
        });
        close();
        reload();
      } catch (e) {
        const errs = e.body && e.body.errors;
        backdrop.querySelector('#f-errors').textContent = errs
          ? Object.entries(errs).map(([k,v]) => `${k}: ${v}`).join(' · ')
          : (e.message || 'Failed');
      }
    };
  }

  function renderRows(rows) {
    const tbody = document.getElementById('bt-rows');
    if (!rows.length) {
      tbody.innerHTML =
        '<tr><td colspan="' + BT_TABLE_COLSPAN + '">No backtests yet. Click New Backtest to start one.</td></tr>';
      const headerBox = document.getElementById('bt-select-all');
      if (headerBox) {
        headerBox.checked = false;
        headerBox.indeterminate = false;
      }
      return;
    }
    tbody.innerHTML = rows.map(r => {
      const m = r.metrics;
      const range = r.range_start ? `${r.range_start.slice(0,10)} – ${r.range_end?.slice(0,10) || ''}` : '—';
      const symsDisplay = r.symbols.length > 2
        ? `${r.symbols.length} syms` : r.symbols.join(', ');
      const pnl = m ? fmtPnL(m.total_pnl) : '—';
      const sharpe = m ? m.sharpe.toFixed(2) : '—';
      const dd = m ? `<span class="bt-neg">${fmtPct(m.max_dd)}</span>` : '—';
      const ntr = m ? m.n_trades : '—';
      const hit = m ? fmtPct(m.hit_rate) : '—';
      const target = r.kind === 'run'
        ? `#backtests/${r.id}` : `#backtests/job-${r.id}`;
      const tagChips = (r.tags || []).map(function (t) {
        return '<span class="tag-chip">' + t + '</span>';
      }).join(' ');
      const canCancel = (r.kind === 'job')
        || (r.kind === 'run' && r.status === 'running');
      const cancelBtn = canCancel
        ? `<button class="bt-cancel-btn"
              data-cancel-kind="${r.kind}"
              data-cancel-id="${r.id}"
              data-cancel-name="${escapeHtml(r.name || '')}"
            >Cancel</button>`
        : '';
      const statusCell = r.status === 'cancelled'
        ? `<span class="bt-status-cancelled">${r.status}</span>`
        : r.status;
      const runtime = fmtBtRuntime(r);
      const selKey = selectionKey(r.kind, r.id);
      const isDisabled = r.kind === 'job'
        || (r.kind === 'run' && r.status === 'running');
      const isChecked = !isDisabled && selection.has(selKey);
      const boxTitle = r.kind === 'job'
        ? 'Cannot delete a queued or running job'
        : (isDisabled ? 'Cancel this backtest first' : 'Select for delete');
      const checkboxHtml =
        '<input type="checkbox" class="bt-row-select"' +
        ' data-sel-key="' + selKey + '"' +
        ' data-kind="' + r.kind + '"' +
        ' data-id="' + r.id + '"' +
        ' data-status="' + (r.status || '') + '"' +
        (isDisabled ? ' disabled' : '') +
        (isChecked ? ' checked' : '') +
        ' title="' + boxTitle + '"' +
        ' onclick="event.stopPropagation()"' +
        '>';
      return `<tr class="clickable" onclick="location.hash='${target}'">
        <td class="bt-sel-col" onclick="event.stopPropagation()">${checkboxHtml}</td>
        <td>${r.name}</td><td>${symsDisplay}</td><td>${range}</td>
        <td>${pnl}</td><td>${sharpe}</td><td>${dd}</td>
        <td>${ntr}</td><td>${hit}</td>
        <td>${statusCell} ${cancelBtn}</td>
        <td>${tagChips}</td>
        <td>${fmtTime(r.finished_at)}</td>
        <td>${runtime}</td>
      </tr>`;
    }).join('');
    if (!tbody.__btCancelDelegated) {
      tbody.addEventListener('click', function (ev) {
        const btn = ev.target && ev.target.closest('.bt-cancel-btn');
        if (btn) {
          ev.stopPropagation();
          cancelBacktest({
            kind: btn.dataset.cancelKind,
            id: parseInt(btn.dataset.cancelId, 10),
            name: btn.dataset.cancelName || '',
          });
          return;
        }
        const box = ev.target && ev.target.matches('.bt-row-select') ? ev.target : null;
        if (box && !box.disabled) {
          // Capture-phase: box.checked still reflects the PRE-toggle state
          // because the browser hasn't yet flipped it (that happens at target).
          // Compute the target state ourselves: invert the pre-click state.
          const targetChecked = !box.checked;
          let handledAsRange = false;
          if (ev.shiftKey && lastCheckedKey && lastCheckedKey !== box.dataset.selKey) {
            const lastBox = tbody.querySelector(
              '.bt-row-select[data-sel-key="' + lastCheckedKey + '"]'
            );
            if (lastBox) {
              // Range select between lastBox and box (inclusive),
              // skipping disabled boxes.
              const all = Array.from(tbody.querySelectorAll('.bt-row-select'))
                .filter(b => !b.disabled);
              const a = all.indexOf(lastBox);
              const b = all.indexOf(box);
              if (a !== -1 && b !== -1) {
                const [lo, hi] = a < b ? [a, b] : [b, a];
                for (let i = lo; i <= hi; i++) {
                  const bx = all[i];
                  bx.checked = targetChecked;
                  const k = bx.dataset.selKey;
                  if (targetChecked) selection.add(k);
                  else selection.delete(k);
                }
                handledAsRange = true;
              }
            }
            // If lastBox was stale (key not in current DOM), fall through.
          }
          if (!handledAsRange) {
            const key = box.dataset.selKey;
            if (targetChecked) selection.add(key);
            else selection.delete(key);
          }
          lastCheckedKey = box.dataset.selKey;
          updateActionBar();
        }
      }, true);  // capture phase: runs BEFORE target's inline stopPropagation()
      tbody.__btCancelDelegated = true;
    }

    // Prune selection keys whose rows are no longer rendered. This keeps
    // the selection set bounded and handles rows that vanish between polls
    // (e.g., deleted by another tab).
    const visibleKeys = new Set(rows.map(r => selectionKey(r.kind, r.id)));
    for (const key of Array.from(selection)) {
      if (!visibleKeys.has(key)) selection.delete(key);
    }

    // Keep header select-all in sync: checked iff every selectable row is selected.
    const headerBox = document.getElementById('bt-select-all');
    if (headerBox) {
      const selectableBoxes = tbody.querySelectorAll('.bt-row-select:not(:disabled)');
      const total = selectableBoxes.length;
      const selected = Array.from(selectableBoxes).filter(b => b.checked).length;
      headerBox.checked = total > 0 && selected === total;
      headerBox.indeterminate = selected > 0 && selected < total;
    }
  }

  async function renderDetail(id, subtab) {
    subtab = subtab || 'summary';
    clearInterval(pollTimer);
    if (id.startsWith('job-')) {
      return renderJobDetail(id.slice(4));
    }
    root().innerHTML =
      '<div class="bt-detail">' +
      '<a class="bt-back" href="#backtests">← Back to list</a>' +
      '<h2 id="bt-detail-title">Run ' + id +
      ' <span id="bt-detail-cancel"></span></h2>' +
      '<div class="subtab-bar" id="bt-detail-subtabs">' +
        '<button class="subtab" data-subtab="summary">Summary</button>' +
        '<button class="subtab" data-subtab="equity">Equity</button>' +
        '<button class="subtab" data-subtab="folds">Folds</button>' +
        '<button class="subtab" data-subtab="symbols">Per-symbol</button>' +
        '<button class="subtab" data-subtab="distributions">Distributions</button>' +
        '<button class="subtab" data-subtab="live">Live-vs-backtest</button>' +
        '<button class="subtab" data-subtab="funnel">Funnel</button>' +
      '</div>' +
      '<div id="bt-detail-panel"></div>' +
      '</div>';

    document.querySelectorAll('#bt-detail-subtabs .subtab').forEach(function (b) {
      b.classList.toggle('active', b.dataset.subtab === subtab);
      b.onclick = function () {
        window.location.hash = '#backtests/' + id + '/' + b.dataset.subtab;
      };
    });

    var panel = document.getElementById('bt-detail-panel');

    try {
      if (subtab === 'summary')            await renderDetailSummary(id, panel);
      else if (subtab === 'equity')        await renderDetailEquity(id, panel);
      else if (subtab === 'folds')         await renderDetailFolds(id, panel);
      else if (subtab === 'symbols')       await renderDetailSymbols(id, panel);
      else if (subtab === 'distributions') await renderDetailDistributions(id, panel);
      else if (subtab === 'live')          await renderDetailLiveOverlay(id, panel);
      else if (subtab === 'funnel')        await renderDetailFunnel(id, panel);
      else panel.innerHTML = '<p class="empty-state">Unknown sub-tab: ' + subtab + '</p>';
    } catch (e) {
      panel.innerHTML = '<p class="bt-error">Error: ' + (e.message || e) + '</p>';
    }
  }

  async function renderDetailSummary(id, panel) {
    var run = await fetchJSON('/api/backtests/' + id);
    panel.innerHTML =
      '<div class="summary-grid">' +
      '<div><strong>Name:</strong> ' + (run.name || '') + '</div>' +
      '<div><strong>Status:</strong> ' + (run.status || '') + '</div>' +
      '<div><strong>Symbols:</strong> ' + (run.symbols || []).join(', ') + '</div>' +
      '<div><strong>Range:</strong> ' + (run.range_start || '') + ' \u2192 ' + (run.range_end || '') + '</div>' +
      '<div><strong>Tags:</strong> <span id="detail-tags"></span></div>' +
      '<div><strong>Notes:</strong> <span id="detail-notes"></span></div>' +
      '</div>' +
      '<h3>Config</h3><pre>' + JSON.stringify(run.config_json || run.config || {}, null, 2) + '</pre>' +
      '<button id="bt-promote">Promote config to live</button>' +
      '<div id="bt-promote-modal"></div>';
    // Tags and notes renderer.
    if (window.BacktestsTagsNotes) {
      window.BacktestsTagsNotes.mount(id, run.tags || [], run.notes || '');
    }
    // Promote button.
    document.getElementById('bt-promote').onclick = function () {
      if (window.BacktestsPromote) window.BacktestsPromote.open(id, run.config_json || run.config || {});
    };
    // Cancel button in header (only while running).
    var cancelSlot = document.getElementById('bt-detail-cancel');
    if (cancelSlot) {
      if (run && run.status === 'running') {
        cancelSlot.innerHTML =
          '<button class="bt-cancel-btn" id="bt-detail-cancel-btn"' +
          ' data-cancel-kind="run"' +
          ' data-cancel-id="' + id + '"' +
          ' data-cancel-name="' + escapeHtml(run.name || String(id)) + '"' +
          '>Cancel</button>';
        var btn = document.getElementById('bt-detail-cancel-btn');
        btn.addEventListener('click', function () {
          cancelBacktest({
            kind: btn.dataset.cancelKind,
            id: parseInt(btn.dataset.cancelId, 10),
            name: btn.dataset.cancelName || '',
          });
        });
      } else {
        cancelSlot.innerHTML = '';
      }
    }
  }

  async function renderDetailEquity(id, panel) {
    panel.innerHTML =
      '<div id="det-eq-host" class="chart-host" style="width:100%;height:360px;"></div>' +
      '<p id="det-eq-summary" style="color:var(--text-muted);"></p>';
    var data = await fetchJSON('/api/backtests/' + id + '/equity');
    var series = (data.series || data || []).map(function (p) {
      return {x: p.timestamp, y: p.equity};
    });
    window.Charts.lineChart(document.getElementById('det-eq-host'), {
      series: [{label: 'Equity', data: series}],
      xIsTime: true, yLabel: 'Equity ($)',
    });
    var fbs = (data.fold_boundaries || []).length;
    document.getElementById('det-eq-summary').textContent =
      (data.series || data).length + ' points · ' + fbs + ' fold boundaries';
  }

  async function renderDetailFolds(id, panel) {
    panel.innerHTML = '<div id="det-folds-host"></div>';
    var folds = await fetchJSON('/api/backtests/' + id + '/folds');
    window.Leaderboard.create({
      mountEl: document.getElementById('det-folds-host'),
      columns: [
        {key: 'fold_index', label: 'Fold', sortable: true},
        {key: 'test_start', label: 'Test Start', sortable: true},
        {key: 'test_end', label: 'Test End', sortable: true},
        {key: 'pnl', label: 'P&L', sortable: true,
         render: function (v) {
           var cls = v >= 0 ? 'positive' : 'negative';
           return '<span class="' + cls + '">$' + v.toFixed(2) + '</span>';
         }},
        {key: 'sharpe', label: 'Sharpe', sortable: true,
         render: function (v) { return v.toFixed(2); }},
        {key: 'max_drawdown', label: 'Max DD', sortable: true,
         render: function (v) { return (v * 100).toFixed(2) + '%'; }},
        {key: 'trades', label: '#Trades', sortable: true},
        {key: 'hit_rate', label: 'Hit Rate', sortable: true,
         render: function (v) { return (v * 100).toFixed(1) + '%'; }},
      ],
      rows: folds,
      initialSort: {key: 'fold_index', order: 'asc'},
    });
  }
  async function renderDetailSymbols(id, panel) {
    panel.innerHTML =
      '<div id="det-sym-chart" class="chart-host" style="width:100%;height:240px;"></div>' +
      '<div id="det-sym-table"></div>';
    var rows = await fetchJSON('/api/backtests/' + id + '/symbols');
    window.Charts.barChart(document.getElementById('det-sym-chart'), {
      labels: rows.map(function (r) { return r.symbol; }),
      values: rows.map(function (r) { return r.total_pnl; }),
      colorByValue: true, yLabel: 'P&L per symbol ($)',
    });
    window.Leaderboard.create({
      mountEl: document.getElementById('det-sym-table'),
      columns: [
        {key: 'symbol', label: 'Symbol', sortable: true},
        {key: 'trades', label: '#Trades', sortable: true},
        {key: 'wins', label: 'Wins', sortable: true},
        {key: 'win_rate', label: 'Win %', sortable: true,
         render: function (v) { return (v * 100).toFixed(1) + '%'; }},
        {key: 'total_pnl', label: 'Total P&L', sortable: true,
         render: function (v) {
           var cls = v >= 0 ? 'positive' : 'negative';
           return '<span class="' + cls + '">$' + v.toFixed(2) + '</span>';
         }},
        {key: 'avg_pnl', label: 'Avg P&L', sortable: true,
         render: function (v) { return '$' + v.toFixed(2); }},
      ],
      rows: rows,
      initialSort: {key: 'total_pnl', order: 'desc'},
    });
  }

  async function renderDetailDistributions(id, panel) {
    panel.innerHTML =
      '<h3>Daily P&L</h3><div id="det-dist-daily" class="chart-host" style="width:100%;height:200px;"></div>' +
      '<h3>Trade P&L histogram</h3><div id="det-dist-hist" class="chart-host" style="width:100%;height:220px;"></div>' +
      '<h3>Hold-time buckets</h3><div id="det-dist-hold" class="chart-host" style="width:100%;height:220px;"></div>' +
      '<h3>Side breakdown</h3><div id="det-dist-side"></div>';

    var data = await fetchJSON('/api/backtests/' + id + '/distributions');
    window.Charts.barChart(document.getElementById('det-dist-daily'), {
      labels: data.daily_pnl.map(function (d) {
        return (d.date || '').slice(0, 10);
      }),
      values: data.daily_pnl.map(function (d) { return d.pnl; }),
      colorByValue: true, yLabel: 'P&L ($)',
    });
    var hist = data.trade_pnl_histogram;
    window.Charts.barChart(document.getElementById('det-dist-hist'), {
      labels: hist.labels.map(function (v) { return String(v); }),
      values: hist.counts, yLabel: 'Count',
    });
    window.Charts.barChart(document.getElementById('det-dist-hold'), {
      labels: data.hold_time_buckets.map(function (b) {
        return b.bucket + ' (' + b.trades + ')';
      }),
      values: data.hold_time_buckets.map(function (b) { return b.avg_pnl; }),
      colorByValue: true, yLabel: 'Avg P&L ($)',
    });
    document.getElementById('det-dist-side').innerHTML =
      '<table class="data-table"><thead><tr>' +
      '<th>Side</th><th>Trades</th><th>Wins</th><th>Win %</th><th>Total P&L</th><th>Avg P&L</th>' +
      '</tr></thead><tbody>' +
      data.side_breakdown.map(function (s) {
        var cls = s.total_pnl >= 0 ? 'positive' : 'negative';
        return '<tr><td>' + s.side + '</td><td>' + s.trades + '</td><td>' + s.wins + '</td>' +
          '<td>' + (s.win_rate * 100).toFixed(1) + '%</td>' +
          '<td class="' + cls + '">$' + s.total_pnl.toFixed(2) + '</td>' +
          '<td>$' + s.avg_pnl.toFixed(2) + '</td></tr>';
      }).join('') + '</tbody></table>';
  }
  async function renderDetailLiveOverlay(id, panel) {
    panel.innerHTML =
      '<div id="det-live-host" class="chart-host" style="width:100%;height:340px;"></div>' +
      '<p id="det-live-note" style="color:var(--text-muted);"></p>';
    var data = await fetchJSON('/api/backtests/' + id + '/live-overlay');
    if (!data.has_overlap) {
      document.getElementById('det-live-note').innerHTML =
        '<strong>No overlap.</strong> No live trades exist for symbols [' +
        (data.symbols || []).join(', ') + '] within the backtest window (' +
        (data.range_start || '') + ' \u2192 ' + (data.range_end || '') + ').';
    }
    var series = [
      {label: 'Backtest equity',
       data: data.backtest_series.map(function (p) {
         return {x: p.timestamp, y: p.equity};
       })},
      {label: 'Live (cumulative P&L from 0)',
       data: data.live_series.map(function (p) {
         return {x: p.timestamp, y: p.equity};
       })},
    ];
    window.Charts.lineChart(document.getElementById('det-live-host'), {
      series: series, xIsTime: true, yLabel: 'Equity / cumulative P&L',
    });
  }

  async function renderDetailFunnel(id, panel) {
    panel.innerHTML =
      '<div id="det-funnel-host" class="chart-host" style="width:100%;height:260px;"></div>' +
      '<p id="det-funnel-note" style="color:var(--text-muted);"></p>';
    var data = await fetchJSON('/api/backtests/' + id + '/funnel');
    var labels = ['Signals', 'Confident (\u2265' +
                  (data.min_confidence * 100).toFixed(0) + '%)',
                  'Orders placed', 'Orders filled', 'Orders closed'];
    var values = [data.signals, data.confident, data.orders_placed,
                   data.orders_filled, data.orders_closed];
    window.Charts.barChart(document.getElementById('det-funnel-host'), {
      labels: labels, values: values, yLabel: 'Count',
    });
    // Drop-off rates between adjacent stages.
    var dropTexts = [];
    for (var i = 1; i < values.length; i++) {
      var prev = values[i - 1], cur = values[i];
      var dropPct = prev > 0 ? (1 - cur / prev) * 100 : 0;
      dropTexts.push(labels[i - 1] + ' \u2192 ' + labels[i] + ': ' + dropPct.toFixed(1) + '% drop');
    }
    document.getElementById('det-funnel-note').textContent = dropTexts.join(' \u00b7 ');
  }

  async function renderJobDetail(jobId) {
    root().innerHTML = `<div class="bt-detail">
      <a href="#backtests">← Back</a>
      <h3 id="bt-job-title">Job #${jobId}</h3>
      <p id="bt-job-status">Loading…</p>
      <details open><summary>Config</summary><pre id="bt-job-config"></pre></details>
      <p id="bt-job-error" class="bt-error"></p>
    </div>`;
    async function tick() {
      try {
        const data = await fetchJSON(`/api/backtests?status=all`);
        const job = data.rows.find(r => r.kind === 'job' && r.id == jobId);
        if (!job) {
          document.getElementById('bt-job-status').textContent =
            'Job completed. Return to overview to find the run.';
          return;
        }
        document.getElementById('bt-job-title').textContent =
          `${job.name} (${job.status})`;
        document.getElementById('bt-job-status').textContent = job.status;
        document.getElementById('bt-job-config').textContent =
          JSON.stringify(job, null, 2);
        if (job.status === 'failed') {
          document.getElementById('bt-job-error').textContent = job.error || '';
        }
      } catch (e) { /* ignore transient */ }
    }
    tick();
    pollTimer = setInterval(tick, 5000);
  }

  function renderHeader(d) {
    document.getElementById('bt-header').innerHTML =
      `<h2>${d.name}</h2>
       <p>${d.symbols.join(', ')} · ${d.timeframe} · ${d.mode} ·
          ${d.range_start?.slice(0,10)} – ${d.range_end?.slice(0,10)} ·
          ${d.metrics.n_folds} folds · ${d.status}</p>`;
  }
  function renderCards(m) {
    const cards = [
      ['P&L', fmtPnL(m.total_pnl)], ['Sharpe', m.sharpe.toFixed(2)],
      ['Sortino', m.sortino.toFixed(2)], ['Max DD', fmtPct(m.max_dd)],
      ['Hit Rate', fmtPct(m.hit_rate)], ['Profit Factor', m.profit_factor.toFixed(2)],
      ['#Trades', m.n_trades], ['#Folds', m.n_folds],
    ];
    document.getElementById('bt-cards').innerHTML = cards.map(
      ([l, v]) => `<div class="bt-card"><div class="label">${l}</div>
                   <div class="value">${v}</div></div>`
    ).join('');
  }
  function renderEquityChart(points, folds, runId) {
    const ctx = document.getElementById('bt-equity').getContext('2d');
    const combined = {
      label: 'Combined',
      data: points.map(p => ({ x: p.t, y: p.equity })),
      borderColor: '#4ade80', pointRadius: 0, borderWidth: 1.5,
    };
    const chart = new Chart(ctx, {
      type: 'line',
      data: { datasets: [combined] },
      options: {
        parsing: false, animation: false,
        scales: { x: { type: 'time', time: { tooltipFormat: 'PP HH:mm' } } },
        plugins: { legend: { onClick: async (_, item) => {
          const fold = folds[item.datasetIndex - 1];
          if (!fold) return;
          const key = `fold-${fold.id}`;
          const existing = chart.data.datasets.find(d => d.key === key);
          if (existing) {
            chart.data.datasets = chart.data.datasets.filter(d => d.key !== key);
          } else {
            const data = await fetchJSON(
              `/api/backtests/${runId}/equity?fold_id=${fold.id}`);
            chart.data.datasets.push({
              key, label: `Fold ${fold.fold_index}`,
              data: data.points.map(p => ({ x: p.t, y: p.equity })),
              pointRadius: 0, borderWidth: 1,
            });
          }
          chart.update();
        } } },
      },
    });
  }
  function renderDrawdown(points) {
    let peak = -Infinity;
    const dd = points.map(p => {
      peak = Math.max(peak, p.equity);
      return { x: p.t, y: p.equity / peak - 1 };
    });
    new Chart(document.getElementById('bt-drawdown').getContext('2d'), {
      type: 'line',
      data: { datasets: [{ label: 'Drawdown', data: dd,
                           borderColor: '#f87171', backgroundColor: 'rgba(248,113,113,.25)',
                           fill: 'origin', pointRadius: 0, borderWidth: 1 }] },
      options: { parsing: false, animation: false,
                 scales: { x: { type: 'time' },
                           y: { ticks: { callback: v => `${(v*100).toFixed(0)}%` }}}},
    });
  }
  function renderFolds(folds) {
    document.getElementById('bt-folds').innerHTML = `
      <thead><tr><th>#</th><th>Train</th><th>Test</th>
        <th>P&amp;L</th><th>Sharpe</th><th>Max DD</th><th>#Trades</th></tr></thead>
      <tbody>${folds.map(f => `<tr>
        <td>${f.fold_index}</td>
        <td>${f.train_start?.slice(0,10)} – ${f.train_end?.slice(0,10)}</td>
        <td>${f.test_start?.slice(0,10)} – ${f.test_end?.slice(0,10)}</td>
        <td>${fmtPnL(f.metrics.total_pnl)}</td>
        <td>${f.metrics.sharpe.toFixed(2)}</td>
        <td><span class="bt-neg">${fmtPct(f.metrics.max_dd)}</span></td>
        <td>${f.metrics.n_trades}</td></tr>`).join('')}</tbody>`;
  }
  function renderConfig(d) {
    document.getElementById('bt-config').textContent =
      JSON.stringify(d.config, null, 2);
  }
  async function renderHistogram(runId) {
    const first = await fetchJSON(`/api/backtests/${runId}/trades?limit=200&offset=0`);
    const pnls = first.trades.map(t => t.pnl).filter(v => v != null);
    if (!pnls.length) return;
    const min = Math.min(...pnls), max = Math.max(...pnls);
    const bins = 30, width = (max - min) / bins || 1;
    const counts = Array(bins).fill(0);
    pnls.forEach(p => counts[Math.min(bins-1, Math.floor((p - min) / width))]++);
    const labels = counts.map((_, i) => (min + width * i).toFixed(2));
    new Chart(document.getElementById('bt-histogram').getContext('2d'), {
      type: 'bar',
      data: { labels, datasets: [{ label: 'Trade P&L', data: counts,
                                    backgroundColor: '#60a5fa' }] },
      options: { animation: false },
    });
  }
  async function loadTrades(runId, offset) {
    const data = await fetchJSON(`/api/backtests/${runId}/trades?limit=50&offset=${offset}`);
    const rows = data.trades.map(t => `<tr>
      <td>${t.entry_time || '—'}</td><td>${t.symbol}</td><td>${t.side}</td>
      <td>${t.entry_price}</td><td>${t.exit_price ?? '—'}</td>
      <td>${t.quantity}</td><td>${fmtPnL(t.pnl)}</td>
      <td>${t.signal_strength?.toFixed(2) ?? '—'}</td>
      <td>${t.fold_id}</td></tr>`).join('');
    document.getElementById('bt-trades').innerHTML = `
      <table class="bt-table"><thead><tr>
        <th>Entry</th><th>Sym</th><th>Side</th><th>Entry$</th><th>Exit$</th>
        <th>Qty</th><th>P&amp;L</th><th>Signal</th><th>Fold</th>
      </tr></thead><tbody>${rows}</tbody></table>
      <div style="margin-top:6px">
        ${offset > 0 ? `<button onclick="BacktestsTab._trades(${runId},${offset-50})">Prev</button>` : ''}
        ${data.trades.length === 50 ? `<button onclick="BacktestsTab._trades(${runId},${offset+50})">Next</button>` : ''}
      </div>`;
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c];
    });
  }

  window.BacktestsTagsNotes = {
    mount: function (runId, tags, notes) {
      var tagsEl = document.getElementById('detail-tags');
      var notesEl = document.getElementById('detail-notes');
      if (!tagsEl || !notesEl) return;

      function renderTags(current) {
        tagsEl.innerHTML =
          current.map(function (t) {
            return '<span class="tag-chip">' + t + '</span>';
          }).join(' ') +
          ' <a href="#" id="tag-edit-link">Edit</a>';
        document.getElementById('tag-edit-link').onclick = function (e) {
          e.preventDefault();
          tagsEl.innerHTML =
            '<input class="tag-editor" id="tag-edit-input" value="' +
              current.join(', ') + '" />' +
            ' <button id="tag-save">Save</button> <button id="tag-cancel">Cancel</button>';
          document.getElementById('tag-save').onclick = async function () {
            var list = document.getElementById('tag-edit-input').value
                         .split(',').map(function (s) { return s.trim(); })
                         .filter(function (s) { return s.length > 0; });
            var r = await fetch('/api/backtests/' + runId + '/tags', {
              method: 'POST',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify({tags: list}),
            });
            if (r.ok) {
              var body = await r.json();
              renderTags(body.tags || []);
            } else {
              alert('Failed to save tags: HTTP ' + r.status);
            }
          };
          document.getElementById('tag-cancel').onclick = function () {
            renderTags(current);
          };
        };
      }
      renderTags(tags || []);

      function renderNotes(current) {
        notesEl.innerHTML =
          '<span>' + (current ? escapeHtml(current) : '<em>(none)</em>') + '</span>' +
          ' <a href="#" id="notes-edit-link">Edit</a>';
        document.getElementById('notes-edit-link').onclick = function (e) {
          e.preventDefault();
          notesEl.innerHTML =
            '<textarea class="notes-editor" id="notes-edit-input">' +
              escapeHtml(current || '') +
            '</textarea><br>' +
            '<button id="notes-save">Save</button> <button id="notes-cancel">Cancel</button>';
          document.getElementById('notes-save').onclick = async function () {
            var text = document.getElementById('notes-edit-input').value;
            var r = await fetch('/api/backtests/' + runId + '/notes', {
              method: 'POST',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify({notes: text}),
            });
            if (r.ok) {
              var body = await r.json();
              renderNotes(body.notes || '');
            } else {
              alert('Failed to save notes: HTTP ' + r.status);
            }
          };
          document.getElementById('notes-cancel').onclick = function () {
            renderNotes(current);
          };
        };
      }
      renderNotes(notes || '');
    },
  };

  window.BacktestsPromote = {
    open: async function (runId, cfg) {
      var modalHost = document.getElementById('bt-promote-modal');
      if (!modalHost) return;
      // Fetch current runtime_config to show diff.
      var rcResp = await fetch('/api/config');
      var rc = await rcResp.json();
      var current = rc.values || {};
      // Consider only execution_* keys for now.
      var rows = [];
      Object.keys(cfg).forEach(function (k) {
        var rtKey = k.startsWith('execution_') ? k : 'execution_' + k;
        if (!(rtKey in current)) return;
        var cur = current[rtKey];
        var nxt = cfg[k];
        if (JSON.stringify(cur) === JSON.stringify(nxt)) return;
        rows.push({key: k, rtKey: rtKey, current: cur, next: nxt});
      });
      if (rows.length === 0) {
        modalHost.innerHTML = '<div class="bt-modal">' +
          '<p>Backtest config matches current runtime config for all execution_* keys. Nothing to promote.</p>' +
          '<button id="bt-promote-close">Close</button></div>';
        document.getElementById('bt-promote-close').onclick = function () {
          modalHost.innerHTML = '';
        };
        return;
      }
      modalHost.innerHTML =
        '<div class="bt-modal">' +
        '<h3>Promote config to live runtime_config</h3>' +
        '<table class="data-table"><thead><tr><th></th><th>Key</th><th>Current</th><th>From backtest</th></tr></thead><tbody>' +
        rows.map(function (r, i) {
          return '<tr><td><input type="checkbox" checked data-idx="' + i + '"></td>' +
            '<td>' + r.rtKey + '</td>' +
            '<td>' + JSON.stringify(r.current) + '</td>' +
            '<td class="positive">' + JSON.stringify(r.next) + '</td></tr>';
        }).join('') + '</tbody></table>' +
        '<p><label>Note: <input id="bt-promote-note" type="text" style="width:300px;"></label></p>' +
        '<button id="bt-promote-apply">Apply</button> ' +
        '<button id="bt-promote-cancel">Cancel</button>' +
        '</div>';

      document.getElementById('bt-promote-cancel').onclick = function () {
        modalHost.innerHTML = '';
      };
      document.getElementById('bt-promote-apply').onclick = async function () {
        var picked = [];
        modalHost.querySelectorAll('input[type=checkbox]').forEach(function (cb) {
          if (cb.checked) picked.push(rows[parseInt(cb.dataset.idx, 10)].key);
        });
        var note = document.getElementById('bt-promote-note').value || '';
        var r = await fetch('/api/backtests/' + runId + '/promote-config', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({keys: picked, note: note}),
        });
        if (r.ok) {
          var body = await r.json();
          modalHost.innerHTML = '<div class="bt-modal">' +
            '<p>Applied ' + body.diff.length + ' change(s).</p>' +
            '<button id="bt-promote-done">Close</button></div>';
          document.getElementById('bt-promote-done').onclick = function () {
            modalHost.innerHTML = '';
          };
        } else {
          var err = await r.text();
          modalHost.innerHTML = '<div class="bt-modal error-state"><p>Error: ' + err + '</p>' +
            '<button id="bt-promote-err">Close</button></div>';
          document.getElementById('bt-promote-err').onclick = function () {
            modalHost.innerHTML = '';
          };
        }
      };
    },
  };

  window.BacktestsTab = Object.assign(window.BacktestsTab || {}, {
    activate() { route(); },
    onHashChange() { route(); },
    _trades: (id, off) => loadTrades(id, off),
  });
})();
