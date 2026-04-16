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

  function route() {
    const hash = window.location.hash || '#backtests';
    const parts = hash.replace(/^#/, '').split('/');
    if (parts[0] === 'backtests' && parts[1]) {
      renderDetail(parts[1]);
    } else {
      renderOverview();
    }
  }

  const state = { status: 'all', q: '', sort: 'finished_at', order: 'desc' };

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
          </select>
          <input id="bt-search" placeholder="search name…" />
          <button id="bt-new">New Backtest</button>
          <button id="bt-refresh">Refresh</button>
        </div>
        <table class="bt-table">
          <thead><tr>
            <th data-sort="name">Name</th>
            <th>Symbols</th>
            <th>Range</th>
            <th data-sort="total_pnl">P&amp;L</th>
            <th data-sort="sharpe">Sharpe</th>
            <th data-sort="max_dd">Max DD</th>
            <th>#Trades</th>
            <th>Hit %</th>
            <th>Status</th>
            <th data-sort="finished_at">Finished</th>
          </tr></thead>
          <tbody id="bt-rows"><tr><td colspan="10">Loading…</td></tr></tbody>
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
        `<tr><td colspan="10" class="bt-error">Failed to load: ${e.message}</td></tr>`;
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
    try {
      const cfg = await fetchJSON('/api/config');
      const v = cfg.values || {};
      if (Array.isArray(v.label_horizons_1m) && v.label_horizons_1m.length)
        horizonsByTf['1m'] = v.label_horizons_1m;
      if (Array.isArray(v.label_horizons_1d) && v.label_horizons_1d.length)
        horizonsByTf['1d'] = v.label_horizons_1d;
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
          <label>Embargo (min)<input id="f-embargo" type="number" value="30" min="1"></label>
        </div>
        <div id="f-errors" class="bt-error"></div>
        <div style="display:flex;justify-content:flex-end;gap:8px">
          <button id="f-cancel">Cancel</button>
          <button id="f-submit">Queue Run</button>
        </div>
      </div>`;
    document.body.appendChild(backdrop);

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
        '<tr><td colspan="10">No backtests yet. Click New Backtest to start one.</td></tr>';
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
      return `<tr class="clickable" onclick="location.hash='${target}'">
        <td>${r.name}</td><td>${symsDisplay}</td><td>${range}</td>
        <td>${pnl}</td><td>${sharpe}</td><td>${dd}</td>
        <td>${ntr}</td><td>${hit}</td><td>${r.status}</td>
        <td>${r.finished_at ? r.finished_at.replace('T',' ').slice(0,16) : '—'}</td>
      </tr>`;
    }).join('');
  }

  async function renderDetail(id) {
    clearInterval(pollTimer);
    if (id.startsWith('job-')) {
      return renderJobDetail(id.slice(4));
    }
    root().innerHTML = `<div class="bt-detail">
      <a href="#backtests">← Back</a>
      <div id="bt-header"></div>
      <div class="bt-cards" id="bt-cards"></div>
      <div class="bt-chart"><canvas id="bt-equity"></canvas></div>
      <div class="bt-chart"><canvas id="bt-drawdown"></canvas></div>
      <h3>Folds</h3>
      <table class="bt-table" id="bt-folds"></table>
      <div class="bt-chart"><canvas id="bt-histogram"></canvas></div>
      <h3>Trades</h3>
      <div id="bt-trades"></div>
      <details><summary>Config</summary><pre id="bt-config"></pre></details>
    </div>`;
    try {
      const [detail, folds, equity] = await Promise.all([
        fetchJSON(`/api/backtests/${id}`),
        fetchJSON(`/api/backtests/${id}/folds`),
        fetchJSON(`/api/backtests/${id}/equity`),
      ]);
      renderHeader(detail);
      renderCards(detail.metrics);
      renderEquityChart(equity.points, folds.folds, id);
      renderDrawdown(equity.points);
      renderFolds(folds.folds);
      renderConfig(detail);
      await loadTrades(id, 0);
      await renderHistogram(id);
    } catch (e) {
      root().innerHTML += `<p class="bt-error">${e.message}</p>`;
    }
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

  window.BacktestsTab = Object.assign(window.BacktestsTab || {}, {
    activate() { route(); },
    onHashChange() { route(); },
    _trades: (id, off) => loadTrades(id, off),
  });
})();
