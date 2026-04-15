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
    document.getElementById('bt-new').onclick = () => {
      // openNewModal is defined in Task 11.
      if (typeof openNewModal === 'function') openNewModal();
    };
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
    root().innerHTML =
      `<div class="bt-detail"><a href="#backtests">← Back</a>` +
      `<p>Detail loading for ${id}…</p></div>`;
    // Populated in Tasks 10-11.
  }

  window.BacktestsTab = {
    activate() { route(); },
    onHashChange() { route(); },
  };
})();
