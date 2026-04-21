const REFRESH_INTERVAL = 30000; // 30 seconds

// -- Tab switching --------------------------------------------------------

const _loadedFragments = new Set();

async function _loadFragment(section) {
    if (_loadedFragments.has(section.id)) return;
    const url = section.dataset.fragment;
    if (!url) return;
    const resp = await fetch(url);
    const html = await resp.text();
    section.innerHTML = html;
    _loadedFragments.add(section.id);
    // Execute any <script> tags in the injected fragment.
    section.querySelectorAll('script').forEach(old => {
        const s = document.createElement('script');
        if (old.src) s.src = old.src; else s.textContent = old.textContent;
        old.parentNode.replaceChild(s, old);
    });
}

document.querySelectorAll('.tab').forEach(btn => {
    btn.addEventListener('click', async () => {
        document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(s => s.classList.remove('active'));
        btn.classList.add('active');
        const section = document.getElementById('tab-' + btn.dataset.tab);
        section.classList.add('active');
        await _loadFragment(section);
        if (btn.dataset.tab === 'backtests' && window.BacktestsTab) {
            window.BacktestsTab.activate();
        }
    });
});

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
window.addEventListener('hashchange', () => {
    const hash = window.location.hash || '';
    if (hash.startsWith('#backtests')) {
        const btn = document.querySelector('[data-tab="backtests"]');
        if (btn) btn.click();
        window.BacktestsTab && window.BacktestsTab.onHashChange(hash);
    }
});

// -- Helpers --------------------------------------------------------------

function $(id) { return document.getElementById(id); }

function fmt(n, decimals) {
    if (n == null) return '--';
    return Number(n).toFixed(decimals !== undefined ? decimals : 2);
}

function fmtPct(n) {
    if (n == null) return '--';
    return (n * 100).toFixed(1) + '%';
}

function fmtMoney(n) {
    if (n == null) return '--';
    var sign = n >= 0 ? '' : '-';
    return sign + '$' + Math.abs(n).toLocaleString('en-US', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    });
}

function fmtDuration(seconds) {
    if (seconds == null) return '--';
    if (seconds < 60) return seconds + 's';
    if (seconds < 3600) return Math.floor(seconds / 60) + 'm';
    var h = Math.floor(seconds / 3600);
    var m = Math.floor((seconds % 3600) / 60);
    return h + 'h ' + m + 'm';
}

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

function setPnlClass(el, value) {
    el.classList.remove('positive', 'negative');
    if (value > 0) el.classList.add('positive');
    else if (value < 0) el.classList.add('negative');
}

function setHtml(id, html) {
    var el = $(id);
    if (el) el.innerHTML = html;
}

// -- API fetch ------------------------------------------------------------

async function api(path) {
    var resp = await fetch(path);
    if (!resp.ok) throw new Error('API error: ' + resp.status);
    return resp.json();
}

// -- Overview -------------------------------------------------------------

async function loadOverview() {
    var status = await api('/api/status');
    var trading = await api('/api/metrics/trading?window=all');

    // System status badge
    var badge = $('system-status');
    if (status.is_halted) {
        badge.textContent = 'HALTED';
        badge.className = 'status-badge status-halted';
    } else {
        badge.textContent = 'ACTIVE';
        badge.className = 'status-badge status-active';
    }

    // Account metrics
    var acct = status.account;
    if (acct) {
        setHtml('ov-equity', fmtMoney(acct.equity));
        setHtml('ov-buying-power', fmtMoney(acct.buying_power));

        var dd = $('ov-drawdown');
        dd.innerHTML = fmtPct(acct.current_drawdown);
        setPnlClass(dd, -(acct.current_drawdown || 0));

        var mdd = $('ov-max-drawdown');
        mdd.innerHTML = fmtPct(acct.max_drawdown);
        setPnlClass(mdd, -(acct.max_drawdown || 0));
    }

    // Trading summary
    var tm = trading.metrics;
    if (tm) {
        setHtml('ov-total-trades', tm.total_trades);
        setHtml('ov-win-rate', fmtPct(tm.win_rate));

        var pnl = $('ov-total-pnl');
        pnl.innerHTML = fmtMoney(tm.total_pnl);
        setPnlClass(pnl, tm.total_pnl);

        setHtml('ov-profit-factor', fmt(tm.profit_factor));
    }

    // Active halts
    var haltsDiv = $('ov-halts');
    if (status.halts.length === 0) {
        haltsDiv.innerHTML = '<p class="empty-state">No active halts</p>';
    } else {
        haltsDiv.innerHTML = status.halts.map(function(h) {
            return '<div class="halt-item">' +
                '<span class="tier">Tier ' + h.tier + '</span> — ' + h.reason +
                '<br><small>' + fmtTime(h.activated_at) + '</small>' +
                '</div>';
        }).join('');
    }

    // Model status table
    var tbody = $('ov-models').querySelector('tbody');
    tbody.innerHTML = status.models.map(function(m) {
        return '<tr>' +
            '<td>' + m.model_id + '</td>' +
            '<td><span class="status-cell ' + m.status + '">' + m.status + '</span></td>' +
            '<td>' + m.reason + '</td>' +
            '<td>' + fmtTime(m.updated_at) + '</td>' +
            '</tr>';
    }).join('');

    $('last-updated').innerHTML = 'Updated: ' + fmtTime(new Date().toISOString());
}


// -- Trades ---------------------------------------------------------------

async function loadTrades() {
    var trades = await api('/api/trades?limit=100');
    var tbody = $('trades-table').querySelector('tbody');
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
}

// -- Orders ---------------------------------------------------------------

async function loadOrders() {
    var orders = await api('/api/orders?limit=100');
    var tbody = $('orders-table').querySelector('tbody');
    tbody.innerHTML = orders.map(function(o) {
        var statusClass = o.status === 'filled' ? 'active' : o.status === 'cancelled' ? 'disabled' : 'warned';
        return '<tr>' +
            '<td>' + o.id + '</td>' +
            '<td>' + o.symbol + '</td>' +
            '<td>' + o.side + '</td>' +
            '<td>' + fmt(o.quantity, 1) + '</td>' +
            '<td>' + o.order_type + '</td>' +
            '<td><span class="status-cell ' + statusClass + '">' + o.status + '</span></td>' +
            '<td>' + fmtPct(o.signal_strength) + '</td>' +
            '<td>' + fmtMoney(o.filled_price) + '</td>' +
            '<td>' + fmtTime(o.submitted_at) + '</td>' +
            '</tr>';
    }).join('');
}

// -- Halts ----------------------------------------------------------------

async function loadHalts() {
    var data = await api('/api/halts');

    var activeDiv = $('halts-active');
    if (data.active.length === 0) {
        activeDiv.innerHTML = '<p class="empty-state">No active halts</p>';
    } else {
        activeDiv.innerHTML = data.active.map(function(h) {
            return '<div class="halt-item">' +
                '<span class="tier">Tier ' + h.tier + '</span> — ' + h.reason +
                (h.direction ? ' (' + h.direction + ')' : '') +
                '<br><small>Since ' + fmtTime(h.activated_at) + '</small>' +
                '</div>';
        }).join('');
    }

    var tbody = $('halts-table').querySelector('tbody');
    tbody.innerHTML = data.history.map(function(h) {
        return '<tr>' +
            '<td>' + h.id + '</td>' +
            '<td>Tier ' + h.tier + '</td>' +
            '<td>' + h.reason + '</td>' +
            '<td>' + (h.direction || '--') + '</td>' +
            '<td>' + fmtTime(h.activated_at) + '</td>' +
            '<td>' + (h.resolved_at ? fmtTime(h.resolved_at) : '<span class="status-cell disabled">Active</span>') + '</td>' +
            '</tr>';
    }).join('');
}

// -- Refresh loop ---------------------------------------------------------

async function refreshAll() {
    try {
        await Promise.all([
            loadOverview(),
            loadTrades(),
            loadOrders(),
            loadHalts(),
        ]);
    } catch (err) {
        console.error('Dashboard refresh error:', err);
    }
}

refreshAll();
setInterval(refreshAll, REFRESH_INTERVAL);
