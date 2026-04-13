const REFRESH_INTERVAL = 30000; // 30 seconds

// -- Tab switching --------------------------------------------------------

document.querySelectorAll('.tab').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(s => s.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
    });
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
    var d = new Date(iso);
    return d.toLocaleString();
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

    $('last-updated').textContent = 'Updated: ' + new Date().toLocaleTimeString();
}

// -- Trading --------------------------------------------------------------

var currentWindow = 'all';

document.querySelectorAll('.window-btn').forEach(function(btn) {
    btn.addEventListener('click', function() {
        document.querySelectorAll('.window-btn').forEach(function(b) { b.classList.remove('active'); });
        btn.classList.add('active');
        currentWindow = btn.dataset.window;
        loadTrading();
    });
});

async function loadTrading() {
    var data = await api('/api/metrics/trading?window=' + currentWindow);
    var m = data.metrics;

    if (!m) {
        ['tr-total-trades', 'tr-win-rate', 'tr-total-pnl', 'tr-profit-factor',
         'tr-sharpe', 'tr-avg-win', 'tr-avg-loss', 'tr-avg-hold'].forEach(function(id) {
            setHtml(id, '--');
        });
        return;
    }

    setHtml('tr-total-trades', m.total_trades);
    setHtml('tr-win-rate', fmtPct(m.win_rate));

    var pnl = $('tr-total-pnl');
    pnl.innerHTML = fmtMoney(m.total_pnl);
    setPnlClass(pnl, m.total_pnl);

    setHtml('tr-profit-factor', fmt(m.profit_factor));
    setHtml('tr-sharpe', fmt(m.sharpe_ratio));

    var avgWin = $('tr-avg-win');
    avgWin.innerHTML = fmtMoney(m.avg_win);
    setPnlClass(avgWin, m.avg_win);

    var avgLoss = $('tr-avg-loss');
    avgLoss.innerHTML = fmtMoney(m.avg_loss);
    setPnlClass(avgLoss, m.avg_loss);

    setHtml('tr-avg-hold', fmtDuration(m.avg_hold_duration_seconds));

    // Per-symbol breakdown
    var symbols = await api('/api/metrics/symbols');
    var grid = $('tr-symbols');
    var entries = Object.entries(symbols);
    if (entries.length === 0) {
        grid.innerHTML = '<p class="empty-state">No symbol data yet</p>';
    } else {
        grid.innerHTML = entries.map(function(entry) {
            var sym = entry[0];
            var sm = entry[1].metrics;
            return '<div class="symbol-card">' +
                '<h3>' + sym + '</h3>' +
                '<div class="stat"><span class="stat-label">Trades</span><span>' + sm.total_trades + '</span></div>' +
                '<div class="stat"><span class="stat-label">Win Rate</span><span>' + fmtPct(sm.win_rate) + '</span></div>' +
                '<div class="stat"><span class="stat-label">P&L</span><span>' + fmtMoney(sm.total_pnl) + '</span></div>' +
                '<div class="stat"><span class="stat-label">Profit Factor</span><span>' + fmt(sm.profit_factor) + '</span></div>' +
                '</div>';
        }).join('');
    }
}

// -- Models ---------------------------------------------------------------

async function loadModels() {
    var models = await api('/api/models');
    var tbody = $('models-table').querySelector('tbody');
    tbody.innerHTML = models.map(function(m) {
        return '<tr>' +
            '<td>' + m.model_id + '</td>' +
            '<td><span class="status-cell ' + m.status + '">' + m.status + '</span></td>' +
            '<td>' + m.reason + '</td>' +
            '<td>' + fmtTime(m.updated_at) + '</td>' +
            '</tr>';
    }).join('');
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
            loadTrading(),
            loadModels(),
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
