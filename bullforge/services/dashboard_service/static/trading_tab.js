// trading_tab.js — Trading tab controller.
// Owns: sub-tab switching, time-window + refresh wiring, router integration,
// per-sub-tab data fetching, and per-symbol drill-down.
//
// Uses Plan 1 components: window.Router, window.TimeWindow, window.Refresh,
// window.Charts, window.Leaderboard, window.DrillDown.
(function () {
  var state = {
    activeSubtab: 'overview',
    window: {from: null, to: null},
    timewindow: null,
    refresh: null,
    currentDrillSymbol: null,
  };

  function $(id) { return document.getElementById(id); }
  function panel() { return $('trading-panel'); }

  function setActiveSubtab(name) {
    state.activeSubtab = name;
    document.querySelectorAll('#trading-subtabs .subtab').forEach(function (b) {
      b.classList.toggle('active', b.dataset.subtab === name);
    });
    render();
  }

  async function fetchJSON(path) {
    var url = path;
    var range = state.window;
    if (range && range.from) {
      url += (url.indexOf('?') >= 0 ? '&' : '?') + 'from=' + encodeURIComponent(range.from);
    }
    if (range && range.to) {
      url += (url.indexOf('?') >= 0 ? '&' : '?') + 'to=' + encodeURIComponent(range.to);
    }
    var r = await fetch(url);
    if (!r.ok) {
      var body = await r.json().catch(function () { return {}; });
      throw new Error('HTTP ' + r.status + ' ' + JSON.stringify(body));
    }
    return r.json();
  }

  var handlers = {
    overview: async function () {
      panel().innerHTML =
        '<div class="card-grid">' +
          '<div class="card"><h3>Total Trades</h3><div id="tov-total-trades" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Win Rate</h3><div id="tov-win-rate" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Total P&amp;L</h3><div id="tov-total-pnl" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Profit Factor</h3><div id="tov-profit-factor" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Sharpe Ratio</h3><div id="tov-sharpe" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Avg Win</h3><div id="tov-avg-win" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Avg Loss</h3><div id="tov-avg-loss" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Avg Hold</h3><div id="tov-avg-hold" class="metric-value">--</div></div>' +
        '</div>' +
        '<div class="card-grid">' +
          '<div class="card"><h3>Current Streak</h3><div id="tov-cur-streak" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Longest Win Streak</h3><div id="tov-long-win" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Longest Loss Streak</h3><div id="tov-long-loss" class="metric-value">--</div></div>' +
          '<div class="card"><h3>Exposure Now</h3><div id="tov-exposure" class="metric-value">--</div></div>' +
        '</div>' +
        '<h3 style="margin-top:1rem;">Per-Symbol Breakdown</h3>' +
        '<div id="tov-symbols" class="symbol-grid"></div>';

      // Reuse existing /api/metrics/trading and /api/metrics/symbols endpoints.
      var [trading, symbols, streaks, exposure] = await Promise.all([
        fetchJSON('/api/metrics/trading?window=all'),
        fetchJSON('/api/metrics/symbols'),
        fetchJSON('/api/trading/streaks'),
        fetchJSON('/api/trading/exposure'),
      ]);

      function fmtMoney(n) {
        if (n == null) return '--';
        var sign = n >= 0 ? '' : '-';
        return sign + '$' + Math.abs(n).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
      }
      function fmtPct(n) { return n == null ? '--' : (n * 100).toFixed(1) + '%'; }
      function fmtDur(s) {
        if (s == null) return '--';
        if (s < 60) return s + 's';
        if (s < 3600) return Math.floor(s / 60) + 'm';
        return Math.floor(s / 3600) + 'h';
      }

      var m = trading.metrics || {};
      $('tov-total-trades').textContent = m.total_trades != null ? m.total_trades : '--';
      $('tov-win-rate').textContent = fmtPct(m.win_rate);
      $('tov-total-pnl').innerHTML = fmtMoney(m.total_pnl);
      $('tov-total-pnl').className = 'metric-value ' + (m.total_pnl >= 0 ? 'positive' : 'negative');
      $('tov-profit-factor').textContent = m.profit_factor != null ? m.profit_factor.toFixed(2) : '--';
      $('tov-sharpe').textContent = m.sharpe_ratio != null ? m.sharpe_ratio.toFixed(2) : '--';
      $('tov-avg-win').innerHTML = fmtMoney(m.avg_win);
      $('tov-avg-loss').innerHTML = fmtMoney(m.avg_loss);
      $('tov-avg-hold').textContent = fmtDur(m.avg_hold_duration_seconds);

      // Streaks
      var streakText = streaks.current_streak_kind === 'win'
        ? '+' + streaks.current_streak_length + ' wins'
        : (streaks.current_streak_kind === 'loss'
            ? '-' + streaks.current_streak_length + ' losses'
            : '--');
      $('tov-cur-streak').innerHTML = '<span class="' +
        (streaks.current_streak_kind === 'win' ? 'positive' : 'negative') +
        '">' + streakText + '</span>';
      $('tov-long-win').innerHTML = '<span class="positive">' + streaks.longest_win_streak + '</span>';
      $('tov-long-loss').innerHTML = '<span class="negative">' + streaks.longest_loss_streak + '</span>';

      // Exposure now = last point in exposure timeline.
      var now_pct = exposure.length > 0 ? exposure[exposure.length - 1].total_exposure_pct : 0;
      $('tov-exposure').textContent = (now_pct * 100).toFixed(1) + '%';

      // Per-symbol grid.
      var symNames = Object.keys(symbols).sort();
      $('tov-symbols').innerHTML = symNames.map(function (sym) {
        var s = symbols[sym].metrics || {};
        var pnlCls = (s.total_pnl || 0) >= 0 ? 'positive' : 'negative';
        return '<a class="symbol-card" href="#trading/symbols/' + sym + '">' +
          '<div class="symbol-name">' + sym + '</div>' +
          '<div class="symbol-stat">Trades: ' + (s.total_trades || 0) + '</div>' +
          '<div class="symbol-stat">Win: ' + fmtPct(s.win_rate) + '</div>' +
          '<div class="symbol-stat ' + pnlCls + '">P&L: ' + fmtMoney(s.total_pnl) + '</div>' +
          '</a>';
      }).join('');
    },
    equity: async function () {
      panel().innerHTML =
        '<p class="subtitle">Realized equity over time. Drawdown shaded where below trailing peak. Toggle SPY overlay to compare.</p>' +
        '<div style="margin-bottom:0.5rem;">' +
        '<label><input type="checkbox" id="eq-spy-toggle"> Overlay SPY buy-and-hold</label>' +
        '</div>' +
        '<div id="eq-chart-host" class="chart-host" style="width:100%;height:420px;"></div>' +
        '<div id="eq-stats" style="margin-top:0.5rem;color:var(--text-muted);"></div>';

      async function render() {
        var includeSpy = $('eq-spy-toggle').checked;
        var equityData = await fetchJSON('/api/trading/equity');
        var series = [{
          label: 'Realized equity',
          data: equityData.map(function (p) {
            return {x: p.timestamp, y: p.realized_equity};
          }),
          color: getComputedStyle(document.documentElement).getPropertyValue('--accent').trim() || '#00b4d8',
        }];
        if (includeSpy) {
          var bench = await fetchJSON('/api/trading/benchmark');
          series.push({
            label: 'SPY buy-and-hold',
            data: bench.spy_series.map(function (p) {
              return {x: p.timestamp, y: p.equity};
            }),
            color: getComputedStyle(document.documentElement).getPropertyValue('--yellow').trim() || '#f39c12',
          });
        }
        window.Charts.lineChart($('eq-chart-host'), {
          series: series, xIsTime: true, yLabel: 'Equity ($)',
        });

        if (equityData.length > 0) {
          var last = equityData[equityData.length - 1];
          $('eq-stats').textContent =
            'Current equity: $' + last.realized_equity.toFixed(2) +
            ' · Peak: $' + last.trailing_peak.toFixed(2) +
            ' · Drawdown: ' + (last.drawdown * 100).toFixed(2) + '%' +
            ' · Trades plotted: ' + equityData.length;
        } else {
          $('eq-stats').textContent = 'No closed trades in window.';
        }
      }
      $('eq-spy-toggle').onchange = render;
      await render();
    },
    distributions: async function () {
      panel().innerHTML =
        '<h3>Daily P&L</h3>' +
        '<div id="dist-daily-host" class="chart-host" style="width:100%;height:220px;"></div>' +
        '<h3 style="margin-top:1rem;">Trade P&L histogram</h3>' +
        '<div id="dist-hist-host" class="chart-host" style="width:100%;height:240px;"></div>' +
        '<div id="dist-hist-summary" style="color:var(--text-muted);font-size:0.85rem;"></div>' +
        '<h3 style="margin-top:1rem;">Hold-time buckets</h3>' +
        '<div id="dist-hold-host" class="chart-host" style="width:100%;height:220px;"></div>' +
        '<h3 style="margin-top:1rem;">Side breakdown</h3>' +
        '<div id="dist-side"></div>';

      var data = await fetchJSON('/api/trading/distributions');

      // Daily P&L bars.
      window.Charts.barChart($('dist-daily-host'), {
        labels: data.daily_pnl.map(function (d) {
          return (d.date || '').slice(0, 10);
        }),
        values: data.daily_pnl.map(function (d) { return d.pnl; }),
        colorByValue: true, yLabel: 'P&L ($)',
      });

      // Histogram.
      var hist = data.trade_pnl_histogram;
      window.Charts.barChart($('dist-hist-host'), {
        labels: hist.labels.map(function (v) { return String(v); }),
        values: hist.counts, colorByValue: false, yLabel: 'Count',
      });
      $('dist-hist-summary').textContent =
        'Median: $' + hist.median.toFixed(2) + ' · Mean: $' + hist.mean.toFixed(2);

      // Hold-time buckets.
      window.Charts.barChart($('dist-hold-host'), {
        labels: data.hold_time_buckets.map(function (b) {
          return b.bucket + ' (' + b.trades + ')';
        }),
        values: data.hold_time_buckets.map(function (b) { return b.avg_pnl; }),
        colorByValue: true, yLabel: 'Avg P&L per trade ($)',
      });

      // Side breakdown table.
      $('dist-side').innerHTML =
        '<table class="data-table" style="max-width:500px;"><thead><tr>' +
        '<th>Side</th><th>Trades</th><th>Wins</th><th>Win Rate</th>' +
        '<th>Total P&L</th><th>Avg P&L</th>' +
        '</tr></thead><tbody>' +
        data.side_breakdown.map(function (s) {
          var cls = (s.total_pnl || 0) >= 0 ? 'positive' : 'negative';
          return '<tr><td>' + s.side + '</td>' +
            '<td>' + s.trades + '</td>' +
            '<td>' + s.wins + '</td>' +
            '<td>' + (s.win_rate * 100).toFixed(1) + '%</td>' +
            '<td class="' + cls + '">$' + s.total_pnl.toFixed(2) + '</td>' +
            '<td>$' + s.avg_pnl.toFixed(2) + '</td></tr>';
        }).join('') + '</tbody></table>';
    },
    streaks: async function () {
      panel().innerHTML = '<p class="empty-state">Streaks & Exposure sub-tab placeholder (Task 7).</p>';
    },
    heatmaps: async function () {
      panel().innerHTML = '<p class="empty-state">Heatmaps sub-tab placeholder (Task 8).</p>';
    },
    benchmark: async function () {
      panel().innerHTML = '<p class="empty-state">Benchmark sub-tab placeholder (Task 9).</p>';
    },
  };

  async function renderDrillDown(symbol) {
    state.currentDrillSymbol = symbol;
    panel().innerHTML = '';
    var dd = window.DrillDown.create({
      mountEl: panel(),
      title: symbol,
      backHref: '#trading/overview',
      breadcrumbs: [
        {label: 'Trading', href: '#trading'},
        {label: 'Overview', href: '#trading/overview'},
        {label: symbol},
      ],
    });
    dd.getContentSlot().innerHTML =
      '<p class="empty-state">Per-symbol drill-down placeholder (Task 10).</p>';
  }

  async function render() {
    if (state.currentDrillSymbol) return;
    var handler = handlers[state.activeSubtab];
    if (!handler) {
      panel().innerHTML = '<p class="empty-state">Unknown sub-tab: ' + state.activeSubtab + '</p>';
      return;
    }
    try {
      await handler();
    } catch (e) {
      panel().innerHTML = '<p class="error-state">Error loading sub-tab: ' + e.message + '</p>';
    }
  }

  function activate() {
    if (!state.timewindow) {
      state.timewindow = window.TimeWindow.create({
        mountEl: $('trading-timewindow-slot'),
        storageKey: 'trading.timewindow',
        onChange: function (range) { state.window = range; render(); },
      });
    }
    if (!state.refresh) {
      state.refresh = window.Refresh.create({
        mountEl: $('trading-refresh-slot'),
        storageKey: 'trading.refresh',
        initialIntervalSec: 30,
        onRefresh: function () { render(); },
      });
    }
    document.querySelectorAll('#trading-subtabs .subtab').forEach(function (b) {
      b.onclick = function () {
        window.Router.navigate('#trading/' + b.dataset.subtab);
      };
    });
  }

  function handleRoute(route) {
    if (route.tab !== 'trading') return;
    activate();
    if (route.entity_kind === 'symbols' && route.entity_id) {
      renderDrillDown(route.entity_id);
    } else if (route.subtab) {
      state.currentDrillSymbol = null;
      setActiveSubtab(route.subtab);
    } else {
      state.currentDrillSymbol = null;
      setActiveSubtab('overview');
    }
  }

  window.addEventListener('DOMContentLoaded', function () {
    if (window.Router) {
      window.Router.subscribe(handleRoute);
      handleRoute(window.Router.current());
    }
  });

  document.addEventListener('click', function (e) {
    var btn = e.target && e.target.closest && e.target.closest('.tab[data-tab="trading"]');
    if (btn) {
      setTimeout(function () {
        window.Router.navigate('#trading/' + (state.activeSubtab || 'overview'));
      }, 0);
    }
  });

  window.TradingTab = {
    _state: state,
    _handlers: handlers,
    _render: render,
    _fetchJSON: fetchJSON,
  };
})();
