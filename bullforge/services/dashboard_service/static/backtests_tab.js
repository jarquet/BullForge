// backtests_tab.js — adds a sub-tab bar above the existing backtests.js list
// and wires in Compare/Sensitivity views.
//
// The existing backtests.js handles `#backtests` (list) and
// `#backtests/<id>` (detail). This script intercepts `#backtests/compare`
// and `#backtests/sensitivity` to render alternate views.
(function () {
  var activeSubtab = 'list';

  function $(id) { return document.getElementById(id); }

  async function fetchJSON(url) {
    var r = await fetch(url);
    if (!r.ok) {
      var body = await r.json().catch(function () { return {}; });
      throw new Error('HTTP ' + r.status + ' ' + JSON.stringify(body));
    }
    return r.json();
  }

  function renderCompare() {
    var root = $('backtests-root');
    root.innerHTML =
      '<p class="subtitle">Pick 2–5 runs (comma-separated IDs in the URL) to overlay their equity curves and compare metrics.</p>' +
      '<div id="compare-input-row">' +
      '<label>Run IDs: <input id="compare-ids-input" placeholder="e.g. 17,22,34" style="width:200px;"></label> ' +
      '<button id="compare-load">Load</button></div>' +
      '<div id="compare-chart-host" class="chart-host" style="width:100%;height:360px;margin-top:1rem;"></div>' +
      '<div id="compare-table" style="margin-top:1rem;"></div>';

    var hash = window.location.hash || '';
    var m = /ids=([^&]+)/.exec(hash);
    var inputEl = $('compare-ids-input');
    if (m) inputEl.value = decodeURIComponent(m[1]);

    async function load() {
      var ids = inputEl.value.trim();
      if (!ids) return;
      window.location.hash = '#backtests/compare?ids=' + encodeURIComponent(ids);
      var data;
      try {
        data = await fetchJSON('/api/backtests/compare?ids=' + encodeURIComponent(ids));
      } catch (e) {
        $('compare-chart-host').innerHTML = '<p class="error-state">Error: ' + e.message + '</p>';
        return;
      }
      var colors = ['#00b4d8', '#f39c12', '#2ecc71', '#e74c3c', '#9b59b6'];
      var series = data.runs.map(function (r, i) {
        // Normalize each run's equity to start at 100000 for shape comparison.
        var s = r.equity_series.slice();
        if (s.length === 0) return {label: r.name, data: [], color: colors[i % 5]};
        var base = s[0].equity || 1;
        return {
          label: r.name,
          color: colors[i % 5],
          data: s.map(function (p) {
            return {x: p.timestamp, y: p.equity * 100000 / base};
          }),
        };
      });
      window.Charts.lineChart($('compare-chart-host'), {
        series: series, xIsTime: true, yLabel: 'Normalized equity (start = 100k)',
      });

      $('compare-table').innerHTML =
        '<table class="data-table"><thead><tr>' +
        '<th>Run</th><th>Symbols</th><th>Total P&L</th><th>Avg Sharpe</th>' +
        '<th>Min Max DD</th><th>Trades</th><th>Avg Hit Rate</th><th>Tags</th>' +
        '</tr></thead><tbody>' +
        data.runs.map(function (r) {
          var mm = r.metrics;
          return '<tr><td><a href="#backtests/' + r.run_id + '">' + r.name + '</a></td>' +
            '<td>' + r.symbols.join(', ') + '</td>' +
            '<td class="' + (mm.total_pnl >= 0 ? 'positive' : 'negative') + '">$' + mm.total_pnl.toFixed(2) + '</td>' +
            '<td>' + mm.avg_sharpe.toFixed(2) + '</td>' +
            '<td class="negative">' + (mm.min_max_dd * 100).toFixed(2) + '%</td>' +
            '<td>' + mm.total_trades + '</td>' +
            '<td>' + (mm.avg_hit_rate * 100).toFixed(1) + '%</td>' +
            '<td>' + (r.tags || []).join(', ') + '</td></tr>';
        }).join('') + '</tbody></table>';
    }
    $('compare-load').onclick = load;
    if (inputEl.value) load();
  }

  async function renderSensitivity() {
    var root = $('backtests-root');
    root.innerHTML =
      '<p class="subtitle">Pick a config parameter that was swept across multiple runs. View P&L, Sharpe, Max DD as a function of that parameter.</p>' +
      '<label>Swept parameter: <input id="sens-param-input" placeholder="e.g. min_confidence" style="width:200px;"></label> ' +
      '<label>Group by: <input id="sens-group-input" value="symbols,timeframe,horizons" style="width:260px;"></label> ' +
      '<button id="sens-load">Load</button>' +
      '<div id="sens-charts" style="margin-top:1rem;"></div>';

    $('sens-load').onclick = async function () {
      var param = $('sens-param-input').value.trim();
      var grp = $('sens-group-input').value.trim();
      if (!param) return;
      var url = '/api/backtests/sensitivity?swept_param=' +
        encodeURIComponent(param) + '&group_by=' + encodeURIComponent(grp);
      var data;
      try {
        data = await fetchJSON(url);
      } catch (e) {
        $('sens-charts').innerHTML = '<p class="error-state">Error: ' + e.message + '</p>';
        return;
      }
      if (!data.points.length) {
        $('sens-charts').innerHTML = '<p class="empty-state">No runs found with config.' + param + '.</p>';
        return;
      }
      // Group points by group_key.
      var groups = {};
      data.points.forEach(function (p) {
        var key = JSON.stringify(p.group_key);
        if (!groups[key]) groups[key] = [];
        groups[key].push(p);
      });

      var html = '';
      Object.keys(groups).forEach(function (gk, gi) {
        html += '<section class="drill-section"><h3>Group: ' + gk + '</h3>' +
          '<div id="sens-pnl-' + gi + '" class="chart-host" style="width:100%;height:180px;"></div>' +
          '<div id="sens-sharpe-' + gi + '" class="chart-host" style="width:100%;height:180px;"></div>' +
          '<div id="sens-maxdd-' + gi + '" class="chart-host" style="width:100%;height:180px;"></div>' +
          '</section>';
      });
      $('sens-charts').innerHTML = html;

      Object.keys(groups).forEach(function (gk, gi) {
        var pts = groups[gk].slice().sort(function (a, b) {
          return a.param_value - b.param_value;
        });
        window.Charts.lineChart($('sens-pnl-' + gi), {
          series: [{label: 'Total P&L',
            data: pts.map(function (p) { return {x: p.param_value, y: p.metrics.total_pnl}; })}],
          xIsTime: false, yLabel: 'Total P&L ($)',
        });
        window.Charts.lineChart($('sens-sharpe-' + gi), {
          series: [{label: 'Avg Sharpe',
            data: pts.map(function (p) { return {x: p.param_value, y: p.metrics.avg_sharpe}; })}],
          xIsTime: false, yLabel: 'Avg Sharpe',
        });
        window.Charts.lineChart($('sens-maxdd-' + gi), {
          series: [{label: 'Min Max DD',
            data: pts.map(function (p) { return {x: p.param_value, y: p.metrics.min_max_dd * 100}; })}],
          xIsTime: false, yLabel: 'Min Max DD (%)',
        });
      });
    };
  }

  function setActiveSubtab(name) {
    activeSubtab = name;
    document.querySelectorAll('#backtests-subtabs .subtab').forEach(function (b) {
      b.classList.toggle('active', b.dataset.subtab === name);
    });
    if (name === 'list') {
      // Defer to existing backtests.js renderOverview() via hash.
      if (window.location.hash.indexOf('#backtests/compare') >= 0 ||
          window.location.hash.indexOf('#backtests/sensitivity') >= 0) {
        window.location.hash = '#backtests';
      } else if (window.BacktestsTab && window.BacktestsTab.activate) {
        window.BacktestsTab.activate();
      }
    } else if (name === 'compare') {
      renderCompare();
    } else if (name === 'sensitivity') {
      renderSensitivity();
    }
  }

  function onHashChange() {
    var hash = window.location.hash || '';
    if (hash.indexOf('#backtests/compare') >= 0) setActiveSubtab('compare');
    else if (hash.indexOf('#backtests/sensitivity') >= 0) setActiveSubtab('sensitivity');
    else if (hash.indexOf('#backtests/') >= 0) {
      // Detail page — handled by backtests.js. Sub-tab bar follows List.
      document.querySelectorAll('#backtests-subtabs .subtab').forEach(function (b) {
        b.classList.toggle('active', b.dataset.subtab === 'list');
      });
    } else {
      setActiveSubtab('list');
    }
  }

  document.querySelectorAll('#backtests-subtabs .subtab').forEach(function (b) {
    b.onclick = function () {
      var n = b.dataset.subtab;
      if (n === 'list') window.location.hash = '#backtests';
      else if (n === 'compare') window.location.hash = '#backtests/compare';
      else if (n === 'sensitivity') window.location.hash = '#backtests/sensitivity';
    };
  });
  window.addEventListener('hashchange', onHashChange);
  window.addEventListener('DOMContentLoaded', onHashChange);
})();
