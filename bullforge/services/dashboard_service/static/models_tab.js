// models_tab.js — Models tab controller.
// Owns: sub-tab switching, time-window + refresh wiring, router integration,
// per-sub-tab data fetching and rendering.
//
// Uses Plan 1 components: window.Router, window.TimeWindow, window.Refresh,
// window.Charts, window.Leaderboard, window.DrillDown.
(function () {
  var state = {
    activeSubtab: 'leaderboard',
    window: {from: null, to: null},
    timewindow: null,
    refresh: null,
    currentDrillId: null,
  };

  function $(id) { return document.getElementById(id); }
  function panel() { return $('models-panel'); }

  function setActiveSubtab(name) {
    state.activeSubtab = name;
    document.querySelectorAll('#models-subtabs .subtab').forEach(function (b) {
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

  // Sub-tab handlers are plugged in by later tasks (Tasks 6-12).
  var handlers = {
    leaderboard: async function () {
      panel().innerHTML = '<div id="models-lb-host"></div>';
      var rows = await fetchJSON('/api/models');
      var host = $('models-lb-host');
      window.Leaderboard.create({
        mountEl: host,
        columns: [
          {key: 'symbol', label: 'Symbol', sortable: true},
          {key: 'timeframe', label: 'TF', sortable: true},
          {key: 'horizon_minutes', label: 'Horizon', sortable: true,
           render: function (v) { return v + 'm'; }},
          {key: 'model_type', label: 'Type', sortable: true},
          {key: 'status', label: 'Status', sortable: true,
           render: function (v) {
             return '<span class="status-cell ' + v + '">' + v + '</span>';
           }},
          {key: 'live_accuracy_7d', label: 'Live 7d', sortable: true,
           render: function (v) { return (v * 100).toFixed(1) + '%'; }},
          {key: 'backtested_accuracy', label: 'Backtest', sortable: true,
           render: function (v) { return (v * 100).toFixed(1) + '%'; }},
          {key: 'fidelity_delta', label: 'Δ (bt−live)', sortable: true,
           render: function (v) {
             var cls = v > 0.05 ? 'negative' : (v > 0.02 ? 'warning' : '');
             return '<span class="' + cls + '">' + (v * 100).toFixed(1) + '%</span>';
           }},
          {key: 'signals_7d', label: 'Signals 7d', sortable: true},
          {key: 'simulated_pnl_7d_pct', label: 'Sim P&L 7d', sortable: true,
           render: function (v) {
             var cls = v >= 0 ? 'positive' : 'negative';
             return '<span class="' + cls + '">' + (v * 100).toFixed(2) + '%</span>';
           }},
          {key: 'accuracy_sparkline_30d', label: '30d', sortable: false,
           render: function (values, row) {
             if (!values || values.length === 0) return '<span class="muted">—</span>';
             return '<span class="sparkline-cell" data-mid="' + row.model_id + '"></span>';
           }},
        ],
        rows: rows,
        getRowHref: function (row) { return '#models/' + row.model_id; },
        initialSort: {key: 'fidelity_delta', order: 'desc'},
      });
      // Render sparklines after leaderboard inserts the cells.
      host.querySelectorAll('.sparkline-cell').forEach(function (el) {
        var row = rows.filter(function (r) {
          return String(r.model_id) === el.getAttribute('data-mid');
        })[0];
        if (row && row.accuracy_sparkline_30d && row.accuracy_sparkline_30d.length > 0) {
          el.style.display = 'inline-block';
          el.style.width = '80px';
          el.style.height = '20px';
          window.Charts.sparkline(el, {values: row.accuracy_sparkline_30d});
        }
      });
    },
    fidelity: async function () {
      panel().innerHTML =
        '<div class="fidelity-panel">' +
        '<p class="subtitle">Per-model live accuracy (7-day rolling) vs backtested baseline. Red line = warning margin, dashed red = critical margin.</p>' +
        '<div class="model-selector"><label>Models: </label>' +
        '<select id="fidelity-model-select" multiple size="6" style="min-width:400px;"></select>' +
        '<button id="fidelity-refresh">Load</button></div>' +
        '<div id="fidelity-chart-host" class="chart-host" style="width:100%;height:360px;margin-top:1rem;"></div>' +
        '</div>';

      var models = await fetchJSON('/api/models');
      var select = $('fidelity-model-select');
      select.innerHTML = models.map(function (m) {
        return '<option value="' + m.model_id + '" selected>' +
          m.symbol + ' · ' + m.timeframe + ' · ' + m.horizon_minutes + 'm · ' + m.model_type +
          '</option>';
      }).join('');
      // Cap to first 8 selected by default for rendering performance.
      Array.prototype.slice.call(select.options).forEach(function (o, i) {
        if (i >= 8) o.selected = false;
      });

      async function load() {
        var selected = Array.prototype.filter.call(select.options,
          function (o) { return o.selected; }).map(function (o) { return o.value; });
        var series = [];
        for (var i = 0; i < selected.length; i++) {
          var mid = selected[i];
          var hist = await fetchJSON('/api/models/' + mid + '/history');
          var m = models.filter(function (mm) {
            return String(mm.model_id) === String(mid);
          })[0];
          var label = m ? (m.symbol + ' · ' + m.horizon_minutes + 'm') : ('#' + mid);
          series.push({
            label: label,
            data: hist.map(function (h) {
              return {x: h.date, y: h.accuracy};
            }),
          });
        }
        window.Charts.lineChart($('fidelity-chart-host'), {
          series: series,
          xIsTime: true,
          yLabel: 'Accuracy',
        });
      }

      $('fidelity-refresh').onclick = load;
      await load();
    },
    calibration: async function () {
      panel().innerHTML = '<p class="empty-state">Calibration sub-tab placeholder (Task 8).</p>';
    },
    attribution: async function () {
      panel().innerHTML = '<p class="empty-state">Attribution sub-tab placeholder (Task 9).</p>';
    },
    timeseries: async function () {
      panel().innerHTML = '<p class="empty-state">Time-series sub-tab placeholder (Task 10).</p>';
    },
    regime: async function () {
      panel().innerHTML = '<p class="empty-state">Regime sub-tab placeholder (Task 11).</p>';
    },
    featimp: async function () {
      panel().innerHTML = '<p class="empty-state">Feature Importance sub-tab placeholder (Task 12).</p>';
    },
  };

  async function renderDrillDown(modelId) {
    state.currentDrillId = modelId;
    panel().innerHTML = '';
    var dd = window.DrillDown.create({
      mountEl: panel(),
      title: 'Model #' + modelId,
      backHref: '#models/leaderboard',
      breadcrumbs: [
        {label: 'Models', href: '#models'},
        {label: 'Leaderboard', href: '#models/leaderboard'},
        {label: 'Model #' + modelId},
      ],
    });
    dd.getContentSlot().innerHTML =
      '<p class="empty-state">Per-model drill-down placeholder (Task 13).</p>';
  }

  async function render() {
    if (state.currentDrillId !== null) {
      // Drill-down rendering handled separately.
      return;
    }
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
    // Mount time-window picker.
    if (!state.timewindow) {
      state.timewindow = window.TimeWindow.create({
        mountEl: $('models-timewindow-slot'),
        storageKey: 'models.timewindow',
        onChange: function (range) {
          state.window = range;
          render();
        },
      });
    }
    // Mount refresh control.
    if (!state.refresh) {
      state.refresh = window.Refresh.create({
        mountEl: $('models-refresh-slot'),
        storageKey: 'models.refresh',
        initialIntervalSec: 30,
        onRefresh: function () { render(); },
      });
    }
    // Wire sub-tab buttons.
    document.querySelectorAll('#models-subtabs .subtab').forEach(function (b) {
      b.onclick = function () {
        window.Router.navigate('#models/' + b.dataset.subtab);
      };
    });
  }

  function handleRoute(route) {
    if (route.tab !== 'models') return;
    activate();
    if (route.entity_kind === 'models' && route.entity_id) {
      renderDrillDown(route.entity_id);
    } else if (route.subtab) {
      state.currentDrillId = null;
      setActiveSubtab(route.subtab);
    } else {
      state.currentDrillId = null;
      setActiveSubtab('leaderboard');
    }
  }

  // Boot on first DOM load + subscribe to future routes.
  window.addEventListener('DOMContentLoaded', function () {
    if (window.Router) {
      window.Router.subscribe(handleRoute);
      handleRoute(window.Router.current());
    }
  });

  // Also handle the case where the top-level tab click switches to models.
  document.addEventListener('click', function (e) {
    var btn = e.target && e.target.closest && e.target.closest('.tab[data-tab="models"]');
    if (btn) {
      // Defer routing to Router.
      setTimeout(function () {
        window.Router.navigate('#models/' + (state.activeSubtab || 'leaderboard'));
      }, 0);
    }
  });

  window.ModelsTab = {
    _state: state,
    _handlers: handlers,
    _render: render,
    _fetchJSON: fetchJSON,
  };
})();
