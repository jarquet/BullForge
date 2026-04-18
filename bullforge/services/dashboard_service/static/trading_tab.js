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
      panel().innerHTML = '<p class="empty-state">Overview sub-tab placeholder (Task 4).</p>';
    },
    equity: async function () {
      panel().innerHTML = '<p class="empty-state">Equity Curve sub-tab placeholder (Task 5).</p>';
    },
    distributions: async function () {
      panel().innerHTML = '<p class="empty-state">Distributions sub-tab placeholder (Task 6).</p>';
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
