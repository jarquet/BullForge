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
      panel().innerHTML = '<p class="empty-state">Leaderboard sub-tab placeholder (Task 6).</p>';
    },
    fidelity: async function () {
      panel().innerHTML = '<p class="empty-state">Fidelity sub-tab placeholder (Task 7).</p>';
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
