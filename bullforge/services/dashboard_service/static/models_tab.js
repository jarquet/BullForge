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
      panel().innerHTML =
        '<div class="calibration-panel">' +
        '<p class="subtitle">Reliability diagram: predicted confidence vs actual hit rate, binned into 10 ranges. Perfect calibration = diagonal line.</p>' +
        '<div class="model-selector"><label>Model: </label>' +
        '<select id="cal-model-select"></select></div>' +
        '<div class="cal-metrics" id="cal-metrics" style="margin:0.5rem 0;color:var(--text-muted)"></div>' +
        '<div id="cal-chart-host" class="chart-host" style="width:100%;height:360px;"></div>' +
        '</div>';

      var models = await fetchJSON('/api/models');
      var select = $('cal-model-select');
      select.innerHTML = models.map(function (m) {
        return '<option value="' + m.model_id + '">' +
          m.symbol + ' · ' + m.timeframe + ' · ' + m.horizon_minutes + 'm · ' + m.model_type +
          '</option>';
      }).join('');

      async function load() {
        var mid = select.value;
        var payload = await fetchJSON('/api/models/' + mid + '/calibration');
        $('cal-metrics').textContent =
          'Samples: ' + payload.sample_size +
          ' · Calibration error: ' + (payload.calibration_error * 100).toFixed(2) + '%';
        var points = payload.bins
          .filter(function (b) { return b.count > 0; })
          .map(function (b) {
            return {x: b.avg_confidence, y: b.hit_rate, r: Math.min(10, 3 + Math.sqrt(b.count))};
          });
        window.Charts.scatterChart($('cal-chart-host'), {
          points: points,
          referenceDiagonal: true,
          xLabel: 'Predicted confidence',
          yLabel: 'Actual hit rate',
        });
      }

      select.onchange = load;
      await load();
    },
    attribution: async function () {
      panel().innerHTML =
        '<div class="attribution-panel">' +
        '<p class="subtitle">Per-model signal count and simulated P&L (if every signal had been traded with unit position). Red/green by P&L sign.</p>' +
        '<div id="attr-signals-host" class="chart-host" style="width:100%;height:260px;margin-bottom:1rem;"></div>' +
        '<div id="attr-pnl-host" class="chart-host" style="width:100%;height:260px;"></div>' +
        '</div>';

      var models = await fetchJSON('/api/models');
      var labels = [];
      var signals = [];
      var pnls = [];

      for (var i = 0; i < models.length; i++) {
        var m = models[i];
        labels.push(m.symbol + ' · ' + m.horizon_minutes + 'm');
        var sim = await fetchJSON('/api/models/' + m.model_id + '/simulated_pnl');
        signals.push(sim.signal_count);
        pnls.push(sim.cumulative_pnl_pct * 100);  // as percent
      }

      window.Charts.barChart($('attr-signals-host'), {
        labels: labels, values: signals,
        yLabel: 'Signal count (over selected window)',
        colorByValue: false,
      });
      window.Charts.barChart($('attr-pnl-host'), {
        labels: labels, values: pnls,
        yLabel: 'Cumulative simulated P&L (%)',
        colorByValue: true,
      });
    },
    timeseries: async function () {
      panel().innerHTML =
        '<div class="timeseries-panel">' +
        '<p class="subtitle">Daily accuracy and signal count for a single model over the selected window.</p>' +
        '<div class="model-selector"><label>Model: </label>' +
        '<select id="ts-model-select"></select></div>' +
        '<div id="ts-acc-host" class="chart-host" style="width:100%;height:220px;margin-top:1rem;"></div>' +
        '<div id="ts-count-host" class="chart-host" style="width:100%;height:180px;margin-top:1rem;"></div>' +
        '</div>';

      var models = await fetchJSON('/api/models');
      var select = $('ts-model-select');
      select.innerHTML = models.map(function (m) {
        return '<option value="' + m.model_id + '">' +
          m.symbol + ' · ' + m.timeframe + ' · ' + m.horizon_minutes + 'm · ' + m.model_type +
          '</option>';
      }).join('');

      async function load() {
        var mid = select.value;
        var hist = await fetchJSON('/api/models/' + mid + '/history');
        window.Charts.lineChart($('ts-acc-host'), {
          series: [{
            label: 'Accuracy',
            data: hist.map(function (h) { return {x: h.date, y: h.accuracy}; }),
          }],
          xIsTime: true,
          yLabel: 'Daily accuracy',
        });
        window.Charts.barChart($('ts-count-host'), {
          labels: hist.map(function (h) { return h.date.slice(0, 10); }),
          values: hist.map(function (h) { return h.total; }),
          yLabel: 'Signal count (per day)',
          colorByValue: false,
        });
      }

      select.onchange = load;
      await load();
    },
    regime: async function () {
      panel().innerHTML =
        '<div class="regime-panel">' +
        '<p class="subtitle">Per-regime accuracy and P&L. Regime classified by SPY 50-bar SMA at prediction time (bull/bear/neutral/unknown).</p>' +
        '<div class="model-selector"><label>Model: </label>' +
        '<select id="regime-model-select"></select></div>' +
        '<div id="regime-acc-host" class="chart-host" style="width:100%;height:260px;margin-top:1rem;"></div>' +
        '<div id="regime-pnl-host" class="chart-host" style="width:100%;height:260px;margin-top:1rem;"></div>' +
        '<div id="regime-table" style="margin-top:1rem;"></div>' +
        '</div>';

      var models = await fetchJSON('/api/models');
      var select = $('regime-model-select');
      select.innerHTML = models.map(function (m) {
        return '<option value="' + m.model_id + '">' +
          m.symbol + ' · ' + m.timeframe + ' · ' + m.horizon_minutes + 'm · ' + m.model_type +
          '</option>';
      }).join('');

      async function load() {
        var mid = select.value;
        var rows = await fetchJSON('/api/models/' + mid + '/regime_breakdown');
        var labels = rows.map(function (r) { return r.regime; });
        var accs = rows.map(function (r) { return r.accuracy * 100; });
        var pnls = rows.map(function (r) { return r.cumulative_pnl_pct * 100; });

        window.Charts.barChart($('regime-acc-host'), {
          labels: labels, values: accs,
          yLabel: 'Accuracy (%)', colorByValue: false,
        });
        window.Charts.barChart($('regime-pnl-host'), {
          labels: labels, values: pnls,
          yLabel: 'Cumulative P&L (%)', colorByValue: true,
        });

        $('regime-table').innerHTML =
          '<table class="data-table"><thead><tr>' +
          '<th>Regime</th><th>Signals</th><th>Correct</th><th>Accuracy</th><th>Cumulative P&L</th>' +
          '</tr></thead><tbody>' +
          rows.map(function (r) {
            return '<tr><td>' + r.regime + '</td>' +
              '<td>' + r.count + '</td>' +
              '<td>' + r.correct + '</td>' +
              '<td>' + (r.accuracy * 100).toFixed(1) + '%</td>' +
              '<td>' + (r.cumulative_pnl_pct * 100).toFixed(2) + '%</td></tr>';
          }).join('') +
          '</tbody></table>';
      }

      select.onchange = load;
      await load();
    },
    featimp: async function () {
      panel().innerHTML =
        '<div class="featimp-panel">' +
        '<p class="subtitle">Top 20 features by importance for the selected model. Unavailable for sequence models (LSTM).</p>' +
        '<div class="model-selector"><label>Model: </label>' +
        '<select id="fi-model-select"></select></div>' +
        '<div id="fi-chart-host" class="chart-host" style="width:100%;height:500px;margin-top:1rem;"></div>' +
        '<div id="fi-note" style="margin-top:0.5rem;color:var(--text-muted);"></div>' +
        '</div>';

      var models = await fetchJSON('/api/models');
      var select = $('fi-model-select');
      select.innerHTML = models.map(function (m) {
        return '<option value="' + m.model_id + '">' +
          m.symbol + ' · ' + m.timeframe + ' · ' + m.horizon_minutes + 'm · ' + m.model_type +
          '</option>';
      }).join('');

      async function load() {
        var mid = select.value;
        var payload = await fetchJSON('/api/models/' + mid + '/feature_importance');
        if (!payload.available) {
          $('fi-chart-host').innerHTML = '';
          $('fi-note').textContent = payload.reason || 'Feature importance not available.';
          return;
        }
        $('fi-note').textContent = '';
        // Horizontal bar: feature names as labels, importances as values.
        var names = payload.features.map(function (f) { return f.name; }).reverse();
        var values = payload.features.map(function (f) { return f.importance; }).reverse();
        window.Charts.barChart($('fi-chart-host'), {
          labels: names, values: values,
          yLabel: 'Importance', colorByValue: false,
        });
      }

      select.onchange = load;
      await load();
    },
    meta_health: async function () {
      // Sliding-window health tile for the meta-shadow path. Surfaces the
      // skip rate (writer-side) + drop rate (publisher-side) over the last
      // 15 minutes of inferences. Tile turns red when the combined rate
      // exceeds 1% — a publisher-vs-writer regression that would otherwise
      // only be visible by row-count comparison between meta_signals_shadow
      // and meta_features.
      panel().innerHTML =
        '<div class="meta-health-panel">' +
        '<p class="subtitle">Sliding 15-minute window of meta-shadow ' +
        'pipeline counters. Skips = writer dropped an empty feature_vector. ' +
        'Drops = publisher filtered non-finite features. Tile turns red ' +
        'when (skips + drops) / inferences exceeds 1%.</p>' +
        '<div id="meta-health-tile" class="health-tile">' +
        '<div class="health-tile-status">Loading…</div>' +
        '</div>' +
        '</div>';

      var data = await fetchJSON('/api/models/meta_shadow_health');
      var tile = $('meta-health-tile');
      // redis_ok=false means we either had no redis_client wired or mget
      // raised. Either way the count fields are meaningless zeros — render
      // a distinct grey "Redis unavailable" state with em-dashes instead of
      // numbers so the operator can't mistake it for a healthy zero baseline.
      var redisOk = data.redis_ok !== false;  // default true for older payloads
      var ratePct = redisOk
        ? (data.skip_plus_drop_rate * 100).toFixed(2) + '%'
        : '—';
      var statusLabel = !redisOk
        ? 'REDIS UNAVAILABLE'
        : (data.is_red ? 'UNHEALTHY' : 'OK');
      // Inline styles keep the tile self-contained so adding it doesn't
      // require a CSS update; the .health-tile-red / .health-tile-unavailable
      // classes are also applied for any future stylesheet that wants to override.
      var bg = !redisOk ? '#555' : (data.is_red ? '#8b0000' : '#1f4d2b');
      var fg = '#fff';
      var extraClass = !redisOk
        ? ' health-tile-unavailable'
        : (data.is_red ? ' health-tile-red' : '');
      tile.className = 'health-tile' + extraClass;
      tile.style.background = bg;
      tile.style.color = fg;
      tile.style.padding = '1.25rem';
      tile.style.borderRadius = '6px';
      tile.style.maxWidth = '480px';
      var fmtCount = function (v) { return redisOk ? String(v) : '—'; };
      tile.innerHTML =
        '<div style="font-size:0.85rem;letter-spacing:0.08em;opacity:0.8;">' +
        'META SHADOW HEALTH (' + data.window_minutes + 'm)</div>' +
        '<div style="font-size:1.6rem;margin:0.25rem 0;font-weight:600;">' +
        statusLabel + ' · ' + ratePct + '</div>' +
        '<table style="width:100%;margin-top:0.75rem;font-size:0.95rem;">' +
        '<tr><td>Inferences</td><td style="text-align:right;">' +
        fmtCount(data.inference_count) + '</td></tr>' +
        '<tr><td>Publisher drops</td><td style="text-align:right;">' +
        fmtCount(data.drop_count) + '</td></tr>' +
        '<tr><td>Writer skips</td><td style="text-align:right;">' +
        fmtCount(data.skip_count) + '</td></tr>' +
        '<tr><td>Threshold</td><td style="text-align:right;">' +
        (data.threshold * 100).toFixed(1) + '%</td></tr>' +
        '</table>';
    },
  };

  async function renderDrillDown(modelId) {
    state.currentDrillId = modelId;
    panel().innerHTML = '';
    var models = await fetchJSON('/api/models');
    var m = models.filter(function (x) { return String(x.model_id) === String(modelId); })[0];
    var title = m
      ? ('Model #' + modelId + ' — ' + m.symbol + ' · ' + m.timeframe + ' · ' + m.horizon_minutes + 'm · ' + m.model_type)
      : ('Model #' + modelId);

    var dd = window.DrillDown.create({
      mountEl: panel(),
      title: title,
      backHref: '#models/leaderboard',
      breadcrumbs: [
        {label: 'Models', href: '#models'},
        {label: 'Leaderboard', href: '#models/leaderboard'},
        {label: m ? (m.symbol + ' · ' + m.horizon_minutes + 'm') : ('#' + modelId)},
      ],
    });
    var slot = dd.getContentSlot();
    slot.innerHTML =
      '<section class="drill-section"><h3>Accuracy over time</h3>' +
      '<div id="dd-ts" class="chart-host" style="width:100%;height:240px;"></div></section>' +
      '<section class="drill-section"><h3>Calibration</h3>' +
      '<div id="dd-cal-metrics" style="color:var(--text-muted);"></div>' +
      '<div id="dd-cal" class="chart-host" style="width:100%;height:300px;"></div></section>' +
      '<section class="drill-section"><h3>Simulated P&L</h3>' +
      '<div id="dd-pnl-summary" style="color:var(--text-muted);"></div>' +
      '<div id="dd-pnl" class="chart-host" style="width:100%;height:240px;"></div></section>' +
      '<section class="drill-section"><h3>Regime breakdown</h3>' +
      '<div id="dd-regime-chart" class="chart-host" style="width:100%;height:220px;"></div>' +
      '<div id="dd-regime-table"></div></section>' +
      '<section class="drill-section"><h3>Feature importance (top 20)</h3>' +
      '<div id="dd-fi" class="chart-host" style="width:100%;height:480px;"></div>' +
      '<div id="dd-fi-note" style="color:var(--text-muted);"></div></section>' +
      '<section class="drill-section"><h3>Recent signals (last 100)</h3>' +
      '<div id="dd-sig-host"></div></section>';

    // Fire all fetches in parallel.
    var [hist, cal, sim, regime, fi, sig] = await Promise.all([
      fetchJSON('/api/models/' + modelId + '/history'),
      fetchJSON('/api/models/' + modelId + '/calibration'),
      fetchJSON('/api/models/' + modelId + '/simulated_pnl'),
      fetchJSON('/api/models/' + modelId + '/regime_breakdown'),
      fetchJSON('/api/models/' + modelId + '/feature_importance'),
      fetchJSON('/api/models/' + modelId + '/signal_history?limit=100'),
    ]);

    // Accuracy over time.
    window.Charts.lineChart($('dd-ts'), {
      series: [{
        label: 'Accuracy',
        data: hist.map(function (h) { return {x: h.date, y: h.accuracy}; }),
      }],
      xIsTime: true, yLabel: 'Daily accuracy',
    });

    // Calibration.
    $('dd-cal-metrics').textContent =
      'Samples: ' + cal.sample_size + ' · Calibration error: ' + (cal.calibration_error * 100).toFixed(2) + '%';
    window.Charts.scatterChart($('dd-cal'), {
      points: cal.bins.filter(function (b) { return b.count > 0; })
        .map(function (b) {
          return {x: b.avg_confidence, y: b.hit_rate, r: Math.min(10, 3 + Math.sqrt(b.count))};
        }),
      referenceDiagonal: true,
      xLabel: 'Predicted confidence', yLabel: 'Actual hit rate',
    });

    // Simulated P&L.
    $('dd-pnl-summary').textContent =
      'Signals: ' + sim.signal_count +
      ' · Cumulative P&L: ' + (sim.cumulative_pnl_pct * 100).toFixed(2) + '%' +
      ' · Avg per signal: ' + (sim.avg_pnl_pct * 100).toFixed(3) + '%' +
      ' · Win rate: ' + (sim.win_rate * 100).toFixed(1) + '%';
    window.Charts.barChart($('dd-pnl'), {
      labels: sim.daily_series.map(function (d) { return d.date.slice(0, 10); }),
      values: sim.daily_series.map(function (d) { return d.pnl_pct * 100; }),
      yLabel: 'Daily P&L (%)', colorByValue: true,
    });

    // Regime.
    window.Charts.barChart($('dd-regime-chart'), {
      labels: regime.map(function (r) { return r.regime; }),
      values: regime.map(function (r) { return r.accuracy * 100; }),
      yLabel: 'Accuracy (%)', colorByValue: false,
    });
    $('dd-regime-table').innerHTML =
      '<table class="data-table"><thead><tr>' +
      '<th>Regime</th><th>Signals</th><th>Accuracy</th><th>P&L</th>' +
      '</tr></thead><tbody>' +
      regime.map(function (r) {
        return '<tr><td>' + r.regime + '</td>' +
          '<td>' + r.count + '</td>' +
          '<td>' + (r.accuracy * 100).toFixed(1) + '%</td>' +
          '<td>' + (r.cumulative_pnl_pct * 100).toFixed(2) + '%</td></tr>';
      }).join('') +
      '</tbody></table>';

    // Feature importance.
    if (fi.available) {
      $('dd-fi-note').textContent = '';
      window.Charts.barChart($('dd-fi'), {
        labels: fi.features.map(function (f) { return f.name; }).reverse(),
        values: fi.features.map(function (f) { return f.importance; }).reverse(),
        yLabel: 'Importance', colorByValue: false,
      });
    } else {
      $('dd-fi').innerHTML = '';
      $('dd-fi-note').textContent = fi.reason || 'Feature importance not available.';
    }

    // Signal history.
    window.Leaderboard.create({
      mountEl: $('dd-sig-host'),
      columns: [
        {key: 'timestamp', label: 'Time', sortable: true,
         render: function (v) { return window.fmtTime ? window.fmtTime(v) : v; }},
        {key: 'symbol', label: 'Symbol', sortable: true},
        {key: 'predicted_direction', label: 'Dir', sortable: true,
         render: function (v) { return v === 1 ? '↑' : (v === -1 ? '↓' : '→'); }},
        {key: 'confidence', label: 'Conf', sortable: true,
         render: function (v) { return (v * 100).toFixed(1) + '%'; }},
        {key: 'entry_price', label: 'Entry', sortable: false,
         render: function (v) { return v != null ? v.toFixed(2) : '—'; }},
        {key: 'exit_price', label: 'Exit', sortable: false,
         render: function (v) { return v != null ? v.toFixed(2) : '—'; }},
        {key: 'is_correct', label: 'Hit?', sortable: false,
         render: function (v) {
           if (v === null || v === undefined) return '—';
           return v ? '<span class="positive">✓</span>' : '<span class="negative">✗</span>';
         }},
        {key: 'pnl_pct', label: 'P&L', sortable: true,
         render: function (v) {
           if (v === null || v === undefined) return '—';
           var cls = v >= 0 ? 'positive' : 'negative';
           return '<span class="' + cls + '">' + (v * 100).toFixed(3) + '%</span>';
         }},
      ],
      rows: sig,
    });
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
