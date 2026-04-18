// components/charts.js
// Thin wrappers over Chart.js for consistent dark-theme styling and
// automatic destroy-on-rerender. heatmapChart is a colored-cell grid
// implemented directly (not Chart.js) because Chart.js heatmap plugin
// adds a dependency we don't want.
//
// Public API:
//   Charts.lineChart(el, {series, xIsTime, yLabel, drawdownShading})
//   Charts.barChart(el, {labels, values, colorByValue, yLabel})
//   Charts.histogramChart(el, {values, bins, xLabel})
//   Charts.scatterChart(el, {points, referenceDiagonal, xLabel, yLabel})
//   Charts.sparkline(el, {values})
//   Charts.heatmapChart(el, {xLabels, yLabels, cells, colorScale})
(function (global) {
  var THEME = {
    text: '#e0e0e0',
    muted: '#888',
    grid: 'rgba(224, 224, 224, 0.08)',
    accent: '#00b4d8',
    green: '#2ecc71',
    red: '#e74c3c',
    yellow: '#f39c12',
    bgCard: '#16213e',
  };

  // Per-host Chart.js instance cache so re-renders destroy prior chart.
  var instances = new WeakMap();

  function _prepHost(host) {
    var prior = instances.get(host);
    if (prior && typeof prior.destroy === 'function') {
      prior.destroy();
    }
    host.innerHTML = '';
    var canvas = document.createElement('canvas');
    canvas.style.width = '100%';
    canvas.style.height = '100%';
    host.appendChild(canvas);
    return canvas;
  }

  function _baseOptions(xIsTime, yLabel) {
    return {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: {labels: {color: THEME.text}},
        tooltip: {backgroundColor: THEME.bgCard, titleColor: THEME.text, bodyColor: THEME.text},
      },
      scales: {
        x: xIsTime
          ? {type: 'time', ticks: {color: THEME.muted}, grid: {color: THEME.grid}}
          : {ticks: {color: THEME.muted}, grid: {color: THEME.grid}},
        y: {ticks: {color: THEME.muted}, grid: {color: THEME.grid},
            title: yLabel ? {display: true, text: yLabel, color: THEME.muted} : undefined},
      },
    };
  }

  function lineChart(host, opts) {
    var canvas = _prepHost(host);
    var chart = new Chart(canvas, {
      type: 'line',
      data: {
        datasets: (opts.series || []).map(function (s, i) {
          return {
            label: s.label,
            data: s.data,
            borderColor: s.color || (i === 0 ? THEME.accent : [THEME.green, THEME.yellow, THEME.red][(i - 1) % 3]),
            backgroundColor: 'transparent',
            borderWidth: 2,
            pointRadius: 0,
            tension: 0.1,
          };
        }),
      },
      options: _baseOptions(!!opts.xIsTime, opts.yLabel),
    });
    instances.set(host, chart);
    return chart;
  }

  function barChart(host, opts) {
    var canvas = _prepHost(host);
    var colors = (opts.values || []).map(function (v) {
      if (!opts.colorByValue) return THEME.accent;
      return v >= 0 ? THEME.green : THEME.red;
    });
    var chart = new Chart(canvas, {
      type: 'bar',
      data: {
        labels: opts.labels || [],
        datasets: [{
          label: opts.yLabel || '', data: opts.values || [],
          backgroundColor: colors,
        }],
      },
      options: _baseOptions(false, opts.yLabel),
    });
    instances.set(host, chart);
    return chart;
  }

  function histogramChart(host, opts) {
    var values = opts.values || [];
    var bins = opts.bins || 20;
    if (values.length === 0) {
      return barChart(host, {labels: [], values: [], yLabel: 'Count'});
    }
    var min = Math.min.apply(null, values);
    var max = Math.max.apply(null, values);
    if (min === max) { min -= 1; max += 1; }
    var width = (max - min) / bins;
    var counts = new Array(bins).fill(0);
    var labels = [];
    for (var i = 0; i < bins; i++) {
      labels.push((min + i * width).toFixed(2));
    }
    values.forEach(function (v) {
      var idx = Math.min(bins - 1, Math.floor((v - min) / width));
      counts[idx]++;
    });
    return barChart(host, {
      labels: labels, values: counts, yLabel: 'Count', colorByValue: false,
    });
  }

  function scatterChart(host, opts) {
    var canvas = _prepHost(host);
    var datasets = [{
      label: 'Points',
      data: (opts.points || []).map(function (p) { return {x: p.x, y: p.y, r: p.r || 3}; }),
      backgroundColor: THEME.accent,
    }];
    if (opts.referenceDiagonal) {
      datasets.push({
        label: 'Perfect',
        type: 'line',
        data: [{x: 0, y: 0}, {x: 1, y: 1}],
        borderColor: THEME.muted,
        borderDash: [4, 4],
        pointRadius: 0,
        borderWidth: 1,
      });
    }
    var chart = new Chart(canvas, {
      type: 'scatter',
      data: {datasets: datasets},
      options: _baseOptions(false, opts.yLabel),
    });
    instances.set(host, chart);
    return chart;
  }

  function sparkline(host, opts) {
    host.innerHTML = '';
    var values = opts.values || [];
    if (values.length === 0) return;
    var w = host.clientWidth || 100;
    var h = host.clientHeight || 24;
    var min = Math.min.apply(null, values);
    var max = Math.max.apply(null, values);
    if (min === max) { min -= 1; max += 1; }
    var pts = values.map(function (v, i) {
      var x = (i / (values.length - 1)) * w;
      var y = h - ((v - min) / (max - min)) * h;
      return x.toFixed(1) + ',' + y.toFixed(1);
    });
    var svg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ' + w + ' ' + h +
      '" width="' + w + '" height="' + h + '" preserveAspectRatio="none">' +
      '<polyline fill="none" stroke="' + THEME.accent + '" stroke-width="1.5" points="' +
      pts.join(' ') + '"/></svg>';
    host.innerHTML = svg;
  }

  function _cellColor(v, scale) {
    // Diverging red-white-green around zero.
    if (v === 0 || v === null || isNaN(v)) return THEME.bgCard;
    var absV = Math.abs(v);
    var intensity = Math.min(1, absV / (scale || 0.01));
    if (v > 0) {
      return 'rgba(46, 204, 113, ' + intensity.toFixed(2) + ')';
    }
    return 'rgba(231, 76, 60, ' + intensity.toFixed(2) + ')';
  }

  function heatmapChart(host, opts) {
    host.innerHTML = '';
    var xLabels = opts.xLabels || [];
    var yLabels = opts.yLabels || [];
    var cells = opts.cells || [];

    // Compute scale if not provided.
    var scale = opts.colorScale;
    if (!scale) {
      var maxAbs = 0;
      cells.forEach(function (row) {
        row.forEach(function (v) { if (typeof v === 'number') maxAbs = Math.max(maxAbs, Math.abs(v)); });
      });
      scale = maxAbs || 0.01;
    }

    var grid = document.createElement('div');
    grid.className = 'heatmap-grid';
    grid.style.display = 'grid';
    grid.style.gridTemplateColumns = '80px repeat(' + xLabels.length + ', 1fr)';
    grid.style.gap = '1px';
    grid.style.fontSize = '11px';
    grid.style.color = THEME.text;

    // Header row.
    var corner = document.createElement('div');
    corner.textContent = '';
    grid.appendChild(corner);
    xLabels.forEach(function (lbl) {
      var cell = document.createElement('div');
      cell.textContent = lbl;
      cell.style.padding = '2px 4px';
      cell.style.textAlign = 'center';
      cell.style.color = THEME.muted;
      grid.appendChild(cell);
    });

    // Data rows.
    yLabels.forEach(function (yLbl, i) {
      var rowHeader = document.createElement('div');
      rowHeader.textContent = yLbl;
      rowHeader.style.padding = '2px 4px';
      rowHeader.style.color = THEME.muted;
      grid.appendChild(rowHeader);
      var row = cells[i] || [];
      for (var j = 0; j < xLabels.length; j++) {
        var v = row[j];
        var cell = document.createElement('div');
        cell.style.background = _cellColor(v, scale);
        cell.style.padding = '6px 2px';
        cell.style.textAlign = 'center';
        cell.style.minHeight = '18px';
        cell.title = yLbl + ' × ' + xLabels[j] + ': ' + (typeof v === 'number' ? v.toFixed(4) : '—');
        cell.textContent = typeof v === 'number' ? v.toFixed(3) : '';
        grid.appendChild(cell);
      }
    });
    host.appendChild(grid);
    return grid;
  }

  global.Charts = {
    lineChart: lineChart,
    barChart: barChart,
    histogramChart: histogramChart,
    scatterChart: scatterChart,
    sparkline: sparkline,
    heatmapChart: heatmapChart,
    THEME: THEME,
  };
})(window);
