// components/leaderboard.js
// Sortable, clickable leaderboard table primitive.
//
// Public API:
//   var lb = Leaderboard.create({
//     mountEl: host,
//     columns: [{key, label, sortable, title, render(value, row)}],
//     rows: [...],
//     getRowHref: function (row) { return '#models/' + row.id; },
//     initialSort: {key: 'accuracy', order: 'desc'},
//   });
//   lb.update(newRows);
// `title` is optional; when set, it becomes the `title=` attribute on the
// `<th>` cell (native browser tooltip on hover) for column-header
// disambiguation.
(function (global) {
  function _compare(a, b, key, order) {
    var av = a[key], bv = b[key];
    if (av === bv) return 0;
    if (av === null || av === undefined) return 1;
    if (bv === null || bv === undefined) return -1;
    if (typeof av === 'number' && typeof bv === 'number') {
      return order === 'asc' ? av - bv : bv - av;
    }
    var as = String(av).toLowerCase(), bs = String(bv).toLowerCase();
    if (as < bs) return order === 'asc' ? -1 : 1;
    if (as > bs) return order === 'asc' ? 1 : -1;
    return 0;
  }

  function _escape(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
    });
  }

  function create(opts) {
    if (!opts || !opts.mountEl) throw new Error('Leaderboard.create requires mountEl');
    var mountEl = opts.mountEl;
    var columns = opts.columns || [];
    var rows = (opts.rows || []).slice();
    var getRowHref = opts.getRowHref || null;
    var sort = opts.initialSort || null;

    function render() {
      var sortedRows = rows.slice();
      if (sort) {
        sortedRows.sort(function (a, b) { return _compare(a, b, sort.key, sort.order); });
      }

      var thead = '<thead><tr>' + columns.map(function (c) {
        var arrow = '';
        if (sort && sort.key === c.key) arrow = sort.order === 'desc' ? ' ▼' : ' ▲';
        var cls = c.sortable ? ' class="sortable"' : '';
        var attr = c.sortable ? ' data-sort-key="' + _escape(c.key) + '"' : '';
        var titleAttr = c.title ? ' title="' + _escape(c.title) + '"' : '';
        return '<th' + cls + attr + titleAttr + '>' + _escape(c.label) + arrow + '</th>';
      }).join('') + '</tr></thead>';

      var tbody = '<tbody>' + sortedRows.map(function (row) {
        var href = getRowHref ? getRowHref(row) : null;
        return '<tr>' + columns.map(function (c, ci) {
          var v = row[c.key];
          var rendered = c.render ? c.render(v, row) : (v == null ? '' : _escape(v));
          if (href && ci === 0) {
            return '<td><a href="' + _escape(href) + '">' + rendered + '</a></td>';
          }
          return '<td>' + rendered + '</td>';
        }).join('') + '</tr>';
      }).join('') + '</tbody>';

      mountEl.innerHTML = '<table class="data-table leaderboard-table">' + thead + tbody + '</table>';

      // Wire up sort clicks.
      mountEl.querySelectorAll('thead th.sortable').forEach(function (th) {
        th.style.cursor = 'pointer';
        th.addEventListener('click', function () {
          var key = th.getAttribute('data-sort-key');
          if (sort && sort.key === key) {
            sort = {key: key, order: sort.order === 'desc' ? 'asc' : 'desc'};
          } else {
            sort = {key: key, order: 'desc'};
          }
          render();
        });
      });
    }

    render();

    return {
      update: function (newRows) {
        rows = newRows.slice();
        render();
      },
      setSort: function (newSort) { sort = newSort; render(); },
      getSort: function () { return sort; },
    };
  }

  global.Leaderboard = {create: create};
})(window);
