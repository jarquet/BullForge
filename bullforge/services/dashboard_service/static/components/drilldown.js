// components/drilldown.js
// Drill-down page frame: back button, title, breadcrumbs, content slot.
//
// Public API:
//   var dd = DrillDown.create({
//     mountEl: host,
//     title: 'Model #42',
//     backHref: '#models/leaderboard',
//     breadcrumbs: [{label, href?}, ...],
//   });
//   dd.setTitle(newTitle);
//   dd.getContentSlot();  // returns <div> for caller to populate
(function (global) {
  function _escape(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
    });
  }

  function create(opts) {
    if (!opts || !opts.mountEl) throw new Error('DrillDown.create requires mountEl');
    var mountEl = opts.mountEl;
    var title = opts.title || '';
    var backHref = opts.backHref || '#';
    var breadcrumbs = opts.breadcrumbs || [];

    mountEl.innerHTML = '';

    var header = document.createElement('div');
    header.className = 'dd-header';
    header.style.display = 'flex';
    header.style.alignItems = 'center';
    header.style.gap = '1em';
    header.style.marginBottom = '1em';

    var back = document.createElement('a');
    back.className = 'dd-back';
    back.href = backHref;
    back.textContent = '← Back';
    back.style.color = '#00b4d8';
    back.style.textDecoration = 'none';
    header.appendChild(back);

    var titleEl = document.createElement('h2');
    titleEl.className = 'dd-title';
    titleEl.style.margin = '0';
    titleEl.textContent = title;
    header.appendChild(titleEl);

    mountEl.appendChild(header);

    var breadcrumbsEl = document.createElement('div');
    breadcrumbsEl.className = 'dd-breadcrumbs';
    breadcrumbsEl.style.color = '#888';
    breadcrumbsEl.style.fontSize = '0.85rem';
    breadcrumbsEl.style.marginBottom = '1em';
    breadcrumbs.forEach(function (b, i) {
      if (i > 0) {
        var sep = document.createElement('span');
        sep.textContent = ' › ';
        breadcrumbsEl.appendChild(sep);
      }
      if (b.href) {
        var a = document.createElement('a');
        a.href = b.href;
        a.textContent = b.label;
        a.style.color = '#888';
        breadcrumbsEl.appendChild(a);
      } else {
        var span = document.createElement('span');
        span.textContent = b.label;
        span.style.color = '#e0e0e0';
        breadcrumbsEl.appendChild(span);
      }
    });
    mountEl.appendChild(breadcrumbsEl);

    var content = document.createElement('div');
    content.className = 'dd-content';
    mountEl.appendChild(content);

    return {
      setTitle: function (newTitle) {
        title = newTitle;
        titleEl.textContent = newTitle;
      },
      getContentSlot: function () { return content; },
    };
  }

  global.DrillDown = {create: create};
})(window);
