// components/router.js
// URL hash router. Parses #tab/subtab/entity-kind/entity-id?query=... into
// a canonical {tab, subtab, entity_kind, entity_id, query} object.
//
// Known entity kinds: "models" (when second segment is numeric),
// "symbols", "backtests" (numeric run id as third segment under backtests/).
//
// Subscribers are notified on hashchange and on explicit navigate().
//
// Public API: window.Router = {parse, navigate, subscribe, current}
(function (global) {
  var subscribers = [];

  var NUMERIC = /^\d+$/;

  function parseQuery(qs) {
    var out = {};
    if (!qs) return out;
    qs.split('&').forEach(function (pair) {
      if (!pair) return;
      var eq = pair.indexOf('=');
      var k = eq >= 0 ? pair.slice(0, eq) : pair;
      var v = eq >= 0 ? pair.slice(eq + 1) : '';
      out[decodeURIComponent(k)] = decodeURIComponent(v);
    });
    return out;
  }

  function parse(hash) {
    if (!hash) hash = '';
    if (hash.charAt(0) === '#') hash = hash.slice(1);

    // Split off query string if present.
    var qIdx = hash.indexOf('?');
    var query = {};
    if (qIdx >= 0) {
      query = parseQuery(hash.slice(qIdx + 1));
      hash = hash.slice(0, qIdx);
    }

    var route = {tab: 'overview', subtab: null, entity_kind: null, entity_id: null, query: query};
    if (!hash) return route;

    var parts = hash.split('/').filter(function (p) { return p.length > 0; });
    if (parts.length === 0) return route;

    route.tab = parts[0];

    if (parts.length === 1) return route;

    // Classify second segment.
    //   #<tab>/<numeric>           → entity (kind = tab) + id = numeric
    //   #<tab>/<text>              → subtab
    //   #<tab>/<kind>/<id>         → entity kind + id
    //   #<tab>/<kind>/<id>/<sub>   → entity kind + id + subtab
    if (parts.length === 2) {
      if (NUMERIC.test(parts[1])) {
        route.entity_kind = parts[0];
        route.entity_id = parts[1];
      } else {
        route.subtab = parts[1];
      }
      return route;
    }

    // 3+ segments.
    route.entity_kind = parts[1];
    route.entity_id = parts[2];
    if (parts.length >= 4) {
      route.subtab = parts[3];
    }
    return route;
  }

  function current() {
    return parse(global.location.hash);
  }

  function notify() {
    var route = current();
    subscribers.slice().forEach(function (fn) {
      try { fn(route); } catch (e) { console.error('Router subscriber error:', e); }
    });
  }

  function navigate(hash) {
    if (hash && hash.charAt(0) !== '#') hash = '#' + hash;
    if (global.location.hash === hash) {
      notify();  // force re-fire if same hash
      return;
    }
    global.location.hash = hash || '';
    // hashchange event will fire notify() too; avoid double firing by
    // only notifying here if we forced same-hash above.
  }

  function subscribe(fn) {
    subscribers.push(fn);
    return function unsubscribe() {
      var i = subscribers.indexOf(fn);
      if (i >= 0) subscribers.splice(i, 1);
    };
  }

  global.addEventListener('hashchange', notify);

  global.Router = {
    parse: parse,
    navigate: navigate,
    subscribe: subscribe,
    current: current,
  };
})(window);
