// components/refresh.js
// Shared refresh control: interval selector + pause/resume button.
// Fires onRefresh() on a user-configurable interval, persisted in
// sessionStorage when a storageKey is provided.
//
// Usage:
//   var ctrl = Refresh.create({
//     mountEl: document.getElementById('slot'),
//     storageKey: 'trading.refresh',
//     onRefresh: function () { loadData(); },
//   });
(function (global) {
  var INTERVALS = [
    {value: 10, label: '10s'},
    {value: 30, label: '30s'},
    {value: 60, label: '60s'},
    {value: 0,  label: 'Manual'},
  ];

  function create(opts) {
    if (!opts || !opts.mountEl) throw new Error('Refresh.create requires mountEl');
    var storageKey = opts.storageKey || null;
    var onRefresh = opts.onRefresh || function () {};
    var tickMs = opts.tickMs || 1000;  // granularity for internal timer

    // Restore persisted state.
    var saved = null;
    if (storageKey) {
      try { saved = JSON.parse(sessionStorage.getItem(storageKey) || 'null'); }
      catch (e) { saved = null; }
    }
    var intervalSec = saved && typeof saved.intervalSec === 'number'
      ? saved.intervalSec
      : (typeof opts.initialIntervalSec === 'number' ? opts.initialIntervalSec : 30);
    var paused = saved && saved.paused === true;

    opts.mountEl.innerHTML = '';
    var select = document.createElement('select');
    select.className = 'refresh-interval';
    INTERVALS.forEach(function (i) {
      var opt = document.createElement('option');
      opt.value = String(i.value);
      opt.textContent = i.label;
      select.appendChild(opt);
    });
    select.value = String(intervalSec);
    opts.mountEl.appendChild(select);

    var btn = document.createElement('button');
    btn.className = 'refresh-btn';
    btn.style.marginLeft = '0.5em';
    function updateBtn() { btn.textContent = paused ? '▶ Resume' : '⏸ Pause'; }
    updateBtn();
    opts.mountEl.appendChild(btn);

    var manualBtn = document.createElement('button');
    manualBtn.className = 'refresh-manual';
    manualBtn.style.marginLeft = '0.5em';
    manualBtn.textContent = '↻ Refresh now';
    opts.mountEl.appendChild(manualBtn);

    var lastFireTs = Date.now();

    function persist() {
      if (!storageKey) return;
      sessionStorage.setItem(storageKey, JSON.stringify({
        intervalSec: intervalSec, paused: paused,
      }));
    }

    function tick() {
      if (paused || intervalSec === 0) return;
      if (Date.now() - lastFireTs >= intervalSec * 1000) {
        lastFireTs = Date.now();
        try { onRefresh(); } catch (e) { console.error('Refresh.onRefresh error:', e); }
      }
    }

    var timerId = setInterval(tick, tickMs);

    select.addEventListener('change', function () {
      intervalSec = parseInt(select.value, 10);
      lastFireTs = Date.now();
      persist();
    });

    btn.addEventListener('click', function () {
      paused = !paused;
      updateBtn();
      persist();
      if (!paused) { lastFireTs = Date.now(); }
    });

    manualBtn.addEventListener('click', function () {
      lastFireTs = Date.now();
      try { onRefresh(); } catch (e) { console.error('Refresh.onRefresh error:', e); }
    });

    function destroy() {
      clearInterval(timerId);
      opts.mountEl.innerHTML = '';
    }

    function pause() { if (!paused) { paused = true; updateBtn(); persist(); } }
    function resume() { if (paused) { paused = false; lastFireTs = Date.now(); updateBtn(); persist(); } }

    return {
      destroy: destroy,
      pause: pause,
      resume: resume,
      isPaused: function () { return paused; },
      getIntervalSec: function () { return intervalSec; },
    };
  }

  global.Refresh = {create: create, INTERVALS: INTERVALS};
})(window);
