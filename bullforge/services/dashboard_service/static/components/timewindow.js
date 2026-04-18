// components/timewindow.js
// Shared time-window picker. Emits {from: ISO|null, to: ISO|null} on change.
// Persists the chosen preset/custom range in sessionStorage under a caller-
// supplied key so switching tabs doesn't reset selections.
//
// Usage:
//   var tw = TimeWindow.create({
//     mountEl: document.getElementById('slot'),
//     storageKey: 'models.timewindow',
//     onChange: function (range) { ... },
//   });
(function (global) {
  var PRESETS = [
    {value: '1d',   label: 'Last 1 day',    days: 1},
    {value: '7d',   label: 'Last 7 days',   days: 7},
    {value: '30d',  label: 'Last 30 days',  days: 30},
    {value: '90d',  label: 'Last 90 days',  days: 90},
    {value: '180d', label: 'Last 180 days', days: 180},
    {value: '1y',   label: 'Last 1 year',   days: 365},
    {value: 'all',  label: 'All time',      days: null},
    {value: 'custom', label: 'Custom…',     days: null},
  ];

  function nowIso() { return new Date().toISOString(); }

  function daysAgoIso(days) {
    var d = new Date(Date.now() - days * 86400 * 1000);
    return d.toISOString();
  }

  function rangeForPreset(preset, custom) {
    if (preset === 'all') return {from: null, to: null};
    if (preset === 'custom') return {from: custom.from, to: custom.to};
    var p = PRESETS.filter(function (x) { return x.value === preset; })[0];
    if (!p || p.days === null) return {from: null, to: null};
    return {from: daysAgoIso(p.days), to: nowIso()};
  }

  function create(opts) {
    if (!opts || !opts.mountEl) throw new Error('TimeWindow.create requires mountEl');
    var storageKey = opts.storageKey || null;
    var onChange = opts.onChange || function () {};

    // Restore persisted state.
    var saved = null;
    if (storageKey) {
      try { saved = JSON.parse(sessionStorage.getItem(storageKey) || 'null'); }
      catch (e) { saved = null; }
    }
    var currentPreset = (saved && saved.preset) || '30d';
    var customFrom = (saved && saved.customFrom) || '';
    var customTo = (saved && saved.customTo) || '';

    // Build DOM.
    opts.mountEl.innerHTML = '';
    var select = document.createElement('select');
    select.className = 'tw-select';
    PRESETS.forEach(function (p) {
      var opt = document.createElement('option');
      opt.value = p.value;
      opt.textContent = p.label;
      select.appendChild(opt);
    });
    select.value = currentPreset;
    opts.mountEl.appendChild(select);

    var customFromInput = document.createElement('input');
    customFromInput.type = 'datetime-local';
    customFromInput.className = 'tw-custom tw-from';
    customFromInput.style.marginLeft = '0.5em';
    var customToInput = document.createElement('input');
    customToInput.type = 'datetime-local';
    customToInput.className = 'tw-custom tw-to';
    customToInput.style.marginLeft = '0.5em';
    opts.mountEl.appendChild(customFromInput);
    opts.mountEl.appendChild(customToInput);

    function updateCustomVisibility() {
      var visible = currentPreset === 'custom';
      customFromInput.style.display = visible ? '' : 'none';
      customToInput.style.display = visible ? '' : 'none';
    }
    updateCustomVisibility();

    function persist() {
      if (!storageKey) return;
      sessionStorage.setItem(storageKey, JSON.stringify({
        preset: currentPreset,
        customFrom: customFrom,
        customTo: customTo,
      }));
    }

    function emit() {
      var range = rangeForPreset(currentPreset, {from: customFrom, to: customTo});
      persist();
      onChange(range);
    }

    function setPreset(preset) {
      currentPreset = preset;
      select.value = preset;
      updateCustomVisibility();
      emit();
    }

    function setCustom(fromIso, toIso) {
      currentPreset = 'custom';
      select.value = 'custom';
      customFrom = fromIso;
      customTo = toIso;
      customFromInput.value = fromIso ? fromIso.slice(0, 16) : '';
      customToInput.value = toIso ? toIso.slice(0, 16) : '';
      updateCustomVisibility();
      emit();
    }

    select.addEventListener('change', function () {
      setPreset(select.value);
    });
    customFromInput.addEventListener('change', function () {
      var v = customFromInput.value;
      customFrom = v ? new Date(v).toISOString() : '';
      if (currentPreset === 'custom') emit();
    });
    customToInput.addEventListener('change', function () {
      var v = customToInput.value;
      customTo = v ? new Date(v).toISOString() : '';
      if (currentPreset === 'custom') emit();
    });

    // Initial emit so the consumer gets the restored state.
    emit();

    return {
      setPreset: setPreset,
      setCustom: setCustom,
      getCurrentPreset: function () { return currentPreset; },
      getCurrentRange: function () {
        return rangeForPreset(currentPreset, {from: customFrom, to: customTo});
      },
    };
  }

  global.TimeWindow = {create: create, PRESETS: PRESETS};
})(window);
