(function () {
  const root = () => document.getElementById('backtests-root');
  let pollTimer = null;

  async function fetchJSON(url, opts) {
    const res = await fetch(url, opts);
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      throw Object.assign(new Error(`HTTP ${res.status}`),
                          { status: res.status, body });
    }
    return res.json();
  }

  function route() {
    const hash = window.location.hash || '#backtests';
    const parts = hash.replace(/^#/, '').split('/');
    if (parts[0] === 'backtests' && parts[1]) {
      renderDetail(parts[1]);
    } else {
      renderOverview();
    }
  }

  function renderOverview() {
    clearInterval(pollTimer);
    root().innerHTML = '<div class="bt-overview"><p>Overview loading…</p></div>';
    // Populated in Task 9.
  }

  async function renderDetail(id) {
    clearInterval(pollTimer);
    root().innerHTML =
      `<div class="bt-detail"><a href="#backtests">← Back</a>` +
      `<p>Detail loading for ${id}…</p></div>`;
    // Populated in Tasks 10-11.
  }

  window.BacktestsTab = {
    activate() { route(); },
    onHashChange() { route(); },
  };
})();
