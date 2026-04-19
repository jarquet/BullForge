"""Drift guard: the dashboard Dockerfile cherry-picks individual files from
services/backtest_service/. If jobs_db or results_db grow a new
`from services.backtest_service.X import ...` dependency and the Dockerfile
isn't updated, the dashboard container will ImportError at startup.

This test parses the dashboard's static import closure rooted in
services.backtest_service and asserts the Dockerfile COPYs each referenced
submodule.
"""
from __future__ import annotations

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_DASHBOARD_DIR = _ROOT / "services" / "dashboard_service"
_BACKTEST_DIR = _ROOT / "services" / "backtest_service"
_DOCKERFILE = _DASHBOARD_DIR / "Dockerfile"

_IMPORT_RE = re.compile(
    r"^\s*from\s+services\.backtest_service\.(\w+)\s+import",
    re.MULTILINE,
)
_COPY_RE = re.compile(
    r"^\s*COPY\s+services/backtest_service/(\S+)",
    re.MULTILINE,
)


def _imported_backtest_submodules() -> set[str]:
    """Every backtest_service submodule transitively imported by the dashboard."""
    seen: set[str] = set()
    queue: list[Path] = sorted(_DASHBOARD_DIR.glob("*.py"))
    while queue:
        src = queue.pop().read_text(encoding="utf-8")
        for match in _IMPORT_RE.finditer(src):
            mod = match.group(1)
            if mod in seen:
                continue
            seen.add(mod)
            sub = _BACKTEST_DIR / f"{mod}.py"
            if sub.exists():
                queue.append(sub)
    return seen


def _copied_backtest_submodules() -> set[str]:
    dockerfile = _DOCKERFILE.read_text(encoding="utf-8")
    copied: set[str] = set()
    for match in _COPY_RE.finditer(dockerfile):
        path = match.group(1)
        if path.endswith(".py"):
            copied.add(Path(path).stem)
    return copied


def test_dashboard_dockerfile_copies_all_imported_backtest_submodules():
    imported = _imported_backtest_submodules()
    copied = _copied_backtest_submodules()
    missing = imported - copied
    assert not missing, (
        "Dashboard imports these backtest_service submodules that the "
        "Dockerfile does not COPY: "
        f"{sorted(missing)}. Add a COPY line for each."
    )
