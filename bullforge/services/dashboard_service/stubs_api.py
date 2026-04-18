from __future__ import annotations

import logging

from aiohttp import web

logger = logging.getLogger(__name__)

STUB_ENDPOINTS: dict[str, dict] = {
    # --- Plan 2: Models domain ---------------------------------------
    "/api/models/{id}/history":             {"enabled_after_plan": "plan_2"},
    "/api/models/{id}/calibration":         {"enabled_after_plan": "plan_2"},
    "/api/models/{id}/simulated_pnl":       {"enabled_after_plan": "plan_2"},
    "/api/models/{id}/feature_importance":  {"enabled_after_plan": "plan_2"},
    "/api/models/{id}/regime_breakdown":    {"enabled_after_plan": "plan_2"},
    "/api/models/{id}/signal_history":      {"enabled_after_plan": "plan_2"},

    # --- Plan 3: Trading domain --------------------------------------
    "/api/trading/equity":                  {"enabled_after_plan": "plan_3"},
    "/api/trading/distributions":           {"enabled_after_plan": "plan_3"},
    "/api/trading/heatmap":                 {"enabled_after_plan": "plan_3"},
    "/api/trading/streaks":                 {"enabled_after_plan": "plan_3"},
    "/api/trading/exposure":                {"enabled_after_plan": "plan_3"},
    "/api/trading/benchmark":               {"enabled_after_plan": "plan_3"},
    "/api/trading/symbols/{sym}":           {"enabled_after_plan": "plan_3"},

    # --- Plan 4: Backtests domain ------------------------------------
    "/api/backtests/{run_id}/equity":       {"enabled_after_plan": "plan_4"},
    "/api/backtests/{run_id}/folds":        {"enabled_after_plan": "plan_4"},
    "/api/backtests/{run_id}/symbols":      {"enabled_after_plan": "plan_4"},
    "/api/backtests/{run_id}/distributions":{"enabled_after_plan": "plan_4"},
    "/api/backtests/{run_id}/live-overlay": {"enabled_after_plan": "plan_4"},
    "/api/backtests/{run_id}/funnel":       {"enabled_after_plan": "plan_4"},
    "/api/backtests/compare":               {"enabled_after_plan": "plan_4"},
    "/api/backtests/sensitivity":           {"enabled_after_plan": "plan_4"},
}


def _make_handler(enabled_after_plan: str):
    async def handler(request: web.Request) -> web.Response:
        return web.json_response(
            {
                "error": "not_implemented",
                "enabled_after_plan": enabled_after_plan,
            },
            status=501,
        )
    return handler


def register_stub_routes(app: web.Application) -> None:
    for path, meta in STUB_ENDPOINTS.items():
        app.router.add_get(path, _make_handler(meta["enabled_after_plan"]))
