from __future__ import annotations

import json
import logging
import re
import secrets
import string
from datetime import datetime

from aiohttp import web

from shared.runtime_config import CONFIG_SCHEMA

logger = logging.getLogger(__name__)

VALID_MODES = {"rolling", "anchored"}
VALID_TIMEFRAMES = {"1m", "1d"}
TIMEFRAME_MINUTES = {"1m": 1, "1d": 1440}
JOB_STATUS_FILTERS = {"all", "queued", "running", "completed", "failed", "cancelled"}

THRESHOLD_KEYS = (
    "min_confidence",
    "min_agreement",
    "direction_band",
    "min_signal_strength",
    "max_position_pct",
    "max_positions",
    "max_exposure",
)


def _validate_threshold_overrides(body: dict) -> dict:
    errors: dict[str, str] = {}
    for key in THRESHOLD_KEYS:
        v = body.get(key)
        if v is None:
            continue
        meta = CONFIG_SCHEMA[f"execution_{key}"]
        t = meta["type"]
        if t == "int":
            if not isinstance(v, int) or isinstance(v, bool):
                errors[key] = "must be integer"
                continue
        elif t == "float":
            if not isinstance(v, (int, float)) or isinstance(v, bool):
                errors[key] = "must be number"
                continue
        if "min" in meta and v < meta["min"]:
            errors[key] = f"must be >= {meta['min']}"
        elif "max" in meta and v > meta["max"]:
            errors[key] = f"must be <= {meta['max']}"
    return errors


def _iso(v):
    return v.isoformat() if isinstance(v, datetime) else v


async def _delete_run_impl(db, run_id: int) -> dict:
    """Delete a terminal backtest run and its non-cascaded sandbox artifacts.

    Returns one of:
      {"status": "deleted",       "run_id": run_id}
      {"status": "not_found",     "run_id": run_id}
      {"status": "still_running", "run_id": run_id,
                                  "current_status": <str>}

    One transaction per run. Locks the run row with ``SELECT ... FOR UPDATE``
    first; if the run is missing or non-terminal, returns without writing.
    Otherwise deletes sandbox rows (not FK-cascaded) and the run row (which
    cascades to folds/trades/equity).
    """
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            current_status = await conn.fetchval(
                "SELECT status FROM backtest_runs WHERE id = $1 FOR UPDATE",
                run_id,
            )
            if current_status is None:
                return {"status": "not_found", "run_id": run_id}
            if current_status not in ("completed", "failed", "cancelled"):
                return {
                    "status": "still_running",
                    "run_id": run_id,
                    "current_status": current_status,
                }

            await conn.execute(
                "DELETE FROM backtest.orders WHERE backtest_run_id = $1",
                run_id,
            )
            await conn.execute(
                "DELETE FROM backtest.predictions WHERE backtest_run_id = $1",
                run_id,
            )
            await conn.execute(
                "DELETE FROM backtest.model_registry WHERE backtest_run_id = $1",
                run_id,
            )
            await conn.execute(
                "DELETE FROM backtest_runs WHERE id = $1",
                run_id,
            )

    return {"status": "deleted", "run_id": run_id}


async def _delete_run(request: web.Request) -> web.Response:
    """DELETE /api/backtests/{id}."""
    try:
        run_id = int(request.match_info["id"])
    except (KeyError, ValueError):
        return web.json_response(
            {"error": "run_id must be an integer"}, status=400,
        )
    db = request.app["db"]
    result = await _delete_run_impl(db, run_id)
    status = result["status"]
    if status == "deleted":
        return web.json_response(result, status=200)
    if status == "not_found":
        return web.json_response(
            {"error": "not found", "run_id": run_id}, status=404,
        )
    # still_running
    return web.json_response(
        {
            "error": "run is still running",
            "run_id": run_id,
            "current_status": result["current_status"],
        },
        status=409,
    )


async def _cancel_job(request: web.Request) -> web.Response:
    """POST /api/backtest-jobs/{job_id}/cancel — cancel by direct job_id."""
    try:
        job_id = int(request.match_info["job_id"])
    except (KeyError, ValueError):
        return web.json_response(
            {"error": "job_id must be an integer"}, status=400,
        )
    jobs = request.app["jobs_db"]
    result = await jobs.cancel(job_id)
    if result == "already_terminal":
        return web.json_response(
            {"error": "job is not queued or running", "job_id": job_id},
            status=409,
        )
    return web.json_response({"job_id": job_id, "status": "cancelled"})


async def _cancel_run(request: web.Request) -> web.Response:
    """POST /api/backtests/{run_id}/cancel — find linked job, cancel it.

    Distinguishes:
      - 404: no backtest_jobs row at all for this run_id (run may not even exist)
      - 409: a job exists but it's already terminal (completed / failed / cancelled)
      - 200: job was queued or running; now flipped to cancelled
    """
    try:
        run_id = int(request.match_info["id"])
    except (KeyError, ValueError):
        return web.json_response(
            {"error": "run_id must be an integer"}, status=400,
        )
    db = request.app["db"]
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, status FROM backtest_jobs
            WHERE run_id = $1
            ORDER BY id DESC LIMIT 1
            """,
            run_id,
        )
    if row is None:
        return web.json_response(
            {"error": "no job for run_id", "run_id": run_id},
            status=404,
        )
    if row["status"] not in ("queued", "running"):
        return web.json_response(
            {
                "error": "job is not queued or running",
                "run_id": run_id,
                "job_id": row["id"],
                "status": row["status"],
            },
            status=409,
        )
    jobs = request.app["jobs_db"]
    result = await jobs.cancel(row["id"])
    if result == "already_terminal":
        # Raced with a worker call that just finished the job; treat as 409.
        return web.json_response(
            {"error": "job is not queued or running", "run_id": run_id},
            status=409,
        )
    return web.json_response(
        {"run_id": run_id, "job_id": row["id"], "status": "cancelled"},
    )


def register_backtests_routes(app: web.Application) -> None:
    app.router.add_get("/api/backtests", _list)
    app.router.add_post("/api/backtests", _create)
    app.router.add_get("/api/backtests/{id}", _detail)
    app.router.add_delete("/api/backtests/{id}", _delete_run)
    app.router.add_get("/api/backtests/{id}/folds", _folds)
    app.router.add_get("/api/backtests/{id}/equity", _equity)
    app.router.add_get("/api/backtests/{id}/trades", _trades)
    app.router.add_post("/api/backtests/{id}/cancel", _cancel_run)
    app.router.add_post("/api/backtest-jobs/{job_id}/cancel", _cancel_job)


async def _list(request: web.Request) -> web.Response:
    results = request.app["results_db"]
    jobs = request.app["jobs_db"]

    status = request.query.get("status", "all")
    if status not in JOB_STATUS_FILTERS:
        return web.json_response({"error": "bad status"}, status=400)
    q = request.query.get("q") or None
    sort = request.query.get("sort", "finished_at")
    order = request.query.get("order", "desc")
    try:
        limit = min(int(request.query.get("limit", 50)), 200)
        offset = int(request.query.get("offset", 0))
    except ValueError:
        return web.json_response({"error": "bad paging"}, status=400)

    rows: list[dict] = []
    if status in ("all", "queued", "running"):
        pending = await jobs.list_pending()
        for j in pending:
            if status != "all" and j["status"] != status:
                continue
            if q and q.lower() not in j["name"].lower():
                continue
            cfg = j["config_json"] if isinstance(j["config_json"], dict) \
                else json.loads(j["config_json"])
            rows.append({
                "kind": "job",
                "id": j["id"],
                "name": j["name"],
                "custom_name": j.get("custom_name"),
                "symbols": cfg.get("symbols", []),
                "range_start": cfg.get("range_start"),
                "range_end": cfg.get("range_end"),
                "status": j["status"],
                "started_at": _iso(j["started_at"]),
                "finished_at": _iso(j["finished_at"]),
                "metrics": None,
                "error": j["error"],
                "tags": [],
                "notes": None,
            })

    if status in ("all", "completed", "failed", "cancelled"):
        run_status = status if status in ("completed", "failed", "cancelled") else "all"
        runs = await results.list_runs(
            status=run_status, q=q, sort=sort, order=order,
            limit=limit, offset=offset,
        )
        for r in runs:
            rows.append({
                "kind": "run",
                "id": r["id"],
                "name": r["name"],
                "custom_name": r.get("custom_name"),
                "symbols": r["symbols"],
                "range_start": _iso(r["range_start"]),
                "range_end": _iso(r["range_end"]),
                "status": r["status"],
                "started_at": _iso(r["started_at"]),
                "finished_at": _iso(r["finished_at"]),
                "metrics": r["metrics"],
                "tags": list(r.get("tags") or []),
                "notes": r.get("notes"),
            })

    return web.json_response({"rows": rows})


_CUSTOM_NAME_ALPHABET = string.ascii_lowercase + string.digits
_CUSTOM_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")


def _random_custom_name() -> str:
    return "".join(
        secrets.choice(_CUSTOM_NAME_ALPHABET) for _ in range(8)
    )


def _compose_full_name(
    custom: str, symbols: list[str], timeframe: str,
    horizons: list[int], range_start: str, range_end: str,
) -> str:
    # symbols: at most 3 inline, else "N-syms"
    if len(symbols) <= 3:
        syms = "-".join(s.replace("/", "") for s in symbols)
    else:
        syms = f"{len(symbols)}syms"
    start = range_start[:10]
    end = range_end[:10]
    hz = "-".join(str(h) for h in sorted(horizons))
    return f"{custom}__{syms}_{timeframe}_{start}_{end}_h{hz}"


def _validate_new_job(body: dict, valid_horizons: set[int]) -> dict:
    errors: dict[str, str] = {}
    custom_name = (body.get("name") or "").strip()
    if custom_name and not _CUSTOM_NAME_RE.match(custom_name):
        errors["name"] = "allowed chars: letters, digits, _ or -, up to 32 chars"
    symbols = body.get("symbols") or []
    if not isinstance(symbols, list) or not symbols:
        errors["symbols"] = "at least one required"
    timeframe = body.get("timeframe")
    if timeframe not in VALID_TIMEFRAMES:
        errors["timeframe"] = f"must be one of {sorted(VALID_TIMEFRAMES)}"
    horizons = body.get("horizons") or []
    if (not isinstance(horizons, list) or not horizons
            or not all(h in valid_horizons for h in horizons)):
        errors["horizons"] = (
            f"subset of currently-labeled horizons {sorted(valid_horizons)} required "
            f"(configure via label_horizons_1m / label_horizons_1d runtime config)"
        )
    try:
        start = datetime.fromisoformat(body["range_start"])
        end = datetime.fromisoformat(body["range_end"])
        if start >= end:
            errors["range_end"] = "must be after range_start"
    except (KeyError, TypeError, ValueError):
        errors["range_start"] = "range_start and range_end must be ISO-8601"
    for key in ("train_days", "test_days", "embargo_min"):
        v = body.get(key)
        if not isinstance(v, int) or v <= 0:
            errors[key] = "positive integer required"
    if "horizons" not in errors and "embargo_min" not in errors and "timeframe" not in errors:
        tf_min = TIMEFRAME_MINUTES[timeframe]
        max_horizon_minutes = max(horizons) * tf_min
        if body["embargo_min"] > max_horizon_minutes:
            errors["embargo_min"] = (
                f"must be <= max(horizons) in minutes ({max_horizon_minutes})"
            )
    mode = body.get("mode")
    if mode not in VALID_MODES:
        errors["mode"] = f"must be one of {sorted(VALID_MODES)}"
    return errors


async def _create(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"error": "bad json"}, status=400)

    rc = request.app["runtime_config"]
    timeframe = body.get("timeframe", "1m")
    horizons_key = (
        f"label_horizons_{timeframe}"
        if timeframe in VALID_TIMEFRAMES
        else "label_horizons_1m"
    )
    valid_horizons = set(await rc.get(horizons_key))
    errors = _validate_new_job(body, valid_horizons)
    if errors:
        return web.json_response({"errors": errors}, status=400)

    threshold_errors = _validate_threshold_overrides(body)
    if threshold_errors:
        return web.json_response({"errors": threshold_errors}, status=400)

    # Snapshot blanks from runtime_config at creation time — reproducibility.
    for key in THRESHOLD_KEYS:
        if body.get(key) is None:
            body[key] = await rc.get(f"execution_{key}")

    jobs = request.app["jobs_db"]
    config = {k: body[k] for k in (
        "symbols", "timeframe", "horizons",
        "range_start", "range_end",
        "train_days", "test_days", "embargo_min", "mode",
        *THRESHOLD_KEYS,
    )}
    custom_name = (body.get("name") or "").strip() or _random_custom_name()
    full_name = _compose_full_name(
        custom=custom_name,
        symbols=config["symbols"],
        timeframe=config["timeframe"],
        horizons=config["horizons"],
        range_start=config["range_start"],
        range_end=config["range_end"],
    )
    job_id = await jobs.insert_job(
        name=full_name, custom_name=custom_name, config=config,
    )
    return web.json_response(
        {"job_id": job_id, "name": full_name, "custom_name": custom_name},
        status=201,
    )


async def _detail(request: web.Request) -> web.Response:
    run_id = int(request.match_info["id"])
    row = await request.app["results_db"].get_run(run_id)
    if row is None:
        return web.json_response({"error": "not found"}, status=404)
    row["range_start"] = _iso(row["range_start"])
    row["range_end"] = _iso(row["range_end"])
    row["started_at"] = _iso(row["started_at"])
    row["finished_at"] = _iso(row["finished_at"])
    return web.json_response(row)


async def _folds(request: web.Request) -> web.Response:
    run_id = int(request.match_info["id"])
    results = request.app["results_db"]

    async with results.db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, fold_index, train_start, train_end,
                   test_start, test_end, metrics_json
            FROM backtest_folds
            WHERE run_id = $1
            ORDER BY fold_index ASC
            """,
            run_id,
        )

    import json as _json
    payload = []
    for r in rows:
        m = r["metrics_json"]
        if isinstance(m, str):
            m = _json.loads(m) if m else {}
        m = m or {}
        payload.append({
            "id": r["id"],
            "fold_index": r["fold_index"],
            "train_start": _iso(r["train_start"]),
            "train_end": _iso(r["train_end"]),
            "test_start": _iso(r["test_start"]),
            "test_end": _iso(r["test_end"]),
            "pnl": float(m.get("pnl") or 0),
            "sharpe": float(m.get("sharpe") or 0),
            "max_drawdown": float(m.get("max_drawdown") or 0),
            "trades": int(m.get("trades") or 0),
            "hit_rate": float(m.get("hit_rate") or 0),
        })
    return web.json_response(payload)


async def _equity(request: web.Request) -> web.Response:
    run_id = int(request.match_info["id"])
    results = request.app["results_db"]

    async with results.db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp, equity, fold_id
            FROM backtest_equity
            WHERE run_id = $1
            ORDER BY timestamp ASC
            """,
            run_id,
        )

    peak = None
    series = []
    fold_boundaries = []
    prev_fold = None
    for r in rows:
        eq = float(r["equity"])
        if peak is None or eq > peak:
            peak = eq
        dd = (eq - peak) / peak if peak else 0.0
        if prev_fold is not None and r["fold_id"] != prev_fold:
            fold_boundaries.append(_iso(r["timestamp"]))
        prev_fold = r["fold_id"]
        series.append({
            "timestamp": _iso(r["timestamp"]),
            "equity": eq, "drawdown": dd,
            "fold_id": r["fold_id"],
        })
    return web.json_response({
        "series": series,
        "fold_boundaries": fold_boundaries,
    })


async def _trades(request: web.Request) -> web.Response:
    run_id = int(request.match_info["id"])
    try:
        limit = min(int(request.query.get("limit", 50)), 200)
        offset = int(request.query.get("offset", 0))
    except ValueError:
        return web.json_response({"error": "bad paging"}, status=400)
    sort = request.query.get("sort", "entry_time")
    order = request.query.get("order", "desc")
    trades = await request.app["results_db"].list_trades(
        run_id, limit=limit, offset=offset, sort=sort, order=order,
    )
    for t in trades:
        t["entry_time"] = _iso(t["entry_time"])
        t["exit_time"] = _iso(t["exit_time"])
    return web.json_response({"trades": trades})
