from __future__ import annotations

from typing import List, Tuple

# Button layout type: list of rows, each row is list of (label, callback_data)
ButtonLayout = List[List[Tuple[str, str]]]


def format_alert(alert: dict) -> Tuple[str, ButtonLayout]:
    alert_type = alert.get("type", "")
    reason = alert.get("reason", "")
    model_id = alert.get("model_id")

    if alert_type == "model_warned":
        text = (
            f"[WARNING] Tier 1: Model {model_id} degraded\n\n"
            f"{reason}"
        )
        buttons = [
            [("Acknowledge", "ack"), ("Disable Model", f"disable:{model_id}")],
        ]

    elif alert_type == "model_disabled":
        text = (
            f"[ALERT] Tier 2: Model {model_id} auto-disabled\n\n"
            f"{reason}"
        )
        buttons = [
            [("Acknowledge", "ack"), ("Re-enable", f"reenable:{model_id}")],
        ]

    elif alert_type == "direction_halt":
        text = (
            f"[CRITICAL] Tier 3: Direction halted\n\n"
            f"{reason}"
        )
        buttons = [
            [("Acknowledge", "ack"), ("Full Halt", "halt_confirm")],
        ]

    elif alert_type == "drawdown_halt":
        text = (
            f"[CRITICAL] Tier 4: Drawdown threshold breached\n\n"
            f"{reason}"
        )
        buttons = [
            [("Acknowledge", "ack"), ("Full Halt", "halt_confirm")],
        ]

    elif alert_type == "majority_halt":
        text = (
            f"[HALT] System halted\n\n"
            f"{reason}"
        )
        buttons = [
            [("Acknowledge", "ack")],
        ]

    else:
        text = f"[INFO] {reason}"
        buttons = [
            [("Acknowledge", "ack")],
        ]

    return text, buttons


def format_confirmation(action: str) -> Tuple[str, ButtonLayout]:
    if action == "full_halt":
        text = (
            "Are you sure you want to halt ALL trading?\n\n"
            "This will stop all signal processing. "
            "Open positions keep their bracket orders on Alpaca."
        )
        buttons = [
            [("Yes, Halt Everything", "halt_yes"), ("Cancel", "halt_no")],
        ]
    else:
        text = f"Confirm action: {action}?"
        buttons = [
            [("Confirm", f"confirm_{action}"), ("Cancel", "cancel")],
        ]

    return text, buttons


def format_status(
    is_halted: bool,
    model_statuses: List[dict],
    active_halts: List[dict],
) -> str:
    lines = ["BullForge System Status", ""]

    if is_halted:
        lines.append("[HALTED] Trading is stopped")
    else:
        lines.append("[ACTIVE] Trading is running")

    if model_statuses:
        lines.append(f"\nModels ({len(model_statuses)}):")
        status_icons = {"active": "[OK]", "warned": "[WARN]", "disabled": "[OFF]"}
        for m in model_statuses:
            icon = status_icons.get(m["status"], "[?]")
            lines.append(f"  {icon} Model {m['model_id']}: {m['status']}")

    if active_halts:
        lines.append(f"\nActive halts ({len(active_halts)}):")
        for h in active_halts:
            lines.append(f"  Tier {h['tier']}: {h['reason']}")

    return "\n".join(lines)


from datetime import datetime, timezone


def format_positions(positions: list[dict]) -> str:
    if not positions:
        return "No open positions."
    lines = [f"Open Positions ({len(positions)})", ""]
    for p in positions:
        side = "LONG" if float(p.get("qty", 0)) > 0 else "SHORT"
        pnl = float(p.get("unrealized_pl", 0.0))
        cur = float(p.get("current_price", 0.0))
        avg = float(p.get("avg_entry_price", 0.0))
        pct = (pnl / (avg * abs(float(p.get("qty", 1))))) * 100 if avg > 0 else 0.0
        lines.append(f"{p['symbol']}: {p['qty']} {side} @ ${avg:.2f}")
        lines.append(f"  Current: ${cur:.2f} | P&L: {pnl:+.2f} ({pct:+.2f}%)")
        lines.append("")
    return "\n".join(lines).rstrip()


def format_trades(trades: list[dict]) -> str:
    if not trades:
        return "No recent trades."
    lines = [f"Recent Trades (last {len(trades)})", ""]
    for t in trades:
        side = "BUY" if t["side"] == "long" else "SELL"
        exit_side = "SELL" if t["side"] == "long" else "BUY"
        hold_min = (t.get("hold_duration_seconds") or 0) // 60
        age = datetime.now(timezone.utc) - t["exit_time"] if t.get("exit_time") else None
        age_str = f"{age.total_seconds() / 3600:.1f}h ago" if age else ""
        lines.append(
            f"{t['symbol']} {side} {t['quantity']:g} @ ${t['entry_price']:.2f} "
            f"-> {exit_side} @ ${t['exit_price']:.2f}"
        )
        lines.append(f"  P&L: {t['pnl']:+.2f} | Hold: {hold_min}m | {age_str}")
        lines.append("")
    return "\n".join(lines).rstrip()


def format_models_summary(models: list[dict]) -> str:
    groups: dict[str, list[dict]] = {"active": [], "warned": [], "disabled": []}
    for m in models:
        groups.setdefault(m["status"], []).append(m)
    lines = [f"Models ({len(models)} total)", ""]
    for status_key, label in [("active", "Active"), ("warned", "Warned"), ("disabled", "Disabled")]:
        group = groups.get(status_key, [])
        if not group:
            continue
        lines.append(f"{label} ({len(group)}):")
        for m in group:
            acc = m.get("metrics", {}).get("accuracy")
            acc_s = f"acc: {acc:.2f}" if acc is not None else "acc: n/a"
            age_h = (datetime.now(timezone.utc) - m["trained_at"]).total_seconds() / 3600
            lines.append(f"  {m['label']} - {acc_s} | {age_h:.0f}h old")
        lines.append("")
    return "\n".join(lines).rstrip()


def format_status_expanded(
    *,
    account: dict,
    positions_count: int,
    exposure_pct: float,
    trading_stats: dict,
    model_counts: dict,
    freshness_summary: str,
    active_halts_count: int,
    last_retrain_iso: str,
    now: datetime,
) -> str:
    equity = account.get("equity", 0.0)
    last_retrain = last_retrain_iso or "never"
    if last_retrain_iso:
        try:
            last_dt = datetime.fromisoformat(last_retrain_iso)
            age_h = (now - last_dt).total_seconds() / 3600
            last_retrain = f"{last_retrain_iso} ({age_h:.0f}h ago)"
        except ValueError:
            pass
    halts_str = f"{active_halts_count} active" if active_halts_count else "None active"
    return (
        "BullForge Status\n\n"
        f"Account: ${equity:,.2f} equity\n"
        f"Positions: {positions_count} open | ${equity * exposure_pct:,.0f} exposure "
        f"({exposure_pct * 100:.1f}%)\n"
        f"Trading: {trading_stats.get('trades_today', 0)} trades today | "
        f"{trading_stats.get('win_rate', 0) * 100:.0f}% win rate | "
        f"{trading_stats.get('pnl_today', 0):+.2f} P&L\n\n"
        f"Models: {model_counts.get('active', 0)} active | "
        f"{model_counts.get('warned', 0)} warned | "
        f"{model_counts.get('disabled', 0)} disabled\n"
        f"Freshness: {freshness_summary}\n"
        f"Halts: {halts_str}\n\n"
        f"Last retrain: {last_retrain}"
    )


def format_halt_confirmation() -> str:
    return "\u26a0\ufe0f Halt ALL trading? This will stop accepting new signals."


def format_resume_confirmation() -> str:
    return "Resume trading? This clears all active halts."
