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
            [("Disable Model", f"disable:{model_id}"), ("Acknowledge", "ack")],
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
            [("Full Halt", "halt_confirm"), ("Acknowledge", "ack")],
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
