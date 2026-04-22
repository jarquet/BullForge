from services.monitoring_service.telegram_alerts import format_alert, format_confirmation


def test_format_model_warned():
    alert = {"tier": 1, "type": "model_warned", "model_id": 5, "reason": "Accuracy degrading"}
    text, buttons = format_alert(alert)

    assert "Tier 1" in text
    assert "Model 5" in text
    assert "Accuracy degrading" in text
    assert len(buttons) == 1
    assert len(buttons[0]) == 2
    assert buttons[0][0] == ("Acknowledge", "ack")
    assert buttons[0][1] == ("Disable Model", "disable:5")


def test_format_model_disabled():
    alert = {"tier": 2, "type": "model_disabled", "model_id": 3, "reason": "Critical threshold"}
    text, buttons = format_alert(alert)

    assert "Tier 2" in text
    assert "Model 3" in text
    assert len(buttons[0]) == 2
    assert buttons[0][0] == ("Acknowledge", "ack")
    assert buttons[0][1] == ("Re-enable", "reenable:3")


def test_format_direction_halt():
    alert = {"tier": 3, "type": "direction_halt", "reason": "Half of models disabled"}
    text, buttons = format_alert(alert)

    assert "Tier 3" in text
    assert len(buttons[0]) == 2
    assert buttons[0][0] == ("Acknowledge", "ack")
    assert buttons[0][1] == ("Full Halt", "halt_confirm")


def test_format_drawdown_halt():
    alert = {"tier": 4, "type": "drawdown_halt", "reason": "Drawdown 12% exceeds threshold"}
    text, buttons = format_alert(alert)

    assert "Tier 4" in text
    assert "Drawdown" in text
    assert len(buttons[0]) == 2
    assert buttons[0][0] == ("Acknowledge", "ack")
    assert buttons[0][1] == ("Full Halt", "halt_confirm")


def test_format_majority_halt():
    alert = {"tier": 4, "type": "majority_halt", "reason": "Majority of models disabled"}
    text, buttons = format_alert(alert)

    assert "System halted" in text.lower() or "HALT" in text
    assert len(buttons[0]) == 1
    assert buttons[0][0] == ("Acknowledge", "ack")


def test_format_unknown_type():
    alert = {"tier": 0, "type": "unknown", "reason": "Something happened"}
    text, buttons = format_alert(alert)

    assert "Something happened" in text
    assert buttons[0][0] == ("Acknowledge", "ack")


def test_format_confirmation_full_halt():
    text, buttons = format_confirmation("full_halt")

    assert "halt" in text.lower()
    assert "sure" in text.lower() or "confirm" in text.lower()
    assert len(buttons[0]) == 2
    assert buttons[0][0][1] == "halt_yes"
    assert buttons[0][1][1] == "halt_no"


def test_format_status_active():
    from services.monitoring_service.telegram_alerts import format_status

    text = format_status(
        is_halted=False,
        model_statuses=[
            {"model_id": 1, "status": "active"},
            {"model_id": 2, "status": "warned"},
        ],
        active_halts=[],
    )

    assert "ACTIVE" in text
    assert "Model 1" in text
    assert "Model 2" in text


def test_format_status_halted():
    from services.monitoring_service.telegram_alerts import format_status

    text = format_status(
        is_halted=True,
        model_statuses=[],
        active_halts=[{"tier": 4, "reason": "Manual halt"}],
    )

    assert "HALTED" in text
    assert "Manual halt" in text


# ---------------------------------------------------------------------------
# position_closed alert format tests
# ---------------------------------------------------------------------------


def _close_alert(**overrides):
    base = {
        "type": "position_closed",
        "tier": 0,
        "trade_id": 12,
        "symbol": "AAPL",
        "side": "long",
        "quantity": 100.0,
        "entry_time": "2026-04-22 13:31:15",
        "entry_price": 187.42,
        "exit_time":  "2026-04-22 15:45:03",
        "exit_price": 189.18,
        "pnl": 176.00,
        "pnl_pct": 0.0094,
        "hold_duration_seconds": 8028,
        "account": {
            "equity": 104_326.12,
            "last_equity": 103_900.00,
            "day_pnl": 426.12,
            "day_pnl_pct": 0.0041,
            "day_trades": 4,
            "day_wins": 3,
            "day_losses": 1,
            "day_win_rate": 0.75,
            "day_realized_pnl": 352.00,
        },
    }
    base.update(overrides)
    return base


def test_format_position_closed_winning_trade():
    text, buttons = format_alert(_close_alert())
    assert "TRADE CLOSED" in text
    assert "AAPL long" in text
    assert "+$176.00" in text
    assert "+0.94%" in text
    assert "2026-04-22 13:31:15" in text
    assert "2026-04-22 15:45:03" in text
    assert "2h 13m 48s" in text
    assert "Equity:" in text
    assert "$104,326.12" in text
    assert "+$426.12" in text
    assert "Realized:" in text
    assert "+$352.00" in text
    assert "4 trades" in text
    assert "Win rate:" in text
    assert "75.0%" in text
    assert "3W / 1L" in text
    assert buttons == []


def test_format_position_closed_losing_trade():
    text, _ = format_alert(_close_alert(pnl=-45.50, pnl_pct=-0.0024))
    assert "-$45.50" in text
    assert "-0.24%" in text


def test_format_position_closed_no_day_trades_yet():
    text, _ = format_alert(_close_alert(account={
        "equity": 100_000.0, "last_equity": 100_000.0,
        "day_pnl": 0.0, "day_pnl_pct": 0.0,
        "day_trades": 0, "day_wins": 0, "day_losses": 0,
        "day_win_rate": 0.0, "day_realized_pnl": 0.0,
    }))
    # With 0 day trades, realized and win-rate lines are omitted.
    assert "Realized:" not in text
    assert "Win rate:" not in text
    # Equity line still present.
    assert "Equity:" in text


def test_format_position_closed_omits_win_rate_when_only_breakevens():
    text, _ = format_alert(_close_alert(account={
        "equity": 100_000.0, "last_equity": 100_000.0,
        "day_pnl": 0.0, "day_pnl_pct": 0.0,
        "day_trades": 1, "day_wins": 0, "day_losses": 0,
        "day_win_rate": 0.0, "day_realized_pnl": 0.0,
    }))
    assert "Realized:" in text     # 1 trade — show realized line
    assert "Win rate:" not in text  # 0W + 0L denominator — hide win-rate line


def test_format_position_closed_short_duration():
    text, _ = format_alert(_close_alert(hold_duration_seconds=42))
    assert "42s" in text
    # Check the Hold line has no minutes segment.
    hold_line = [l for l in text.splitlines() if l.startswith("Hold:")][0]
    assert "m " not in hold_line
