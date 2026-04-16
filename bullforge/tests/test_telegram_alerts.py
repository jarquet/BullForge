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
