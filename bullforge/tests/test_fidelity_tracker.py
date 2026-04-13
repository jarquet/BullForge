import pytest
from unittest.mock import AsyncMock, MagicMock
from services.monitoring_service.fidelity import FidelityTracker


@pytest.fixture
def mock_db():
    return AsyncMock()


@pytest.fixture
def tracker(mock_db, settings):
    return FidelityTracker(mock_db, settings)


@pytest.mark.asyncio
async def test_evaluate_model_active(tracker, mock_db):
    mock_db.query_prediction_outcomes = AsyncMock(return_value=[
        {"predicted_direction": 1, "confidence": 0.8, "price_move": 0.5, "is_correct": True},
        {"predicted_direction": -1, "confidence": 0.7, "price_move": -0.3, "is_correct": True},
        {"predicted_direction": 1, "confidence": 0.6, "price_move": 0.2, "is_correct": True},
    ] * 7)  # 21 outcomes, all correct

    status, reason = await tracker.evaluate_model(model_id=5, backtested_accuracy=0.65)

    assert status == "active"
    assert "Within expected range" in reason


@pytest.mark.asyncio
async def test_evaluate_model_warned(tracker, mock_db):
    # 20 outcomes: 12 correct (60%), backtested 68% — drop of 8% > warning margin (5%)
    correct = [{"predicted_direction": 1, "confidence": 0.8, "price_move": 0.5, "is_correct": True}] * 12
    wrong = [{"predicted_direction": 1, "confidence": 0.7, "price_move": -0.3, "is_correct": False}] * 8
    mock_db.query_prediction_outcomes = AsyncMock(return_value=correct + wrong)

    status, reason = await tracker.evaluate_model(model_id=5, backtested_accuracy=0.68)

    assert status == "warned"
    assert "degrading" in reason.lower()


@pytest.mark.asyncio
async def test_evaluate_model_disabled(tracker, mock_db):
    # 20 outcomes: 10 correct (50%), backtested 65% — drop of 15% > critical margin (10%)
    correct = [{"predicted_direction": 1, "confidence": 0.8, "price_move": 0.5, "is_correct": True}] * 10
    wrong = [{"predicted_direction": 1, "confidence": 0.7, "price_move": -0.3, "is_correct": False}] * 10
    mock_db.query_prediction_outcomes = AsyncMock(return_value=correct + wrong)

    status, reason = await tracker.evaluate_model(model_id=5, backtested_accuracy=0.65)

    assert status == "disabled"
    assert "below backtested" in reason.lower()


@pytest.mark.asyncio
async def test_evaluate_model_insufficient_data(tracker, mock_db):
    # Only 5 outcomes, need 20 minimum
    mock_db.query_prediction_outcomes = AsyncMock(return_value=[
        {"predicted_direction": 1, "confidence": 0.8, "price_move": 0.5, "is_correct": True},
    ] * 5)

    status, reason = await tracker.evaluate_model(model_id=5, backtested_accuracy=0.65)

    assert status == "active"
    assert "Insufficient data" in reason


@pytest.mark.asyncio
async def test_evaluate_model_profit_factor_below_one(tracker, mock_db):
    correct = [{"predicted_direction": 1, "confidence": 0.8, "price_move": 0.5, "is_correct": True}] * 11
    wrong = [{"predicted_direction": 1, "confidence": 0.7, "price_move": -0.3, "is_correct": False}] * 9
    mock_db.query_prediction_outcomes = AsyncMock(return_value=correct + wrong)

    # 55% accuracy, backtested 61% — drop of 6% > warning margin (5%)
    status, reason = await tracker.evaluate_model(model_id=5, backtested_accuracy=0.61)

    assert status == "warned"


@pytest.mark.asyncio
async def test_evaluate_all_models(tracker, mock_db):
    models = [
        MagicMock(id=1, metrics={"accuracy": 0.65}),
        MagicMock(id=2, metrics={"accuracy": 0.70}),
    ]
    mock_db.query_all_active_models = AsyncMock(return_value=models)

    # Model 1: good accuracy, Model 2: bad accuracy
    async def mock_outcomes(model_id, limit):
        if model_id == 1:
            return [{"is_correct": True}] * 15 + [{"is_correct": False}] * 5
        else:
            return [{"is_correct": True}] * 8 + [{"is_correct": False}] * 12

    mock_db.query_prediction_outcomes = AsyncMock(side_effect=mock_outcomes)

    results = await tracker.evaluate_all_models()

    assert len(results) == 2
    assert results[0] == (1, "active", pytest.approx(0.75, abs=0.01))
    assert results[1][0] == 2
    assert results[1][1] == "disabled"  # 40% vs 70% backtested
