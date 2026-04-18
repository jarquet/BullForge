from __future__ import annotations

import logging

from shared.db import Database

logger = logging.getLogger(__name__)


async def persist_daily_accuracy(
    db: Database,
    window_days: int = 30,
) -> int:
    """Write a model_fidelity snapshot per active base model.

    Each snapshot holds the rolling live accuracy over `window_days` worth
    of recent predictions (limit=window_days*100 as a practical cap),
    plus the backtested accuracy from the model's metadata.

    Models with no outcomes in the window are skipped.
    Returns the number of snapshots written.
    """
    active = await db.query_all_active_models()
    written = 0
    for model in active:
        # Skip meta-model rows — they aren't tracked by fidelity here.
        if getattr(model, "model_type", "") in ("meta_entry", "meta_exec"):
            continue
        outcomes = await db.query_prediction_outcomes(
            model.id, limit=window_days * 100,
        )
        if not outcomes:
            continue
        correct = sum(1 for o in outcomes if o["is_correct"])
        live_accuracy = correct / len(outcomes)
        backtested_accuracy = float(
            (getattr(model, "metrics", None) or {}).get("accuracy", 0.0)
        )
        metrics = {
            "live_accuracy": live_accuracy,
            "backtested_accuracy": backtested_accuracy,
            "sample_size": len(outcomes),
            "window_days": window_days,
        }
        await db.insert_metric_snapshot(
            "model_fidelity", str(model.id), "all", metrics,
        )
        written += 1
    return written
