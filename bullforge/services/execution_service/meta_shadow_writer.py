from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from shared.db import Database
from shared.models import MetaFeatureRow, MetaSignalShadow

logger = logging.getLogger(__name__)


class MetaShadowWriter:
    """Persist incoming `model:meta_signals` payloads to the shadow tables.

    Writes one `meta_signals_shadow` row per signal in the payload, plus
    one `meta_features` row per unique direction in the payload (source='live',
    labels NULL — MetaLabelBackfiller fills them later).
    """

    def __init__(self, *, db: Database):
        self._db = db

    async def handle_message(self, message: str) -> None:
        try:
            payload: dict[str, Any] = json.loads(message)
            symbol = payload["symbol"]
            ts = datetime.fromisoformat(payload["timestamp"])
            signals = payload.get("signals") or []
            feature_vector = payload.get("feature_vector") or {}
        except Exception:
            logger.exception("MetaShadowWriter: malformed payload")
            return

        if not signals:
            return

        seen_dirs: set[int] = set()
        for sig in signals:
            try:
                shadow = MetaSignalShadow(
                    symbol=symbol, timestamp=ts,
                    direction=int(sig["direction"]),
                    estimator_type=str(sig["estimator_type"]),
                    entry_probability=float(sig["entry_probability"]),
                    kelly_fraction=sig.get("kelly_fraction"),
                    pair_index=sig.get("pair_index"),
                    target_pct=sig.get("target_pct"),
                    stop_pct=sig.get("stop_pct"),
                    model_id=sig.get("model_id"),
                )
                await self._db.insert_meta_signal_shadow(shadow)
            except Exception:
                logger.exception(
                    "MetaShadowWriter: failed to insert shadow for %s %s",
                    symbol, sig.get("direction"),
                )
                continue

            d = int(sig["direction"])
            if d in seen_dirs:
                continue
            seen_dirs.add(d)
            if not feature_vector:
                # Publisher contract: model_service forwards the assembled
                # L2 vector with every model:meta_signals event. An empty
                # dict here means a publisher bug or a pre-warmup race.
                # Fail loud rather than mask it with a placeholder — the
                # MetaFeatureRow guard would reject the empty dict anyway,
                # and the historical {"_": 0.0} fallback poisoned the
                # meta_features table with useless stub rows.
                logger.warning(
                    "MetaShadowWriter: empty feature_vector for %s dir=%s "
                    "ts=%s — skipping meta_features insert",
                    symbol, d, ts,
                )
                continue
            try:
                row = MetaFeatureRow(
                    symbol=symbol, timestamp=ts, direction=d,
                    feature_vector=feature_vector,
                    source="live",
                )
                await self._db.insert_meta_feature(row)
            except Exception:
                logger.exception(
                    "MetaShadowWriter: failed to insert feature row %s %s",
                    symbol, d,
                )
