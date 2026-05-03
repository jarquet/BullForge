from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Any, Optional

from shared.db import Database
from shared.models import MetaFeatureRow, MetaSignalShadow
from shared.redis import RedisClient

logger = logging.getLogger(__name__)

# Sliding-window counter prefix. One bucket per minute (UTC epoch minutes), keyed
# `<prefix>:<minute_bucket>` where `minute_bucket = int(time.time() // 60)`.
# Each bucket gets a 30-minute TTL so the dashboard's 15-minute-window query
# always sees a stable trailing tail without unbounded growth. The dashboard
# sums the most recent 15 buckets — see services/dashboard_service/models_api.py
# `_handle_meta_shadow_health`.
META_SHADOW_SKIPS_KEY_PREFIX = "bullforge:metrics:meta_shadow_skips"
_BUCKET_TTL_SECONDS = 1800  # 30 minutes


class MetaShadowWriter:
    """Persist incoming `model:meta_signals` payloads to the shadow tables.

    Writes one `meta_signals_shadow` row per signal in the payload, plus
    one `meta_features` row per unique direction in the payload (source='live',
    labels NULL — MetaLabelBackfiller fills them later).

    Optionally increments a Redis sliding-window counter every time an empty
    `feature_vector` payload is skipped, so the dashboard's "Meta shadow
    health" tile can surface publisher regressions that would otherwise only
    be visible by row-count comparison. The redis_client is optional —
    counter is observability-only and never gates persistence.
    """

    def __init__(
        self,
        *,
        db: Database,
        redis_client: Optional[RedisClient] = None,
    ):
        self._db = db
        self._redis = redis_client

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
                await self._increment_skip_counter()
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

    async def _increment_skip_counter(self) -> None:
        """Bump the sliding-window skip counter for the current minute bucket.

        Observability-only — failures are logged but never propagate to
        callers (we don't want a transient Redis hiccup to stop the writer
        from persisting subsequent shadow rows).
        """
        if self._redis is None or self._redis.client is None:
            return
        try:
            bucket = int(time.time() // 60)
            key = f"{META_SHADOW_SKIPS_KEY_PREFIX}:{bucket}"
            await self._redis.client.incr(key)
            await self._redis.client.expire(key, _BUCKET_TTL_SECONDS)
        except Exception:
            logger.exception(
                "MetaShadowWriter: failed to increment skip counter",
            )
