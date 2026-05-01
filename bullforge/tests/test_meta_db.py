from datetime import datetime, timedelta, timezone
import pytest

from shared.config import Settings
from shared.db import Database
from shared.models import MetaFeatureRow


def _ts(minute: int = 0):
    return datetime(2026, 4, 28, 14, minute, 0, tzinfo=timezone.utc)


@pytest.fixture
async def db():
    settings = Settings()
    db = Database(settings)
    await db.connect()
    async with db.pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM meta_features WHERE symbol = 'TST_META'",
        )
    yield db
    async with db.pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM meta_features WHERE symbol = 'TST_META'",
        )
    await db.disconnect()


@pytest.mark.asyncio
async def test_query_unlabeled_meta_features_filters_by_age(db):
    old = MetaFeatureRow(symbol="TST_META", timestamp=_ts(0), direction=1,
                         feature_vector={"x": 1.0}, source="live")
    fresh = MetaFeatureRow(symbol="TST_META", timestamp=_ts(58), direction=1,
                           feature_vector={"x": 1.0}, source="live")
    await db.insert_meta_features_batch([old, fresh])

    cutoff = _ts(30)
    rows = await db.query_unlabeled_meta_features(older_than=cutoff)
    syms = {(r.symbol, r.timestamp) for r in rows}
    assert ("TST_META", _ts(0)) in syms
    assert ("TST_META", _ts(58)) not in syms


@pytest.mark.asyncio
async def test_query_unlabeled_meta_features_excludes_labeled(db):
    labeled = MetaFeatureRow(symbol="TST_META", timestamp=_ts(0), direction=1,
                             feature_vector={"x": 1.0}, source="live",
                             entry_label=1, optimal_pair_idx=0, realized_ev=0.01)
    await db.insert_meta_features_batch([labeled])
    rows = await db.query_unlabeled_meta_features(older_than=_ts(30))
    assert all(r.symbol != "TST_META" for r in rows)


@pytest.mark.asyncio
async def test_update_meta_feature_labels(db):
    row = MetaFeatureRow(symbol="TST_META", timestamp=_ts(0), direction=1,
                         feature_vector={"x": 1.0}, source="live")
    await db.insert_meta_features_batch([row])
    await db.update_meta_feature_labels(
        symbol="TST_META", timestamp=_ts(0), direction=1,
        entry_label=1, optimal_pair_idx=2, realized_ev=0.012,
    )
    rows = await db.query_meta_features("TST_META", direction=1, source="live")
    assert len(rows) == 1
    assert rows[0].entry_label == 1
    assert rows[0].optimal_pair_idx == 2
    assert rows[0].realized_ev == pytest.approx(0.012)


@pytest.mark.asyncio
async def test_insert_meta_features_batch_skip_preserves_live(db):
    live = MetaFeatureRow(symbol="TST_META", timestamp=_ts(0), direction=1,
                          feature_vector={"v": 1.0}, source="live")
    await db.insert_meta_features_batch([live])

    replay = MetaFeatureRow(symbol="TST_META", timestamp=_ts(0), direction=1,
                            feature_vector={"v": 999.0}, source="replay",
                            entry_label=0, optimal_pair_idx=0, realized_ev=-0.01)
    await db.insert_meta_features_batch([replay], on_conflict="skip")

    rows = await db.query_meta_features("TST_META", direction=1)
    assert len(rows) == 1
    assert rows[0].source == "live"
    assert rows[0].feature_vector == {"v": 1.0}
