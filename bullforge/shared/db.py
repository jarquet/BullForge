from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Union

import asyncpg

from shared.config import Settings
from datetime import datetime, timedelta, timezone

from shared.models import Bar, FeatureRow, Label, ModelMetadata, Prediction, Order, Trade, ModelStatus, MetaFeatureRow, MetaSignalShadow, BacktestScope

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UpsertResult:
    order: "Order"
    old_status: Optional[str]
    new_status: str
    inserted: bool


# Alpaca reports more statuses than our canonical set. Map them.
_ALPACA_STATUS_MAP = {
    "new": "submitted",
    "accepted": "submitted",
    "pending_new": "submitted",
    "accepted_for_bidding": "submitted",
    "held": "submitted",
    "pending_cancel": "submitted",
    "pending_replace": "submitted",
    "calculated": "submitted",
    "suspended": "submitted",
    "replaced": "cancelled",
    "canceled": "cancelled",
    "cancelled": "cancelled",
    "rejected": "rejected",
    "filled": "filled",
    "partially_filled": "partially_filled",
    "expired": "expired",
    "done_for_day": "expired",
    "stopped": "filled",
}


def _require_utc(dt: datetime, name: str = "datetime") -> datetime:
    """Validate that a datetime is timezone-aware and UTC.

    Raises ValueError if `dt` is naive or has a non-UTC tzinfo. Returns
    `dt` unchanged on the happy path so callers can chain it inline.

    Project invariant: all datetimes in BullForge are UTC. This helper
    enforces that at the DB-helper boundary so a future caller passing
    a naive datetime gets a loud failure instead of plausible-but-wrong
    results from asyncpg's session-default timezone semantics.
    """
    if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be UTC, got tzinfo={dt.tzinfo}")
    return dt


def _normalize_alpaca_status(raw: str) -> str:
    mapped = _ALPACA_STATUS_MAP.get(raw)
    if mapped is None:
        raise ValueError(f"unknown Alpaca order status: {raw!r}")
    return mapped


def _scope_filter(
    scope: Optional[BacktestScope], *, next_arg: int,
) -> tuple[str, tuple]:
    """Returns a (' AND backtest_run_id ... AND fold_index ...', args) pair to
    append to an existing parameterized WHERE clause.

    `next_arg` is the 1-based index of the next placeholder. For scope=None,
    the clause pins both columns to NULL so live reads never leak sandbox
    rows; for a set scope, the clause equates both columns to the scope's
    ids via $N and $N+1.
    """
    if scope is None:
        return " AND backtest_run_id IS NULL AND fold_index IS NULL", ()
    return (
        f" AND backtest_run_id = ${next_arg} AND fold_index = ${next_arg + 1}",
        (scope.backtest_run_id, scope.fold_index),
    )


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _build_add_column_ddl(table: str, col: asyncpg.Record) -> str:
    udt = col["udt_name"]
    data_type = col["data_type"]

    if data_type == "ARRAY":
        element = udt.lstrip("_")
        type_sql = f"{element}[]"
    elif data_type == "USER-DEFINED":
        type_sql = udt
    elif udt in ("varchar", "bpchar") and col["character_maximum_length"]:
        type_sql = f"{udt}({col['character_maximum_length']})"
    elif udt == "numeric" and col["numeric_precision"] is not None:
        if col["numeric_scale"] is not None:
            type_sql = f"numeric({col['numeric_precision']},{col['numeric_scale']})"
        else:
            type_sql = f"numeric({col['numeric_precision']})"
    else:
        type_sql = udt

    parts = [
        f"ALTER TABLE backtest.{_quote_ident(table)}",
        f"ADD COLUMN IF NOT EXISTS {_quote_ident(col['column_name'])} {type_sql}",
    ]
    if col["column_default"] is not None:
        parts.append(f"DEFAULT {col['column_default']}")
    if col["is_nullable"] == "NO":
        parts.append("NOT NULL")
    return " ".join(parts)


_INDEX_HEAD_RE = re.compile(
    r"^CREATE (?:UNIQUE )?INDEX \S+ ON (?:ONLY )?\S+\s+",
    re.IGNORECASE,
)


def _index_body(indexdef: str) -> str:
    """
    Returns the portion of a pg_indexes.indexdef after the
    `CREATE [UNIQUE] INDEX <name> ON <schema>.<table> ` prefix. Two indexes
    with the same body differ only by schema/name and are equivalent.
    """
    return _INDEX_HEAD_RE.sub("", indexdef, count=1)


_INDEX_REWRITE_RE = re.compile(
    r"^(CREATE (?:UNIQUE )?INDEX )(\S+)( ON (?:ONLY )?)(\S+)(\s)",
    re.IGNORECASE,
)


def _rewrite_index_for_backtest(
    indexdef: str, orig_name: str, table: str
) -> Optional[str]:
    """
    Rewrites a public-schema index DDL to target backtest.<table>, using
    CREATE INDEX IF NOT EXISTS and a non-colliding index name.
    Returns None if the DDL doesn't match the expected shape.
    """
    m = _INDEX_REWRITE_RE.match(indexdef)
    if not m:
        return None
    new_name = orig_name if orig_name.startswith("backtest_") else f"backtest_{orig_name}"
    create_prefix, _name, on_middle, _target, trailing_ws = m.groups()
    head = (
        f"{create_prefix}IF NOT EXISTS {new_name}"
        f"{on_middle}backtest.{_quote_ident(table)}{trailing_ws}"
    )
    return head + indexdef[m.end():]


class Database:
    # Session-level advisory lock key used to serialize startup DDL
    # (migrations + sandbox sync) across processes connecting to the same
    # database. Without this, ~8 services calling connect() concurrently
    # race through CREATE/DROP INDEX in the migrations, and pg_get_indexdef
    # can transiently return NULL during the window — crashing _index_body.
    _MIGRATION_LOCK_KEY = 0x42554C4C  # 'BULL'

    def __init__(self, settings: Settings, *, sandbox: bool = False):
        self._settings = settings
        self._sandbox = sandbox
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        server_settings = {}
        if self._sandbox:
            server_settings["search_path"] = "backtest,public"
        self.pool = await asyncpg.create_pool(
            host=self._settings.timescaledb_host,
            port=self._settings.timescaledb_port,
            user=self._settings.timescaledb_user,
            password=self._settings.timescaledb_password,
            database=self._settings.timescaledb_db,
            min_size=2,
            max_size=10,
            server_settings=server_settings or None,
        )
        # Sandbox pools rely on the live pool having already applied
        # migrations (incl. 013 which creates the `backtest` schema).
        if not self._sandbox:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "SELECT pg_advisory_lock($1)", self._MIGRATION_LOCK_KEY,
                )
                try:
                    await self._run_migrations(conn)
                    await self._sync_sandbox_schema(conn)
                finally:
                    await conn.execute(
                        "SELECT pg_advisory_unlock($1)",
                        self._MIGRATION_LOCK_KEY,
                    )

    async def disconnect(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def _run_migrations(
        self,
        conn: asyncpg.Connection,
        *,
        migrations_dir: Optional[Path] = None,
    ) -> None:
        # The default path is the only one used in production. Tests pass an
        # alternate path to exercise the ledger logic against synthetic
        # migrations (see tests/test_db.py::test_ledger_skips_already_applied).
        if migrations_dir is None:
            migrations_dir = Path(__file__).parent.parent / "migrations"
        sql_files = sorted(migrations_dir.glob("*.sql"))

        # Self-bootstrap the ledger. CREATE TABLE IF NOT EXISTS is idempotent,
        # so this runs every connect and no-ops once the table exists.
        # Living inline (rather than as a numbered migration file) avoids the
        # chicken-and-egg of "the ledger guards every file but who guards the
        # ledger creator?".
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            "filename TEXT PRIMARY KEY, "
            "applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
            ")"
        )

        applied = {
            r["filename"]
            for r in await conn.fetch("SELECT filename FROM schema_migrations")
        }

        # Cutover detection: an empty ledger on a DB whose schema is already
        # populated implies this is the first connect after B1 ships against
        # a healthy production database. Bulk-insert every current filename
        # as already-applied so the 22 idempotent migrations don't all
        # re-run once on the cutover deploy. `bars_1m` is the canary because
        # 001_create_bars.sql creates it and every populated DB has it.
        if not applied:
            schema_populated = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                "WHERE table_schema='public' AND table_name='bars_1m')"
            )
            if schema_populated:
                for sql_file in sql_files:
                    await conn.execute(
                        "INSERT INTO schema_migrations (filename) VALUES ($1) "
                        "ON CONFLICT DO NOTHING",
                        sql_file.name,
                    )
                return

        for sql_file in sql_files:
            if sql_file.name in applied:
                continue
            sql = sql_file.read_text()
            # Run the migration and the ledger insert in one transaction so
            # a connection drop between them can't leave the migration
            # applied but unrecorded (which would cause a re-run on the next
            # connect — safe given idempotency, but undesirable noise).
            async with conn.transaction():
                await conn.execute(sql)
                await conn.execute(
                    "INSERT INTO schema_migrations (filename) VALUES ($1) "
                    "ON CONFLICT DO NOTHING",
                    sql_file.name,
                )

    async def _sync_sandbox_schema(self, conn: asyncpg.Connection) -> None:
        """
        Ensures every table in the `backtest` schema structurally matches its
        `public` namesake: any column present in public but missing in backtest
        is added; any index defined on public but not on backtest is created.

        This exists because migration 014 seeds `backtest.<table>` via
        `LIKE public.<table> INCLUDING ALL` — a one-time snapshot. Later
        migrations that `ALTER TABLE <name>` without a schema qualifier hit
        only `public`, so the sandbox copy silently drifts. The backtest
        worker writes via `search_path=backtest,public`, so drift surfaces
        as `UndefinedColumnError` on inserts.

        Runs after `_run_migrations()` on every live-pool `connect()`. Idempotent.
        All DDL uses IF NOT EXISTS, so concurrent workers are safe.

        Only ADD COLUMN and CREATE INDEX drift is repaired automatically.
        Column drops, type changes, default changes, and constraint drift
        are logged as warnings for human attention — those are rare and
        have too many safe-rollout variants to auto-apply.
        """
        tables = await conn.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'backtest' AND table_type = 'BASE TABLE'"
        )
        for row in tables:
            await self._sync_sandbox_table(conn, row["table_name"])

    async def _sync_sandbox_table(self, conn: asyncpg.Connection, table: str) -> None:
        public_exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = $1)",
            table,
        )
        if not public_exists:
            logger.warning(
                "sync_sandbox: backtest.%s has no public counterpart; skipping",
                table,
            )
            return
        await self._sync_sandbox_columns(conn, table)
        await self._sync_sandbox_indexes(conn, table)

    async def _sync_sandbox_columns(self, conn: asyncpg.Connection, table: str) -> None:
        rows = await conn.fetch(
            """
            SELECT table_schema, column_name, data_type, udt_name,
                   is_nullable, column_default, character_maximum_length,
                   numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_name = $1 AND table_schema IN ('public', 'backtest')
            """,
            table,
        )
        public = {r["column_name"]: r for r in rows if r["table_schema"] == "public"}
        backtest = {r["column_name"]: r for r in rows if r["table_schema"] == "backtest"}

        for name in sorted(set(public) - set(backtest)):
            stmt = _build_add_column_ddl(table, public[name])
            logger.info("sync_sandbox: %s", stmt)
            await conn.execute(stmt)

        for name in sorted(set(backtest) - set(public)):
            logger.warning(
                "sync_sandbox: backtest.%s.%s has no public counterpart; "
                "manual review required (may be safe to DROP)",
                table, name,
            )

        for name in sorted(set(public) & set(backtest)):
            p, b = public[name], backtest[name]
            for attr in ("udt_name", "is_nullable", "column_default",
                         "character_maximum_length", "numeric_precision",
                         "numeric_scale"):
                if p[attr] != b[attr]:
                    logger.warning(
                        "sync_sandbox: attribute drift on backtest.%s.%s (%s): "
                        "public=%r backtest=%r; manual ALTER required",
                        table, name, attr, p[attr], b[attr],
                    )

    async def _sync_sandbox_indexes(self, conn: asyncpg.Connection, table: str) -> None:
        rows = await conn.fetch(
            """
            SELECT schemaname, indexname, indexdef
            FROM pg_indexes
            WHERE tablename = $1 AND schemaname IN ('public', 'backtest')
            """,
            table,
        )
        # pg_get_indexdef() (which backs pg_indexes.indexdef) returns NULL for
        # indexes it can't render — invalid indexes from a failed CREATE INDEX
        # CONCURRENTLY, indexes mid-drop, or indexes with un-renderable
        # opclasses. Skip these with a warning rather than crashing the live
        # pool's connect(); operator can investigate via pg_index/pg_class.
        usable_rows = []
        for r in rows:
            if r["indexdef"] is None:
                logger.warning(
                    "sync_sandbox: %s.%s has NULL indexdef; skipping "
                    "(check pg_index for invalid/dropping state)",
                    r["schemaname"], r["indexname"],
                )
                continue
            usable_rows.append(r)
        rows = usable_rows
        public_idx = [r for r in rows if r["schemaname"] == "public"]
        backtest_bodies = {_index_body(r["indexdef"]) for r in rows if r["schemaname"] == "backtest"}

        for r in public_idx:
            body = _index_body(r["indexdef"])

            # Skip unscoped partial unique indexes on model_registry that would
            # enforce cross-run uniqueness. These are replaced by scoped versions
            # in migration 018 that include (backtest_run_id, fold_index).
            # The unscoped indexes block concurrent backtest runs on overlapping
            # (symbol, timeframe, horizon_minutes, model_type) combos.
            if table == "model_registry" and "WHERE" in r["indexdef"] and "is_active" in r["indexdef"]:
                # Only skip if there's a scoped equivalent in backtest
                scoped_pattern = f"(backtest_run_id, fold_index"
                scoped_exists = any(scoped_pattern in _index_body(row["indexdef"])
                                   for row in rows
                                   if row["schemaname"] == "backtest" and "WHERE" in row["indexdef"])
                if scoped_exists:
                    logger.debug("sync_sandbox: skipping unscoped unique index %s on %s; "
                                "scoped version exists in backtest", r["indexname"], table)
                    continue

            if body in backtest_bodies:
                continue
            new_ddl = _rewrite_index_for_backtest(r["indexdef"], r["indexname"], table)
            if new_ddl is None:
                logger.warning(
                    "sync_sandbox: could not rewrite index %r for backtest; skipping",
                    r["indexname"],
                )
                continue
            logger.info("sync_sandbox: %s", new_ddl)
            await conn.execute(new_ddl)

    async def insert_bar(self, bar: Bar) -> None:
        table = "bars_1m" if bar.timeframe == "1m" else "bars_daily"
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {table} (symbol, timestamp, open, high, low, close, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, timestamp)
                DO UPDATE SET open = $3, high = $4, low = $5, close = $6, volume = $7
                """,
                bar.symbol,
                bar.timestamp,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
            )

    async def insert_bars(self, bars: List[Bar]) -> None:
        if not bars:
            return
        table = "bars_1m" if bars[0].timeframe == "1m" else "bars_daily"
        async with self.pool.acquire() as conn:
            await conn.executemany(
                f"""
                INSERT INTO {table} (symbol, timestamp, open, high, low, close, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, timestamp)
                DO UPDATE SET open = $3, high = $4, low = $5, close = $6, volume = $7
                """,
                [
                    (b.symbol, b.timestamp, b.open, b.high, b.low, b.close, b.volume)
                    for b in bars
                ],
            )

    async def query_bars(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 100,
        since: Optional[Union[str, datetime]] = None,
    ) -> List[Bar]:
        if isinstance(since, datetime):
            _require_utc(since, "since")
        table = "bars_1m" if timeframe == "1m" else "bars_daily"
        query = f"""
            SELECT symbol, timestamp, open, high, low, close, volume
            FROM {table}
            WHERE symbol = $1
        """
        args: list = [symbol]
        if since:
            # asyncpg refuses to coerce str → TIMESTAMPTZ when binding the
            # parameter, so accept either form and normalize to datetime.
            if isinstance(since, str):
                since = datetime.fromisoformat(since)
                _require_utc(since, "since")
            query += " AND timestamp >= $2"
            args.append(since)
        query += " ORDER BY timestamp DESC LIMIT $" + str(len(args) + 1)
        args.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [
            Bar(
                symbol=r["symbol"],
                timestamp=r["timestamp"],
                open=r["open"],
                high=r["high"],
                low=r["low"],
                close=r["close"],
                volume=r["volume"],
                timeframe=timeframe,
            )
            for r in rows
        ]

    async def query_bars_between(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> list[dict]:
        _require_utc(start, "start")
        _require_utc(end, "end")
        table = "bars_1m" if timeframe == "1m" else "bars_daily"
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT timestamp, open, high, low, close, volume
                FROM {table}
                WHERE symbol = $1 AND timestamp >= $2 AND timestamp < $3
                ORDER BY timestamp ASC
                """,
                symbol, start, end,
            )
        return [dict(r) for r in rows]

    async def get_bar_range(
        self, symbol: str, timeframe: str
    ) -> tuple[Optional[datetime], Optional[datetime]]:
        """Return (earliest_ts, latest_ts) for (symbol, timeframe), or (None, None)."""
        table = "bars_1m" if timeframe == "1m" else "bars_daily"
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT MIN(timestamp) AS lo, MAX(timestamp) AS hi "
                f"FROM {table} WHERE symbol = $1",
                symbol,
            )
        if row is None or row["lo"] is None:
            return None, None
        return row["lo"], row["hi"]

    async def query_latest_close(
        self, symbol: str, at: datetime,
    ) -> Optional[float]:
        """Return the close of the most recent bars_1m row at-or-before ``at``,
        or None if no such row exists.

        Used by the meta-shadow inference path to scale ATR% by the real
        close (atr_pct = atr_14 / close) instead of the sma_20 stand-in
        previously carried on the feature row. Restricted to the 1-minute
        table because meta-shadow only fires on 1m feature events.
        """
        _require_utc(at, "at")
        async with self.pool.acquire() as conn:
            close = await conn.fetchval(
                """
                SELECT close FROM bars_1m
                WHERE symbol = $1 AND timestamp <= $2
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                symbol, at,
            )
        return float(close) if close is not None else None

    async def insert_feature_row(self, row: FeatureRow) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO features (symbol, timestamp, timeframe, features)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (symbol, timestamp, timeframe)
                DO UPDATE SET features = $4
                """,
                row.symbol,
                row.timestamp,
                row.timeframe,
                json.dumps(row.features),
            )

    async def insert_feature_rows(self, rows: List[FeatureRow]) -> None:
        if not rows:
            return
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO features (symbol, timestamp, timeframe, features)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (symbol, timestamp, timeframe)
                DO UPDATE SET features = $4
                """,
                [
                    (r.symbol, r.timestamp, r.timeframe, json.dumps(r.features))
                    for r in rows
                ],
            )

    async def query_feature_rows(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 100,
    ) -> List[FeatureRow]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, timestamp, timeframe, features
                FROM features
                WHERE symbol = $1 AND timeframe = $2
                ORDER BY timestamp DESC
                LIMIT $3
                """,
                symbol,
                timeframe,
                limit,
            )
        return [
            FeatureRow(
                symbol=r["symbol"],
                timestamp=r["timestamp"],
                timeframe=r["timeframe"],
                features=json.loads(r["features"]),
            )
            for r in rows
        ]

    async def insert_label(self, label: Label) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO labels (symbol, timestamp, timeframe, horizon_minutes,
                                    forward_return, direction)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (symbol, timestamp, timeframe, horizon_minutes)
                DO UPDATE SET forward_return = $5, direction = $6
                """,
                label.symbol,
                label.timestamp,
                label.timeframe,
                label.horizon_minutes,
                label.forward_return,
                label.direction,
            )

    async def insert_labels(self, labels: List[Label]) -> None:
        if not labels:
            return
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO labels (symbol, timestamp, timeframe, horizon_minutes,
                                    forward_return, direction)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (symbol, timestamp, timeframe, horizon_minutes)
                DO UPDATE SET forward_return = $5, direction = $6
                """,
                [
                    (
                        l.symbol, l.timestamp, l.timeframe,
                        l.horizon_minutes, l.forward_return, l.direction,
                    )
                    for l in labels
                ],
            )

    async def query_labels(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        limit: int = 100,
    ) -> List[Label]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, timestamp, timeframe, horizon_minutes,
                       forward_return, direction
                FROM labels
                WHERE symbol = $1 AND timeframe = $2 AND horizon_minutes = $3
                ORDER BY timestamp DESC
                LIMIT $4
                """,
                symbol,
                timeframe,
                horizon_minutes,
                limit,
            )
        return [
            Label(
                symbol=r["symbol"],
                timestamp=r["timestamp"],
                timeframe=r["timeframe"],
                horizon_minutes=r["horizon_minutes"],
                forward_return=r["forward_return"],
                direction=r["direction"],
            )
            for r in rows
        ]

    async def query_training_data(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        *,
        as_of: datetime,
        limit: int = 10000,
    ) -> list[dict]:
        """
        Query training rows where feature_timestamp + horizon <= as_of.

        as_of is a REQUIRED keyword argument to prevent accidental omission
        that would leak future data into training.
        """
        _require_utc(as_of, "as_of")
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT f.timestamp, f.features, l.direction, l.forward_return
                FROM features f
                INNER JOIN labels l
                    ON f.symbol = l.symbol
                    AND f.timestamp = l.timestamp
                    AND f.timeframe = l.timeframe
                WHERE f.symbol = $1 AND f.timeframe = $2 AND l.horizon_minutes = $3
                  AND f.timestamp + ($3 * INTERVAL '1 minute') <= $4
                ORDER BY f.timestamp ASC
                LIMIT $5
                """,
                symbol, timeframe, horizon_minutes, as_of, limit,
            )
        return [dict(r) for r in rows]

    async def insert_model_metadata(
        self,
        meta: ModelMetadata,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> int:
        run_id = scope.backtest_run_id if scope else None
        fold = scope.fold_index if scope else None
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO model_registry
                    (symbol, timeframe, horizon_minutes, model_type, artifact_path,
                     metrics, feature_columns, trained_at, train_samples, test_samples,
                     is_active, estimator_type, backtest_run_id, fold_index)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                RETURNING id
                """,
                meta.symbol,
                meta.timeframe,
                meta.horizon_minutes,
                meta.model_type,
                meta.artifact_path,
                json.dumps(meta.metrics),
                json.dumps(meta.feature_columns),
                meta.trained_at,
                meta.train_samples,
                meta.test_samples,
                meta.is_active,
                meta.estimator_type,
                run_id,
                fold,
            )
        return row_id

    async def query_active_model(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> Optional[ModelMetadata]:
        scope_clause, scope_args = _scope_filter(scope, next_arg=4)
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT id, symbol, timeframe, horizon_minutes, model_type,
                       artifact_path, metrics, feature_columns, trained_at,
                       train_samples, test_samples, is_active, estimator_type,
                       backtest_run_id, fold_index
                FROM model_registry
                WHERE symbol = $1 AND timeframe = $2
                    AND horizon_minutes = $3 AND is_active = TRUE
                    {scope_clause}
                ORDER BY trained_at DESC
                LIMIT 1
                """,
                symbol, timeframe, horizon_minutes, *scope_args,
            )
        if row is None:
            return None
        return ModelMetadata(
            id=row["id"],
            symbol=row["symbol"],
            timeframe=row["timeframe"],
            horizon_minutes=row["horizon_minutes"],
            model_type=row["model_type"],
            artifact_path=row["artifact_path"],
            metrics=json.loads(row["metrics"]),
            feature_columns=json.loads(row["feature_columns"]),
            trained_at=row["trained_at"],
            train_samples=row["train_samples"],
            test_samples=row["test_samples"],
            is_active=row["is_active"],
            estimator_type=row["estimator_type"],
        )

    async def query_all_active_models(
        self,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> List[ModelMetadata]:
        scope_clause, scope_args = _scope_filter(scope, next_arg=1)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT id, symbol, timeframe, horizon_minutes, model_type,
                       artifact_path, metrics, feature_columns, trained_at,
                       train_samples, test_samples, is_active, estimator_type,
                       backtest_run_id, fold_index
                FROM model_registry
                WHERE is_active = TRUE
                    {scope_clause}
                ORDER BY symbol, timeframe, horizon_minutes
                """,
                *scope_args,
            )
        return [
            ModelMetadata(
                id=r["id"],
                symbol=r["symbol"],
                timeframe=r["timeframe"],
                horizon_minutes=r["horizon_minutes"],
                model_type=r["model_type"],
                artifact_path=r["artifact_path"],
                metrics=json.loads(r["metrics"]),
                feature_columns=json.loads(r["feature_columns"]),
                trained_at=r["trained_at"],
                train_samples=r["train_samples"],
                test_samples=r["test_samples"],
                is_active=r["is_active"],
                estimator_type=r["estimator_type"],
            )
            for r in rows
        ]

    async def deactivate_models(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        *,
        model_type: Optional[str] = None,
        estimator_type: Optional[str] = None,
        artifact_path_like: Optional[str] = None,
        scope: Optional[BacktestScope] = None,
    ) -> None:
        """Deactivate active models for the given key.

        When `model_type` is given, only rows with that model_type are
        deactivated. This is required so retraining one model_type (e.g.,
        lstm) does not silently flip another model_type's just-trained
        active row to is_active=FALSE — that bug left cpu-phase base models
        orphaned once the gpu phase ran. When `estimator_type` is also
        given, the predicate narrows further; this matters for meta models
        where multiple estimator_types share a model_type.
        When `artifact_path_like` is given (an SQL LIKE pattern), the
        predicate narrows to rows whose artifact_path matches. This is the
        only way to scope a deactivation to a meta direction (long vs.
        short) since direction is encoded in the artifact filename rather
        than as a column. Without it, training the second direction's
        meta_entry deactivated the first direction's just-inserted active
        row, leaving only one direction alive per symbol.
        With all filters None, all active rows for the (symbol, timeframe,
        horizon, scope) key are deactivated (legacy behavior, preserved
        so backtest scope cleanup and other call sites still work).
        """
        scope_clause, scope_args = _scope_filter(scope, next_arg=4)
        extra_clauses: list[str] = []
        extra_args: list = []
        next_idx = 4 + len(scope_args)
        if model_type is not None:
            extra_clauses.append(f"AND model_type = ${next_idx}")
            extra_args.append(model_type)
            next_idx += 1
        if estimator_type is not None:
            extra_clauses.append(f"AND estimator_type = ${next_idx}")
            extra_args.append(estimator_type)
            next_idx += 1
        if artifact_path_like is not None:
            extra_clauses.append(f"AND artifact_path LIKE ${next_idx}")
            extra_args.append(artifact_path_like)
            next_idx += 1
        extra = (" " + " ".join(extra_clauses)) if extra_clauses else ""
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE model_registry
                SET is_active = FALSE
                WHERE symbol = $1 AND timeframe = $2
                    AND horizon_minutes = $3 AND is_active = TRUE
                    {scope_clause}{extra}
                """,
                symbol, timeframe, horizon_minutes, *scope_args, *extra_args,
            )

    async def insert_prediction(
        self,
        pred: Prediction,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> None:
        run_id = scope.backtest_run_id if scope else None
        fold = scope.fold_index if scope else None
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO predictions
                    (symbol, timestamp, timeframe, horizon_minutes,
                     model_id, predicted_direction, confidence,
                     backtest_run_id, fold_index)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (symbol, timestamp, timeframe, horizon_minutes)
                DO UPDATE SET model_id = $5, predicted_direction = $6, confidence = $7
                """,
                pred.symbol, pred.timestamp, pred.timeframe,
                pred.horizon_minutes, pred.model_id,
                pred.predicted_direction, pred.confidence,
                run_id, fold,
            )

    async def insert_predictions(
        self,
        preds: List[Prediction],
        *,
        scope: Optional[BacktestScope] = None,
    ) -> None:
        if not preds:
            return
        run_id = scope.backtest_run_id if scope else None
        fold = scope.fold_index if scope else None
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO predictions
                    (symbol, timestamp, timeframe, horizon_minutes,
                     model_id, predicted_direction, confidence,
                     backtest_run_id, fold_index)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (symbol, timestamp, timeframe, horizon_minutes)
                DO UPDATE SET model_id = $5, predicted_direction = $6, confidence = $7
                """,
                [
                    (
                        p.symbol, p.timestamp, p.timeframe,
                        p.horizon_minutes, p.model_id,
                        p.predicted_direction, p.confidence,
                        run_id, fold,
                    )
                    for p in preds
                ],
            )

    async def query_predictions(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        limit: int = 100,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> List[Prediction]:
        scope_clause, scope_args = _scope_filter(scope, next_arg=4)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT symbol, timestamp, timeframe, horizon_minutes,
                       model_id, predicted_direction, confidence
                FROM predictions
                WHERE symbol = $1 AND timeframe = $2 AND horizon_minutes = $3
                    {scope_clause}
                ORDER BY timestamp DESC
                LIMIT ${4 + len(scope_args)}
                """,
                symbol, timeframe, horizon_minutes, *scope_args, limit,
            )
        return [
            Prediction(
                symbol=r["symbol"],
                timestamp=r["timestamp"],
                timeframe=r["timeframe"],
                horizon_minutes=r["horizon_minutes"],
                model_id=r["model_id"],
                predicted_direction=r["predicted_direction"],
                confidence=r["confidence"],
            )
            for r in rows
        ]

    async def query_models_for_symbol(
        self,
        symbol: str,
        *,
        include_inactive: bool = True,
        scope: Optional[BacktestScope] = None,
    ) -> list[ModelMetadata]:
        """Return all models for `symbol`, including inactive (i.e. older
        retrain cycles). Used by the meta-replay path to map old prediction
        model_ids back to their logical slot (timeframe, horizon, model_type)
        so predictions from prior model versions still teach meta-models
        after a base retrain."""
        scope_clause, scope_args = _scope_filter(scope, next_arg=2)
        active_clause = "" if include_inactive else " AND is_active = TRUE"
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT id, symbol, timeframe, horizon_minutes, model_type,
                       artifact_path, metrics, feature_columns, trained_at,
                       train_samples, test_samples, is_active, estimator_type,
                       backtest_run_id, fold_index
                FROM model_registry
                WHERE symbol = $1{active_clause}
                    {scope_clause}
                ORDER BY trained_at DESC
                """,
                symbol, *scope_args,
            )
        return [
            ModelMetadata(
                id=r["id"],
                symbol=r["symbol"],
                timeframe=r["timeframe"],
                horizon_minutes=r["horizon_minutes"],
                model_type=r["model_type"],
                artifact_path=r["artifact_path"],
                metrics=json.loads(r["metrics"]),
                feature_columns=json.loads(r["feature_columns"]),
                trained_at=r["trained_at"],
                train_samples=r["train_samples"],
                test_samples=r["test_samples"],
                is_active=r["is_active"],
                estimator_type=r["estimator_type"],
            )
            for r in rows
        ]

    async def query_prediction_outcomes_for_slot(
        self,
        *,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        model_type: str,
        limit: int = 200,
    ) -> list[dict]:
        """Same shape as query_prediction_outcomes(model_id), but filters
        by a logical slot (symbol, timeframe, horizon, model_type) instead
        of a single model_id. Aggregates outcomes across every model_id
        the symbol has ever had at that slot — needed so a freshly-retrained
        base model inherits its slot's accuracy history immediately rather
        than starting at zero on every retrain."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    p.predicted_direction,
                    p.confidence,
                    b_exit.close - b_entry.close AS price_move,
                    CASE
                        WHEN p.predicted_direction = 1
                             AND b_exit.close > b_entry.close THEN TRUE
                        WHEN p.predicted_direction = -1
                             AND b_exit.close < b_entry.close THEN TRUE
                        WHEN p.predicted_direction = 0
                             AND ABS(b_exit.close - b_entry.close)
                                 / NULLIF(b_entry.close, 0) < 0.001 THEN TRUE
                        ELSE FALSE
                    END AS is_correct
                FROM predictions p
                JOIN model_registry mr ON mr.id = p.model_id
                JOIN bars_1m b_entry
                    ON b_entry.symbol = p.symbol
                    AND b_entry.timestamp = p.timestamp
                JOIN bars_1m b_exit
                    ON b_exit.symbol = p.symbol
                    AND b_exit.timestamp = p.timestamp
                        + (p.horizon_minutes * interval '1 minute')
                WHERE mr.symbol = $1 AND mr.timeframe = $2
                  AND mr.horizon_minutes = $3 AND mr.model_type = $4
                  AND p.backtest_run_id IS NULL AND p.fold_index IS NULL
                ORDER BY p.timestamp DESC
                LIMIT $5
                """,
                symbol, timeframe, horizon_minutes, model_type, limit,
            )
        return [dict(r) for r in rows]

    async def count_predictions_in_window(
        self,
        *,
        start: datetime,
        end: datetime,
    ) -> int:
        """Count live predictions (any symbol/timeframe/horizon) in [start, end).

        Used by the meta-retrain pre-flight check: if zero, the meta phase
        logs once and skips, instead of running N × M trainer invocations
        that all hit "Insufficient data" silently.
        """
        _require_utc(start, "start")
        _require_utc(end, "end")
        async with self.pool.acquire() as conn:
            n = await conn.fetchval(
                """
                SELECT COUNT(*)::INT FROM predictions
                WHERE timestamp >= $1 AND timestamp < $2
                  AND backtest_run_id IS NULL AND fold_index IS NULL
                """,
                start, end,
            )
        return int(n or 0)

    async def query_predictions_in_range(
        self,
        *,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> list[dict]:
        """Return prediction rows in [start, end) for symbol/timeframe.

        Returns dicts (not Prediction dataclasses) so callers can dict-key
        them by timestamp. Spans all horizons and all model_ids in range.
        """
        _require_utc(start, "start")
        _require_utc(end, "end")
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, timestamp, timeframe, horizon_minutes,
                       model_id, predicted_direction, confidence
                FROM predictions
                WHERE symbol = $1 AND timeframe = $2
                  AND timestamp >= $3 AND timestamp < $4
                  AND backtest_run_id IS NULL AND fold_index IS NULL
                ORDER BY timestamp ASC
                """,
                symbol, timeframe, start, end,
            )
        return [dict(r) for r in rows]

    async def insert_order(
        self,
        order: Order,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> int:
        run_id = scope.backtest_run_id if scope else None
        fold = scope.fold_index if scope else None
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO orders
                    (symbol, side, quantity, order_type, submitted_at,
                     signal_strength, alpaca_order_id, status,
                     filled_at, filled_price, filled_quantity, limit_price,
                     origin, last_reconciled_at, reconcile_source,
                     backtest_run_id, fold_index)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                        $13, $14, $15, $16, $17)
                RETURNING id
                """,
                order.symbol, order.side, order.quantity, order.order_type,
                order.submitted_at, order.signal_strength,
                order.alpaca_order_id, order.status,
                order.filled_at, order.filled_price, order.filled_quantity,
                order.limit_price, order.origin, order.last_reconciled_at,
                order.reconcile_source,
                run_id, fold,
            )
        return row_id

    async def insert_order_if_new(
        self,
        order: Order,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> Optional[int]:
        """INSERT that does nothing on alpaca_order_id conflict.
        Returns the inserted row id, or None if a row already existed."""
        run_id = scope.backtest_run_id if scope else None
        fold = scope.fold_index if scope else None
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                """
                INSERT INTO orders
                  (symbol, side, quantity, order_type, submitted_at,
                   signal_strength, alpaca_order_id, status,
                   origin, last_reconciled_at, reconcile_source,
                   backtest_run_id, fold_index)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,
                        COALESCE($9,'internal'),$10,$11,$12,$13)
                ON CONFLICT (alpaca_order_id) WHERE alpaca_order_id IS NOT NULL DO NOTHING
                RETURNING id
                """,
                order.symbol, order.side, order.quantity, order.order_type,
                order.submitted_at, order.signal_strength,
                order.alpaca_order_id, order.status,
                order.origin, order.last_reconciled_at, order.reconcile_source,
                run_id, fold,
            )

    async def update_order_status(
        self,
        order_id: int,
        status: str,
        filled_at: Optional[datetime] = None,
        filled_price: Optional[float] = None,
        filled_quantity: Optional[float] = None,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> None:
        if filled_at is not None:
            _require_utc(filled_at, "filled_at")
        scope_clause, scope_args = _scope_filter(scope, next_arg=6)
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE orders
                SET status = $2,
                    filled_at = COALESCE($3, filled_at),
                    filled_price = COALESCE($4, filled_price),
                    filled_quantity = COALESCE($5, filled_quantity)
                WHERE id = $1
                    {scope_clause}
                """,
                order_id, status, filled_at, filled_price, filled_quantity,
                *scope_args,
            )

    async def query_orders(
        self,
        symbol: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> list[Order]:
        query = """SELECT id, symbol, side, quantity, order_type, submitted_at,
                          signal_strength, alpaca_order_id, status,
                          filled_at, filled_price, filled_quantity, limit_price,
                          origin, last_reconciled_at, reconcile_source
                   FROM orders WHERE TRUE"""
        args: list = []
        idx = 1

        if symbol is not None:
            query += f" AND symbol = ${idx}"
            args.append(symbol)
            idx += 1

        if status is not None:
            query += f" AND status = ${idx}"
            args.append(status)
            idx += 1

        scope_clause, scope_args = _scope_filter(scope, next_arg=idx)
        query += scope_clause
        args.extend(scope_args)
        idx += len(scope_args)

        query += f" ORDER BY submitted_at DESC LIMIT ${idx}"
        args.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [
            Order(
                id=r["id"],
                symbol=r["symbol"],
                side=r["side"],
                quantity=r["quantity"],
                order_type=r["order_type"],
                submitted_at=r["submitted_at"],
                signal_strength=r["signal_strength"],
                alpaca_order_id=r["alpaca_order_id"],
                status=r["status"],
                filled_at=r["filled_at"],
                filled_price=r["filled_price"],
                filled_quantity=r["filled_quantity"],
                limit_price=r["limit_price"],
                origin=r["origin"],
                last_reconciled_at=r["last_reconciled_at"],
                reconcile_source=r["reconcile_source"],
            )
            for r in rows
        ]

    async def get_order_by_alpaca_id(
        self,
        alpaca_order_id: str,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> Optional[Order]:
        scope_clause, scope_args = _scope_filter(scope, next_arg=2)
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT id, symbol, side, quantity, order_type, submitted_at,
                       signal_strength, alpaca_order_id, status,
                       filled_at, filled_price, filled_quantity, limit_price,
                       origin, last_reconciled_at, reconcile_source
                FROM orders
                WHERE alpaca_order_id = $1
                    {scope_clause}
                """,
                alpaca_order_id, *scope_args,
            )
        if row is None:
            return None
        return Order(
            id=row["id"], symbol=row["symbol"], side=row["side"],
            quantity=row["quantity"], order_type=row["order_type"],
            submitted_at=row["submitted_at"],
            signal_strength=row["signal_strength"],
            alpaca_order_id=row["alpaca_order_id"], status=row["status"],
            filled_at=row["filled_at"], filled_price=row["filled_price"],
            filled_quantity=row["filled_quantity"],
            limit_price=row["limit_price"],
            origin=row["origin"],
            last_reconciled_at=row["last_reconciled_at"],
            reconcile_source=row["reconcile_source"],
        )

    async def query_non_terminal_orders(
        self, within: timedelta,
    ) -> list[Order]:
        """Return non-terminal orders submitted within the given lookback window."""
        cutoff = datetime.now(timezone.utc) - within
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, symbol, side, quantity, order_type, submitted_at,
                       signal_strength, alpaca_order_id, status,
                       filled_at, filled_price, filled_quantity, limit_price,
                       origin, last_reconciled_at, reconcile_source
                FROM orders
                WHERE status NOT IN ('filled','cancelled','rejected','expired')
                  AND submitted_at >= $1
                  AND alpaca_order_id IS NOT NULL
                ORDER BY submitted_at DESC
                """,
                cutoff,
            )
        return [
            Order(
                id=r["id"], symbol=r["symbol"], side=r["side"],
                quantity=r["quantity"], order_type=r["order_type"],
                submitted_at=r["submitted_at"],
                signal_strength=r["signal_strength"],
                alpaca_order_id=r["alpaca_order_id"], status=r["status"],
                filled_at=r["filled_at"], filled_price=r["filled_price"],
                filled_quantity=r["filled_quantity"],
                limit_price=r["limit_price"],
                origin=r["origin"],
                last_reconciled_at=r["last_reconciled_at"],
                reconcile_source=r["reconcile_source"],
            )
            for r in rows
        ]

    async def upsert_order_from_alpaca(
        self, snapshot: dict, source: str,
    ) -> UpsertResult:
        alpaca_id = snapshot["id"]
        canonical_status = _normalize_alpaca_status(snapshot["status"])
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # SELECT FOR UPDATE captures the pre-image (for old_status
                # in the returned UpsertResult) and locks any existing row.
                # On a missing row no lock is taken, so two concurrent
                # writers can both observe existing=None - the INSERT ...
                # ON CONFLICT below makes the actual write atomic at the
                # unique-index level. That is what eliminates the
                # UniqueViolationError race between OrderExecutor's
                # submit-side INSERT and the trade-update stream consumer.
                existing = await conn.fetchrow(
                    "SELECT status, origin FROM orders "
                    "WHERE alpaca_order_id = $1 FOR UPDATE",
                    alpaca_id,
                )
                old_status: Optional[str] = (
                    existing["status"] if existing is not None else None
                )
                now_ts = datetime.now(timezone.utc)
                row = await conn.fetchrow(
                    """
                    INSERT INTO orders
                      (symbol, side, quantity, order_type, submitted_at,
                       signal_strength, alpaca_order_id, status,
                       filled_at, filled_price, filled_quantity,
                       origin, last_reconciled_at, reconcile_source)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
                            'external',$12,$13)
                    ON CONFLICT (alpaca_order_id)
                      WHERE alpaca_order_id IS NOT NULL
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        filled_at = COALESCE(EXCLUDED.filled_at,
                                             orders.filled_at),
                        filled_price = COALESCE(EXCLUDED.filled_price,
                                                orders.filled_price),
                        filled_quantity = COALESCE(EXCLUDED.filled_quantity,
                                                   orders.filled_quantity),
                        last_reconciled_at = EXCLUDED.last_reconciled_at,
                        reconcile_source = EXCLUDED.reconcile_source
                    RETURNING id, symbol, side, quantity, order_type,
                              submitted_at, signal_strength, alpaca_order_id,
                              status, filled_at, filled_price, filled_quantity,
                              limit_price, origin, last_reconciled_at,
                              reconcile_source,
                              (xmax = 0) AS was_inserted
                    """,
                    snapshot["symbol"],
                    snapshot["side"],
                    float(snapshot["qty"]),
                    snapshot.get("order_type", "market"),
                    snapshot.get("submitted_at") or now_ts,
                    0.0,
                    alpaca_id,
                    canonical_status,
                    snapshot.get("filled_at"),
                    snapshot.get("filled_avg_price"),
                    snapshot.get("filled_qty"),
                    now_ts,
                    source,
                )
                inserted = bool(row["was_inserted"])
        order = Order(
            id=row["id"], symbol=row["symbol"], side=row["side"],
            quantity=row["quantity"], order_type=row["order_type"],
            submitted_at=row["submitted_at"],
            signal_strength=row["signal_strength"],
            alpaca_order_id=row["alpaca_order_id"], status=row["status"],
            filled_at=row["filled_at"], filled_price=row["filled_price"],
            filled_quantity=row["filled_quantity"],
            limit_price=row["limit_price"],
            origin=row["origin"],
            last_reconciled_at=row["last_reconciled_at"],
            reconcile_source=row["reconcile_source"],
        )
        return UpsertResult(
            order=order, old_status=old_status,
            new_status=canonical_status, inserted=inserted,
        )

    # ── Trade methods ──────────────────────────────────────────────

    async def insert_trade(self, trade: Trade) -> int:
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO trades
                    (symbol, side, entry_time, exit_time, entry_price, exit_price,
                     quantity, pnl, hold_duration_seconds, entry_order_id, exit_order_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING id
                """,
                trade.symbol,
                trade.side,
                trade.entry_time,
                trade.exit_time,
                trade.entry_price,
                trade.exit_price,
                trade.quantity,
                trade.pnl,
                trade.hold_duration_seconds,
                trade.entry_order_id,
                trade.exit_order_id,
            )
        return row_id

    async def update_trade_exit(
        self,
        trade_id: int,
        exit_time: datetime,
        exit_price: float,
        exit_order_id: int,
        pnl: float,
        hold_duration_seconds: int,
    ) -> None:
        _require_utc(exit_time, "exit_time")
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE trades
                SET exit_time = $2, exit_price = $3, exit_order_id = $4,
                    pnl = $5, hold_duration_seconds = $6
                WHERE id = $1
                """,
                trade_id,
                exit_time,
                exit_price,
                exit_order_id,
                pnl,
                hold_duration_seconds,
            )

    async def get_open_trade(self, symbol: str) -> Optional[Trade]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, symbol, side, entry_time, exit_time, entry_price,
                       exit_price, quantity, pnl, hold_duration_seconds,
                       entry_order_id, exit_order_id
                FROM trades
                WHERE symbol = $1 AND exit_time IS NULL
                ORDER BY entry_time DESC
                LIMIT 1
                """,
                symbol,
            )
        if row is None:
            return None
        return Trade(
            id=row["id"],
            symbol=row["symbol"],
            side=row["side"],
            entry_time=row["entry_time"],
            exit_time=row["exit_time"],
            entry_price=row["entry_price"],
            exit_price=row["exit_price"],
            quantity=row["quantity"],
            pnl=row["pnl"],
            hold_duration_seconds=row["hold_duration_seconds"],
            entry_order_id=row["entry_order_id"],
            exit_order_id=row["exit_order_id"],
        )

    async def get_open_trades(self, symbol: str) -> list[Trade]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, symbol, side, entry_time, exit_time, entry_price,
                       exit_price, quantity, pnl, hold_duration_seconds,
                       entry_order_id, exit_order_id
                FROM trades
                WHERE symbol = $1 AND exit_time IS NULL
                ORDER BY entry_time ASC
                """,
                symbol,
            )
        return [
            Trade(
                id=r["id"],
                symbol=r["symbol"],
                side=r["side"],
                entry_time=r["entry_time"],
                exit_time=r["exit_time"],
                entry_price=r["entry_price"],
                exit_price=r["exit_price"],
                quantity=r["quantity"],
                pnl=r["pnl"],
                hold_duration_seconds=r["hold_duration_seconds"],
                entry_order_id=r["entry_order_id"],
                exit_order_id=r["exit_order_id"],
            )
            for r in rows
        ]

    async def existing_alpaca_order_ids(
        self, ids: list[str],
    ) -> set[str]:
        """Return the subset of ``ids`` already present in the orders table.

        Used by the replay CLI to skip orders that are already tracked locally;
        feeding them through ReconcilerCore would re-interpret them against the
        current DB state, which is wrong for orders that predate local tracking.
        """
        if not ids:
            return set()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT alpaca_order_id FROM orders "
                "WHERE alpaca_order_id = ANY($1::text[])",
                ids,
            )
        return {r["alpaca_order_id"] for r in rows}

    async def query_closed_trades(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[Trade]:
        if since is not None:
            _require_utc(since, "since")
        query = """SELECT id, symbol, side, entry_time, exit_time, entry_price,
                          exit_price, quantity, pnl, hold_duration_seconds,
                          entry_order_id, exit_order_id
                   FROM trades WHERE exit_time IS NOT NULL"""
        args: list = []
        idx = 1

        if symbol is not None:
            query += f" AND symbol = ${idx}"
            args.append(symbol)
            idx += 1

        if since is not None:
            query += f" AND exit_time >= ${idx}"
            args.append(since)
            idx += 1

        query += f" ORDER BY exit_time DESC LIMIT ${idx}"
        args.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [
            Trade(
                id=r["id"],
                symbol=r["symbol"],
                side=r["side"],
                entry_time=r["entry_time"],
                exit_time=r["exit_time"],
                entry_price=r["entry_price"],
                exit_price=r["exit_price"],
                quantity=r["quantity"],
                pnl=r["pnl"],
                hold_duration_seconds=r["hold_duration_seconds"],
                entry_order_id=r["entry_order_id"],
                exit_order_id=r["exit_order_id"],
            )
            for r in rows
        ]

    async def insert_trade_with_exit(self, trade: Trade) -> int:
        """Insert a trade already marked closed (exit_time, exit_price, pnl set)."""
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO trades
                    (symbol, side, entry_time, entry_price, quantity, entry_order_id,
                     exit_time, exit_price, pnl, hold_duration_seconds, exit_order_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING id
                """,
                trade.symbol, trade.side, trade.entry_time,
                trade.entry_price, trade.quantity, trade.entry_order_id,
                trade.exit_time, trade.exit_price, trade.pnl,
                trade.hold_duration_seconds, trade.exit_order_id,
            )
        return row_id

    async def query_equity_curve(
        self,
        starting_equity: float,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        max_points: int = 2000,
    ) -> list[dict]:
        """Equity curve walked from closed trades.

        Returns list of {timestamp, realized_pnl, realized_equity, drawdown,
        trailing_peak}. Downsampled to at most max_points via LTTB-like
        thinning (uniform stride — acceptable for our scale).
        """
        if from_dt is not None:
            _require_utc(from_dt, "from_dt")
        if to_dt is not None:
            _require_utc(to_dt, "to_dt")
        query = """
            SELECT exit_time, pnl
            FROM trades
            WHERE exit_time IS NOT NULL AND pnl IS NOT NULL
        """
        args: list = []
        idx = 1
        if from_dt is not None:
            query += f" AND exit_time >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            query += f" AND exit_time < ${idx}"
            args.append(to_dt); idx += 1
        query += " ORDER BY exit_time ASC"

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)

        series: list[dict] = []
        equity = starting_equity
        peak = starting_equity
        for r in rows:
            equity += float(r["pnl"])
            if equity > peak:
                peak = equity
            drawdown = (equity - peak) / peak if peak > 0 else 0.0
            series.append({
                "timestamp": r["exit_time"],
                "realized_pnl": float(r["pnl"]),
                "realized_equity": equity,
                "trailing_peak": peak,
                "drawdown": drawdown,
            })

        if len(series) > max_points:
            stride = len(series) // max_points
            series = series[::stride]
        return series

    async def query_distributions(
        self,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> dict:
        """Daily P&L, trade P&L histogram, hold-time buckets, and side breakdown.

        Returns {daily_pnl, trade_pnl_histogram, hold_time_buckets,
        side_breakdown}. Each sub-object shaped for direct Chart.js rendering.
        """
        if from_dt is not None:
            _require_utc(from_dt, "from_dt")
        if to_dt is not None:
            _require_utc(to_dt, "to_dt")
        base_where = "WHERE exit_time IS NOT NULL AND pnl IS NOT NULL"
        args: list = []
        idx = 1
        if from_dt is not None:
            base_where += f" AND exit_time >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            base_where += f" AND exit_time < ${idx}"
            args.append(to_dt); idx += 1

        async with self.pool.acquire() as conn:
            daily = await conn.fetch(
                f"""
                SELECT DATE_TRUNC('day', exit_time) AS date,
                       SUM(pnl) AS pnl, COUNT(*) AS trades
                FROM trades {base_where}
                GROUP BY DATE_TRUNC('day', exit_time)
                ORDER BY date ASC
                """,
                *args,
            )
            all_trades = await conn.fetch(
                f"""SELECT pnl, hold_duration_seconds, side FROM trades {base_where}""",
                *args,
            )

        pnls = [float(t["pnl"]) for t in all_trades]
        holds = [int(t["hold_duration_seconds"]) for t in all_trades
                 if t["hold_duration_seconds"] is not None]

        # Trade P&L histogram (20 bins).
        if pnls:
            lo, hi = min(pnls), max(pnls)
            if lo == hi:
                lo -= 1.0; hi += 1.0
            bins = 20
            width = (hi - lo) / bins
            counts = [0] * bins
            for v in pnls:
                i = min(bins - 1, int((v - lo) / width))
                counts[i] += 1
            labels = [round(lo + i * width, 2) for i in range(bins)]
        else:
            labels, counts = [], []

        # Hold-time buckets: <5m, 5-30m, 30m-2h, 2h-8h, 8h-1d, overnight, multi-day.
        hold_buckets = [
            ("<5m", 0, 300),
            ("5-30m", 300, 1800),
            ("30m-2h", 1800, 7200),
            ("2h-8h", 7200, 28800),
            ("8h-1d", 28800, 86400),
            ("overnight", 86400, 172800),
            ("multi-day", 172800, 10**9),
        ]
        hold_table = []
        for name, lo_s, hi_s in hold_buckets:
            bucket_trades = [t for t in all_trades
                              if t["hold_duration_seconds"] is not None
                              and lo_s <= t["hold_duration_seconds"] < hi_s]
            n = len(bucket_trades)
            avg_pnl = (sum(float(t["pnl"]) for t in bucket_trades) / n) if n > 0 else 0.0
            hold_table.append({"bucket": name, "trades": n, "avg_pnl": avg_pnl})

        # Side breakdown.
        def side_stats(side):
            rows = [t for t in all_trades if t["side"] == side]
            n = len(rows)
            wins = sum(1 for t in rows if float(t["pnl"]) > 0)
            total_pnl = sum(float(t["pnl"]) for t in rows)
            return {
                "side": side,
                "trades": n,
                "wins": wins,
                "win_rate": (wins / n) if n > 0 else 0.0,
                "total_pnl": total_pnl,
                "avg_pnl": (total_pnl / n) if n > 0 else 0.0,
            }

        return {
            "daily_pnl": [
                {"date": r["date"], "pnl": float(r["pnl"]), "trades": r["trades"]}
                for r in daily
            ],
            "trade_pnl_histogram": {
                "labels": labels, "counts": counts,
                "median": sorted(pnls)[len(pnls) // 2] if pnls else 0.0,
                "mean": (sum(pnls) / len(pnls)) if pnls else 0.0,
            },
            "hold_time_buckets": hold_table,
            "side_breakdown": [side_stats("long"), side_stats("short")],
        }

    async def query_streaks(
        self,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> dict:
        """Current streak (kind + length), longest streaks, and W/L ribbon.

        Returns {current_streak_kind: 'win'|'loss', current_streak_length,
        longest_win_streak, longest_loss_streak, ribbon: [{timestamp, win}]}.
        """
        if from_dt is not None:
            _require_utc(from_dt, "from_dt")
        if to_dt is not None:
            _require_utc(to_dt, "to_dt")
        query = (
            "SELECT exit_time, pnl FROM trades "
            "WHERE exit_time IS NOT NULL AND pnl IS NOT NULL"
        )
        args: list = []
        idx = 1
        if from_dt is not None:
            query += f" AND exit_time >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            query += f" AND exit_time < ${idx}"
            args.append(to_dt); idx += 1
        query += " ORDER BY exit_time ASC"

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)

        ribbon = []
        longest_win = 0
        longest_loss = 0
        run_win = 0
        run_loss = 0
        for r in rows:
            win = float(r["pnl"]) > 0
            ribbon.append({"timestamp": r["exit_time"], "win": win})
            if win:
                run_win += 1; run_loss = 0
                longest_win = max(longest_win, run_win)
            else:
                run_loss += 1; run_win = 0
                longest_loss = max(longest_loss, run_loss)

        current_kind = "win" if (ribbon and ribbon[-1]["win"]) else "loss"
        current_length = run_win if current_kind == "win" else run_loss
        if not ribbon:
            current_kind = "none"; current_length = 0
        return {
            "current_streak_kind": current_kind,
            "current_streak_length": current_length,
            "longest_win_streak": longest_win,
            "longest_loss_streak": longest_loss,
            "ribbon": ribbon,
        }

    async def query_heatmap_time_of_day(
        self,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> dict:
        """Heatmap grid: avg P&L per (weekday, half-hour UTC bucket).

        Returns {x_labels: ['00:00', '00:30', ...], y_labels: ['Mon', ...,
        'Fri'], cells: [[val|None, ...], ...]}. Half-hour buckets across full
        UTC day (48 columns). Weekdays Mon-Fri (skip weekends since equities
        don't trade).
        """
        if from_dt is not None:
            _require_utc(from_dt, "from_dt")
        if to_dt is not None:
            _require_utc(to_dt, "to_dt")
        query = (
            "SELECT EXTRACT(DOW FROM entry_time)::INT AS dow, "
            "       EXTRACT(HOUR FROM entry_time)::INT AS hh, "
            "       EXTRACT(MINUTE FROM entry_time)::INT AS mm, "
            "       pnl "
            "FROM trades WHERE exit_time IS NOT NULL AND pnl IS NOT NULL"
        )
        args: list = []
        idx = 1
        if from_dt is not None:
            query += f" AND entry_time >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            query += f" AND entry_time < ${idx}"
            args.append(to_dt); idx += 1

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)

        # 5 weekdays × 48 half-hour columns.
        x_labels = [f"{h:02d}:{m:02d}" for h in range(24) for m in (0, 30)]
        y_labels = ["Mon", "Tue", "Wed", "Thu", "Fri"]
        sums = [[0.0] * 48 for _ in range(5)]
        counts = [[0] * 48 for _ in range(5)]
        for r in rows:
            # Postgres DOW: Sun=0, Mon=1, ..., Sat=6. Skip weekends.
            dow = r["dow"]
            if dow < 1 or dow > 5:
                continue
            y = dow - 1
            x = r["hh"] * 2 + (1 if r["mm"] >= 30 else 0)
            sums[y][x] += float(r["pnl"])
            counts[y][x] += 1
        cells = [
            [(sums[y][x] / counts[y][x]) if counts[y][x] > 0 else None
             for x in range(48)]
            for y in range(5)
        ]
        return {"x_labels": x_labels, "y_labels": y_labels, "cells": cells}

    async def query_exposure_timeline(
        self,
        equity: float,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        resolution_minutes: int = 5,
    ) -> list[dict]:
        """Time-bucketed exposure stacked by symbol.

        Returns [{timestamp, total_exposure_pct, by_symbol: {SYM: pct, ...}}].
        For each bucket, sums market value of all positions open at the
        bucket's start time, divided by equity.
        """
        if from_dt is not None:
            _require_utc(from_dt, "from_dt")
        if to_dt is not None:
            _require_utc(to_dt, "to_dt")
        query = (
            "SELECT symbol, entry_time, exit_time, entry_price, quantity "
            "FROM trades WHERE exit_time IS NOT NULL"
        )
        args: list = []
        idx = 1
        if from_dt is not None:
            query += f" AND entry_time >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            query += f" AND exit_time < ${idx}"
            args.append(to_dt); idx += 1
        async with self.pool.acquire() as conn:
            trades = await conn.fetch(query, *args)

        if not trades:
            return []
        min_t = min(t["entry_time"] for t in trades) if not from_dt else from_dt
        max_t = max(t["exit_time"] for t in trades) if not to_dt else to_dt
        if max_t <= min_t:
            return []

        step = timedelta(minutes=resolution_minutes)
        timeline: list[dict] = []
        t = min_t
        while t <= max_t:
            by_symbol: dict[str, float] = {}
            total = 0.0
            for tr in trades:
                if tr["entry_time"] <= t < tr["exit_time"]:
                    mv = float(tr["entry_price"]) * float(tr["quantity"])
                    by_symbol[tr["symbol"]] = by_symbol.get(tr["symbol"], 0.0) + mv
                    total += mv
            pct = total / equity if equity > 0 else 0.0
            timeline.append({
                "timestamp": t,
                "total_exposure_pct": pct,
                "by_symbol": {k: v / equity for k, v in by_symbol.items()},
            })
            t += step
        return timeline

    async def query_benchmark(
        self,
        starting_equity: float,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> dict:
        """Strategy equity vs SPY buy-and-hold + stats.

        Returns {strategy_series, spy_series, stats:{annualized_return,
        annualized_vol, sharpe, max_drawdown, beta, alpha}}.
        """
        if from_dt is not None:
            _require_utc(from_dt, "from_dt")
        if to_dt is not None:
            _require_utc(to_dt, "to_dt")
        import math
        # Strategy equity (daily bucketed for clean alignment with SPY dailies).
        equity_query = (
            "SELECT DATE_TRUNC('day', exit_time) AS day, SUM(pnl) AS pnl "
            "FROM trades WHERE exit_time IS NOT NULL AND pnl IS NOT NULL"
        )
        args: list = []
        idx = 1
        if from_dt is not None:
            equity_query += f" AND exit_time >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            equity_query += f" AND exit_time < ${idx}"
            args.append(to_dt); idx += 1
        equity_query += " GROUP BY DATE_TRUNC('day', exit_time) ORDER BY day ASC"

        spy_query = (
            "SELECT timestamp, close FROM bars_daily "
            "WHERE symbol = 'SPY'"
        )
        spy_args: list = []
        sidx = 1
        if from_dt is not None:
            spy_query += f" AND timestamp >= ${sidx}"
            spy_args.append(from_dt); sidx += 1
        if to_dt is not None:
            spy_query += f" AND timestamp < ${sidx}"
            spy_args.append(to_dt); sidx += 1
        spy_query += " ORDER BY timestamp ASC"

        async with self.pool.acquire() as conn:
            equity_rows = await conn.fetch(equity_query, *args)
            spy_rows = await conn.fetch(spy_query, *spy_args)

        # Build strategy series: cumulative equity per day.
        strat = []
        e = starting_equity
        for r in equity_rows:
            e += float(r["pnl"])
            strat.append({"timestamp": r["day"], "equity": e})

        # Normalize SPY to starting_equity.
        spy_series = []
        if spy_rows:
            start_close = float(spy_rows[0]["close"])
            for r in spy_rows:
                spy_series.append({
                    "timestamp": r["timestamp"],
                    "equity": starting_equity * float(r["close"]) / start_close,
                })

        # Compute daily strategy returns + SPY returns.
        def daily_returns(series):
            out = []
            for i in range(1, len(series)):
                prev = series[i - 1]["equity"]
                cur = series[i]["equity"]
                if prev > 0:
                    out.append((cur / prev) - 1.0)
            return out

        strat_returns = daily_returns(strat)
        spy_returns = daily_returns(spy_series)

        def mean(xs): return sum(xs) / len(xs) if xs else 0.0
        def var(xs):
            if len(xs) < 2: return 0.0
            m = mean(xs)
            return sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
        def stdev(xs): return math.sqrt(var(xs))

        def annualize_return(xs):
            if not xs: return 0.0
            # Compound product: Π(1+r_i) - 1, then annualize by days_per_year ^ (1/len).
            prod = 1.0
            for r in xs: prod *= (1.0 + r)
            if prod <= 0: return -1.0
            return prod ** (252 / len(xs)) - 1.0 if len(xs) > 0 else 0.0

        ann_return = annualize_return(strat_returns)
        ann_vol = stdev(strat_returns) * math.sqrt(252)
        rf = 0.0  # risk-free rate simplification
        sharpe = (ann_return - rf) / ann_vol if ann_vol > 0 else 0.0

        # Max drawdown from strategy series.
        max_dd = 0.0
        peak = starting_equity
        for p in strat:
            if p["equity"] > peak: peak = p["equity"]
            dd = (p["equity"] - peak) / peak if peak > 0 else 0.0
            if dd < max_dd: max_dd = dd

        # Beta & alpha vs SPY — align return series by length (truncate to min).
        n = min(len(strat_returns), len(spy_returns))
        beta = alpha = 0.0
        if n >= 2:
            a = strat_returns[-n:]
            b = spy_returns[-n:]
            mb = mean(b)
            vb = var(b)
            if vb > 0:
                cov = sum((a[i] - mean(a)) * (b[i] - mb) for i in range(n)) / (n - 1)
                beta = cov / vb
                ann_spy = annualize_return(b)
                alpha = ann_return - (rf + beta * (ann_spy - rf))

        return {
            "strategy_series": strat,
            "spy_series": spy_series,
            "stats": {
                "annualized_return": ann_return,
                "annualized_vol": ann_vol,
                "sharpe": sharpe,
                "max_drawdown": max_dd,
                "beta": beta,
                "alpha": alpha,
            },
        }

    async def query_symbol_drilldown(
        self,
        symbol: str,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> dict:
        """Per-symbol payload: equity series, trades, side breakdown,
        win rate over time (daily).
        """
        if from_dt is not None:
            _require_utc(from_dt, "from_dt")
        if to_dt is not None:
            _require_utc(to_dt, "to_dt")
        base_where = "WHERE symbol = $1 AND exit_time IS NOT NULL AND pnl IS NOT NULL"
        args: list = [symbol]
        idx = 2
        if from_dt is not None:
            base_where += f" AND exit_time >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            base_where += f" AND exit_time < ${idx}"
            args.append(to_dt); idx += 1

        async with self.pool.acquire() as conn:
            all_trades = await conn.fetch(
                f"""
                SELECT id, side, entry_time, exit_time, entry_price,
                       exit_price, quantity, pnl, hold_duration_seconds
                FROM trades {base_where}
                ORDER BY exit_time ASC
                """,
                *args,
            )
            daily = await conn.fetch(
                f"""
                SELECT DATE_TRUNC('day', exit_time) AS date,
                       COUNT(*) AS total,
                       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END)::INT AS wins,
                       SUM(pnl) AS pnl
                FROM trades {base_where}
                GROUP BY DATE_TRUNC('day', exit_time)
                ORDER BY date ASC
                """,
                *args,
            )

        # Per-symbol equity: start at 0, cumulate P&L.
        equity = 0.0
        peak = 0.0
        equity_series = []
        for t in all_trades:
            equity += float(t["pnl"])
            if equity > peak: peak = equity
            dd = (equity - peak) / peak if peak > 0 else 0.0
            equity_series.append({
                "timestamp": t["exit_time"],
                "equity": equity, "drawdown": dd,
            })

        def side_stats(side):
            rows = [t for t in all_trades if t["side"] == side]
            n = len(rows)
            wins = sum(1 for t in rows if float(t["pnl"]) > 0)
            total_pnl = sum(float(t["pnl"]) for t in rows)
            return {
                "side": side, "trades": n, "wins": wins,
                "win_rate": (wins / n) if n > 0 else 0.0,
                "total_pnl": total_pnl,
            }

        return {
            "symbol": symbol,
            "trade_count": len(all_trades),
            "equity_series": equity_series,
            "trades": [
                {
                    "id": t["id"], "side": t["side"],
                    "entry_time": t["entry_time"], "exit_time": t["exit_time"],
                    "entry_price": float(t["entry_price"]),
                    "exit_price": float(t["exit_price"]) if t["exit_price"] else None,
                    "quantity": float(t["quantity"]),
                    "pnl": float(t["pnl"]) if t["pnl"] else None,
                    "hold_duration_seconds": t["hold_duration_seconds"],
                }
                for t in all_trades
            ],
            "side_breakdown": [side_stats("long"), side_stats("short")],
            "win_rate_over_time": [
                {
                    "date": r["date"],
                    "total": r["total"],
                    "wins": r["wins"],
                    "win_rate": (r["wins"] / r["total"]) if r["total"] > 0 else 0.0,
                    "pnl": float(r["pnl"] or 0),
                }
                for r in daily
            ],
        }

    async def query_trades_closed_before(
        self, symbol: str, as_of: datetime
    ) -> list[dict]:
        """Trades for symbol with exit_time strictly before as_of. Excludes open trades."""
        _require_utc(as_of, "as_of")
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, symbol, side, entry_time, entry_price, quantity,
                       exit_time, exit_price, pnl
                FROM trades
                WHERE symbol = $1 AND exit_time IS NOT NULL AND exit_time < $2
                ORDER BY exit_time ASC
                """,
                symbol, as_of,
            )
        return [dict(r) for r in rows]

    # ── Model status methods ───────────────────────────────────────

    async def upsert_model_status(
        self, model_id: int, status: str, reason: str,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO model_status (model_id, status, reason, updated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (model_id)
                DO UPDATE SET status = $2, reason = $3, updated_at = NOW()
                """,
                model_id,
                status,
                reason,
            )

    async def get_all_model_statuses(self) -> list[ModelStatus]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT model_id, status, reason, updated_at
                FROM model_status
                ORDER BY model_id
                """
            )
        return [
            ModelStatus(
                model_id=r["model_id"],
                status=r["status"],
                reason=r["reason"] or "",
                updated_at=r["updated_at"],
            )
            for r in rows
        ]

    # ── Prediction outcome methods ─────────────────────────────────

    async def query_prediction_outcomes(
        self, model_id: int, limit: int = 100,
    ) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    p.predicted_direction,
                    p.confidence,
                    b_exit.close - b_entry.close AS price_move,
                    CASE
                        WHEN p.predicted_direction = 1
                             AND b_exit.close > b_entry.close THEN TRUE
                        WHEN p.predicted_direction = -1
                             AND b_exit.close < b_entry.close THEN TRUE
                        WHEN p.predicted_direction = 0
                             AND ABS(b_exit.close - b_entry.close)
                                 / NULLIF(b_entry.close, 0) < 0.001 THEN TRUE
                        ELSE FALSE
                    END AS is_correct
                FROM predictions p
                JOIN bars_1m b_entry
                    ON b_entry.symbol = p.symbol
                    AND b_entry.timestamp = p.timestamp
                JOIN bars_1m b_exit
                    ON b_exit.symbol = p.symbol
                    AND b_exit.timestamp = p.timestamp
                        + (p.horizon_minutes * interval '1 minute')
                WHERE p.model_id = $1
                ORDER BY p.timestamp DESC
                LIMIT $2
                """,
                model_id,
                limit,
            )
        return [dict(r) for r in rows]

    # ── Model performance query methods ───────────────────────────

    async def query_accuracy_history(
        self,
        model_id: int,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> list[dict]:
        """Daily-bucketed hit rate for a model over [from_dt, to_dt).

        Returns rows: {date, total, correct, accuracy}. Ordered by date asc.
        A prediction counts as 'correct' by the same rules as
        query_prediction_outcomes: direction matches price movement, flat is
        ±0.1% of entry.
        """
        if from_dt is not None:
            _require_utc(from_dt, "from_dt")
        if to_dt is not None:
            _require_utc(to_dt, "to_dt")
        query = """
            SELECT
                DATE_TRUNC('day', p.timestamp) AS date,
                COUNT(*) AS total,
                SUM(CASE
                    WHEN (p.predicted_direction = 1 AND b_exit.close > b_entry.close) THEN 1
                    WHEN (p.predicted_direction = -1 AND b_exit.close < b_entry.close) THEN 1
                    WHEN (p.predicted_direction = 0
                          AND ABS(b_exit.close - b_entry.close)
                              / NULLIF(b_entry.close, 0) < 0.001) THEN 1
                    ELSE 0
                END)::INT AS correct
            FROM predictions p
            JOIN bars_1m b_entry
                ON b_entry.symbol = p.symbol
                AND b_entry.timestamp = p.timestamp
            JOIN bars_1m b_exit
                ON b_exit.symbol = p.symbol
                AND b_exit.timestamp = p.timestamp
                    + (p.horizon_minutes * interval '1 minute')
            WHERE p.model_id = $1
        """
        args: list = [model_id]
        idx = 2
        if from_dt is not None:
            query += f" AND p.timestamp >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            query += f" AND p.timestamp < ${idx}"
            args.append(to_dt); idx += 1
        query += """
            GROUP BY DATE_TRUNC('day', p.timestamp)
            ORDER BY date ASC
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [
            {
                "date": r["date"],
                "total": r["total"],
                "correct": r["correct"],
                "accuracy": (r["correct"] / r["total"]) if r["total"] > 0 else 0.0,
            }
            for r in rows
        ]

    async def query_calibration_bins(
        self,
        model_id: int,
        num_bins: int = 10,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> list[dict]:
        """Reliability-diagram bins: predicted confidence vs actual hit rate.

        Returns num_bins rows: {bin_low, bin_high, count, correct,
        avg_confidence, hit_rate}. All bins returned even if empty.
        """
        if from_dt is not None:
            _require_utc(from_dt, "from_dt")
        if to_dt is not None:
            _require_utc(to_dt, "to_dt")
        query = """
            WITH outcomes AS (
                SELECT
                    p.confidence,
                    CASE
                        WHEN p.predicted_direction = 1 AND b_exit.close > b_entry.close THEN 1
                        WHEN p.predicted_direction = -1 AND b_exit.close < b_entry.close THEN 1
                        WHEN p.predicted_direction = 0
                             AND ABS(b_exit.close - b_entry.close)
                                 / NULLIF(b_entry.close, 0) < 0.001 THEN 1
                        ELSE 0
                    END AS is_correct
                FROM predictions p
                JOIN bars_1m b_entry
                    ON b_entry.symbol = p.symbol
                    AND b_entry.timestamp = p.timestamp
                JOIN bars_1m b_exit
                    ON b_exit.symbol = p.symbol
                    AND b_exit.timestamp = p.timestamp
                        + (p.horizon_minutes * interval '1 minute')
                WHERE p.model_id = $1
        """
        args: list = [model_id]
        idx = 2
        if from_dt is not None:
            query += f" AND p.timestamp >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            query += f" AND p.timestamp < ${idx}"
            args.append(to_dt); idx += 1
        query += f"""
            )
            SELECT
                bin_idx,
                COUNT(*) AS count,
                SUM(is_correct)::INT AS correct,
                AVG(confidence) AS avg_confidence,
                AVG(is_correct::FLOAT) AS hit_rate
            FROM (
                SELECT
                    LEAST(FLOOR(confidence * ${idx})::INT, ${idx} - 1) AS bin_idx,
                    confidence, is_correct
                FROM outcomes
            ) t
            GROUP BY bin_idx
            ORDER BY bin_idx ASC
        """
        args.append(num_bins)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)

        by_idx = {r["bin_idx"]: r for r in rows}
        result = []
        for i in range(num_bins):
            r = by_idx.get(i)
            lo = i / num_bins
            hi = (i + 1) / num_bins
            if r is None:
                result.append({
                    "bin_low": lo, "bin_high": hi,
                    "count": 0, "correct": 0,
                    "avg_confidence": 0.0, "hit_rate": 0.0,
                })
            else:
                result.append({
                    "bin_low": lo, "bin_high": hi,
                    "count": r["count"], "correct": r["correct"],
                    "avg_confidence": float(r["avg_confidence"]),
                    "hit_rate": float(r["hit_rate"]),
                })
        return result

    async def query_simulated_pnl(
        self,
        model_id: int,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> dict:
        """Simulated P&L if every signal had been traded with unit position.

        Returns {signal_count, cumulative_pnl_pct, avg_pnl_pct, win_rate,
        daily_series: [{date, pnl_pct, count}, ...]}.
        P&L per signal = predicted_direction * (exit_close - entry_close) / entry_close.
        """
        if from_dt is not None:
            _require_utc(from_dt, "from_dt")
        if to_dt is not None:
            _require_utc(to_dt, "to_dt")
        query = """
            WITH signals AS (
                SELECT
                    p.timestamp,
                    p.predicted_direction::FLOAT *
                        (b_exit.close - b_entry.close) / NULLIF(b_entry.close, 0)
                        AS pnl_pct
                FROM predictions p
                JOIN bars_1m b_entry
                    ON b_entry.symbol = p.symbol
                    AND b_entry.timestamp = p.timestamp
                JOIN bars_1m b_exit
                    ON b_exit.symbol = p.symbol
                    AND b_exit.timestamp = p.timestamp
                        + (p.horizon_minutes * interval '1 minute')
                WHERE p.model_id = $1
        """
        args: list = [model_id]
        idx = 2
        if from_dt is not None:
            query += f" AND p.timestamp >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            query += f" AND p.timestamp < ${idx}"
            args.append(to_dt); idx += 1
        query += """
            )
            SELECT
                DATE_TRUNC('day', timestamp) AS date,
                COUNT(*) AS count,
                SUM(pnl_pct) AS daily_pnl_pct,
                SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END)::INT AS wins
            FROM signals
            GROUP BY DATE_TRUNC('day', timestamp)
            ORDER BY date ASC
        """
        async with self.pool.acquire() as conn:
            daily_rows = await conn.fetch(query, *args)

        total_count = sum(r["count"] for r in daily_rows)
        total_pnl = sum(r["daily_pnl_pct"] or 0 for r in daily_rows)
        total_wins = sum(r["wins"] for r in daily_rows)

        return {
            "signal_count": total_count,
            "cumulative_pnl_pct": float(total_pnl),
            "avg_pnl_pct": float(total_pnl / total_count) if total_count > 0 else 0.0,
            "win_rate": (total_wins / total_count) if total_count > 0 else 0.0,
            "daily_series": [
                {
                    "date": r["date"],
                    "pnl_pct": float(r["daily_pnl_pct"] or 0),
                    "count": r["count"],
                }
                for r in daily_rows
            ],
        }

    async def query_regime_breakdown(
        self,
        model_id: int,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        sma_window: int = 50,
        neutral_band_pct: float = 0.001,
    ) -> list[dict]:
        """Per-regime accuracy + P&L for a model.

        Regime classification at each prediction timestamp:
          - bull: SPY close > SPY 50-bar SMA by >0.1% (configurable)
          - bear: SPY close < SPY 50-bar SMA by >0.1%
          - neutral: within ±0.1% of SMA
        Each prediction is classified independently based on the SPY bar at
        its timestamp (or the nearest earlier SPY bar, if missing).

        Returns rows: {regime, count, correct, accuracy, cumulative_pnl_pct}.
        """
        if from_dt is not None:
            _require_utc(from_dt, "from_dt")
        if to_dt is not None:
            _require_utc(to_dt, "to_dt")
        query = f"""
            WITH spy_series AS (
                SELECT
                    b.timestamp,
                    b.close AS spy_close,
                    AVG(b.close) OVER (
                        ORDER BY b.timestamp
                        ROWS BETWEEN {sma_window - 1} PRECEDING AND CURRENT ROW
                    ) AS sma
                FROM bars_1m b
                WHERE b.symbol = 'SPY'
            ),
            classified AS (
                SELECT
                    p.timestamp,
                    p.predicted_direction,
                    CASE
                        WHEN s.sma IS NULL THEN 'unknown'
                        WHEN (s.spy_close - s.sma) / NULLIF(s.sma, 0) > $2 THEN 'bull'
                        WHEN (s.spy_close - s.sma) / NULLIF(s.sma, 0) < -$2 THEN 'bear'
                        ELSE 'neutral'
                    END AS regime,
                    CASE
                        WHEN p.predicted_direction = 1 AND b_exit.close > b_entry.close THEN 1
                        WHEN p.predicted_direction = -1 AND b_exit.close < b_entry.close THEN 1
                        WHEN p.predicted_direction = 0
                             AND ABS(b_exit.close - b_entry.close)
                                 / NULLIF(b_entry.close, 0) < 0.001 THEN 1
                        ELSE 0
                    END AS is_correct,
                    p.predicted_direction::FLOAT *
                        (b_exit.close - b_entry.close) / NULLIF(b_entry.close, 0)
                        AS pnl_pct
                FROM predictions p
                LEFT JOIN spy_series s
                    ON s.timestamp = (
                        SELECT MAX(s2.timestamp)
                        FROM spy_series s2
                        WHERE s2.timestamp <= p.timestamp
                    )
                JOIN bars_1m b_entry
                    ON b_entry.symbol = p.symbol AND b_entry.timestamp = p.timestamp
                JOIN bars_1m b_exit
                    ON b_exit.symbol = p.symbol
                    AND b_exit.timestamp = p.timestamp
                        + (p.horizon_minutes * interval '1 minute')
                WHERE p.model_id = $1
        """
        args: list = [model_id, neutral_band_pct]
        idx = 3
        if from_dt is not None:
            query += f" AND p.timestamp >= ${idx}"
            args.append(from_dt); idx += 1
        if to_dt is not None:
            query += f" AND p.timestamp < ${idx}"
            args.append(to_dt); idx += 1
        query += """
            )
            SELECT
                regime,
                COUNT(*) AS count,
                SUM(is_correct)::INT AS correct,
                AVG(is_correct::FLOAT) AS accuracy,
                SUM(pnl_pct) AS cumulative_pnl_pct
            FROM classified
            GROUP BY regime
            ORDER BY regime
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [
            {
                "regime": r["regime"],
                "count": r["count"],
                "correct": r["correct"],
                "accuracy": float(r["accuracy"] or 0),
                "cumulative_pnl_pct": float(r["cumulative_pnl_pct"] or 0),
            }
            for r in rows
        ]

    async def query_signal_history(
        self,
        model_id: int,
        limit: int = 100,
    ) -> list[dict]:
        """Recent predictions for a model with outcome info, newest first.

        Returns: {timestamp, symbol, timeframe, horizon_minutes,
                  predicted_direction, confidence, entry_price, exit_price,
                  is_correct, pnl_pct}. `is_correct`/`pnl_pct` are NULL when
                  the exit bar doesn't yet exist.
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    p.timestamp, p.symbol, p.timeframe, p.horizon_minutes,
                    p.predicted_direction, p.confidence,
                    b_entry.close AS entry_price,
                    b_exit.close  AS exit_price,
                    CASE
                        WHEN b_exit.close IS NULL THEN NULL
                        WHEN p.predicted_direction = 1
                             AND b_exit.close > b_entry.close THEN TRUE
                        WHEN p.predicted_direction = -1
                             AND b_exit.close < b_entry.close THEN TRUE
                        WHEN p.predicted_direction = 0
                             AND ABS(b_exit.close - b_entry.close)
                                 / NULLIF(b_entry.close, 0) < 0.001 THEN TRUE
                        ELSE FALSE
                    END AS is_correct,
                    CASE
                        WHEN b_exit.close IS NULL THEN NULL
                        ELSE p.predicted_direction::FLOAT
                             * (b_exit.close - b_entry.close)
                             / NULLIF(b_entry.close, 0)
                    END AS pnl_pct
                FROM predictions p
                LEFT JOIN bars_1m b_entry
                    ON b_entry.symbol = p.symbol
                    AND b_entry.timestamp = p.timestamp
                LEFT JOIN bars_1m b_exit
                    ON b_exit.symbol = p.symbol
                    AND b_exit.timestamp = p.timestamp
                        + (p.horizon_minutes * interval '1 minute')
                WHERE p.model_id = $1
                ORDER BY p.timestamp DESC
                LIMIT $2
                """,
                model_id, limit,
            )
        return [dict(r) for r in rows]

    async def query_models_with_leaderboard_stats(
        self,
        model_ids: list[int],
    ) -> dict[int, dict]:
        """Batch lookup of 7-day live stats + 30-day sparkline per model.

        Returns {model_id: {live_accuracy_7d, signals_7d, simulated_pnl_7d_pct,
                            accuracy_sparkline_30d}}. Models with no data get
        zeros/empty sparkline.
        """
        if not model_ids:
            return {}

        # 7-day live stats via one aggregate query.
        async with self.pool.acquire() as conn:
            stats_rows = await conn.fetch(
                """
                SELECT
                    p.model_id,
                    COUNT(*) AS signals,
                    SUM(CASE
                        WHEN p.predicted_direction = 1 AND b_exit.close > b_entry.close THEN 1
                        WHEN p.predicted_direction = -1 AND b_exit.close < b_entry.close THEN 1
                        WHEN p.predicted_direction = 0
                             AND ABS(b_exit.close - b_entry.close)
                                 / NULLIF(b_entry.close, 0) < 0.001 THEN 1
                        ELSE 0
                    END)::FLOAT AS correct,
                    SUM(p.predicted_direction::FLOAT
                        * (b_exit.close - b_entry.close)
                        / NULLIF(b_entry.close, 0)) AS pnl_pct
                FROM predictions p
                JOIN bars_1m b_entry
                    ON b_entry.symbol = p.symbol
                    AND b_entry.timestamp = p.timestamp
                JOIN bars_1m b_exit
                    ON b_exit.symbol = p.symbol
                    AND b_exit.timestamp = p.timestamp
                        + (p.horizon_minutes * interval '1 minute')
                WHERE p.model_id = ANY($1)
                    AND p.timestamp >= NOW() - INTERVAL '7 days'
                GROUP BY p.model_id
                """,
                model_ids,
            )

            # 30-day daily sparkline via one aggregate query.
            sparkline_rows = await conn.fetch(
                """
                SELECT
                    p.model_id,
                    DATE_TRUNC('day', p.timestamp) AS date,
                    SUM(CASE
                        WHEN p.predicted_direction = 1 AND b_exit.close > b_entry.close THEN 1
                        WHEN p.predicted_direction = -1 AND b_exit.close < b_entry.close THEN 1
                        WHEN p.predicted_direction = 0
                             AND ABS(b_exit.close - b_entry.close)
                                 / NULLIF(b_entry.close, 0) < 0.001 THEN 1
                        ELSE 0
                    END)::FLOAT / NULLIF(COUNT(*), 0) AS accuracy
                FROM predictions p
                JOIN bars_1m b_entry
                    ON b_entry.symbol = p.symbol
                    AND b_entry.timestamp = p.timestamp
                JOIN bars_1m b_exit
                    ON b_exit.symbol = p.symbol
                    AND b_exit.timestamp = p.timestamp
                        + (p.horizon_minutes * interval '1 minute')
                WHERE p.model_id = ANY($1)
                    AND p.timestamp >= NOW() - INTERVAL '30 days'
                GROUP BY p.model_id, DATE_TRUNC('day', p.timestamp)
                ORDER BY p.model_id, date ASC
                """,
                model_ids,
            )

        result: dict[int, dict] = {}
        for mid in model_ids:
            result[mid] = {
                "live_accuracy_7d": 0.0,
                "signals_7d": 0,
                "simulated_pnl_7d_pct": 0.0,
                "accuracy_sparkline_30d": [],
            }
        for r in stats_rows:
            mid = r["model_id"]
            signals = r["signals"] or 0
            result[mid]["signals_7d"] = signals
            result[mid]["live_accuracy_7d"] = (
                (r["correct"] / signals) if signals > 0 else 0.0
            )
            result[mid]["simulated_pnl_7d_pct"] = float(r["pnl_pct"] or 0.0)
        for r in sparkline_rows:
            mid = r["model_id"]
            result[mid]["accuracy_sparkline_30d"].append(
                float(r["accuracy"] or 0.0)
            )
        return result

    # ── Halt event methods ─────────────────────────────────────────

    async def insert_halt_event(
        self, tier: int, reason: str, direction: Optional[str] = None,
    ) -> int:
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO halt_events (tier, reason, direction)
                VALUES ($1, $2, $3)
                RETURNING id
                """,
                tier,
                reason,
                direction,
            )
        return row_id

    async def get_active_halts(self) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, tier, reason, direction, activated_at
                FROM halt_events
                WHERE resolved_at IS NULL
                ORDER BY activated_at DESC
                """
            )
        return [dict(r) for r in rows]

    async def resolve_halt_event(self, halt_id: int) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE halt_events
                SET resolved_at = NOW()
                WHERE id = $1
                """,
                halt_id,
            )

    # ── Metric snapshot methods ────────────────────────────────────

    async def insert_metric_snapshot(
        self,
        category: str,
        scope: str,
        window: str,
        metrics: dict,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO metric_snapshots (category, scope, "window", metrics)
                VALUES ($1, $2, $3, $4)
                """,
                category,
                scope,
                window,
                json.dumps(metrics),
            )

    async def query_latest_metric_snapshot(
        self,
        category: str,
        scope: str,
        window: str,
    ) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, timestamp, category, scope, "window", metrics
                FROM metric_snapshots
                WHERE category = $1 AND scope = $2 AND "window" = $3
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                category,
                scope,
                window,
            )
        if row is None:
            return None
        return {
            "id": row["id"],
            "timestamp": row["timestamp"],
            "category": row["category"],
            "scope": row["scope"],
            "window": row["window"],
            "metrics": json.loads(row["metrics"]),
        }

    async def query_metric_snapshots(
        self,
        category: str,
        scope: str,
        window: str,
        limit: int = 60,
        *,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> list[dict]:
        if from_dt is not None:
            _require_utc(from_dt, "from_dt")
        if to_dt is not None:
            _require_utc(to_dt, "to_dt")
        query = """
            SELECT id, timestamp, category, scope, window, metrics
            FROM metric_snapshots
            WHERE category = $1 AND scope = $2 AND window = $3
        """
        args: list = [category, scope, window]
        idx = 4
        if from_dt is not None:
            query += f" AND timestamp >= ${idx}"
            args.append(from_dt)
            idx += 1
        if to_dt is not None:
            query += f" AND timestamp < ${idx}"
            args.append(to_dt)
            idx += 1
        query += f" ORDER BY timestamp DESC LIMIT ${idx}"
        args.append(limit)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [
            {
                "id": r["id"],
                "timestamp": r["timestamp"],
                "category": r["category"],
                "scope": r["scope"],
                "window": r["window"],
                "metrics": json.loads(r["metrics"]) if isinstance(r["metrics"], str) else r["metrics"],
            }
            for r in rows
        ]

    async def query_halt_history(self, limit: int = 50) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, tier, reason, direction, activated_at, resolved_at
                FROM halt_events
                ORDER BY activated_at DESC
                LIMIT $1
                """,
                limit,
            )
        return [dict(r) for r in rows]

    # ── Freshness & model-detail queries (Phase 6B) ────────────────

    async def query_latest_bar_timestamps(self) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT symbol, MAX(timestamp) AS last_bar "
                "FROM bars_1m GROUP BY symbol ORDER BY symbol"
            )
        return [dict(r) for r in rows]

    async def query_latest_feature_timestamps(self) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT symbol, MAX(timestamp) AS last_feature "
                "FROM features GROUP BY symbol ORDER BY symbol"
            )
        return [dict(r) for r in rows]

    async def query_symbol_bar_stats(self, symbol: str) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT COUNT(*) AS bar_count, MAX(timestamp) AS last_bar "
                "FROM bars_1m WHERE symbol = $1",
                symbol,
            )
        return {
            "symbol": symbol,
            "bar_count": row["bar_count"],
            "last_bar": row["last_bar"],
        }

    async def query_model_details(self) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT mr.id AS model_id, mr.symbol, mr.timeframe,
                       mr.horizon_minutes, mr.model_type, mr.trained_at,
                       mr.metrics, mr.is_active,
                       COALESCE(ms.status, 'active') AS status,
                       COALESCE(ms.reason, '') AS reason,
                       ms.updated_at AS status_updated_at
                FROM model_registry mr
                LEFT JOIN model_status ms ON mr.id = ms.model_id
                WHERE mr.is_active = TRUE
                ORDER BY mr.symbol, mr.timeframe, mr.horizon_minutes,
                         mr.model_type
                """
            )
        result = []
        for r in rows:
            d = dict(r)
            if isinstance(d.get("metrics"), str):
                d["metrics"] = json.loads(d["metrics"])
            result.append(d)
        return result

    # ── Meta-feature methods ──────────────────────────────────────

    async def insert_meta_feature(self, row: MetaFeatureRow) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO meta_features
                    (symbol, timestamp, direction, feature_vector,
                     entry_label, optimal_pair_idx, realized_ev, source)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (symbol, timestamp, direction) DO UPDATE SET
                    feature_vector = EXCLUDED.feature_vector,
                    entry_label = EXCLUDED.entry_label,
                    optimal_pair_idx = EXCLUDED.optimal_pair_idx,
                    realized_ev = EXCLUDED.realized_ev,
                    source = EXCLUDED.source
                """,
                row.symbol, row.timestamp, row.direction,
                json.dumps(row.feature_vector),
                row.entry_label, row.optimal_pair_idx,
                row.realized_ev, row.source,
            )

    async def insert_meta_features_batch(
        self,
        rows: list[MetaFeatureRow],
        *,
        on_conflict: str = "upsert",
    ) -> None:
        """Batch-insert meta_features rows.

        Modes:
        * ``upsert`` — always overwrite the existing row.
        * ``skip`` — never overwrite; keeps the existing row as-is.
        * ``replace_replay`` — overwrite only if the existing row's
          source is ``'replay'``; live shadow rows stay untouched. This
          is what the meta retrain phase wants: re-write its own prior
          replay rows when the assembler schema changes (so a 1m-only
          retrain replaces older all-timeframe rows), but never clobber
          live-mode rows that may already be labeled.
        """
        if not rows:
            return
        if on_conflict == "skip":
            conflict_clause = "ON CONFLICT (symbol, timestamp, direction) DO NOTHING"
        elif on_conflict == "upsert":
            conflict_clause = (
                "ON CONFLICT (symbol, timestamp, direction) DO UPDATE SET "
                "feature_vector = EXCLUDED.feature_vector, "
                "entry_label = EXCLUDED.entry_label, "
                "optimal_pair_idx = EXCLUDED.optimal_pair_idx, "
                "realized_ev = EXCLUDED.realized_ev, "
                "source = EXCLUDED.source"
            )
        elif on_conflict == "replace_replay":
            conflict_clause = (
                "ON CONFLICT (symbol, timestamp, direction) DO UPDATE SET "
                "feature_vector = EXCLUDED.feature_vector, "
                "entry_label = EXCLUDED.entry_label, "
                "optimal_pair_idx = EXCLUDED.optimal_pair_idx, "
                "realized_ev = EXCLUDED.realized_ev, "
                "source = EXCLUDED.source "
                "WHERE meta_features.source = 'replay'"
            )
        else:
            raise ValueError(f"unknown on_conflict mode: {on_conflict!r}")

        async with self.pool.acquire() as conn:
            await conn.executemany(
                f"""
                INSERT INTO meta_features
                    (symbol, timestamp, direction, feature_vector,
                     entry_label, optimal_pair_idx, realized_ev, source)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                {conflict_clause}
                """,
                [(r.symbol, r.timestamp, r.direction,
                  json.dumps(r.feature_vector),
                  r.entry_label, r.optimal_pair_idx, r.realized_ev, r.source)
                 for r in rows],
            )

    async def query_meta_features(
        self, symbol: str, direction: int, limit: int = 10000, source: Optional[str] = None,
    ) -> list[MetaFeatureRow]:
        cond = "WHERE symbol = $1 AND direction = $2"
        params: list = [symbol, direction]
        if source is not None:
            cond += " AND source = $3"
            params.append(source)
        params.append(limit)
        limit_param = f"${len(params)}"
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT symbol, timestamp, direction, feature_vector,
                       entry_label, optimal_pair_idx, realized_ev, source
                FROM meta_features {cond}
                ORDER BY timestamp DESC LIMIT {limit_param}
                """, *params,
            )
        return [
            MetaFeatureRow(
                symbol=r["symbol"], timestamp=r["timestamp"], direction=r["direction"],
                feature_vector=json.loads(r["feature_vector"]), source=r["source"],
                entry_label=r["entry_label"], optimal_pair_idx=r["optimal_pair_idx"],
                realized_ev=r["realized_ev"],
            ) for r in rows
        ]

    async def count_meta_features(self, symbol: str, direction: int) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM meta_features WHERE symbol = $1 AND direction = $2",
                symbol, direction,
            )

    async def query_unlabeled_meta_features(
        self, *, older_than: datetime, limit: int = 5000,
    ) -> list[MetaFeatureRow]:
        """Live rows older than `older_than` with NULL labels.

        Used by MetaLabelBackfiller. Returns at most `limit` rows ordered by
        timestamp ASC so the oldest get labeled first.
        """
        _require_utc(older_than, "older_than")
        async with self.pool.acquire() as conn:
            recs = await conn.fetch(
                """
                SELECT symbol, timestamp, direction, feature_vector,
                       entry_label, optimal_pair_idx, realized_ev, source
                FROM meta_features
                WHERE source = 'live'
                  AND entry_label IS NULL
                  AND timestamp <= $1
                ORDER BY timestamp ASC
                LIMIT $2
                """,
                older_than, limit,
            )
        return [
            MetaFeatureRow(
                symbol=r["symbol"], timestamp=r["timestamp"],
                direction=r["direction"],
                feature_vector=json.loads(r["feature_vector"]),
                source=r["source"], entry_label=r["entry_label"],
                optimal_pair_idx=r["optimal_pair_idx"],
                realized_ev=r["realized_ev"],
            ) for r in recs
        ]

    async def update_meta_feature_labels(
        self, *, symbol: str, timestamp: datetime, direction: int,
        entry_label: int, optimal_pair_idx: int, realized_ev: float,
    ) -> None:
        _require_utc(timestamp, "timestamp")
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE meta_features
                SET entry_label = $4,
                    optimal_pair_idx = $5,
                    realized_ev = $6
                WHERE symbol = $1 AND timestamp = $2 AND direction = $3
                """,
                symbol, timestamp, direction,
                entry_label, optimal_pair_idx, realized_ev,
            )

    # ── Meta-signal shadow methods ────────────────────────────────

    async def insert_meta_signal_shadow(self, sig: MetaSignalShadow) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                """
                INSERT INTO meta_signals_shadow
                    (symbol, timestamp, direction, estimator_type,
                     entry_probability, kelly_fraction, pair_index,
                     target_pct, stop_pct, model_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                RETURNING id
                """,
                sig.symbol, sig.timestamp, sig.direction, sig.estimator_type,
                sig.entry_probability, sig.kelly_fraction, sig.pair_index,
                sig.target_pct, sig.stop_pct, sig.model_id,
            )

    # ------------------------------------------------------------------
    # Backtest tags / notes / runtime_config_audit
    # ------------------------------------------------------------------

    async def set_backtest_tags(self, run_id: int, tags: list[str]) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE backtest_runs SET tags = $1 WHERE id = $2",
                tags, run_id,
            )

    async def get_backtest_tags(self, run_id: int) -> list[str]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchval(
                "SELECT tags FROM backtest_runs WHERE id = $1", run_id,
            )
        return list(row) if row else []

    async def set_backtest_notes(self, run_id: int, notes: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE backtest_runs SET notes = $1 WHERE id = $2",
                notes, run_id,
            )

    async def get_backtest_notes(self, run_id: int) -> Optional[str]:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT notes FROM backtest_runs WHERE id = $1", run_id,
            )

    async def insert_runtime_config_audit(
        self, key: str, old_value: Optional[str], new_value: str,
        source_backtest_run_id: Optional[int] = None,
        note: Optional[str] = None,
    ) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                """
                INSERT INTO runtime_config_audit
                    (key, old_value, new_value, source_backtest_run_id, note)
                VALUES ($1, $2, $3, $4, $5) RETURNING id
                """,
                key, old_value, new_value, source_backtest_run_id, note,
            )

    # ------------------------------------------------------------------
    # Backtest extra query methods
    # ------------------------------------------------------------------

    async def query_backtest_symbols(self, run_id: int) -> list[dict]:
        """Per-symbol aggregation within a backtest run.

        Returns [{symbol, trades, wins, win_rate, total_pnl, avg_pnl}].
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    symbol,
                    COUNT(*) AS trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END)::INT AS wins,
                    SUM(pnl)::DOUBLE PRECISION AS total_pnl
                FROM backtest_trades
                WHERE run_id = $1 AND exit_time IS NOT NULL AND pnl IS NOT NULL
                GROUP BY symbol
                ORDER BY total_pnl DESC
                """,
                run_id,
            )
        return [
            {
                "symbol": r["symbol"],
                "trades": r["trades"],
                "wins": r["wins"],
                "win_rate": (r["wins"] / r["trades"]) if r["trades"] > 0 else 0.0,
                "total_pnl": float(r["total_pnl"] or 0.0),
                "avg_pnl": (float(r["total_pnl"] or 0.0) / r["trades"])
                           if r["trades"] > 0 else 0.0,
            }
            for r in rows
        ]

    async def query_backtest_distributions(self, run_id: int) -> dict:
        """Same shape as Plan 3's query_distributions but scoped to one backtest run."""
        async with self.pool.acquire() as conn:
            all_trades = await conn.fetch(
                """
                SELECT pnl, side,
                       EXTRACT(EPOCH FROM (exit_time - entry_time))::INT AS hold_seconds
                FROM backtest_trades
                WHERE run_id = $1 AND exit_time IS NOT NULL AND pnl IS NOT NULL
                """,
                run_id,
            )
            daily = await conn.fetch(
                """
                SELECT DATE_TRUNC('day', exit_time) AS date,
                       SUM(pnl) AS pnl, COUNT(*) AS trades
                FROM backtest_trades
                WHERE run_id = $1 AND exit_time IS NOT NULL AND pnl IS NOT NULL
                GROUP BY DATE_TRUNC('day', exit_time)
                ORDER BY date ASC
                """,
                run_id,
            )

        pnls = [float(t["pnl"]) for t in all_trades]
        holds = [int(t["hold_seconds"]) for t in all_trades
                 if t["hold_seconds"] is not None]

        # 20-bin histogram
        labels: list = []
        counts: list = []
        if pnls:
            lo, hi = min(pnls), max(pnls)
            if lo == hi: lo -= 1.0; hi += 1.0
            bins = 20
            width = (hi - lo) / bins
            counts = [0] * bins
            for v in pnls:
                i = min(bins - 1, int((v - lo) / width))
                counts[i] += 1
            labels = [round(lo + i * width, 2) for i in range(bins)]

        # Hold-time buckets (same 7 as Plan 3)
        hold_buckets_def = [
            ("<5m", 0, 300),
            ("5-30m", 300, 1800),
            ("30m-2h", 1800, 7200),
            ("2h-8h", 7200, 28800),
            ("8h-1d", 28800, 86400),
            ("overnight", 86400, 172800),
            ("multi-day", 172800, 10**9),
        ]
        hold_table = []
        for name, lo_s, hi_s in hold_buckets_def:
            bucket = [t for t in all_trades
                      if t["hold_seconds"] is not None
                      and lo_s <= t["hold_seconds"] < hi_s]
            n = len(bucket)
            avg = sum(float(t["pnl"]) for t in bucket) / n if n > 0 else 0.0
            hold_table.append({"bucket": name, "trades": n, "avg_pnl": avg})

        def side_stats(side):
            rows = [t for t in all_trades if t["side"] == side]
            n = len(rows)
            wins = sum(1 for t in rows if float(t["pnl"]) > 0)
            total = sum(float(t["pnl"]) for t in rows)
            return {
                "side": side, "trades": n, "wins": wins,
                "win_rate": (wins / n) if n > 0 else 0.0,
                "total_pnl": total,
                "avg_pnl": (total / n) if n > 0 else 0.0,
            }

        return {
            "daily_pnl": [
                {"date": r["date"], "pnl": float(r["pnl"]), "trades": r["trades"]}
                for r in daily
            ],
            "trade_pnl_histogram": {
                "labels": labels, "counts": counts,
                "count": len(pnls),
                "median": sorted(pnls)[len(pnls) // 2] if pnls else 0.0,
                "mean": (sum(pnls) / len(pnls)) if pnls else 0.0,
            },
            "hold_time_buckets": hold_table,
            "side_breakdown": [side_stats("long"), side_stats("short")],
        }

    async def query_backtest_live_overlay(self, run_id: int) -> dict:
        """Overlay: live vs backtest equity for the run's symbols/window.

        Returns {has_overlap, backtest_series, live_series, symbols,
                 range_start, range_end}.
        """
        async with self.pool.acquire() as conn:
            run = await conn.fetchrow(
                """
                SELECT symbols, range_start, range_end
                FROM backtest_runs WHERE id = $1
                """,
                run_id,
            )
            if run is None:
                return {"has_overlap": False, "backtest_series": [],
                        "live_series": [], "symbols": [],
                        "range_start": None, "range_end": None}

            # Backtest series: concatenate equity across folds.
            bt_rows = await conn.fetch(
                """
                SELECT timestamp, equity
                FROM backtest_equity
                WHERE run_id = $1
                ORDER BY timestamp ASC
                """,
                run_id,
            )
            # Live series: walk closed live trades for the same symbols within
            # the backtest's time window. scope=None (live-only): public.trades
            # has no backtest_run_id column, so no filter needed there.
            live_rows = await conn.fetch(
                """
                SELECT exit_time, pnl
                FROM trades
                WHERE symbol = ANY($1::TEXT[])
                    AND exit_time IS NOT NULL
                    AND pnl IS NOT NULL
                    AND exit_time >= $2
                    AND exit_time < $3
                ORDER BY exit_time ASC
                """,
                list(run["symbols"]), run["range_start"], run["range_end"],
            )

        bt_series = [
            {"timestamp": r["timestamp"], "equity": float(r["equity"])}
            for r in bt_rows
        ]
        # Live equity walked from 0 (symbol-scoped; compare shape not level).
        live_series = []
        cum = 0.0
        for r in live_rows:
            cum += float(r["pnl"])
            live_series.append({
                "timestamp": r["exit_time"], "equity": cum,
            })
        return {
            "has_overlap": len(live_series) > 0,
            "backtest_series": bt_series,
            "live_series": live_series,
            "symbols": list(run["symbols"]),
            "range_start": run["range_start"],
            "range_end": run["range_end"],
        }

    async def query_backtest_funnel(
        self, run_id: int, min_confidence: float = 0.6,
    ) -> dict:
        """Signal-to-trade funnel for a backtest run.

        Reads predictions + orders scoped to run_id (any fold). Returns
        {signals, confident, orders_placed, orders_filled, orders_closed}.
        """
        async with self.pool.acquire() as conn:
            signals = await conn.fetchval(
                """
                SELECT COUNT(*)::INT FROM predictions
                WHERE backtest_run_id = $1
                """,
                run_id,
            )
            confident = await conn.fetchval(
                """
                SELECT COUNT(*)::INT FROM predictions
                WHERE backtest_run_id = $1 AND confidence >= $2
                """,
                run_id, min_confidence,
            )
            orders_placed = await conn.fetchval(
                """
                SELECT COUNT(*)::INT FROM orders WHERE backtest_run_id = $1
                """,
                run_id,
            )
            orders_filled = await conn.fetchval(
                """
                SELECT COUNT(*)::INT FROM orders
                WHERE backtest_run_id = $1 AND status = 'filled'
                """,
                run_id,
            )
            orders_closed = await conn.fetchval(
                """
                SELECT COUNT(*)::INT FROM backtest_trades
                WHERE run_id = $1 AND exit_time IS NOT NULL
                """,
                run_id,
            )
        return {
            "signals": signals or 0,
            "confident": confident or 0,
            "orders_placed": orders_placed or 0,
            "orders_filled": orders_filled or 0,
            "orders_closed": orders_closed or 0,
            "min_confidence": min_confidence,
        }

    async def query_backtest_compare(
        self, run_ids: list[int],
    ) -> dict:
        """Overlay-ready payload for N runs. Returns aggregate equity series
        per run + aggregated metrics (sum/mean across folds).
        """
        if not run_ids:
            return {"runs": []}

        async with self.pool.acquire() as conn:
            runs = await conn.fetch(
                """
                SELECT id, name, symbols, tags, notes
                FROM backtest_runs WHERE id = ANY($1::BIGINT[])
                ORDER BY id
                """,
                run_ids,
            )
            equity = await conn.fetch(
                """
                SELECT run_id, timestamp, equity
                FROM backtest_equity
                WHERE run_id = ANY($1::BIGINT[])
                ORDER BY run_id, timestamp ASC
                """,
                run_ids,
            )
            metrics = await conn.fetch(
                """
                SELECT run_id, SUM((metrics_json->>'pnl')::DOUBLE PRECISION) AS total_pnl,
                       AVG((metrics_json->>'sharpe')::DOUBLE PRECISION) AS avg_sharpe,
                       MIN((metrics_json->>'max_drawdown')::DOUBLE PRECISION) AS min_max_dd,
                       SUM((metrics_json->>'trades')::INT) AS total_trades,
                       AVG((metrics_json->>'hit_rate')::DOUBLE PRECISION) AS avg_hit_rate
                FROM backtest_folds
                WHERE run_id = ANY($1::BIGINT[])
                GROUP BY run_id
                """,
                run_ids,
            )

        eq_by_run: dict[int, list[dict]] = {rid: [] for rid in run_ids}
        for e in equity:
            eq_by_run[e["run_id"]].append({
                "timestamp": e["timestamp"], "equity": float(e["equity"]),
            })
        mx_by_run = {m["run_id"]: m for m in metrics}

        result = []
        for r in runs:
            m = mx_by_run.get(r["id"], {})
            result.append({
                "run_id": r["id"], "name": r["name"],
                "symbols": list(r["symbols"]),
                "tags": list(r["tags"] or []),
                "notes": r["notes"],
                "equity_series": eq_by_run.get(r["id"], []),
                "metrics": {
                    "total_pnl": float(m.get("total_pnl") or 0.0),
                    "avg_sharpe": float(m.get("avg_sharpe") or 0.0),
                    "min_max_dd": float(m.get("min_max_dd") or 0.0),
                    "total_trades": int(m.get("total_trades") or 0),
                    "avg_hit_rate": float(m.get("avg_hit_rate") or 0.0),
                },
            })
        return {"runs": result}

    async def query_backtest_sensitivity(
        self,
        group_by: list[str],
        swept_param: str,
    ) -> dict:
        """Runs sharing group_by dims but differing on swept_param.

        For each run, extracts swept_param's value from config_json and
        aggregates test-fold metrics. Returns {swept_param, group_by,
        points: [{param_value, metrics}]}.
        """
        # Build a canonical group key from (symbols, timeframe, horizons).
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT r.id, r.symbols, r.timeframe, r.horizons, r.config_json,
                       SUM((f.metrics_json->>'pnl')::DOUBLE PRECISION) AS total_pnl,
                       AVG((f.metrics_json->>'sharpe')::DOUBLE PRECISION) AS avg_sharpe,
                       MIN((f.metrics_json->>'max_drawdown')::DOUBLE PRECISION) AS min_max_dd
                FROM backtest_runs r
                LEFT JOIN backtest_folds f ON f.run_id = r.id
                WHERE r.status = 'completed'
                GROUP BY r.id, r.symbols, r.timeframe, r.horizons, r.config_json
                """
            )

        def group_key(r):
            parts = []
            if "symbols" in group_by:
                parts.append(tuple(r["symbols"]))
            if "timeframe" in group_by:
                parts.append(r["timeframe"])
            if "horizons" in group_by:
                parts.append(tuple(r["horizons"]))
            return tuple(parts)

        # Keep only runs whose config_json has the swept_param.
        points = []
        for r in rows:
            cfg = r["config_json"] if isinstance(r["config_json"], dict) \
                else (json.loads(r["config_json"]) if r["config_json"] else {})
            if swept_param not in cfg:
                continue
            points.append({
                "run_id": r["id"],
                "group_key": list(group_key(r)),
                "param_value": cfg[swept_param],
                "metrics": {
                    "total_pnl": float(r["total_pnl"] or 0.0),
                    "avg_sharpe": float(r["avg_sharpe"] or 0.0),
                    "min_max_dd": float(r["min_max_dd"] or 0.0),
                },
            })
        points.sort(key=lambda p: (str(p["group_key"]), p["param_value"]))
        return {
            "swept_param": swept_param,
            "group_by": group_by,
            "points": points,
        }

    async def query_meta_signals_shadow(
        self, symbol: str, limit: int = 100, estimator_type: Optional[str] = None,
    ) -> list[MetaSignalShadow]:
        cond = "WHERE symbol = $1"
        params: list = [symbol]
        if estimator_type is not None:
            cond += " AND estimator_type = $2"
            params.append(estimator_type)
        params.append(limit)
        limit_param = f"${len(params)}"
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT id, symbol, timestamp, direction, estimator_type,
                       entry_probability, kelly_fraction, pair_index,
                       target_pct, stop_pct, model_id
                FROM meta_signals_shadow {cond}
                ORDER BY timestamp DESC LIMIT {limit_param}
                """, *params,
            )
        return [
            MetaSignalShadow(
                id=r["id"], symbol=r["symbol"], timestamp=r["timestamp"],
                direction=r["direction"], estimator_type=r["estimator_type"],
                entry_probability=r["entry_probability"], kelly_fraction=r["kelly_fraction"],
                pair_index=r["pair_index"], target_pct=r["target_pct"],
                stop_pct=r["stop_pct"], model_id=r["model_id"],
            ) for r in rows
        ]
