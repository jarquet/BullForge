from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

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
            await self._run_migrations()
            await self._sync_sandbox_schema()

    async def disconnect(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def _run_migrations(self) -> None:
        migrations_dir = Path(__file__).parent.parent / "migrations"
        sql_files = sorted(migrations_dir.glob("*.sql"))
        async with self.pool.acquire() as conn:
            for sql_file in sql_files:
                sql = sql_file.read_text()
                await conn.execute(sql)

    async def _sync_sandbox_schema(self) -> None:
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
        async with self.pool.acquire() as conn:
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
        public_idx = [r for r in rows if r["schemaname"] == "public"]
        backtest_bodies = {_index_body(r["indexdef"]) for r in rows if r["schemaname"] == "backtest"}

        for r in public_idx:
            body = _index_body(r["indexdef"])
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
        since: Optional[str] = None,
    ) -> List[Bar]:
        table = "bars_1m" if timeframe == "1m" else "bars_daily"
        query = f"""
            SELECT symbol, timestamp, open, high, low, close, volume
            FROM {table}
            WHERE symbol = $1
        """
        args = [symbol]
        if since:
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
        if as_of.tzinfo is None or as_of.tzinfo != timezone.utc:
            raise ValueError(f"as_of must be UTC, got tzinfo={as_of.tzinfo}")
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
        scope: Optional[BacktestScope] = None,
    ) -> None:
        scope_clause, scope_args = _scope_filter(scope, next_arg=4)
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE model_registry
                SET is_active = FALSE
                WHERE symbol = $1 AND timeframe = $2
                    AND horizon_minutes = $3 AND is_active = TRUE
                    {scope_clause}
                """,
                symbol, timeframe, horizon_minutes, *scope_args,
            )

    async def insert_prediction(self, pred: Prediction) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO predictions
                    (symbol, timestamp, timeframe, horizon_minutes,
                     model_id, predicted_direction, confidence)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, timestamp, timeframe, horizon_minutes)
                DO UPDATE SET model_id = $5, predicted_direction = $6, confidence = $7
                """,
                pred.symbol,
                pred.timestamp,
                pred.timeframe,
                pred.horizon_minutes,
                pred.model_id,
                pred.predicted_direction,
                pred.confidence,
            )

    async def insert_predictions(self, preds: List[Prediction]) -> None:
        if not preds:
            return
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO predictions
                    (symbol, timestamp, timeframe, horizon_minutes,
                     model_id, predicted_direction, confidence)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, timestamp, timeframe, horizon_minutes)
                DO UPDATE SET model_id = $5, predicted_direction = $6, confidence = $7
                """,
                [
                    (
                        p.symbol, p.timestamp, p.timeframe,
                        p.horizon_minutes, p.model_id,
                        p.predicted_direction, p.confidence,
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
    ) -> List[Prediction]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, timestamp, timeframe, horizon_minutes,
                       model_id, predicted_direction, confidence
                FROM predictions
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

    async def insert_order(self, order: Order) -> int:
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO orders
                    (symbol, side, quantity, order_type, submitted_at,
                     signal_strength, alpaca_order_id, status,
                     filled_at, filled_price, filled_quantity, limit_price,
                     origin, last_reconciled_at, reconcile_source)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                        $13, $14, $15)
                RETURNING id
                """,
                order.symbol,
                order.side,
                order.quantity,
                order.order_type,
                order.submitted_at,
                order.signal_strength,
                order.alpaca_order_id,
                order.status,
                order.filled_at,
                order.filled_price,
                order.filled_quantity,
                order.limit_price,
                order.origin,
                order.last_reconciled_at,
                order.reconcile_source,
            )
        return row_id

    async def insert_order_if_new(self, order: Order) -> Optional[int]:
        """INSERT that does nothing on alpaca_order_id conflict.
        Returns the inserted row id, or None if a row already existed."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                """
                INSERT INTO orders
                  (symbol, side, quantity, order_type, submitted_at,
                   signal_strength, alpaca_order_id, status,
                   origin, last_reconciled_at, reconcile_source)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,
                        COALESCE($9,'internal'),$10,$11)
                ON CONFLICT (alpaca_order_id) WHERE alpaca_order_id IS NOT NULL DO NOTHING
                RETURNING id
                """,
                order.symbol, order.side, order.quantity, order.order_type,
                order.submitted_at, order.signal_strength,
                order.alpaca_order_id, order.status,
                order.origin, order.last_reconciled_at, order.reconcile_source,
            )

    async def update_order_status(
        self,
        order_id: int,
        status: str,
        filled_at: Optional[datetime] = None,
        filled_price: Optional[float] = None,
        filled_quantity: Optional[float] = None,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE orders
                SET status = $2,
                    filled_at = COALESCE($3, filled_at),
                    filled_price = COALESCE($4, filled_price),
                    filled_quantity = COALESCE($5, filled_quantity)
                WHERE id = $1
                """,
                order_id,
                status,
                filled_at,
                filled_price,
                filled_quantity,
            )

    async def query_orders(
        self,
        symbol: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
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

    async def get_order_by_alpaca_id(self, alpaca_order_id: str) -> Optional[Order]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, symbol, side, quantity, order_type, submitted_at,
                       signal_strength, alpaca_order_id, status,
                       filled_at, filled_price, filled_quantity, limit_price,
                       origin, last_reconciled_at, reconcile_source
                FROM orders
                WHERE alpaca_order_id = $1
                """,
                alpaca_order_id,
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
        inserted = False
        old_status: Optional[str] = None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                existing = await conn.fetchrow(
                    "SELECT status, origin FROM orders "
                    "WHERE alpaca_order_id = $1 FOR UPDATE",
                    alpaca_id,
                )
                now_ts = datetime.now(timezone.utc)
                if existing is None:
                    row = await conn.fetchrow(
                        """
                        INSERT INTO orders
                          (symbol, side, quantity, order_type, submitted_at,
                           signal_strength, alpaca_order_id, status,
                           filled_at, filled_price, filled_quantity,
                           origin, last_reconciled_at, reconcile_source)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
                                'external',$12,$13)
                        RETURNING id, symbol, side, quantity, order_type,
                                  submitted_at, signal_strength, alpaca_order_id,
                                  status, filled_at, filled_price, filled_quantity,
                                  limit_price, origin, last_reconciled_at,
                                  reconcile_source
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
                    inserted = True

                else:
                    old_status = existing["status"]
                    row = await conn.fetchrow(
                        """
                        UPDATE orders
                        SET status = $2,
                            filled_at = COALESCE($3, filled_at),
                            filled_price = COALESCE($4, filled_price),
                            filled_quantity = COALESCE($5, filled_quantity),
                            last_reconciled_at = $6,
                            reconcile_source = $7
                        WHERE alpaca_order_id = $1
                        RETURNING id, symbol, side, quantity, order_type,
                                  submitted_at, signal_strength, alpaca_order_id,
                                  status, filled_at, filled_price, filled_quantity,
                                  limit_price, origin, last_reconciled_at,
                                  reconcile_source
                        """,
                        alpaca_id,
                        canonical_status,
                        snapshot.get("filled_at"),
                        snapshot.get("filled_avg_price"),
                        snapshot.get("filled_qty"),
                        now_ts,
                        source,
                    )
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

    async def query_closed_trades(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[Trade]:
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

    async def query_trades_closed_before(
        self, symbol: str, as_of: datetime
    ) -> list[dict]:
        """Trades for symbol with exit_time strictly before as_of. Excludes open trades."""
        if as_of.tzinfo is None or as_of.tzinfo != timezone.utc:
            raise ValueError(f"as_of must be UTC, got {as_of.tzinfo}")
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
    ) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, timestamp, category, scope, "window", metrics
                FROM metric_snapshots
                WHERE category = $1 AND scope = $2 AND "window" = $3
                ORDER BY timestamp DESC
                LIMIT $4
                """,
                category,
                scope,
                window,
                limit,
            )
        return [
            {
                "id": r["id"],
                "timestamp": r["timestamp"],
                "category": r["category"],
                "scope": r["scope"],
                "window": r["window"],
                "metrics": json.loads(r["metrics"]),
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

    async def insert_meta_features_batch(self, rows: list[MetaFeatureRow]) -> None:
        if not rows:
            return
        async with self.pool.acquire() as conn:
            await conn.executemany(
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
                [(r.symbol, r.timestamp, r.direction, json.dumps(r.feature_vector),
                  r.entry_label, r.optimal_pair_idx, r.realized_ev, r.source) for r in rows],
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
