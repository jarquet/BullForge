from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Callable, Optional

from shared.db import Database
from shared.models import BacktestScope
from shared.runtime_config import RuntimeConfig

from services.backtest_service.equity_tracker import EquityTracker
from services.backtest_service.fill_model import FillModel
from services.backtest_service.metrics import (
    max_drawdown,
    sharpe_ratio,
    sortino_ratio,
    summarize_trades,
)
from services.backtest_service.results_db import BacktestResultsDB
from services.backtest_service.simulated_broker import SimulatedBroker
from services.backtest_service.splitter import Fold, WalkForwardSplitter
from services.backtest_service.exceptions import BacktestCancelled

from services.execution_service.order_executor import OrderExecutor
from services.execution_service.position_tracker import PositionTracker
from services.execution_service.risk_manager import RiskManager
from services.execution_service.signal_aggregator import SignalAggregator

from services.model_service.inference import InferenceEngine
from services.model_service.model_store import ModelStore
from services.model_service.trainer import Trainer

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BacktestConfig:
    name: str
    symbols: list[str]
    timeframe: str           # "1m" or "1d"
    horizons: list[int]      # e.g. [5, 10, 30]
    range_start: datetime
    range_end: datetime
    train_days: int
    test_days: int
    embargo_minutes: int
    mode: str                # "rolling" | "anchored"
    custom_name: str | None = None
    starting_cash: float = 100_000.0
    slippage_bps: float = 5.0
    max_volume_pct: float = 0.10
    periods_per_year: int = 390 * 252  # 1m equities default
    min_confidence: Optional[float] = None
    min_agreement: Optional[int] = None
    direction_band: Optional[float] = None
    min_signal_strength: Optional[float] = None
    max_position_pct: Optional[float] = None
    max_positions: Optional[int] = None
    max_exposure: Optional[float] = None


class BacktestEngine:
    def __init__(
        self, *,
        db: Database,
        runtime_config: RuntimeConfig,
        model_store: ModelStore,
        results_db: BacktestResultsDB,
    ):
        self._db = db
        self._rc = runtime_config
        self._model_store = model_store
        self._results = results_db

    async def run(
        self,
        cfg: BacktestConfig,
        *,
        on_run_created: Optional[Callable[[int], Awaitable[None]]] = None,
        check_cancelled: Optional[Callable[[], Awaitable[bool]]] = None,
    ) -> int:
        run_id = await self._results.insert_run(
            name=cfg.name,
            custom_name=cfg.custom_name,
            symbols=cfg.symbols, timeframe=cfg.timeframe, horizons=cfg.horizons,
            range_start=cfg.range_start, range_end=cfg.range_end,
            train_days=cfg.train_days, test_days=cfg.test_days,
            embargo_minutes=cfg.embargo_minutes, mode=cfg.mode,
            config={
                "starting_cash": cfg.starting_cash,
                "slippage_bps": cfg.slippage_bps,
                "max_volume_pct": cfg.max_volume_pct,
            },
        )
        if on_run_created is not None:
            await on_run_created(run_id)
        try:
            splitter = WalkForwardSplitter(
                train_days=cfg.train_days,
                test_days=cfg.test_days,
                embargo_minutes=cfg.embargo_minutes,
                mode=cfg.mode,
            )
            for fold in splitter.split(cfg.range_start, cfg.range_end):
                logger.info(
                    "Fold %d: train [%s..%s] test [%s..%s]",
                    fold.index, fold.train_start, fold.train_end,
                    fold.test_start, fold.test_end,
                )
                await self._run_fold(
                    run_id, cfg, fold, check_cancelled=check_cancelled,
                )
            await self._results.finish_run(run_id=run_id, status="completed")
        except BacktestCancelled:
            await self._results.finish_run(run_id=run_id, status="cancelled")
            raise
        except Exception:
            await self._results.finish_run(run_id=run_id, status="failed")
            raise
        return run_id

    async def _run_fold(
        self, run_id: int, cfg: BacktestConfig, fold: Fold,
        *,
        check_cancelled: Optional[Callable[[], Awaitable[bool]]] = None,
    ) -> None:
        if check_cancelled is not None and await check_cancelled():
            raise BacktestCancelled()

        fold_id = await self._results.insert_fold(
            run_id=run_id, fold_index=fold.index,
            train_start=fold.train_start, train_end=fold.train_end,
            test_start=fold.test_start, test_end=fold.test_end,
        )
        scope = BacktestScope(backtest_run_id=run_id, fold_index=fold.index)

        # 1) Train one model per (symbol, horizon) using only train-window data.
        trainer = Trainer(self._db, self._model_store, scope=scope)
        for symbol in cfg.symbols:
            for horizon in cfg.horizons:
                meta = await trainer.train(
                    symbol, cfg.timeframe, horizon,
                    as_of=fold.train_end,
                )
                if meta is None:
                    logger.warning(
                        "No model trained for %s %dm in fold %d",
                        symbol, horizon, fold.index,
                    )

        # 2) Build inference engine restricted to this fold's models.
        inference = InferenceEngine(self._db, self._model_store, scope=scope)
        await inference.load_models()  # picks up the just-trained active models

        # 3) Build sim broker + execution stack.
        fill_model = FillModel(
            slippage_bps=cfg.slippage_bps,
            max_volume_pct=cfg.max_volume_pct,
        )
        broker = SimulatedBroker(
            starting_cash=cfg.starting_cash, fill_model=fill_model,
        )
        tracker = PositionTracker(broker)
        overrides = {
            "min_confidence": cfg.min_confidence,
            "min_agreement": cfg.min_agreement,
            "direction_band": cfg.direction_band,
            "min_signal_strength": cfg.min_signal_strength,
            "max_position_pct": cfg.max_position_pct,
            "max_positions": cfg.max_positions,
            "max_exposure": cfg.max_exposure,
        }
        risk = RiskManager(runtime_config=self._rc, overrides=overrides)
        executor = OrderExecutor(
            broker=broker, risk_manager=risk, position_tracker=tracker,
            db=self._db, core=_NoopReconciler(),
            scope=scope,
        )

        equity = EquityTracker()
        aggregator = SignalAggregator(
            runtime_config=self._rc, overrides=overrides,
        )
        completed_trades: list[dict] = []
        open_trade: dict[str, dict] = {}

        # 4) Replay test-window bars chronologically per symbol.
        all_bars = []
        next_bar_lookup: dict[tuple[str, datetime], dict] = {}
        for symbol in cfg.symbols:
            bars = await self._db.query_bars_between(
                symbol, cfg.timeframe, fold.test_start, fold.test_end,
            )
            for b in bars:
                all_bars.append((b["timestamp"], symbol, b))
            for prev, nxt in zip(bars, bars[1:]):
                next_bar_lookup[(symbol, prev["timestamp"])] = nxt
        all_bars.sort(key=lambda t: t[0])

        # Multi-symbol runs share `ts` across symbols; record equity ONCE per
        # ts (at the boundary when ts changes) using the latest account
        # snapshot, so later symbols don't overwrite earlier ones at the same
        # ts (PK is (run_id, fold_id, timestamp)).
        last_ts: Optional[datetime] = None
        last_account: Optional[dict] = None
        for i, (ts, symbol, bar) in enumerate(all_bars):
            if check_cancelled is not None and i % 100 == 0 and await check_cancelled():
                raise BacktestCancelled()
            if last_ts is not None and ts != last_ts and last_account is not None:
                equity.record(
                    last_ts,
                    equity=last_account["equity"],
                    cash=last_account["cash"],
                )

            broker.set_clock(ts)
            broker.mark_to_market(symbol, bar["close"])

            nxt = next_bar_lookup.get((symbol, ts))
            if nxt is not None:
                broker.set_next_bar(symbol, nxt)

            preds = await inference.predict(symbol, cfg.timeframe)
            pred_dicts = [
                {
                    "model_id": p.model_id,
                    "direction": p.predicted_direction,
                    "confidence": p.confidence,
                    "horizon_minutes": p.horizon_minutes,
                }
                for p in preds
            ]
            signal = await aggregator.aggregate(symbol, ts, pred_dicts)
            if signal is not None and nxt is not None:
                order = await executor.process_signal(signal, bar["close"])
                if order is not None:
                    await self._record_trade(
                        symbol, order, signal, ts, open_trade,
                        completed_trades, broker,
                    )

            last_account = await broker.get_account()
            last_ts = ts

        # Flush the final ts after the loop exits.
        if last_ts is not None and last_account is not None:
            equity.record(
                last_ts,
                equity=last_account["equity"],
                cash=last_account["cash"],
            )

        # 5) Persist equity + trades + metrics for the fold.
        await self._results.insert_equity_points_many(
            run_id=run_id, fold_id=fold_id,
            points=[
                {"timestamp": pt.timestamp, "equity": pt.equity, "cash": pt.cash}
                for pt in equity.points()
            ],
        )
        await self._results.insert_trades_many(
            run_id=run_id, fold_id=fold_id, trades=completed_trades,
        )

        rets = equity.returns()
        equity_curve = [pt.equity for pt in equity.points()]
        metrics = {
            "sharpe": sharpe_ratio(rets, periods_per_year=cfg.periods_per_year),
            "sortino": sortino_ratio(rets, periods_per_year=cfg.periods_per_year),
            "max_drawdown": max_drawdown(equity_curve),
            "final_equity": equity_curve[-1] if equity_curve else cfg.starting_cash,
            "total_return_pct": (
                (equity_curve[-1] / cfg.starting_cash - 1.0) * 100.0
                if equity_curve else 0.0
            ),
            **{f"trade_{k}": v for k, v in summarize_trades(completed_trades).items()},
        }
        await self._results.update_fold_metrics(fold_id=fold_id, metrics=metrics)

    async def _record_trade(
        self, symbol, order, signal, ts,
        open_trade: dict, completed: list, broker,
    ) -> None:
        # The `Order` row returned by `process_signal` reflects DB insert state
        # (status='submitted', filled_price=NULL) because the backtest uses a
        # no-op reconciler. Pull the actual fill from the simulated broker.
        fill_snap = await broker.get_order(order.alpaca_order_id)
        if fill_snap["status"] != "filled":
            # Broker may reject (volume cap, no next-bar); skip recording.
            # In normal backtest flow this branch is unreachable because
            # `_execute_open` returns None for non-fills before we get here;
            # kept defensive for non-scope direct callers (unit tests).
            logger.debug(
                "skipping trade record for %s order %s: status=%s",
                symbol, order.alpaca_order_id, fill_snap["status"],
            )
            return
        existing = open_trade.get(symbol)
        if existing is None:
            open_trade[symbol] = {
                "symbol": symbol,
                "side": "long" if signal.direction == 1 else "short",
                "entry_time": ts,
                "entry_price": float(fill_snap["filled_avg_price"]),
                "quantity": float(order.quantity),
                "exit_time": None,
                "exit_price": None,
                "pnl": None,
                "signal_strength": float(signal.strength),
            }
            return
        entry = existing
        exit_price = float(fill_snap["filled_avg_price"])
        sign = 1.0 if entry["side"] == "long" else -1.0
        pnl = (exit_price - entry["entry_price"]) * entry["quantity"] * sign
        entry["exit_time"] = ts
        entry["exit_price"] = exit_price
        entry["pnl"] = pnl
        completed.append(entry)
        del open_trade[symbol]


class _NoopReconciler:
    """Sim-only stub: ReconcilerCore is for live order/event reconciliation;
    in backtests fills are synchronous so reconciliation is a no-op."""

    async def apply_alpaca_order(self, snap: dict, *, source: str) -> None:
        return None
