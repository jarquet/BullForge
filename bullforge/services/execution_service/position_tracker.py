from __future__ import annotations

from typing import Optional

from services.execution_service.alpaca_broker import AlpacaBroker


class PositionTracker:
    def __init__(self, broker: AlpacaBroker):
        self._broker = broker
        self._positions: dict[str, dict] = {}
        self._account: dict = {}

    async def refresh(self) -> None:
        self._account = await self._broker.get_account()
        positions_list = await self._broker.get_positions()
        self._positions = {p["symbol"]: p for p in positions_list}

    @property
    def account(self) -> dict:
        return dict(self._account)

    @property
    def positions(self) -> dict[str, dict]:
        return dict(self._positions)

    def get_position(self, symbol: str) -> Optional[dict]:
        pos = self._positions.get(symbol)
        return dict(pos) if pos is not None else None

    def has_position(self, symbol: str) -> bool:
        return symbol in self._positions
