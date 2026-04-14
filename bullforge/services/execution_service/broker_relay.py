from __future__ import annotations

import json
import logging
from typing import Any, Callable

from shared.redis import RedisClient

logger = logging.getLogger(__name__)


class BrokerRelay:
    """
    Subscribes to `request:broker` and proxies method calls to the AlpacaBroker,
    publishing responses to `response:broker:{correlation_id}`.

    This pattern lets monitoring_service query broker state without Alpaca keys.
    """

    def __init__(self, redis_client: RedisClient, broker):
        self._redis = redis_client
        self._broker = broker
        self._methods: dict[str, Callable] = {
            "get_positions": self._get_positions,
            "get_account": self._get_account,
        }

    async def _get_positions(self, params: dict) -> Any:
        return await self._broker.get_positions()

    async def _get_account(self, params: dict) -> Any:
        return await self._broker.get_account()

    async def start(self) -> None:
        await self._redis.subscribe("request:broker", self.handle_request)
        logger.info("BrokerRelay subscribed to request:broker")

    async def handle_request(self, message: str) -> None:
        try:
            data = json.loads(message)
            method = data["method"]
            cid = data["correlation_id"]
            params = data.get("params", {})
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("malformed broker request: %s", e)
            return

        handler = self._methods.get(method)
        channel = f"response:broker:{cid}"
        if handler is None:
            await self._redis.publish(channel, json.dumps({
                "ok": False, "error": f"unknown method: {method}",
            }))
            return

        try:
            result = await handler(params)
            await self._redis.publish(channel, json.dumps({
                "ok": True, "result": result,
            }, default=str))
        except Exception as e:
            logger.exception("broker call %s failed", method)
            await self._redis.publish(channel, json.dumps({
                "ok": False, "error": str(e),
            }))
