from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

from shared.redis import RedisClient


class BrokerClient:
    """Issues broker requests over Redis and awaits correlated response."""

    def __init__(self, redis_client: RedisClient, timeout: float = 10.0):
        self._redis = redis_client
        self._timeout = timeout

    async def _call(self, method: str, **params: Any) -> Any:
        cid = str(uuid.uuid4())
        response_channel = f"response:broker:{cid}"
        fut: asyncio.Future = asyncio.get_running_loop().create_future()

        async def handler(msg: str) -> None:
            if not fut.done():
                fut.set_result(json.loads(msg))

        await self._redis.subscribe(response_channel, handler)
        try:
            await self._redis.publish("request:broker", json.dumps({
                "method": method, "correlation_id": cid, "params": params,
            }))
            try:
                data = await asyncio.wait_for(fut, timeout=self._timeout)
            except asyncio.TimeoutError:
                raise TimeoutError(f"broker relay timeout after {self._timeout}s")
        finally:
            await self._redis.unsubscribe(response_channel)

        if not data.get("ok"):
            raise RuntimeError(f"broker error: {data.get('error')}")
        return data["result"]

    async def get_positions(self) -> list[dict]:
        return await self._call("get_positions")

    async def get_account(self) -> dict:
        return await self._call("get_account")
