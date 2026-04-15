from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable, Awaitable, Dict, Optional

import redis.asyncio as aioredis

from shared.config import Settings

logger = logging.getLogger(__name__)


class RedisClient:
    def __init__(self, settings: Settings):
        self._settings = settings
        self.client: Optional[aioredis.Redis] = None
        self._pubsub: Optional[aioredis.client.PubSub] = None
        self._handlers: Dict[str, Callable[[str], Awaitable[None]]] = {}
        self._listener_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        self.client = aioredis.Redis(
            host=self._settings.redis_host,
            port=self._settings.redis_port,
            decode_responses=True,
        )
        self._pubsub = self.client.pubsub()
        self._listener_task = asyncio.create_task(self._listen())

    async def disconnect(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        if self._pubsub:
            await self._pubsub.aclose()
        if self.client:
            await self.client.aclose()

    async def publish(self, channel: str, message: str) -> None:
        await self.client.publish(channel, message)

    async def subscribe(
        self, channel: str, handler: Callable[[str], Awaitable[None]]
    ) -> None:
        self._handlers[channel] = handler
        await self._pubsub.subscribe(channel)

    async def unsubscribe(self, channel: str) -> None:
        await self._pubsub.unsubscribe(channel)
        self._handlers.pop(channel, None)

    async def _listen(self) -> None:
        while True:
            try:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=0.1
                )
                if message and message["type"] == "message":
                    channel = message["channel"]
                    handler = self._handlers.get(channel)
                    if handler:
                        await handler(message["data"])
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in redis pubsub listener")
                await asyncio.sleep(0.1)
