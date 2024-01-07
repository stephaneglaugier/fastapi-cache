import asyncio
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional, Tuple

from pyignite import AioClient
from pyignite.datatypes.cache_config import CacheAtomicityMode
from pyignite.datatypes.expiry_policy import ExpiryPolicy
from pyignite.datatypes.prop_codes import PROP_EXPIRY_POLICY, PROP_NAME
from pyignite.exceptions import CacheError

from fastapi_cache.types import Backend


class PyIgniteBackend(Backend):
    def __init__(self, ignite_address: str, ignite_port: int, cache_name: str):
        self.ignite_address = ignite_address
        self.ignite_port = ignite_port
        self.cache_name = cache_name

    @property
    def _now(self) -> int:
        return int(time.time())

    async def _get_cache(self):
        client = AioClient()
        await client.connect(self.ignite_address, self.ignite_port)
        return await client.get_or_create_cache(self.cache_name)

    async def get_with_ttl(self, key: str) -> Tuple[int, Optional[bytes]]:
        cache = await self._get_cache()
        try:
            value = await cache.get(key)
            if value:
                _, ttl_ts = value
                return ttl_ts - self._now, value[0]
            return 0, None
        finally:
            await cache.close()

    async def get(self, key: str) -> Optional[bytes]:
        ttl, value = await self.get_with_ttl(key)
        return value

    async def set(
        self, key: str, value: bytes, expire: Optional[int] = None
    ) -> None:
        cache = await self._get_cache()
        try:
            ttl_ts = self._now + (expire or 0)
            await cache.put(key, (value, ttl_ts))
        finally:
            await cache.close()

    async def clear(
        self, namespace: Optional[str] = None, key: Optional[str] = None
    ) -> int:
        cache = await self._get_cache()
        try:
            count = 0
            if key:
                await cache.remove(key)
                count = 1
            elif namespace:
                # Clearing by namespace is not directly supported by pyignite,
                # implement a mechanism to track and clear keys by namespace if needed.
                pass
            return count
        finally:
            await cache.close()
