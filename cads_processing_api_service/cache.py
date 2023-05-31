import asyncio
import functools
from typing import Any, Callable

import cachetools
import cachetools.keys


def async_cached(
    cache: cachetools.Cache,
    key: Callable[..., Any] = cachetools.keys.hashkey,
    locks: dict[str, asyncio.Lock] | None = None,
) -> Callable[..., Any]:
    if locks is None:
        locks = {}

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if not asyncio.iscoroutinefunction(func):
            raise TypeError("func must be a coroutine function")

        async def wrapper(*args, **kwargs):
            k = key(*args, **kwargs)
            try:
                lock = locks[k]
            except KeyError:
                lock = asyncio.Lock()
                locks[k] = lock
            async with lock:
                try:
                    value = cache[k]
                    return value
                except KeyError:
                    pass
                value = await func(*args, **kwargs)
                try:
                    cache[k] = value
                except ValueError:
                    pass
                return value

        return functools.wraps(func)(wrapper)

    return decorator
