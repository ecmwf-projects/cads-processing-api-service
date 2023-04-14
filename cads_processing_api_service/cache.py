import asyncio
import contextlib
import functools
from typing import Any, Callable, Optional

import cachetools
import cachetools.keys


def async_cached(
    cache: cachetools.Cache,
    key: Callable[..., Any] = cachetools.keys.hashkey,
    lock: Optional[contextlib.AbstractContextManager] = None,
) -> Callable[..., Any]:
    lock = lock or contextlib.nullcontext()

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if not asyncio.iscoroutinefunction(func):
            raise TypeError("func must be a coroutine function")

        async def wrapper(*args, **kwargs):
            k = key(*args, **kwargs)
            try:
                async with lock:
                    value = cache[k]
                    return value
            except KeyError:
                pass
            value = await func(*args, **kwargs)
            try:
                async with lock:
                    cache[k] = value
            except ValueError:
                pass
            return value

        return functools.wraps(func)(wrapper)

    return decorator
