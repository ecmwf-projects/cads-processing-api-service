import asyncio
import functools
from typing import Any, Callable

import cachetools
import cachetools.keys
import structlog

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


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
                    # if "authenticate" in str(func):
                    #     logger.info(
                    #         "cache hit", func=func, key=k, kwargs=kwargs, args=args
                    #     )
                    return value
                except KeyError:
                    # if "authenticate" in str(func):
                    #     logger.info(
                    #         "cache miss", func=func, key=k, kwargs=kwargs, args=args
                    #     )
                    pass
                value = await func(*args, **kwargs)
                try:
                    cache[k] = value
                except ValueError:
                    pass
                return value

        return functools.wraps(func)(wrapper)

    return decorator
