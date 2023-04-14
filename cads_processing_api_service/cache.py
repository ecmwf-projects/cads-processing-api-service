import asyncio
import contextlib
import functools
from typing import Any, Callable, Optional

import cachetools
import cachetools.keys
import structlog

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


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
            async with lock:
                try:
                    value = cache[k]
                    if "lookup" in str(func):
                        logger.info(
                            "cache hit", func=func, key=k, kwargs=kwargs, args=args
                        )
                    return value
                except KeyError:
                    if "lookup" in str(func):
                        logger.info(
                            "cache miss", func=func, key=k, kwargs=kwargs, args=args
                        )
                    pass
                value = await func(*args, **kwargs)
                try:
                    cache[k] = value
                except ValueError:
                    pass
            return value

        return functools.wraps(func)(wrapper)

    return decorator
