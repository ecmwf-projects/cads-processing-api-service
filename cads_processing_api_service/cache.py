import asyncio
import functools

import cachetools.keys


class NullContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        return None


def async_cached(cache, key=cachetools.keys.hashkey, lock=None):
    lock = lock or NullContext()

    def decorator(func):
        if not asyncio.iscoroutinefunction(func):
            return ValueError("func must be a coroutine")

        async def wrapper(*args, **kwargs):
            k = key(*args, **kwargs)
            try:
                async with lock:
                    return cache[k]

            except KeyError:
                pass  # key not found

            val = await func(*args, **kwargs)

            try:
                async with lock:
                    cache[k] = val

            except ValueError:
                pass  # val too large

            return val

        return functools.wraps(func)(wrapper)

    return decorator
