import asyncio


def async_cached(cache, key, lock):
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

    return decorator
