import limits

memory_storage = limits.storage.MemoryStorage()
moving_window = limits.strategies.FixedWindowRateLimiter(memory_storage)
one_per_minute = limits.parse("5/minute")
