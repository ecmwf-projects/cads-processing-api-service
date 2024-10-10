import limits
import structlog

from . import config, exceptions, models

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

storage = limits.storage.MemoryStorage()
limiter = limits.strategies.FixedWindowRateLimiter(storage)


def check_rate_limit(method: str, auth_info: models.AuthInfo) -> None:
    rate_limits_config: dict[str, list[str]] = config.ensure_settings().rate_limits.get(
        method, {}
    )
    rate_limit_ids: list[str] = rate_limits_config.get(auth_info.request_origin, [])
    rate_limits = [limits.parse(rate_limit_id) for rate_limit_id in rate_limit_ids]
    rate_limits_exceeded = [
        rate_limit
        for rate_limit in rate_limits
        if not limiter.hit(rate_limit, auth_info.user_uid)
    ]
    if rate_limits_exceeded:
        rate_limiters_reset_time = [
            limiter.get_window_stats(rate_limit, auth_info.user_uid).reset_time
            for rate_limit in rate_limits_exceeded
        ]
        expiry = storage.get_expiry(auth_info.user_uid)
        time_to_wait = max(
            [reset_time - expiry for reset_time in rate_limiters_reset_time]
        )
        raise exceptions.RateLimitExceeded(
            detail=f"Rate limit exceeded. Please wait {time_to_wait} seconds.",
            retry_after=time_to_wait,
        )
