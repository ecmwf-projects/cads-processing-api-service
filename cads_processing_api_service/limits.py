"""CADS Processing API rate limits."""

# Copyright 2022, European Union.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

from typing import Any

import limits
import structlog

from . import config, exceptions, models

SETTINGS = config.settings

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

storage = config.RATE_LIMITS_STORAGE
limiter = config.RATE_LIMITS_LIMITER


def get_rate_limits(
    rate_limits_config: config.RateLimitsConfig,
    route: str,
    method: str,
    request_origin: str,
    route_param: str | None = None,
) -> list[str]:
    """Get the rate limits for a specific route and method."""
    rate_limits = rate_limits_config.model_dump()
    route_rate_limits: dict[str, Any] = rate_limits.get(route, {})
    if route_param is not None:
        route_rate_limits: dict[str, Any] = route_rate_limits.get(route_param, {})
    method_rate_limits: dict[str, Any] = route_rate_limits.get(method, {})
    rate_limit_ids: list[str] = method_rate_limits.get(request_origin, [])
    return rate_limit_ids


def check_rate_limits_for_user(
    user_uid: str, rate_limits: list[limits.RateLimitItem]
) -> None:
    rate_limits_exceeded = [
        rate_limit
        for rate_limit in rate_limits
        if not limiter.hit(rate_limit, user_uid)
    ]
    if rate_limits_exceeded:
        rate_limiters_reset_time = [
            limiter.get_window_stats(rate_limit, user_uid).reset_time
            for rate_limit in rate_limits_exceeded
        ]
        expiry = storage.get_expiry(user_uid)
        time_to_wait = round(
            max([reset_time - expiry for reset_time in rate_limiters_reset_time])
        )
        raise exceptions.RateLimitExceeded(
            detail=f"Rate limit exceeded. Please wait {time_to_wait} seconds.",
            retry_after=time_to_wait,
        )
    return None


def check_rate_limits(
    rate_limits_config: config.RateLimitsConfig,
    route: str,
    method: str,
    auth_info: models.AuthInfo,
    route_param: str | None = None,
) -> None:
    """Check if the rate limits are exceeded."""
    request_origin = auth_info.request_origin
    user_uid = auth_info.user_uid
    rate_limits = get_rate_limits(
        rate_limits_config, route, method, request_origin, route_param
    )
    if not rate_limits:
        rate_limits = get_rate_limits(
            rate_limits_config, route, method, request_origin, "default"
        )
    if not rate_limits:
        rate_limits = get_rate_limits(
            rate_limits_config, "default", method, request_origin
        )
    check_rate_limits_for_user(user_uid, rate_limits)
    return None
