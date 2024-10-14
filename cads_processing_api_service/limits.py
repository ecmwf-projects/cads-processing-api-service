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

import limits
import structlog

from . import config, exceptions, models

SETTINGS = config.settings

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

storage = config.RATE_LIMITS_STORAGE
limiter = config.RATE_LIMITS_LIMITER


def check_rate_limits_for_user(
    user_uid: str, rate_limits: limits.RateLimitItem
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
        time_to_wait = max(
            [reset_time - expiry for reset_time in rate_limiters_reset_time]
        )
        raise exceptions.RateLimitExceeded(
            detail=f"Rate limit exceeded. Please wait {time_to_wait} seconds.",
            retry_after=time_to_wait,
        )
    return None


def check_rate_limits(
    method_rate_limits: config.RateLimitsMethodConfig,
    auth_info: models.AuthInfo,
) -> None:
    """Check if the rate limits are exceeded."""
    user_uid = auth_info.user_uid
    request_origin = auth_info.request_origin
    rate_limit_ids = getattr(method_rate_limits, request_origin)
    rate_limits = [limits.parse(rate_limit_id) for rate_limit_id in rate_limit_ids]
    check_rate_limits_for_user(user_uid, request_origin, rate_limits)
    return None
