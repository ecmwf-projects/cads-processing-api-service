# Copyright 2022, European Union.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# mypy: ignore-errors

import limits
import pytest

import cads_processing_api_service.limits
from cads_processing_api_service import exceptions


def test_check_rate_limits_for_user() -> None:
    rate_limit_ids = ["1/second"]
    rate_limits = [limits.parse(rate_limit_id) for rate_limit_id in rate_limit_ids]
    user_uid = "00"
    with pytest.raises(exceptions.RateLimitExceeded):
        for _ in range(2):
            cads_processing_api_service.limits.check_rate_limits_for_user(
                user_uid, rate_limits
            )

    rate_limit_ids = ["1/second", "1/minute"]
    rate_limits = [limits.parse(rate_limit_id) for rate_limit_id in rate_limit_ids]
    user_uid = "01"
    with pytest.raises(exceptions.RateLimitExceeded) as exc:
        for _ in range(2):
            cads_processing_api_service.limits.check_rate_limits_for_user(
                user_uid, rate_limits
            )
    assert exc.value.retry_after == 60

    rate_limit_ids = ["2/second"]
    rate_limits = [limits.parse(rate_limit_id) for rate_limit_id in rate_limit_ids]
    user_uid = "02"
    cads_processing_api_service.limits.check_rate_limits_for_user(user_uid, rate_limits)
