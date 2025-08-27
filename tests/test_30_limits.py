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
from cads_processing_api_service import config, exceptions


def test_get_rate_limits() -> None:
    rate_limits = {"/jobs/{job_id}": {"get": {"api": ["2/second"]}}}
    rate_limits_config = config.RateLimitsConfig(**rate_limits)

    route = "jobs_jobsid"
    method = "get"
    request_origin = "api"
    rate_limits = cads_processing_api_service.limits.get_rate_limits(
        rate_limits_config, route, method, request_origin
    )
    exp_rate_limits = ["2/second"]
    assert rate_limits == exp_rate_limits


def test_get_rate_limits_route_param() -> None:
    rate_limits = {
        "/processes/{process_id}/execution": {
            "process_id": {"post": {"api": ["2/second"]}}
        }
    }
    rate_limits_config = config.RateLimitsConfig(**rate_limits)

    route = "processes_processid_execution"
    route_param = "process_id"
    method = "post"
    request_origin = "api"
    rate_limits = cads_processing_api_service.limits.get_rate_limits(
        rate_limits_config, route, method, request_origin, route_param
    )
    exp_rate_limits = ["2/second"]
    assert rate_limits == exp_rate_limits


def test_get_rate_limits_defaulted_actual_value() -> None:
    rate_limits = {
        "/jobs/{job_id}": {"get": {"api": ["2/second"]}},
        "default": {"get": {"api": ["1/second"]}},
    }
    rate_limits_config = config.RateLimitsConfig(**rate_limits)

    route = "jobs_jobsid"
    method = "get"
    request_origin = "api"
    rate_limits = cads_processing_api_service.limits.get_rate_limits_defaulted(
        rate_limits_config, route, method, request_origin
    )
    exp_rate_limits = ["2/second"]
    assert rate_limits == exp_rate_limits


def test_get_rate_limits_defaulted_default_value() -> None:
    rate_limits = {
        "/jobs/{job_id}": {"post": {"api": ["2/second"]}},
        "/jobs": {"get": {"api": ["2/second"]}},
        "default": {"post": {"ui": ["1/second"]}},
    }
    rate_limits_config = config.RateLimitsConfig(**rate_limits)

    route = "jobs_jobsid"
    method = "post"
    request_origin = "ui"
    rate_limits = cads_processing_api_service.limits.get_rate_limits_defaulted(
        rate_limits_config, route, method, request_origin
    )
    exp_rate_limits = ["1/second"]
    assert rate_limits == exp_rate_limits

    route = "jobs"
    method = "post"
    request_origin = "ui"
    rate_limits = cads_processing_api_service.limits.get_rate_limits_defaulted(
        rate_limits_config, route, method, request_origin
    )
    exp_rate_limits = ["1/second"]
    assert rate_limits == exp_rate_limits

    route = "processes_processid_execute"
    method = "post"
    request_origin = "ui"
    rate_limits = cads_processing_api_service.limits.get_rate_limits_defaulted(
        rate_limits_config, route, method, request_origin
    )
    exp_rate_limits = ["1/second"]
    assert rate_limits == exp_rate_limits


def test_get_rate_limits_defaulted_route_param_actual_value() -> None:
    rate_limits = {
        "/processes/{process_id}/execution": {
            "test_process_id": {"post": {"api": ["2/second"]}}
        },
        "default": {"post": {"ui": ["1/second"]}},
    }
    rate_limits_config = config.RateLimitsConfig(**rate_limits)

    route = "processes_processid_execution"
    method = "post"
    request_origin = "api"
    route_param = "test_process_id"
    rate_limits = cads_processing_api_service.limits.get_rate_limits_defaulted(
        rate_limits_config, route, method, request_origin, route_param
    )
    exp_rate_limits = ["2/second"]
    assert rate_limits == exp_rate_limits


def test_get_rate_limits_defaulted_route_param_default_value() -> None:
    rate_limits = {
        "/processes/{process_id}/execution": {
            "test_process_id": {"post": {"api": ["2/second"]}},
            "default": {"post": {"api": ["1/second"]}},
        },
        "default": {"post": {"ui": ["1/minute"]}},
    }
    rate_limits_config = config.RateLimitsConfig(**rate_limits)

    route = "processes_processid_execution"
    method = "post"
    request_origin = "api"
    route_param = "missing_test_process_id"
    rate_limits = cads_processing_api_service.limits.get_rate_limits_defaulted(
        rate_limits_config, route, method, request_origin, route_param
    )
    exp_rate_limits = ["1/second"]
    assert rate_limits == exp_rate_limits

    route = "processes_processid_execution"
    method = "post"
    request_origin = "ui"
    route_param = "missing_test_process_id"
    rate_limits = cads_processing_api_service.limits.get_rate_limits_defaulted(
        rate_limits_config, route, method, request_origin, route_param
    )
    exp_rate_limits = ["1/minute"]
    assert rate_limits == exp_rate_limits


def test_get_rate_limits_undefined() -> None:
    rate_limits = {"/jobs": {"get": {"api": ["2/second"]}}}
    rate_limits_config = config.RateLimitsConfig.model_validate(rate_limits)

    route = "jobs"
    method = "get"
    request_origin = "ui"
    rate_limits = cads_processing_api_service.limits.get_rate_limits(
        rate_limits_config, route, method, request_origin
    )
    exp_rate_limits = []
    assert rate_limits == exp_rate_limits

    route = "jobs"
    method = "post"
    request_origin = "ui"
    rate_limits = cads_processing_api_service.limits.get_rate_limits(
        rate_limits_config, route, method, request_origin
    )
    exp_rate_limits = []
    assert rate_limits == exp_rate_limits

    route = "job"
    method = "get"
    request_origin = "ui"
    rate_limits = cads_processing_api_service.limits.get_rate_limits(
        rate_limits_config, route, method, request_origin
    )
    exp_rate_limits = []
    assert rate_limits == exp_rate_limits


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
    assert exc.value.retry_after == pytest.approx(60)

    rate_limit_ids = ["2/second"]
    rate_limits = [limits.parse(rate_limit_id) for rate_limit_id in rate_limit_ids]
    user_uid = "02"
    cads_processing_api_service.limits.check_rate_limits_for_user(user_uid, rate_limits)
