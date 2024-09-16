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

import unittest.mock

import cads_broker
import pytest

from cads_processing_api_service import auth, exceptions, models


def test_check_licences() -> None:
    required_licences = {("licence_1", 1), ("licence_2", 2)}
    accepted_licences = {("licence_1", 1), ("licence_2", 2), ("licence_3", 3)}
    missing_licences = auth.check_licences(required_licences, accepted_licences)
    assert len(missing_licences) == 0

    required_licences = {("licence_1", 1), ("licence_2", 2)}
    accepted_licences = {("licence_1", 1), ("licence_2", 1)}
    with pytest.raises(exceptions.PermissionDenied):
        missing_licences = auth.check_licences(required_licences, accepted_licences)


def test_verify_permission() -> None:
    job = cads_broker.SystemRequest(**{"user_uid": "abc123"})
    user_uid = "abc123"
    try:
        auth.verify_permission(user_uid, job)
    except exceptions.PermissionDenied as exc:
        assert False, f"'{user_uid} / {job}' raised an exception {exc}"

    user_uid = "def456"
    with pytest.raises(exceptions.PermissionDenied):
        auth.verify_permission(user_uid, job)


def test_verify_if_disabled() -> None:
    test_disabled_reason = "test_disabled_reason"
    test_user_role = None
    with pytest.raises(exceptions.PermissionDenied):
        auth.verify_if_disabled(test_disabled_reason, test_user_role)

    test_disabled_reason = "test_disabled_reason"
    test_user_role = "manager"
    auth.verify_if_disabled(test_disabled_reason, test_user_role)

    test_disabled_reason = None
    test_user_role = None
    auth.verify_if_disabled(test_disabled_reason, test_user_role)


def test_verify_cost() -> None:
    with unittest.mock.patch(
        "cads_processing_api_service.costing.compute_costing"
    ) as mock_compute_costing:
        mock_compute_costing.return_value = models.CostingInfo(
            costs={"cost_id_1": 10.0, "cost_id_2": 10.0},
            limits={"cost_id_1": 20.0, "cost_id_2": 20.0},
        )
        costs = auth.verify_cost({}, {})
        assert costs == {"cost_id_1": 10.0, "cost_id_2": 10.0}

        mock_compute_costing.return_value = models.CostingInfo(
            costs={"cost_id_1": 10.0, "cost_id_2": 10.0},
            limits={"cost_id_1": 5.0, "cost_id_2": 20.0},
        )
        with pytest.raises(exceptions.PermissionDenied):
            auth.verify_cost({}, {}, "api")
