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

import cads_broker
import pytest

from cads_processing_api_service import auth, exceptions, models


def test_format_missing_licences_message(mocker) -> None:
    request_url = "http://base_url/api/v1/processes/process_id/execution"
    process_id = "test_process_id"
    missing_licences_message_template = "{dataset_licences_url}"
    res = auth.format_missing_licences_message(
        request_url,
        process_id,
        missing_licences_message_template=missing_licences_message_template,
    )
    exp = "http://base_url/datasets/test_process_id?tab=download#manage-licences"
    assert res == exp

    request_url = "https://base_url/api/v1/processes/process_id/execution"
    process_id = "test_process_id"
    missing_licences_message_template = "{dataset_licences_url}"
    portal_id = "missing_test_portal_id"
    mocker.patch(
        "cads_processing_api_service.auth.SETTINGS.portals",
        {"test_portal_id": "test_portal_netloc"},
    )
    res = auth.format_missing_licences_message(
        request_url, process_id, portal_id, missing_licences_message_template
    )
    exp = "https://base_url/datasets/test_process_id?tab=download#manage-licences"
    assert res == exp

    request_url = "https://base_url/api/v1/processes/process_id/execution"
    process_id = "test_process_id"
    missing_licences_message_template = "{dataset_licences_url}"
    portal_id = "test_portal_id"
    mocker.patch(
        "cads_processing_api_service.auth.SETTINGS.portals",
        {"test_portal_id": "test_portal_netloc"},
    )
    res = auth.format_missing_licences_message(
        request_url, process_id, portal_id, missing_licences_message_template
    )
    exp = "https://test_portal_netloc/datasets/test_process_id?tab=download#manage-licences"
    assert res == exp


def test_verify_licences() -> None:
    accepted_licences = {("licence_1", 1), ("licence_2", 2), ("licence_3", 3)}
    required_licences = {("licence_1", 1), ("licence_2", 2)}
    request_url = "http://base_url/api/v1/processes/process_id/execution"
    process_id = "process_id"
    missing_licences = auth.verify_licences(
        accepted_licences, required_licences, request_url, process_id
    )
    assert len(missing_licences) == 0

    accepted_licences = {("licence_1", 1), ("licence_2", 1)}
    required_licences = {("licence_1", 1), ("licence_2", 2)}
    with pytest.raises(exceptions.PermissionDenied):
        missing_licences = auth.verify_licences(
            accepted_licences, required_licences, request_url, process_id
        )


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


def test_verify_cost(mocker) -> None:
    mocker.patch(
        "cads_processing_api_service.costing.compute_costing",
        return_value=models.CostingInfo(
            costs={"cost_id_1": 10.0, "cost_id_2": 10.0},
            limits={"cost_id_1": 20.0, "cost_id_2": 20.0},
        ),
    )
    costs = auth.verify_cost({}, {}, "api")
    assert costs == {"cost_id_1": 10.0, "cost_id_2": 10.0}

    mocker.patch(
        "cads_processing_api_service.costing.compute_costing",
        return_value=models.CostingInfo(
            costs={"cost_id_1": 10.0, "cost_id_2": 10.0},
            limits={"cost_id_1": 5.0, "cost_id_2": 20.0},
        ),
    )
    with pytest.raises(exceptions.PermissionDenied):
        auth.verify_cost({}, {}, "api")
