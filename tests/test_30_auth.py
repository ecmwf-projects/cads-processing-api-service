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

import cads_broker
import pytest

from cads_processing_api_service import auth, exceptions


def test_get_contextual_accepted_licences() -> None:
    execution_content: dict[str, list[dict[str, str | int]] | None] = {
        "acceptedLicences": [
            {"id": "licence", "revision": 0},
            {"id": "licence", "revision": 0},
        ]
    }
    licences = auth.get_contextual_accepted_licences(execution_content)
    exp_licences = {("licence", 0)}
    assert licences == exp_licences

    execution_content = {"acceptedLicences": None}
    licences = auth.get_contextual_accepted_licences(execution_content)
    exp_licences = set()
    assert licences == exp_licences


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
