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

import unittest.mock
from typing import Optional

import cads_broker
import ogc_api_processes_fastapi.exceptions
import ogc_api_processes_fastapi.models
import pytest
import sqlalchemy
import sqlalchemy.orm.exc

from cads_processing_api_service import clients, exceptions, models


def test_parse_sortby() -> None:
    sortby = "my_custom_id_asc"
    sort_params = clients.parse_sortby(sortby)
    exp_sort_params = ("my_custom_id", "asc")
    assert sort_params == exp_sort_params

    sortby = "my_custom_id_desc"
    sort_params = clients.parse_sortby(sortby)
    exp_sort_params = ("my_custom_id", "desc")
    assert sort_params == exp_sort_params


def test_get_compare_and_sort_method_name() -> None:
    for sort_dir, back in zip(("asc", "desc", "desc"), (True, False, None)):
        methods = clients.get_compare_and_sort_method_name(sort_dir, back)
        exp_compare_method_name = "__lt__"
        exp_sort_method_name = "desc"
        assert methods["compare_method_name"] == exp_compare_method_name
        assert methods["sort_method_name"] == exp_sort_method_name

    for sort_dir, back in zip(("asc", "asc", "desc"), (False, None, True)):
        methods = clients.get_compare_and_sort_method_name(sort_dir, back)
        exp_compare_method_name = "__gt__"
        exp_sort_method_name = "asc"
        assert methods["compare_method_name"] == exp_compare_method_name
        assert methods["sort_method_name"] == exp_sort_method_name

    with pytest.raises(ValueError):
        methods = clients.get_compare_and_sort_method_name("ascending", True)


def test_encode_decode_base64() -> None:
    exp_decoded = "2022-10-24T12:24:29.919877"
    encoded = clients.encode_base64(exp_decoded)
    decoded = clients.decode_base64(encoded)
    assert decoded == exp_decoded


def test_apply_metadata_filters() -> None:
    job_table = cads_broker.database.SystemRequest
    statement = sqlalchemy.select(job_table)
    metadata_filters = {"user_id": [0]}
    statement = clients.apply_metadata_filters(statement, job_table, metadata_filters)
    compiled_statement = statement.compile()
    exp_params = {"param_1": [0], "request_metadata_1": "user_id"}
    exp_substatement = (
        "WHERE (system_requests.request_metadata ->> :request_metadata_1) "
        "IN (__[POSTCOMPILE_param_1])"
    )
    assert compiled_statement.params == exp_params
    assert exp_substatement in compiled_statement.string


def test_apply_job_filters() -> None:
    job_table = cads_broker.database.SystemRequest
    statement = sqlalchemy.select(job_table)
    filters = {"process_id": ["process"], "status": ["successful", "failed"]}
    statement = clients.apply_job_filters(statement, job_table, filters)
    compiled_statement = statement.compile()
    exp_params = {"process_id_1": ["process"], "status_1": ["successful", "failed"]}
    exp_substatement = (
        "WHERE system_requests.process_id IN (__[POSTCOMPILE_process_id_1]) "
        "AND system_requests.status IN (__[POSTCOMPILE_status_1])"
    )
    assert compiled_statement.params == exp_params
    assert exp_substatement in compiled_statement.string


def test_apply_bookmark() -> None:
    job_table = cads_broker.database.SystemRequest
    statement = sqlalchemy.select(job_table)
    cursor = "MjAyMi0xMC0yNCAxMzozMjowMy4xNzgzOTc="
    back = False
    sort_key, sort_dir = clients.parse_sortby(
        clients.JobSortCriterion.created_at_asc.name
    )
    statement = clients.apply_bookmark(
        statement, job_table, cursor, back, sort_key, sort_dir
    )
    compiled_statement = statement.compile()
    exp_params = {"created_at_1": "2022-10-24 13:32:03.178397"}
    exp_substatement = "WHERE system_requests.created_at > :created_at_1"
    assert compiled_statement.params == exp_params
    assert exp_substatement in compiled_statement.string


def test_apply_sorting() -> None:
    job_table = cads_broker.database.SystemRequest
    statement = sqlalchemy.select(job_table)
    back = False
    sort_key, sort_dir = clients.parse_sortby(
        clients.JobSortCriterion.created_at_asc.name
    )
    statement = clients.apply_sorting(statement, job_table, back, sort_key, sort_dir)
    compiled_statement = statement.compile()
    exp_substatement = "ORDER BY system_requests.created_at ASC"
    assert exp_substatement in compiled_statement.string


def test_make_cursor() -> None:
    jobs = [
        ogc_api_processes_fastapi.models.StatusInfo(
            jobID="a0",
            status="successful",
            type="process",
            created="2022-10-24T13:34:02.321682",
        ),
        ogc_api_processes_fastapi.models.StatusInfo(
            jobID="a1",
            status="successful",
            type="process",
            created="2022-10-24T13:32:02.321682",
        ),
        ogc_api_processes_fastapi.models.StatusInfo(
            jobID="a1",
            status="successful",
            type="process",
            created="2022-10-24T13:30:02.321682",
        ),
    ]

    cursor = clients.make_cursor(jobs, "created", "next")
    cursor_decoded = clients.decode_base64(cursor)
    exp_cursor_decoded = str(jobs[-1].created)
    assert cursor_decoded == exp_cursor_decoded

    cursor = clients.make_cursor(jobs, "created", "prev")
    cursor_decoded = clients.decode_base64(cursor)
    exp_cursor_decoded = str(jobs[0].created)
    assert cursor_decoded == exp_cursor_decoded

    with pytest.raises(ValueError):
        cursor = clients.make_cursor(jobs, "created", "previous")


def test_make_pagination_qs() -> None:
    jobs = [
        ogc_api_processes_fastapi.models.StatusInfo(
            jobID="a0",
            status="successful",
            type="process",
            created="2022-10-24T13:34:02.321682",
        ),
        ogc_api_processes_fastapi.models.StatusInfo(
            jobID="a1",
            status="successful",
            type="process",
            created="2022-10-24T13:32:02.321682",
        ),
        ogc_api_processes_fastapi.models.StatusInfo(
            jobID="a1",
            status="successful",
            type="process",
            created="2022-10-24T13:30:02.321682",
        ),
    ]

    pagination_qs = clients.make_pagination_qs(jobs, "created")
    exp_qs = ogc_api_processes_fastapi.models.PaginationQueryParameters(
        next={"cursor": clients.encode_base64(str(jobs[-1].created)), "back": False},
        prev={"cursor": clients.encode_base64(str(jobs[0].created)), "back": True},
    )
    assert pagination_qs == exp_qs


def test_get_contextual_accepted_licences() -> None:
    execution_content: dict[str, Optional[list[dict[str, str | int]]]] = {
        "acceptedLicences": [
            {"id": "licence", "revision": 0},
            {"id": "licence", "revision": 0},
        ]
    }
    licences = clients.get_contextual_accepted_licences(execution_content)
    exp_licences = {("licence", 0)}
    assert licences == exp_licences

    execution_content = {"acceptedLicences": None}
    licences = clients.get_contextual_accepted_licences(execution_content)
    exp_licences = set()
    assert licences == exp_licences


def test_check_licences() -> None:
    required_licences = {("licence_1", 1), ("licence_2", 2)}
    accepted_licences = {("licence_1", 1), ("licence_2", 2), ("licence_3", 3)}
    missing_licences = clients.check_licences(required_licences, accepted_licences)
    assert len(missing_licences) == 0

    required_licences = {("licence_1", 1), ("licence_2", 2)}
    accepted_licences = {("licence_1", 1), ("licence_2", 1)}
    with pytest.raises(exceptions.PermissionDenied):
        missing_licences = clients.check_licences(required_licences, accepted_licences)


def test_check_token() -> None:
    token = "token"

    verification_endpoint, auth_header = clients.check_token(pat=token)
    exp_verification_endpoint = "/account/verification/pat"
    exp_auth_header = {"PRIVATE-TOKEN": token}
    assert verification_endpoint == exp_verification_endpoint
    assert auth_header == exp_auth_header

    verification_endpoint, auth_header = clients.check_token(jwt=token)
    exp_verification_endpoint = "/account/verification/oidc"
    exp_auth_header = {"Authorization": token}
    assert verification_endpoint == exp_verification_endpoint
    assert auth_header == exp_auth_header

    with pytest.raises(exceptions.PermissionDenied):
        verification_endpoint, auth_header = clients.check_token()


def test_verify_permission() -> None:
    job = {"request_metadata": {"user_id": 0}}
    user = {"id": 0}
    try:
        clients.verify_permission(user, job)
    except exceptions.PermissionDenied as exc:
        assert False, f"'{user} / {job}' raised an exception {exc}"

    user = {"id": 1}
    with pytest.raises(exceptions.PermissionDenied):
        clients.verify_permission(user, job)


def test_dictify_job() -> None:
    request = cads_broker.database.SystemRequest(request_id=0, status="failed")
    exp_job = {"request_id": 0, "status": "failed"}
    res_job = clients.dictify_job(request)
    assert isinstance(res_job, dict)
    assert all([key in res_job and res_job[key] == exp_job[key] for key in exp_job])


def test_get_job_from_broker_db() -> None:
    test_job_id = "1234"
    with unittest.mock.patch("cads_broker.database.get_request") as mock_get_request:
        mock_get_request.return_value = cads_broker.database.SystemRequest(
            user_id=0, request_uid=test_job_id
        )
        job = clients.get_job_from_broker_db(test_job_id)
    assert isinstance(job, dict)
    assert job["user_id"] == 0
    assert job["request_uid"] == test_job_id

    with unittest.mock.patch("cads_broker.database.get_request") as mock_get_request:
        mock_get_request.side_effect = sqlalchemy.exc.StatementError(
            message=None, statement=None, params=None, orig=None
        )
        with pytest.raises(ogc_api_processes_fastapi.exceptions.NoSuchJob):
            job = clients.get_job_from_broker_db(test_job_id)

    with unittest.mock.patch("cads_broker.database.get_request") as mock_get_request:
        mock_get_request.side_effect = sqlalchemy.orm.exc.NoResultFound()
        with pytest.raises(ogc_api_processes_fastapi.exceptions.NoSuchJob):
            job = clients.get_job_from_broker_db("1234")


def test_get_results_from_broker_db() -> None:
    job = {"status": "successful", "request_uid": "1234"}
    with unittest.mock.patch(
        "cads_broker.database.get_request_result"
    ) as mock_get_request_result:
        mock_get_request_result.return_value = {
            "args": [
                {"key": "value"},
            ]
        }
        results = clients.get_results_from_broker_db(job)
    exp_results = {"asset": {"value": {"key": "value"}}}
    assert results == exp_results

    job = {"status": "failed", "request_uid": "1234", "response_traceback": "traceback"}
    with pytest.raises(ogc_api_processes_fastapi.exceptions.JobResultsFailed):
        results = clients.get_results_from_broker_db(job)

    job = {
        "status": "accepted",
        "request_uid": "1234",
    }
    with pytest.raises(ogc_api_processes_fastapi.exceptions.ResultsNotReady):
        results = clients.get_results_from_broker_db(job)

    job = {
        "status": "running",
        "request_uid": "1234",
    }
    with pytest.raises(ogc_api_processes_fastapi.exceptions.ResultsNotReady):
        results = clients.get_results_from_broker_db(job)


def test_make_status_info() -> None:
    job = {
        "status": "running",
        "request_uid": "1234",
        "process_id": "1234",
        "created_at": "2023-01-01T16:20:12.175021",
        "started_at": "2023-01-01T16:20:12.175021",
        "finished_at": "2023-01-01T16:20:12.175021",
        "updated_at": "2023-01-01T16:20:12.175021",
    }
    status_info = clients.make_status_info(job, add_results=False)
    exp_status_info = models.StatusInfo(
        type="process",
        jobID=job["request_uid"],
        processID=job["process_id"],
        status=job["status"],
        created=job["created_at"],
        started=job["started_at"],
        finished=job["finished_at"],
        updated=job["updated_at"],
    )
    assert status_info == exp_status_info

    exp_results = {"key": "value"}
    with unittest.mock.patch(
        "cads_processing_api_service.clients.get_results_from_broker_db"
    ) as mock_get_results_from_broker_db:
        mock_get_results_from_broker_db.return_value = exp_results
        status_info = clients.make_status_info(job)
    assert status_info.results == exp_results

    with unittest.mock.patch(
        "cads_processing_api_service.clients.get_results_from_broker_db"
    ) as mock_get_results_from_broker_db:
        mock_get_results_from_broker_db.side_effect = (
            ogc_api_processes_fastapi.exceptions.JobResultsFailed
        )
        status_info = clients.make_status_info(job)
    exp_results_keys = ("type", "title", "detail")
    assert all([key in status_info.results.keys() for key in exp_results_keys])

    with unittest.mock.patch(
        "cads_processing_api_service.clients.get_results_from_broker_db"
    ) as mock_get_results_from_broker_db:
        mock_get_results_from_broker_db.side_effect = (
            ogc_api_processes_fastapi.exceptions.ResultsNotReady
        )
        status_info = clients.make_status_info(job)
    assert status_info.results is None
