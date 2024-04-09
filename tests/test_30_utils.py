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

import datetime
import unittest.mock
import uuid
from typing import Any

import cacholote
import cads_broker
import ogc_api_processes_fastapi.exceptions
import ogc_api_processes_fastapi.models
import pytest
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.orm.exc

from cads_processing_api_service import models, utils


def test_parse_sortby() -> None:
    sortby = "my_custom_id_asc"
    sort_params = utils.parse_sortby(sortby)
    exp_sort_params = ("my_custom_id", "asc")
    assert sort_params == exp_sort_params

    sortby = "my_custom_id_desc"
    sort_params = utils.parse_sortby(sortby)
    exp_sort_params = ("my_custom_id", "desc")
    assert sort_params == exp_sort_params


def test_get_compare_and_sort_method_name() -> None:
    for sort_dir, back in zip(("asc", "desc", "desc"), (True, False, None)):
        methods = utils.get_compare_and_sort_method_name(sort_dir, back)
        exp_compare_method_name = "__lt__"
        exp_sort_method_name = "desc"
        assert methods["compare_method_name"] == exp_compare_method_name
        assert methods["sort_method_name"] == exp_sort_method_name

    for sort_dir, back in zip(("asc", "asc", "desc"), (False, None, True)):
        methods = utils.get_compare_and_sort_method_name(sort_dir, back)
        exp_compare_method_name = "__gt__"
        exp_sort_method_name = "asc"
        assert methods["compare_method_name"] == exp_compare_method_name
        assert methods["sort_method_name"] == exp_sort_method_name

    with pytest.raises(ValueError):
        methods = utils.get_compare_and_sort_method_name("ascending", True)


def test_encode_decode_base64() -> None:
    exp_decoded = "2022-10-24T12:24:29.919877"
    encoded = utils.encode_base64(exp_decoded)
    decoded = utils.decode_base64(encoded)
    assert decoded == exp_decoded


def test_apply_metadata_filters() -> None:
    job_table = cads_broker.database.SystemRequest
    statement = sqlalchemy.select(job_table)
    metadata_filters = {"user_id": ["0"]}
    statement = utils.apply_metadata_filters(statement, job_table, metadata_filters)
    compiled_statement = statement.compile()
    exp_params = {"param_1": ["0"], "request_metadata_1": "user_id"}
    exp_substatement = (
        "WHERE (system_requests.request_metadata ->> :request_metadata_1) "
        "IN (__[POSTCOMPILE_param_1])"
    )
    assert compiled_statement.params == exp_params
    assert exp_substatement in compiled_statement.string


def test_apply_job_filters() -> None:
    job_table = cads_broker.database.SystemRequest
    statement = sqlalchemy.select(job_table)
    filters = {
        "process_id": ["process"],
        "status": ["successful", "failed"],
    }
    statement = utils.apply_job_filters(statement, job_table, filters)
    compiled_statement = statement.compile()
    exp_params = {
        "process_id_1": ["process"],
        "status_1": ["successful", "failed"],
        "status_2": "dismissed",
    }
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
    sort_key, sort_dir = utils.parse_sortby(utils.JobSortCriterion.created_at_asc.name)
    statement = utils.apply_bookmark(
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
    sort_key, sort_dir = utils.parse_sortby(utils.JobSortCriterion.created_at_asc.name)
    statement = utils.apply_sorting(statement, job_table, back, sort_key, sort_dir)
    compiled_statement = statement.compile()
    exp_substatement = "ORDER BY system_requests.created_at ASC"
    assert exp_substatement in compiled_statement.string


def test_make_cursor() -> None:
    jobs = [
        ogc_api_processes_fastapi.models.StatusInfo(
            jobID="a0",
            status=ogc_api_processes_fastapi.models.StatusCode.successful,
            type=ogc_api_processes_fastapi.models.JobType.process,
            created=datetime.datetime.strptime(
                "2022-10-24T13:34:02.321682", "%Y-%m-%dT%H:%M:%S.%f"
            ),
        ),
        ogc_api_processes_fastapi.models.StatusInfo(
            jobID="a1",
            status=ogc_api_processes_fastapi.models.StatusCode.successful,
            type=ogc_api_processes_fastapi.models.JobType.process,
            created=datetime.datetime.strptime(
                "2022-10-24T13:34:02.321682", "%Y-%m-%dT%H:%M:%S.%f"
            ),
        ),
        ogc_api_processes_fastapi.models.StatusInfo(
            jobID="a1",
            status=ogc_api_processes_fastapi.models.StatusCode.successful,
            type=ogc_api_processes_fastapi.models.JobType.process,
            created=datetime.datetime.strptime(
                "2022-10-24T13:30:02.321682", "%Y-%m-%dT%H:%M:%S.%f"
            ),
        ),
    ]

    cursor = utils.make_cursor(jobs, "created", "next")
    cursor_decoded = utils.decode_base64(cursor)
    exp_cursor_decoded = str(jobs[-1].created)
    assert cursor_decoded == exp_cursor_decoded

    cursor = utils.make_cursor(jobs, "created", "prev")
    cursor_decoded = utils.decode_base64(cursor)
    exp_cursor_decoded = str(jobs[0].created)
    assert cursor_decoded == exp_cursor_decoded

    with pytest.raises(ValueError):
        cursor = utils.make_cursor(jobs, "created", "previous")


def test_make_pagination_query_params() -> None:
    jobs = [
        ogc_api_processes_fastapi.models.StatusInfo(
            jobID="a0",
            status=ogc_api_processes_fastapi.models.StatusCode.successful,
            type=ogc_api_processes_fastapi.models.JobType.process,
            created=datetime.datetime.strptime(
                "2022-10-24T13:34:02.321682", "%Y-%m-%dT%H:%M:%S.%f"
            ),
        ),
        ogc_api_processes_fastapi.models.StatusInfo(
            jobID="a1",
            status=ogc_api_processes_fastapi.models.StatusCode.successful,
            type=ogc_api_processes_fastapi.models.JobType.process,
            created=datetime.datetime.strptime(
                "2022-10-24T13:32:02.321682", "%Y-%m-%dT%H:%M:%S.%f"
            ),
        ),
        ogc_api_processes_fastapi.models.StatusInfo(
            jobID="a1",
            status=ogc_api_processes_fastapi.models.StatusCode.successful,
            type=ogc_api_processes_fastapi.models.JobType.process,
            created=datetime.datetime.strptime(
                "2022-10-24T13:30:02.321682", "%Y-%m-%dT%H:%M:%S.%f"
            ),
        ),
    ]

    pagination_query_params = utils.make_pagination_query_params(jobs, "created")
    exp_qs = ogc_api_processes_fastapi.models.PaginationQueryParameters(
        next={"cursor": utils.encode_base64(str(jobs[-1].created)), "back": "False"},
        prev={"cursor": utils.encode_base64(str(jobs[0].created)), "back": "True"},
    )
    assert pagination_query_params == exp_qs


def test_dictify_job() -> None:
    request_uid = uuid.uuid4()
    request = cads_broker.database.SystemRequest(
        request_uid=request_uid, status="failed"
    )
    exp_job = {"request_uid": request_uid, "status": "failed"}
    res_job = utils.dictify_job(request)
    assert isinstance(res_job, dict)
    assert all([key in res_job and res_job[key] == exp_job[key] for key in exp_job])


def test_get_job_from_broker_db() -> None:
    test_job_id = "1234"
    mock_session = unittest.mock.Mock(spec=sqlalchemy.orm.Session)
    with unittest.mock.patch("cads_broker.database.get_request") as mock_get_request:
        mock_get_request.return_value = cads_broker.database.SystemRequest(
            request_uid=test_job_id
        )
        job = utils.get_job_from_broker_db(test_job_id, session=mock_session)
    assert isinstance(job, cads_broker.SystemRequest)
    assert job.request_uid == test_job_id

    with unittest.mock.patch("cads_broker.database.get_request") as mock_get_request:
        mock_get_request.side_effect = cads_broker.database.NoResultFound()
        with pytest.raises(ogc_api_processes_fastapi.exceptions.NoSuchJob):
            job = utils.get_job_from_broker_db(test_job_id, session=mock_session)

    with unittest.mock.patch("cads_broker.database.get_request") as mock_get_request:
        mock_get_request.side_effect = cads_broker.database.NoResultFound()
        with pytest.raises(ogc_api_processes_fastapi.exceptions.NoSuchJob):
            job = utils.get_job_from_broker_db("1234", session=mock_session)


def test_get_results_from_job() -> None:
    mock_session = unittest.mock.Mock(spec=sqlalchemy.orm.Session)
    job = cads_broker.SystemRequest(
        **{
            "status": "successful",
            "request_uid": "1234",
            "cache_entry": cacholote.database.CacheEntry(
                result={"args": [{"key": "value"}]}
            ),
        }
    )
    results = utils.get_results_from_job(job, session=mock_session)
    exp_results = {"asset": {"value": {"key": "value"}}}
    assert results == exp_results

    job = cads_broker.SystemRequest(
        **{
            "status": "failed",
            "request_uid": "1234",
        }
    )
    with pytest.raises(ogc_api_processes_fastapi.exceptions.JobResultsFailed) as exc:
        with unittest.mock.patch(
            "cads_processing_api_service.utils.get_job_events"
        ) as mock_get_job_events:
            mock_get_job_events.return_value = [
                "2024-01-01T16:20:12.175021",
                "error message",
            ]
            results = utils.get_results_from_job(job, session=mock_session)
        assert exc.value.traceback == "error message"

    job = cads_broker.SystemRequest(**{"status": "accepted", "request_uid": "1234"})
    with pytest.raises(ogc_api_processes_fastapi.exceptions.ResultsNotReady):
        results = utils.get_results_from_job(job, session=mock_session)

    job = cads_broker.SystemRequest(**{"status": "running", "request_uid": "1234"})
    with pytest.raises(ogc_api_processes_fastapi.exceptions.ResultsNotReady):
        results = utils.get_results_from_job(job, session=mock_session)


def test_make_status_info() -> None:
    job: dict[str, Any] = {
        "status": "running",
        "request_uid": "1234",
        "process_id": "1234",
        "created_at": "2023-01-01T16:20:12.175021",
        "started_at": "2023-01-01T16:20:12.175021",
        "finished_at": "2023-01-01T16:20:12.175021",
        "updated_at": "2023-01-01T16:20:12.175021",
        "request_body": {"request": {"product_type": ["reanalysis"]}},
    }
    status_info = utils.make_status_info(job)
    exp_status_info = models.StatusInfo(
        type=ogc_api_processes_fastapi.models.JobType.process,
        jobID=job["request_uid"],
        processID=job["process_id"],
        status=job["status"],
        created=job["created_at"],
        started=job["started_at"],
        finished=job["finished_at"],
        updated=job["updated_at"],
    )
    assert status_info == exp_status_info
