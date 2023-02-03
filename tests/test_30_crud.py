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

import cads_broker
import ogc_api_processes_fastapi.exceptions
import ogc_api_processes_fastapi.models
import pytest
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.orm.exc

from cads_processing_api_service import clients, crud, utils


def test_parse_sortby() -> None:
    sortby = "my_custom_id_asc"
    sort_params = crud.parse_sortby(sortby)
    exp_sort_params = ("my_custom_id", "asc")
    assert sort_params == exp_sort_params

    sortby = "my_custom_id_desc"
    sort_params = crud.parse_sortby(sortby)
    exp_sort_params = ("my_custom_id", "desc")
    assert sort_params == exp_sort_params


def test_get_compare_and_sort_method_name() -> None:
    for sort_dir, back in zip(("asc", "desc", "desc"), (True, False, None)):
        methods = crud.get_compare_and_sort_method_name(sort_dir, back)
        exp_compare_method_name = "__lt__"
        exp_sort_method_name = "desc"
        assert methods["compare_method_name"] == exp_compare_method_name
        assert methods["sort_method_name"] == exp_sort_method_name

    for sort_dir, back in zip(("asc", "asc", "desc"), (False, None, True)):
        methods = crud.get_compare_and_sort_method_name(sort_dir, back)
        exp_compare_method_name = "__gt__"
        exp_sort_method_name = "asc"
        assert methods["compare_method_name"] == exp_compare_method_name
        assert methods["sort_method_name"] == exp_sort_method_name

    with pytest.raises(ValueError):
        methods = crud.get_compare_and_sort_method_name("ascending", True)


def test_encode_decode_base64() -> None:
    exp_decoded = "2022-10-24T12:24:29.919877"
    encoded = utils.encode_base64(exp_decoded)
    decoded = utils.decode_base64(encoded)
    assert decoded == exp_decoded


def test_apply_metadata_filters() -> None:
    job_table = cads_broker.database.SystemRequest
    statement = sqlalchemy.select(job_table)
    metadata_filters = {"user_id": ["0"]}
    statement = crud.apply_metadata_filters(statement, job_table, metadata_filters)
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
    statement = crud.apply_job_filters(statement, job_table, filters)
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
    sort_key, sort_dir = crud.parse_sortby(clients.JobSortCriterion.created_at_asc.name)  # type: ignore
    statement = crud.apply_bookmark(
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
    sort_key, sort_dir = crud.parse_sortby(clients.JobSortCriterion.created_at_asc.name)  # type: ignore
    statement = crud.apply_sorting(statement, job_table, back, sort_key, sort_dir)
    compiled_statement = statement.compile()
    exp_substatement = "ORDER BY system_requests.created_at ASC"
    assert exp_substatement in compiled_statement.string


def test_dictify_job() -> None:
    request = cads_broker.database.SystemRequest(request_id=0, status="failed")
    exp_job = {"request_id": 0, "status": "failed"}
    res_job = crud.dictify_job(request)
    assert isinstance(res_job, dict)
    assert all([key in res_job and res_job[key] == exp_job[key] for key in exp_job])


def test_get_job_from_broker_db() -> None:
    test_job_id = "1234"
    mock_session = unittest.mock.Mock(spec=sqlalchemy.orm.Session)
    with unittest.mock.patch(
        "cads_broker.database.get_request_in_session"
    ) as mock_get_request:
        mock_get_request.return_value = cads_broker.database.SystemRequest(
            request_uid=test_job_id
        )
        job = crud.get_job_from_broker_db(test_job_id, session=mock_session)
    assert isinstance(job, dict)
    assert job["request_uid"] == test_job_id

    with unittest.mock.patch(
        "cads_broker.database.get_request_in_session"
    ) as mock_get_request:
        mock_get_request.side_effect = sqlalchemy.exc.StatementError(
            message=None, statement=None, params=None, orig=None
        )
        with pytest.raises(ogc_api_processes_fastapi.exceptions.NoSuchJob):
            job = crud.get_job_from_broker_db(test_job_id, session=mock_session)

    with unittest.mock.patch(
        "cads_broker.database.get_request_in_session"
    ) as mock_get_request:
        mock_get_request.side_effect = sqlalchemy.orm.exc.NoResultFound()
        with pytest.raises(ogc_api_processes_fastapi.exceptions.NoSuchJob):
            job = crud.get_job_from_broker_db("1234", session=mock_session)


def test_get_results_from_broker_db() -> None:
    job = {"status": "successful", "request_uid": "1234"}
    mock_session = unittest.mock.Mock(spec=sqlalchemy.orm.Session)
    with unittest.mock.patch(
        "cads_broker.database.get_request_result_in_session"
    ) as mock_get_request_result:
        mock_get_request_result.return_value = {
            "args": [
                {"key": "value"},
            ]
        }
        results = crud.get_results_from_broker_db(job, session=mock_session)
    exp_results = {"asset": {"value": {"key": "value"}}}
    assert results == exp_results

    job = {"status": "failed", "request_uid": "1234", "response_traceback": "traceback"}
    with pytest.raises(ogc_api_processes_fastapi.exceptions.JobResultsFailed):
        results = crud.get_results_from_broker_db(job, session=mock_session)

    job = {
        "status": "accepted",
        "request_uid": "1234",
    }
    with pytest.raises(ogc_api_processes_fastapi.exceptions.ResultsNotReady):
        results = crud.get_results_from_broker_db(job, session=mock_session)

    job = {
        "status": "running",
        "request_uid": "1234",
    }
    with pytest.raises(ogc_api_processes_fastapi.exceptions.ResultsNotReady):
        results = crud.get_results_from_broker_db(job, session=mock_session)
