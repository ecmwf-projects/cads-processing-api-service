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

import cads_broker  # type: ignore
import ogc_api_processes_fastapi.responses
import pytest
import sqlalchemy

from cads_processing_api_service import clients


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


def test_apply_jobs_filters() -> None:
    job_table = cads_broker.database.SystemRequest
    statement = sqlalchemy.select(job_table)
    filters = {"process_id": ["process"], "status": ["successful", "failed"]}
    statement = clients.apply_jobs_filters(statement, job_table, filters)
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
    bookmark = {"cursor": "MjAyMi0xMC0yNCAxMzozMjowMy4xNzgzOTc=", "back": False}
    sorting = {"sort_key": "created", "sort_dir": "desc"}
    statement = clients.apply_bookmark(statement, job_table, bookmark, sorting)
    compiled_statement = statement.compile()
    exp_params = {"created_at_1": "2022-10-24 13:32:03.178397"}
    exp_substatement = "WHERE system_requests.created_at < :created_at_1"
    assert compiled_statement.params == exp_params
    assert exp_substatement in compiled_statement.string


def test_apply_sorting() -> None:
    job_table = cads_broker.database.SystemRequest
    statement = sqlalchemy.select(job_table)
    bookmark = {"cursor": "MjAyMi0xMC0yNCAxMzozMjowMy4xNzgzOTc=", "back": False}
    sorting = {"sort_key": "created", "sort_dir": "desc"}
    statement = clients.apply_sorting(statement, job_table, bookmark, sorting)
    compiled_statement = statement.compile()
    exp_substatement = "ORDER BY system_requests.created_at DESC"
    assert exp_substatement in compiled_statement.string


def test_make_jobs_query_statement() -> None:
    job_table = cads_broker.database.SystemRequest
    filters = {"process_id": ["process"], "status": ["successful", "failed"]}
    sorting = {"sort_key": "created", "sort_dir": "desc"}
    bookmark = {"cursor": "MjAyMi0xMC0yNCAxMzozMjowMy4xNzgzOTc=", "back": False}
    limit = 10
    statement = clients.make_jobs_query_statement(
        job_table, filters, sorting, bookmark, limit
    )
    compiled_statement = statement.compile()
    exp_substatement = "system_requests.created_at < :created_at_1"
    assert exp_substatement in compiled_statement.string

    bookmark = {"cursor": None, "back": None}
    statement = clients.make_jobs_query_statement(
        job_table, filters, sorting, bookmark, limit
    )
    compiled_statement = statement.compile()
    exp_substatement = "WHERE system_requests.created_at < :created_at_1"
    assert exp_substatement not in compiled_statement.string


def test_make_cursor() -> None:
    jobs = [
        ogc_api_processes_fastapi.responses.StatusInfo(
            jobID="a0",
            status="successful",
            type="process",
            created="2022-10-24T13:34:02.321682",
        ),
        ogc_api_processes_fastapi.responses.StatusInfo(
            jobID="a1",
            status="successful",
            type="process",
            created="2022-10-24T13:32:02.321682",
        ),
        ogc_api_processes_fastapi.responses.StatusInfo(
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
        ogc_api_processes_fastapi.responses.StatusInfo(
            jobID="a0",
            status="successful",
            type="process",
            created="2022-10-24T13:34:02.321682",
        ),
        ogc_api_processes_fastapi.responses.StatusInfo(
            jobID="a1",
            status="successful",
            type="process",
            created="2022-10-24T13:32:02.321682",
        ),
        ogc_api_processes_fastapi.responses.StatusInfo(
            jobID="a1",
            status="successful",
            type="process",
            created="2022-10-24T13:30:02.321682",
        ),
    ]

    pagination_qs = clients.make_pagination_qs(jobs, "created")
    exp_qs = ogc_api_processes_fastapi.responses.PaginationQueryParameters(
        next={"cursor": clients.encode_base64(str(jobs[-1].created)), "back": False},
        prev={"cursor": clients.encode_base64(str(jobs[0].created)), "back": True},
    )
    assert pagination_qs == exp_qs
