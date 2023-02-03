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
from typing import Any

import ogc_api_processes_fastapi.exceptions
import ogc_api_processes_fastapi.models
import pytest
import sqlalchemy.orm

from cads_processing_api_service import models, response, utils


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

    cursor = response.make_cursor(jobs, "created", "next")
    cursor_decoded = utils.decode_base64(cursor)
    exp_cursor_decoded = str(jobs[-1].created)
    assert cursor_decoded == exp_cursor_decoded

    cursor = response.make_cursor(jobs, "created", "prev")
    cursor_decoded = utils.decode_base64(cursor)
    exp_cursor_decoded = str(jobs[0].created)
    assert cursor_decoded == exp_cursor_decoded

    with pytest.raises(ValueError):
        cursor = response.make_cursor(jobs, "created", "previous")


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

    pagination_qs = response.make_pagination_qs(jobs, "created")
    exp_qs = ogc_api_processes_fastapi.models.PaginationQueryParameters(
        next={"cursor": utils.encode_base64(str(jobs[-1].created)), "back": False},
        prev={"cursor": utils.encode_base64(str(jobs[0].created)), "back": True},
    )
    assert pagination_qs == exp_qs


def test_make_status_info() -> None:
    job: dict[str, Any] = {
        "status": "running",
        "request_uid": "1234",
        "process_id": "1234",
        "created_at": "2023-01-01T16:20:12.175021",
        "started_at": "2023-01-01T16:20:12.175021",
        "finished_at": "2023-01-01T16:20:12.175021",
        "updated_at": "2023-01-01T16:20:12.175021",
        "request_body": {"kwargs": {"request": {"product_type": ["reanalysis"]}}},
    }
    mock_session = unittest.mock.Mock(spec=sqlalchemy.orm.Session)
    status_info = response.make_status_info(
        job, session=mock_session, add_results=False
    )
    exp_status_info = models.StatusInfo(
        type="process",
        jobID=job["request_uid"],
        processID=job["process_id"],
        status=job["status"],
        created=job["created_at"],
        started=job["started_at"],
        finished=job["finished_at"],
        updated=job["updated_at"],
        request=job["request_body"]["kwargs"]["request"],
    )
    assert status_info == exp_status_info

    exp_results = {"key": "value"}
    with unittest.mock.patch(
        "cads_processing_api_service.crud.get_results_from_broker_db"
    ) as mock_get_results_from_broker_db:
        mock_get_results_from_broker_db.return_value = exp_results
        status_info = response.make_status_info(job, session=mock_session)
    assert status_info.results == exp_results

    with unittest.mock.patch(
        "cads_processing_api_service.crud.get_results_from_broker_db"
    ) as mock_get_results_from_broker_db:
        mock_get_results_from_broker_db.side_effect = (
            ogc_api_processes_fastapi.exceptions.JobResultsFailed
        )
        status_info = response.make_status_info(job, session=mock_session)
    exp_results_keys = ("type", "title", "detail")
    results = status_info.results
    assert results is not None
    assert all([key in results.keys() for key in exp_results_keys])

    with unittest.mock.patch(
        "cads_processing_api_service.crud.get_results_from_broker_db"
    ) as mock_get_results_from_broker_db:
        mock_get_results_from_broker_db.side_effect = (
            ogc_api_processes_fastapi.exceptions.ResultsNotReady
        )
        status_info = response.make_status_info(job, session=mock_session)
    assert status_info.results is None
