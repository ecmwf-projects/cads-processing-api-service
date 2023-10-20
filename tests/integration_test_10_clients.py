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

import time
import urllib.parse
import uuid
from typing import Any

import pytest
import requests

EXISTING_PROCESS_ID = "reanalysis-era5-pressure-levels"
EXISTING_PROCESS_LICENCE_ID = "licence-to-use-copernicus-products"
EXISTING_PROCESS_LICENCE_DATA = {"revision": 12}
NON_EXISTING_PROCESS_ID = "non-existing-dataset"
NON_EXISTING_JOB_ID = str(uuid.uuid4())
POST_PROCESS_REQUEST_BODY_SUCCESS = {
    "inputs": {
        "product_type": ["reanalysis"],
        "format": "grib",
        "variable": ["temperature"],
        "pressure_level": ["1"],
        "year": ["1971"],
        "month": ["01"],
        "day": ["25"],
        "time": ["06:00"],
    }
}
POST_PROCESS_REQUEST_BODY_SUCCESS_W_LICENCES = {
    "inputs": {
        "product_type": ["reanalysis"],
        "format": ["grib"],
        "variable": ["temperature"],
        "pressure_level": ["1"],
        "year": ["1971"],
        "month": ["01"],
        "day": ["25"],
        "time": ["06:00"],
    },
    "acceptedLicences": [{"id": "licence-to-use-copernicus-products", "revision": 12}],
}
POST_PROCESS_REQUEST_BODY_FAIL = {
    "inputs": {
        "product_type": ["reanalysis"],
        "format": ["grib"],
        "variable": ["nno-existing-variable"],
        "pressure_level": ["1"],
        "year": ["1971"],
        "month": ["01"],
        "day": ["25"],
        "time": ["06:00"],
    }
}
POST_PROCESS_REQUEST_BODY_SLOW = {
    "inputs": {
        "product_type": ["reanalysis"],
        "format": "grib",
        "variable": ["temperature"],
        "pressure_level": ["1"],
        "year": [f"{year}" for year in range(1959, 2022)],
        "month": [f"{month:02}" for month in range(1, 13)],
        "day": [f"{day:02}" for day in range(1, 32)],
        "time": [f"{hour:02}:00" for hour in range(0, 24)],
    }
}
TEST_PAT_1 = "00000000-0000-4000-a000-000000000000"
TEST_PAT_2 = "00000000-0000-3000-abcd-000000000001"
TEST_PAT_ANON = "00112233-4455-6677-c899-aabbccddeeff"
INVALID_PAT = "0123"
AUTH_HEADERS_VALID_1 = {"PRIVATE-TOKEN": TEST_PAT_1}
AUTH_HEADERS_VALID_2 = {"PRIVATE-TOKEN": TEST_PAT_2}
AUTH_HEADERS_VALID_ANON = {"PRIVATE-TOKEN": TEST_PAT_ANON}
AUTH_HEADERS_INVALID = {"PRIVATE-TOKEN": INVALID_PAT}
AUTH_HEADERS_MISSING: dict[str, str] = {}


def accept_licence(
    dev_env_prof_api_url: str,
    licence_id: str = EXISTING_PROCESS_LICENCE_ID,
    licence_data: dict[str, Any] = EXISTING_PROCESS_LICENCE_DATA,
    auth_headers: dict[str, str] = AUTH_HEADERS_VALID_1,
) -> requests.Response:
    request_url = urllib.parse.urljoin(
        dev_env_prof_api_url, f"account/licences/{licence_id}"
    )
    response = requests.put(request_url, headers=auth_headers, json=licence_data)
    return response


def submit_job(
    dev_env_proc_api_url: str,
    process_id: str = EXISTING_PROCESS_ID,
    request_body: dict[str, Any] = POST_PROCESS_REQUEST_BODY_SUCCESS,
    auth_headers: dict[str, str] = AUTH_HEADERS_VALID_1,
) -> requests.Response:
    request_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"processes/{process_id}/execution"
    )
    response = requests.post(
        request_url,
        json=request_body,
        headers=auth_headers,
    )
    return response


def delete_job(
    dev_env_proc_api_url: str,
    job_id: str,
    auth_headers: dict[str, str] = AUTH_HEADERS_VALID_1,
) -> requests.Response:
    response = requests.delete(
        urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}"),
        headers=auth_headers,
    )
    return response


def test_get_processes(dev_env_proc_api_url: str) -> None:
    response = requests.get(urllib.parse.urljoin(dev_env_proc_api_url, "processes"))
    response_status_code = response.status_code
    exp_status_code = 200
    assert response_status_code == exp_status_code

    response_body = response.json()
    exp_keys = ("links", "processes")
    assert all([key in response_body for key in exp_keys])

    number_of_processes = len(response_body["processes"])
    exp_number_of_processes = 10
    assert number_of_processes == exp_number_of_processes


def test_get_processes_limit_sorting(dev_env_proc_api_url: str) -> None:
    request_url = urllib.parse.urljoin(
        dev_env_proc_api_url, "processes?limit=4&sortby=-id"
    )
    response = requests.get(request_url)
    response_body = response.json()
    assert len(response_body["processes"]) == 4
    ids = [process["id"] for process in response_body["processes"]]
    sorted_ids = sorted(ids)
    assert ids == list(reversed(sorted_ids))

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "processes?limit=4")
    response = requests.get(request_url)
    response_body = response.json()
    assert len(response_body["processes"]) == 4
    ids = [process["id"] for process in response_body["processes"]]
    sorted_ids = sorted(ids)
    assert ids == sorted_ids


def test_get_processes_pagination(dev_env_proc_api_url: str) -> None:
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "processes?limit=4")
    response = requests.get(request_url)
    response_body = response.json()
    ids = [process["id"] for process in response_body["processes"]]

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "processes?limit=2")
    response = requests.get(request_url)
    response_body = response.json()
    assert len(response_body["processes"]) == 2
    links = response_body["links"]
    exp_rels = ["next", "prev"]
    all_rels = [link["rel"] for link in links]
    assert all(rel in all_rels for rel in exp_rels)
    first_page_ids = [process["id"] for process in response_body["processes"]]
    assert first_page_ids == ids[:2]

    for link in links:
        if link["rel"] == "next":
            request_url = link["href"]
    response = requests.get(request_url)
    response_body = response.json()
    assert len(response_body["processes"]) == 2
    second_page_created_ids = [process["id"] for process in response_body["processes"]]
    assert second_page_created_ids == ids[2:4]
    links = response_body["links"]

    for link in links:
        if link["rel"] == "prev":
            request_url = link["href"]
    response = requests.get(request_url)
    response_body = response.json()
    assert len(response_body["processes"]) == 2
    third_page_created_ids = [process["id"] for process in response_body["processes"]]
    assert third_page_created_ids == first_page_ids


def test_get_process(dev_env_proc_api_url: str) -> None:
    process_id = EXISTING_PROCESS_ID
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"processes/{process_id}")
    response = requests.get(request_url)
    response_status_code = response.status_code
    exp_status_code = 200
    assert response_status_code == exp_status_code

    response_body = response.json()
    exp_keys = (
        "title",
        "description",
        "id",
        "version",
        "jobControlOptions",
        "outputTransmission",
        "links",
        "inputs",
        "outputs",
    )
    assert all([key in response_body for key in exp_keys])


def test_get_process_exc_no_such_process(dev_env_proc_api_url: str) -> None:
    process_id = NON_EXISTING_PROCESS_ID
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"processes/{process_id}")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
    response_status_code = response.status_code
    exp_status_code = 404
    assert response_status_code == exp_status_code


def test_post_process_execution_stored_accepted_licences(
    dev_env_proc_api_url: str,
    dev_env_prof_api_url: str,
) -> None:
    response = accept_licence(dev_env_prof_api_url)
    response_status_code = response.status_code
    exp_status_codes = (201, 409)
    assert response_status_code in exp_status_codes

    response = submit_job(dev_env_proc_api_url)
    response_body = response.json()
    exp_keys = (
        "processID",
        "type",
        "jobID",
        "status",
        "created",
        "updated",
        "links",
    )
    assert all([key in response_body for key in exp_keys])
    exp_process_id = EXISTING_PROCESS_ID
    assert response_body["processID"] == exp_process_id
    exp_status = "accepted"
    assert response_body["status"] == exp_status

    response = delete_job(
        dev_env_proc_api_url,
        response_body["jobID"],
    )


def test_post_process_execution_context_accepted_licences(
    dev_env_proc_api_url: str,
) -> None:
    response = submit_job(
        dev_env_proc_api_url,
        request_body=POST_PROCESS_REQUEST_BODY_SUCCESS_W_LICENCES,
        auth_headers=AUTH_HEADERS_VALID_2,
    )
    response_status_code = response.status_code
    exp_status_code = 201
    assert response_status_code == exp_status_code

    response = delete_job(
        dev_env_proc_api_url,
        response.json()["jobID"],
        auth_headers=AUTH_HEADERS_VALID_2,
    )


def test_post_process_execution_anon_user(
    dev_env_proc_api_url: str,
) -> None:
    response = submit_job(dev_env_proc_api_url, auth_headers=AUTH_HEADERS_VALID_ANON)
    response_status_code = response.status_code
    exp_status_code = 403
    assert response_status_code == exp_status_code

    response = submit_job(
        dev_env_proc_api_url,
        request_body=POST_PROCESS_REQUEST_BODY_SUCCESS_W_LICENCES,
        auth_headers=AUTH_HEADERS_VALID_ANON,
    )
    response_status_code = response.status_code
    exp_status_code = 201
    assert response_status_code == exp_status_code

    response = delete_job(
        dev_env_proc_api_url,
        response.json()["jobID"],
        auth_headers=AUTH_HEADERS_VALID_ANON,
    )


def test_post_process_execution_not_authorized(
    dev_env_proc_api_url: str,
) -> None:
    response = submit_job(dev_env_proc_api_url, auth_headers=AUTH_HEADERS_MISSING)
    exp_status_code = 401
    assert response.status_code == exp_status_code

    response = submit_job(dev_env_proc_api_url, auth_headers=AUTH_HEADERS_INVALID)
    exp_status_code = 401
    assert response.status_code == exp_status_code


def test_post_process_execution_missing_licences(
    dev_env_proc_api_url: str,
) -> None:
    response = submit_job(dev_env_proc_api_url, auth_headers=AUTH_HEADERS_VALID_2)
    response_status_code = response.status_code
    exp_status_code = 403
    assert response_status_code == exp_status_code


def test_post_process_execution_exc_no_such_process(dev_env_proc_api_url: str) -> None:
    process_id = NON_EXISTING_PROCESS_ID
    response = submit_job(dev_env_proc_api_url, process_id, {})
    response_status_code = response.status_code
    exp_status_code = 404
    assert response_status_code == exp_status_code


def test_get_job_not_authorized(
    dev_env_proc_api_url: str, dev_env_prof_api_url: str
) -> None:
    response = accept_licence(dev_env_prof_api_url)
    response = submit_job(dev_env_proc_api_url)
    job_id = response.json()["jobID"]
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}")
    response = requests.get(request_url, headers=AUTH_HEADERS_MISSING)
    exp_status_code = 401
    assert response.status_code == exp_status_code

    response = requests.get(
        request_url,
        headers=AUTH_HEADERS_INVALID,
    )
    exp_status_code = 401
    assert response.status_code == exp_status_code

    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_2)
    response_status_code = response.status_code
    exp_status_code = 403
    assert response_status_code == exp_status_code

    response = delete_job(dev_env_proc_api_url, job_id)


def test_get_job(dev_env_proc_api_url: str, dev_env_prof_api_url: str) -> None:
    response = accept_licence(dev_env_prof_api_url)
    response = submit_job(dev_env_proc_api_url)
    job_id = response.json()["jobID"]
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
    response_status_code = response.status_code
    exp_status_code = 200
    assert response_status_code == exp_status_code

    response_body = response.json()
    exp_keys = (
        "processID",
        "type",
        "jobID",
        "status",
        "created",
        "updated",
        "links",
    )
    assert all([key in response_body for key in exp_keys])
    exp_status = ("accepted", "running", "successful")
    assert response_body["status"] in exp_status

    request_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"jobs/{job_id}?statistics=True&request=True&log=True"
    )
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
    response_status_code = response.status_code
    exp_status_code = 200
    assert response_status_code == exp_status_code

    response_body = response.json()
    exp_add_keys = ("statistics", "request", "log")
    assert all([key in response_body for key in exp_add_keys])
    exp_statistics_keys = (
        "adaptor_entry_point",
        "running_requests_per_user_adaptor",
        "queued_requests_per_user_adaptor",
        "running_requests_per_adaptor",
        "queued_requests_per_adaptor",
        "active_users_per_adaptor",
        "waiting_users_per_adaptor",
        "qos_status",
    )
    assert all([key in response_body["statistics"] for key in exp_statistics_keys])
    exp_request_keys = ("ids", "labels")
    assert all([key in response_body["request"] for key in exp_request_keys])
    assert isinstance(response_body["log"], list)

    response = delete_job(dev_env_proc_api_url, job_id)


def test_get_job_successful(
    dev_env_proc_api_url: str, dev_env_prof_api_url: str
) -> None:
    response = accept_licence(dev_env_prof_api_url)
    response = submit_job(dev_env_proc_api_url)
    job_id = response.json()["jobID"]
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
    response.raise_for_status()
    response_body = response.json()
    while response_body["status"] not in ("successful", "failed"):
        time.sleep(5)
        response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
        response_body = response.json()
    if response_body["status"] == "successful":
        exp_keys = (
            "processID",
            "type",
            "jobID",
            "status",
            "created",
            "updated",
            "finished",
            "links",
        )
        assert all([key in response_body for key in exp_keys])
        exp_results_link = {
            "href": f"{request_url}/results",
            "rel": "results",
        }
        assert exp_results_link in response_body["links"]

        response = delete_job(dev_env_proc_api_url, job_id)

    else:
        pytest.skip(f"Job {job_id} unexpectedly failed")


def test_get_job_exc_no_such_job(dev_env_proc_api_url: str) -> None:
    job_id = NON_EXISTING_JOB_ID
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
    response_status_code = response.status_code

    exp_status_code = 404
    assert response_status_code == exp_status_code


def test_get_job_results(request, dev_env_proc_api_url: str) -> None:  # type: ignore
    response = submit_job(dev_env_proc_api_url)
    response_body = response.json()
    job_id = response_body["jobID"]
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}")
    while response_body["status"] not in ("successful", "failed"):
        time.sleep(5)
        response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
        response_body = response.json()
    if response_body["status"] == "successful":
        request_url = urllib.parse.urljoin(
            dev_env_proc_api_url, f"jobs/{job_id}/results"
        )
        response = requests.get(request_url, headers=AUTH_HEADERS_VALID_2)
        response_status_code = response.status_code
        exp_status_code = 403
        assert response_status_code == exp_status_code

        response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
        response_status = response.status_code
        exp_status_code = 200
        assert response_status == exp_status_code

        response_body = response.json()
        exp_keys = ("asset",)
        assert all([key in response_body for key in exp_keys])

        exp_asset_keys = ("value",)
        assert all([key in response_body["asset"] for key in exp_asset_keys])

        exp_value_keys = (
            "type",
            "href",
            "file:checksum",
            "file:size",
            "file:local_path",
        )
        assert all([key in response_body["asset"]["value"] for key in exp_value_keys])

        response = delete_job(dev_env_proc_api_url, job_id)

    else:
        pytest.skip(f"Job {job_id} unexpectedly failed")


def test_get_job_results_exc_no_such_job(dev_env_proc_api_url: str) -> None:
    job_id = NON_EXISTING_JOB_ID
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}/results")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
    response_status_code = response.status_code

    exp_status_code = 404
    assert response_status_code == exp_status_code


def test_get_job_results_exc_job_results_failed(dev_env_proc_api_url: str) -> None:
    response = submit_job(
        dev_env_proc_api_url, request_body=POST_PROCESS_REQUEST_BODY_FAIL
    )
    job_id = response.json()["jobID"]
    job_status = response.json()["status"]
    while job_status != "failed":
        response = requests.get(
            urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}"),
            headers=AUTH_HEADERS_VALID_1,
        )
        job_status = response.json()["status"]
        time.sleep(3)

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}/results")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
    response_status_code = response.status_code
    assert response_status_code != 200

    response_body = response.json()
    exp_response_body = {
        "type": "RuntimeError",
        "title": "job failed",
        "instance": request_url,
    }
    assert [(key, val) in response_body for key, val in exp_response_body.items()]

    response = delete_job(dev_env_proc_api_url, job_id)


def test_get_job_results_exc_results_not_ready(dev_env_proc_api_url: str) -> None:
    response = submit_job(
        dev_env_proc_api_url, request_body=POST_PROCESS_REQUEST_BODY_SLOW
    )
    job_id = response.json()["jobID"]
    job_status = response.json()["status"]
    while job_status not in ("accepted", "running"):
        response = requests.get(
            urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}"),
            headers=AUTH_HEADERS_VALID_1,
        )
        job_status = response.json()["status"]
        time.sleep(3)

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}/results")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
    response_status_code = response.status_code
    assert response_status_code == 404

    response = delete_job(dev_env_proc_api_url, job_id)


def test_get_jobs_not_authorized(dev_env_proc_api_url: str) -> None:
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "jobs")
    response = requests.get(request_url, headers=AUTH_HEADERS_MISSING)
    exp_status_code = 401
    assert response.status_code == exp_status_code

    response = requests.get(request_url, headers=AUTH_HEADERS_INVALID)
    exp_status_code = 401
    assert response.status_code == exp_status_code


def test_get_jobs(dev_env_proc_api_url: str) -> None:
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "jobs")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
    exp_status_code = 200
    assert response.status_code == exp_status_code

    response_body = response.json()
    exp_keys = ("jobs", "links")
    assert all([key in response_body for key in exp_keys])


def test_get_jobs_different_user(dev_env_proc_api_url: str) -> None:
    number_of_new_jobs = 1
    job_ids: list[str] = []
    for _ in range(number_of_new_jobs):
        response = submit_job(
            dev_env_proc_api_url,
            request_body=POST_PROCESS_REQUEST_BODY_SUCCESS_W_LICENCES,
            auth_headers=AUTH_HEADERS_VALID_2,
        )
        job_ids.append(response.json()["jobID"])
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "jobs")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_2)
    response_status_code = response.status_code
    exp_status_code = 200
    assert response_status_code == exp_status_code
    response_body = response.json()
    exp_number_of_jobs = 1
    assert len(response_body["jobs"]) == exp_number_of_jobs

    for job_id in job_ids:
        response = delete_job(
            dev_env_proc_api_url, job_id, auth_headers=AUTH_HEADERS_VALID_2
        )


def test_get_jobs_limit_sorting(
    dev_env_proc_api_url: str, dev_env_prof_api_url: str
) -> None:
    response = accept_licence(dev_env_prof_api_url)
    number_of_new_jobs = 5
    job_ids = []
    for _ in range(number_of_new_jobs):
        response = submit_job(dev_env_proc_api_url)
        job_ids.append(response.json()["jobID"])
    request_url = urllib.parse.urljoin(
        dev_env_proc_api_url, "jobs?limit=4&sortby=created"
    )
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
    response_body = response.json()
    assert len(response_body["jobs"]) == 4
    created_datetimes = [job["created"] for job in response_body["jobs"]]
    sorted_datetimes = sorted(created_datetimes)
    assert created_datetimes == sorted_datetimes

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "jobs?limit=4")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
    response_body = response.json()
    assert len(response_body["jobs"]) == 4
    created_datetimes = [job["created"] for job in response_body["jobs"]]
    sorted_datetimes = sorted(created_datetimes)
    assert created_datetimes == list(reversed(sorted_datetimes))

    for job_id in job_ids:
        response = delete_job(dev_env_proc_api_url, job_id)


def test_get_jobs_pagination(
    dev_env_proc_api_url: str, dev_env_prof_api_url: str
) -> None:
    response = accept_licence(dev_env_prof_api_url)
    number_of_new_jobs = 5
    job_ids = []
    for _ in range(number_of_new_jobs):
        response = submit_job(dev_env_proc_api_url)
        job_ids.append(response.json()["jobID"])
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "jobs?limit=4")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
    response_body = response.json()
    created_datetimes = [job["created"] for job in response_body["jobs"]]

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "jobs?limit=2")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
    response_body = response.json()
    assert len(response_body["jobs"]) == 2
    links = response_body["links"]
    exp_rels = ["next", "prev"]
    all_rels = [link["rel"] for link in links]
    assert all(rel in all_rels for rel in exp_rels)
    first_page_created_datetimes = [job["created"] for job in response_body["jobs"]]
    assert first_page_created_datetimes == created_datetimes[:2]

    for link in links:
        if link["rel"] == "next":
            request_url = link["href"]
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
    response_body = response.json()
    assert len(response_body["jobs"]) == 2
    second_page_created_datetimes = [job["created"] for job in response_body["jobs"]]
    assert second_page_created_datetimes == created_datetimes[2:4]
    links = response_body["links"]

    for link in links:
        if link["rel"] == "prev":
            request_url = link["href"]
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
    response_body = response.json()
    assert len(response_body["jobs"]) == 2
    third_page_created_datetimes = [job["created"] for job in response_body["jobs"]]
    assert third_page_created_datetimes == first_page_created_datetimes

    for job_id in job_ids:
        response = delete_job(dev_env_proc_api_url, job_id)


def test_get_jobs_filters(dev_env_proc_api_url: str, dev_env_prof_api_url: str) -> None:
    response = accept_licence(dev_env_prof_api_url)
    number_of_new_jobs = 5
    job_ids = []
    for _ in range(number_of_new_jobs):
        response = submit_job(dev_env_proc_api_url)
        job_ids.append(response.json()["jobID"])
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "jobs?status=successful")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
    response_body = response.json()
    assert all([job["status"] == "successful" for job in response_body["jobs"]])

    request_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"jobs?processID={NON_EXISTING_PROCESS_ID}"
    )
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_1)
    response_body = response.json()
    assert len(response_body["jobs"]) == 0

    for job_id in job_ids:
        response = delete_job(dev_env_proc_api_url, job_id)


def test_delete_job(dev_env_proc_api_url: str, dev_env_prof_api_url: str) -> None:
    response = accept_licence(dev_env_prof_api_url)
    response = submit_job(dev_env_proc_api_url)
    exp_status_code = 201
    assert response.status_code == exp_status_code

    response_body = response.json()
    job_id = response_body["jobID"]

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}")
    response = requests.delete(request_url, headers=AUTH_HEADERS_VALID_1)
    response_status_code = response.status_code
    exp_status_code = 200
    assert response_status_code == exp_status_code

    exp_keys = (
        "processID",
        "type",
        "jobID",
        "status",
        "created",
        "updated",
        "links",
    )
    assert all([key in response_body for key in exp_keys])

    response_body = response.json()
    exp_status = "dismissed"
    assert response_body["status"] == exp_status

    response = requests.delete(request_url, headers=AUTH_HEADERS_VALID_1)
    response_status_code = response.status_code
    exp_status_code = 404
    assert response_status_code == exp_status_code


def test_delete_job_not_athorized(
    dev_env_proc_api_url: str, dev_env_prof_api_url: str
) -> None:
    response = accept_licence(dev_env_prof_api_url)
    response = submit_job(dev_env_proc_api_url)
    exp_status_code = 201
    assert response.status_code == exp_status_code

    response_body = response.json()
    job_id = response_body["jobID"]

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}")
    response = requests.delete(request_url, headers=AUTH_HEADERS_MISSING)
    exp_status_code = 401
    assert response.status_code == exp_status_code

    response = requests.delete(
        request_url,
        headers=AUTH_HEADERS_INVALID,
    )
    exp_status_code = 401
    assert response.status_code == exp_status_code

    response = requests.get(request_url, headers=AUTH_HEADERS_VALID_2)
    response_status_code = response.status_code
    exp_status_code = 403
    assert response_status_code == exp_status_code

    response = delete_job(dev_env_proc_api_url, job_id)


def test_constraints(dev_env_proc_api_url: str) -> None:
    process_id = EXISTING_PROCESS_ID
    request_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"processes/{process_id}/constraints"
    )
    request_body: dict[str, dict[str, Any]] = {"inputs": {}}

    response = requests.post(
        request_url,
        json=request_body,
    )

    assert response.status_code == 200
