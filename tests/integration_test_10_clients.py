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

import pytest
import requests

EXISTING_PROCESS_ID = "reanalysis-era5-pressure-levels"
NON_EXISTING_PROCESS_ID = "non-existing-dataset"
NON_EXISTING_JOB_ID = "1234"
POST_PROCESS_REQUEST_BODY_SUCCESS = {
    "inputs": {
        "product_type": "reanalysis",
        "format": "grib",
        "variable": "temperature",
        "pressure_level": "1",
        "year": "1971",
        "month": "01",
        "day": "25",
        "time": "06:00",
    }
}
POST_PROCESS_REQUEST_BODY_FAIL = {
    "inputs": {
        "product_type": "non-existing-product-type",
        "format": "grib",
        "variable": "temperature",
        "pressure_level": "1",
        "year": "1971",
        "month": "01",
        "day": "25",
        "time": "06:00",
    }
}
POST_PROCESS_REQUEST_BODY_SLOW = {
    "inputs": {
        "product_type": "reanalysis",
        "format": "grib",
        "variable": "temperature",
        "pressure_level": "1",
        "year": [f"{year}" for year in range(1959, 2022)],
        "month": [f"{month:02}" for month in range(1, 13)],
        "day": [f"{day:02}" for day in range(1, 32)],
        "time": [f"{hour:02}:00" for hour in range(0, 24)],
    }
}
VALID_PAT = "mysecretpat"
INVALID_PAT = "0123"
AUTH_HEADERS_VALID = {
    "Authorization": f"Bearer {VALID_PAT}",
    "Enable-Authorization": "True",
}
AUTH_HEADERS_INVALID = {
    "Authorization": f"Bearer {INVALID_PAT}",
    "Enable-Authorization": "True",
}
AUTH_HEADERS_MISSING = {
    "Enable-Authorization": "True",
}


def test_get_processes(dev_env_proc_api_url: str) -> None:
    response = requests.get(urllib.parse.urljoin(dev_env_proc_api_url, "processes"))
    response_status_code = response.status_code
    exp_status_code = 200
    assert response_status_code == exp_status_code

    response_body = response.json()
    exp_keys = ("links", "processes")
    assert all([key in response_body for key in exp_keys])

    number_of_processes = len(response_body["processes"])
    exp_number_of_processes = 6
    assert number_of_processes == exp_number_of_processes

    limit = 2
    response_limit = requests.get(
        urllib.parse.urljoin(dev_env_proc_api_url, f"processes?limit={limit}")
    )
    response_status_code = response.status_code

    exp_status_code = 200
    assert response_status_code == exp_status_code

    response_limit_body = response_limit.json()
    processes = response_limit_body["processes"]
    number_of_processes = len(processes)
    exp_number_of_processes = 2
    assert number_of_processes == exp_number_of_processes

    exp_processes = response_body["processes"][:2]
    assert processes == exp_processes


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
        "keywords",
        "id",
        "version",
        "jobControlOptions",
        "outputTransmission",
        "links",
        "inputs",
        "outputs",
    )
    assert all([key in response_body for key in exp_keys])


def test_post_process_execution(  # type: ignore
    request,
    dev_env_proc_api_url: str,
) -> None:
    process_id = EXISTING_PROCESS_ID
    request_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"processes/{process_id}/execute"
    )
    response = requests.post(
        request_url,
        json=POST_PROCESS_REQUEST_BODY_SUCCESS,
        headers=AUTH_HEADERS_MISSING,
    )
    exp_status_code = 401
    assert response.status_code == exp_status_code

    response = requests.post(
        request_url,
        json=POST_PROCESS_REQUEST_BODY_SUCCESS,
        headers=AUTH_HEADERS_INVALID,
    )
    exp_status_code = 403
    assert response.status_code == exp_status_code

    response = requests.post(
        request_url,
        json=POST_PROCESS_REQUEST_BODY_SUCCESS,
        headers=AUTH_HEADERS_VALID,
    )
    response_status_code = response.status_code
    exp_status_code = 201
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

    exp_process_id = process_id
    assert response_body["processID"] == exp_process_id

    exp_status = "accepted"
    assert response_body["status"] == exp_status

    request.config.cache.set("job_id", response_body["jobID"])


def test_get_job(request, dev_env_proc_api_url: str) -> None:  # type: ignore
    job_id = request.config.cache.get("job_id", None)
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}")
    response = requests.get(request_url, headers=AUTH_HEADERS_MISSING)
    exp_status_code = 401
    assert response.status_code == exp_status_code

    response = requests.get(
        request_url,
        headers=AUTH_HEADERS_INVALID,
    )
    exp_status_code = 403
    assert response.status_code == exp_status_code

    response = requests.get(request_url, headers=AUTH_HEADERS_VALID)
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
    exp_status = "accepted"
    assert response_body["status"] == exp_status


def test_get_job_successful(request, dev_env_proc_api_url: str) -> None:  # type: ignore
    job_id = request.config.cache.get("job_id", None)
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID)
    response_body = response.json()
    while response_body["status"] not in ("successful", "failed"):
        time.sleep(5)
        response = requests.get(request_url, headers=AUTH_HEADERS_VALID)
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

        request.config.cache.set("results_url", exp_results_link["href"])

    else:
        pytest.skip("Job {job_id} unexpectedly failed")


def test_get_job_successful_results(request) -> None:  # type: ignore
    request_url = request.config.cache.get("results_url", None)
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID)
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


def test_get_jobs(dev_env_proc_api_url: str) -> None:
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "jobs")
    response = requests.get(request_url, headers=AUTH_HEADERS_MISSING)
    exp_status_code = 401
    assert response.status_code == exp_status_code

    response = requests.get(request_url, headers=AUTH_HEADERS_INVALID)
    exp_status_code = 403
    assert response.status_code == exp_status_code

    response = requests.get(request_url, headers=AUTH_HEADERS_VALID)
    exp_status_code = 200
    assert response.status_code == exp_status_code

    response_body = response.json()
    exp_keys = ("jobs", "links")
    assert all([key in response_body for key in exp_keys])

    process_id = EXISTING_PROCESS_ID
    number_of_new_jobs = 3
    request_execute_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"processes/{process_id}/execute"
    )
    for _ in range(number_of_new_jobs):
        requests.post(
            request_execute_url,
            json=POST_PROCESS_REQUEST_BODY_FAIL,
            headers=AUTH_HEADERS_VALID,
        )

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "jobs?limit=4&dir=asc")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID)
    response_body = response.json()
    assert len(response_body["jobs"]) == 4
    created_datetimes = [job["created"] for job in response_body["jobs"]]
    sorted_datetimes = sorted(created_datetimes)
    assert created_datetimes == sorted_datetimes

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "jobs?limit=4")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID)
    response_body = response.json()
    assert len(response_body["jobs"]) == 4
    created_datetimes = [job["created"] for job in response_body["jobs"]]
    sorted_datetimes = sorted(created_datetimes)
    assert created_datetimes == list(reversed(sorted_datetimes))

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "jobs?limit=2")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID)
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
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID)
    response_body = response.json()
    assert len(response_body["jobs"]) == 2
    second_page_created_datetimes = [job["created"] for job in response_body["jobs"]]
    assert second_page_created_datetimes == created_datetimes[2:4]
    links = response_body["links"]

    for link in links:
        if link["rel"] == "prev":
            request_url = link["href"]
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID)
    response_body = response.json()
    assert len(response_body["jobs"]) == 2
    third_page_created_datetimes = [job["created"] for job in response_body["jobs"]]
    assert third_page_created_datetimes == first_page_created_datetimes

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "jobs?status=successful")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID)
    response_body = response.json()
    assert [job["status"] == "successful" for job in response_body["jobs"]]

    request_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"jobs?processID={NON_EXISTING_PROCESS_ID}"
    )
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID)
    response_body = response.json()
    assert len(response_body["jobs"]) == 0


def test_delete_job(dev_env_proc_api_url: str) -> None:
    process_id = EXISTING_PROCESS_ID
    request_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"processes/{process_id}/execute"
    )
    response = requests.post(
        request_url,
        json=POST_PROCESS_REQUEST_BODY_SUCCESS,
        headers=AUTH_HEADERS_VALID,
    )
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
    exp_status_code = 403
    assert response.status_code == exp_status_code

    response = requests.delete(request_url, headers=AUTH_HEADERS_VALID)
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

    response = requests.delete(request_url, headers=AUTH_HEADERS_VALID)
    response_status_code = response.status_code
    exp_status_code = 404
    assert response_status_code == exp_status_code


def test_get_process_exc_no_such_process(dev_env_proc_api_url: str) -> None:
    process_id = NON_EXISTING_PROCESS_ID
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"processes/{process_id}")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID)
    response_status_code = response.status_code
    exp_status_code = 404
    assert response_status_code == exp_status_code

    response_body = response.json()
    exp_response_body = {
        "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/no-such-process",
        "title": "process not found",
        "detail": f"process {process_id} has not been found",
        "instance": request_url,
    }
    assert response_body == exp_response_body


def test_post_process_execute_exc_no_such_process(dev_env_proc_api_url: str) -> None:
    process_id = NON_EXISTING_PROCESS_ID
    request_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"processes/{process_id}/execute"
    )
    response = requests.post(request_url, json={}, headers=AUTH_HEADERS_VALID)
    response_status_code = response.status_code
    exp_status_code = 404
    assert response_status_code == exp_status_code

    response_body = response.json()
    exp_response_body = {
        "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/no-such-process",
        "title": "process not found",
        "detail": f"process {process_id} has not been found",
        "instance": request_url,
    }
    assert response_body == exp_response_body


def test_get_job_exc_no_such_job(dev_env_proc_api_url: str) -> None:
    job_id = NON_EXISTING_JOB_ID
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID)
    response_status_code = response.status_code

    exp_status_code = 404
    assert response_status_code == exp_status_code

    response_body = response.json()
    exp_response_body = {
        "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/no-such-job",
        "title": "job not found",
        "detail": f"job {job_id} has not been found",
        "instance": request_url,
    }
    assert response_body == exp_response_body


def test_get_job_results_exc_no_such_job(dev_env_proc_api_url: str) -> None:
    job_id = NON_EXISTING_JOB_ID
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}/results")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID)
    response_status_code = response.status_code

    exp_status_code = 404
    assert response_status_code == exp_status_code

    response_body = response.json()
    exp_response_body = {
        "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/no-such-job",
        "title": "job not found",
        "detail": f"job {job_id} has not been found",
        "instance": request_url,
    }
    assert response_body == exp_response_body


def test_get_job_results_exc_job_results_failed(dev_env_proc_api_url: str) -> None:
    process_id = EXISTING_PROCESS_ID
    response = requests.post(
        urllib.parse.urljoin(dev_env_proc_api_url, f"processes/{process_id}/execute"),
        json=POST_PROCESS_REQUEST_BODY_FAIL,
        headers=AUTH_HEADERS_VALID,
    )
    if response.status_code != 201:
        pytest.skip("Job sumbission unexpectedly failed")
    job_id = response.json()["jobID"]
    job_status = response.json()["status"]
    while job_status != "failed":
        response = requests.get(
            urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}"),
            headers=AUTH_HEADERS_VALID,
        )
        job_status = response.json()["status"]
        time.sleep(3)

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}/results")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID)
    response_status_code = response.status_code
    assert response_status_code != 200

    response_body = response.json()
    exp_response_body = {
        "type": "RuntimeError",
        "title": "job failed",
        "instance": request_url,
    }
    assert [(key, val) in response_body for key, val in exp_response_body.items()]


def test_get_job_results_exc_results_not_ready(dev_env_proc_api_url: str) -> None:

    process_id = EXISTING_PROCESS_ID
    response = requests.post(
        urllib.parse.urljoin(dev_env_proc_api_url, f"processes/{process_id}/execute"),
        json=POST_PROCESS_REQUEST_BODY_SLOW,
        headers=AUTH_HEADERS_VALID,
    )
    if response.status_code != 201:
        pytest.skip("Job sumbission unexpectedly failed")
    job_id = response.json()["jobID"]
    job_status = response.json()["status"]
    while job_status not in ("accepted", "running"):
        response = requests.get(
            urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}"),
            headers=AUTH_HEADERS_VALID,
        )
        job_status = response.json()["status"]
        time.sleep(3)

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}/results")
    response = requests.get(request_url, headers=AUTH_HEADERS_VALID)
    response_status_code = response.status_code
    assert response_status_code == 404

    response_body = response.json()
    exp_response_body = {
        "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/result-not-ready",
        "title": "job results not ready",
        "detail": f"job {job_id} results are not yet ready",
        "instance": request_url,
    }
    assert [(key, val) in response_body for key, val in exp_response_body.items()]
