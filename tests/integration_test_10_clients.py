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
POST_PROCESS_REQUEST_BODY = {
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


def test_get_processes(dev_env_proc_api_url: str) -> None:

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "processes")
    response = requests.get(request_url)
    response_status_code = response.status_code
    response_content = response.json()

    exp_status_code = 200
    assert response_status_code == exp_status_code

    exp_keys = ("links", "processes")
    assert all([key in response_content for key in exp_keys])

    number_of_processes = len(response_content["processes"])
    exp_number_of_processes = 5
    assert number_of_processes == exp_number_of_processes

    limit = 2
    offset = 1
    request_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"processes?limit={limit}&offset={offset}"
    )
    response_limit = requests.get(request_url)
    response_status_code = response.status_code
    response_content = response.json()

    exp_status_code = 200
    assert response_status_code == exp_status_code

    response_limit_content = response_limit.json()
    processes = response_limit_content["processes"]
    number_of_processes = len(processes)
    exp_number_of_processes = 2
    assert number_of_processes == exp_number_of_processes

    exp_processes = response_content["processes"][1:3]
    assert processes == exp_processes


def test_get_process(dev_env_proc_api_url: str) -> None:

    process_id = EXISTING_PROCESS_ID
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"processes/{process_id}")
    response = requests.get(request_url)
    response_status_code = response.status_code
    response_content = response.json()

    exp_status_code = 200
    assert response_status_code == exp_status_code

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
    assert all([key in response_content for key in exp_keys])


def test_post_process_execute(  # type: ignore
    request,
    dev_env_proc_api_url: str,
) -> None:

    process_id = EXISTING_PROCESS_ID
    request_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"processes/{process_id}/execute"
    )
    response = requests.post(request_url, json=POST_PROCESS_REQUEST_BODY)
    response_status_code = response.status_code
    response_content = response.json()

    exp_status_code = 201
    assert response_status_code == exp_status_code

    exp_keys = (
        "processID",
        "type",
        "jobID",
        "status",
        "created",
        "updated",
        "metadata",
        "links",
    )
    assert all([key in response_content for key in exp_keys])

    exp_process_id = process_id
    assert response_content["processID"] == exp_process_id

    exp_status = "accepted"
    assert response_content["status"] == exp_status

    request.config.cache.set("job_id", response_content["jobID"])


def test_get_job(request, dev_env_proc_api_url: str) -> None:  # type: ignore

    job_id = request.config.cache.get("job_id", None)

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}")
    response = requests.get(request_url)
    response_status_code = response.status_code
    response_content = response.json()

    exp_status_code = 200
    assert response_status_code == exp_status_code

    exp_status = "accepted"
    assert response_content["status"] == exp_status

    exp_keys = (
        "processID",
        "type",
        "jobID",
        "status",
        "created",
        "updated",
        "metadata",
        "links",
    )
    assert all([key in response_content for key in exp_keys])


def test_get_job_successful(request, dev_env_proc_api_url: str) -> None:  # type: ignore

    job_id = request.config.cache.get("job_id", None)

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}")
    response = requests.get(request_url)
    response_content = response.json()

    while response_content["status"] not in ("successful", "failed"):
        time.sleep(5)
        response = requests.get(request_url)
        response_content = response.json()

    if response_content["status"] == "successful":
        exp_keys = (
            "processID",
            "type",
            "jobID",
            "status",
            "created",
            "updated",
            "finished",
            "metadata",
            "links",
        )
        assert all([key in response_content for key in exp_keys])

        exp_results_link = {
            "href": f"{request_url}/results",
            "rel": "results",
        }
        assert exp_results_link in response_content["links"]

        request.config.cache.set("results_url", exp_results_link["href"])

    else:
        pytest.skip("Job {job_id} unexpectedly failed")


def test_get_job_successful_results(request) -> None:  # type: ignore

    request_url = request.config.cache.get("results_url", None)
    response = requests.get(request_url)
    response_status = response.status_code

    exp_status_code = 200
    assert response_status == exp_status_code


def test_get_jobs(dev_env_proc_api_url: str) -> None:

    process_id = EXISTING_PROCESS_ID
    number_of_jobs = 3
    request_execute_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"processes/{process_id}/execute"
    )
    for i in range(number_of_jobs):
        print(i)
        requests.post(request_execute_url, json=POST_PROCESS_REQUEST_BODY)

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "jobs")
    response = requests.get(request_url)
    exp_status_code = 200

    assert response.status_code == exp_status_code

    response_content = response.json()
    exp_keys = ("jobs", "links")

    assert all([key in response_content for key in exp_keys])

    response_jobs = response_content["jobs"]
    print(response_jobs)
    actual_number_of_jobs = len(response_jobs)
    exp_number_of_jobs = number_of_jobs + 1

    assert actual_number_of_jobs == exp_number_of_jobs


def test_get_process_exceptions(dev_env_proc_api_url: str) -> None:

    process_id = NON_EXISTING_PROCESS_ID
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"processes/{process_id}")
    response = requests.get(request_url)
    response_status_code = response.status_code
    response_content = response.json()

    exp_status_code = 404
    assert response_status_code == exp_status_code

    exp_response_content = {
        "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/no-such-process",
        "title": "process not found",
        "detail": f"process {process_id} has not been found",
        "instance": request_url,
    }
    assert response_content == exp_response_content


def test_post_process_execute_exceptions(dev_env_proc_api_url: str) -> None:

    process_id = NON_EXISTING_PROCESS_ID
    request_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"processes/{process_id}/execute"
    )
    response = requests.post(request_url, json={})
    exp_status_code = 404

    assert response.status_code == exp_status_code

    response_content = response.json()
    exp_response_content = {
        "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/no-such-process",
        "title": "process not found",
        "detail": f"process {process_id} has not been found",
        "instance": request_url,
    }

    assert response_content == exp_response_content


def test_get_job_exceptions(dev_env_proc_api_url: str) -> None:

    job_id = NON_EXISTING_JOB_ID
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}")
    response = requests.get(request_url)
    exp_status_code = 404

    assert response.status_code == exp_status_code

    response_content = response.json()
    exp_response_content = {
        "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/no-such-job",
        "title": "job not found",
        "detail": f"job {job_id} has not been found",
        "instance": request_url,
    }

    assert response_content == exp_response_content


def test_get_job_results_exceptions(dev_env_proc_api_url: str) -> None:

    job_id = NON_EXISTING_JOB_ID
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}/results")
    response = requests.get(request_url)
    exp_status_code = 404

    assert response.status_code == exp_status_code

    response_content = response.json()
    exp_response_content = {
        "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/no-such-job",
        "title": "job not found",
        "detail": f"job {job_id} has not been found",
        "instance": request_url,
    }

    assert response_content == exp_response_content
