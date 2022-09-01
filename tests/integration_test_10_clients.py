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

import urllib.parse

import requests

EXISTING_PROCESS_ID = "reanalysis-era5-land"
NON_EXISTING_PROCESS_ID = "non-existing-dataset"
NON_EXISTING_JOB_ID = "1234"


def test_get_processes(dev_env_proc_api_url: str) -> None:

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, "processes")
    response = requests.get(request_url)
    exp_status_code = 200

    assert response.status_code == exp_status_code

    response_content = response.json()
    exp_keys = ("links", "processes")

    assert all([key in response_content for key in exp_keys])

    number_of_processes = len(response_content["processes"])
    exp_number_of_processes = 3

    assert number_of_processes == exp_number_of_processes

    limit = 2
    offset = 1
    request_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"processes?limit={limit}&offset={offset}"
    )
    response_limit = requests.get(request_url)
    exp_status_code = 200

    assert response.status_code == exp_status_code

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
    exp_status_code = 200

    assert response.status_code == exp_status_code

    response_content = response.json()
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


def test_get_process_exceptions(dev_env_proc_api_url: str) -> None:

    process_id = NON_EXISTING_PROCESS_ID
    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"processes/{process_id}")
    response = requests.get(request_url)
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


def test_post_process_execute(dev_env_proc_api_url: str) -> None:

    process_id = EXISTING_PROCESS_ID
    request_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"processes/{process_id}/execute"
    )
    response = requests.post(request_url, json={})
    exp_status_code = 201

    assert response.status_code == exp_status_code

    response_content = response.json()
    exp_keys = ("processID", "type", "jobID", "status", "created", "updated", "links")

    assert all([key in response_content for key in exp_keys])


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


def test_get_jobs(dev_env_proc_api_url: str) -> None:

    process_id = EXISTING_PROCESS_ID
    number_of_jobs = 4
    request_execute_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"processes/{process_id}/execute"
    )
    for i in range(number_of_jobs):
        print(i)
        requests.post(request_execute_url, json={})

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


def test_get_job(dev_env_proc_api_url: str) -> None:

    process_id = EXISTING_PROCESS_ID
    request_execute_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"processes/{process_id}/execute"
    )
    response_execute_content = requests.post(request_execute_url, json={}).json()
    job_id = response_execute_content["jobID"]

    request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}")
    response = requests.get(request_url)
    exp_status_code = 200

    assert response.status_code == exp_status_code


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
