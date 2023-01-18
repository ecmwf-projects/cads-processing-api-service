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

TEST_CASES = {
    # "cams-global-reanalysis-eac4": {
    #     "process_id": "cams-global-reanalysis-eac4",
    #     "execution_content": {
    #         "inputs": {
    #             "format": "grib",
    #             "variable": ["2m_temperature"],
    #             "date": "2003-01-02/2003-01-03",
    #             "time": ["00:00"],
    #         },
    #         "acceptedLicences": [
    #             {"id": "licence-to-use-copernicus-products", "revision": 12}
    #         ],
    #     },
    # },
    "cams-global-reanalysis-eac4-monthly": {
        "process_id": "cams-global-reanalysis-eac4-monthly",
        "execution_content": {
            "inputs": {
                "format": "grib",
                "variable": ["carbon_monoxide"],
                "pressure_level": ["1"],
                "model_level": ["60"],
                "year": ["2003"],
                "month": ["01"],
                "product_type": "monthly_mean_by_hour_of_day",
                "time": ["00:00"],
                "area": [
                    20,
                    -30,
                    -20,
                    30,
                ],
            },
            "acceptedLicences": [
                {"id": "licence-to-use-copernicus-products", "revision": 12}
            ],
        },
    },
    "derived-near-surface-meteorological-variables": {
        "process_id": "derived-near-surface-meteorological-variables",
        "execution_content": {
            "inputs": {
                "format": "zip",
                "variable": ["near_surface_air_temperature"],
                "reference_dataset": ["cru"],
                "version": ["2.1"],
                "year": "1979",
                "month": "01",
            },
            "acceptedLicences": [
                {"id": "licence-to-use-copernicus-products", "revision": 12}
            ],
        },
    },
    "derived-near-surface-meteorological-variables_asurf": {
        "process_id": "derived-near-surface-meteorological-variables",
        "execution_content": {
            "inputs": {
                "format": "zip",
                "variable": ["grid_point_altitude"],
                "version": ["2.1"],
                "reference_dataset": ["cru"],
            },
            "acceptedLicences": [
                {"id": "licence-to-use-copernicus-products", "revision": 12}
            ],
        },
    },
    "reanalysis-era5-land": {
        "process_id": "reanalysis-era5-land",
        "execution_content": {
            "inputs": {
                "variable": ["2m_dewpoint_temperature"],
                "year": "1950",
                "month": "01",
                "day": ["01"],
                "time": ["01:00"],
                "area": [
                    20,
                    -30,
                    -20,
                    30,
                ],
                "format": "grib",
            },
            "acceptedLicences": [
                {"id": "licence-to-use-copernicus-products", "revision": 12}
            ],
        },
    },
    "reanalysis-era5-land-monthly-means": {
        "process_id": "reanalysis-era5-land-monthly-means",
        "execution_content": {
            "inputs": {
                "product_type": ["monthly_averaged_reanalysis_by_hour_of_day"],
                "variable": ["2m_dewpoint_temperature"],
                "year": ["1950"],
                "month": ["01"],
                "time": ["00:00"],
                "area": [
                    20,
                    -30,
                    -20,
                    30,
                ],
                "format": "grib",
            },
            "acceptedLicences": [
                {"id": "licence-to-use-copernicus-products", "revision": 12}
            ],
        },
    },
    "reanalysis-era5-pressure-levels": {
        "process_id": "reanalysis-era5-pressure-levels",
        "execution_content": {
            "inputs": {
                "product_type": ["reanalysis"],
                "format": "grib",
                "variable": ["temperature"],
                "pressure_level": ["1"],
                "year": ["1971"],
                "month": ["01"],
                "day": ["25"],
                "time": ["06:00"],
                "area": [
                    20,
                    -30,
                    -20,
                    30,
                ],
            },
            "acceptedLicences": [
                {"id": "licence-to-use-copernicus-products", "revision": 12}
            ],
        },
    },
    "reanalysis-era5-single-levels": {
        "process_id": "reanalysis-era5-single-levels",
        "execution_content": {
            "inputs": {
                "product_type": ["reanalysis"],
                "format": "grib",
                "variable": ["10m_u_component_of_wind"],
                "year": ["1959"],
                "month": ["01"],
                "day": ["01"],
                "time": ["00:00"],
                "area": [
                    20,
                    -30,
                    -20,
                    30,
                ],
            },
            "acceptedLicences": [
                {"id": "licence-to-use-copernicus-products", "revision": 12}
            ],
        },
    },
}
TEST_PAT = "mysecretpat"
TEST_AUTH_HEADERS = {"PRIVATE-TOKEN": TEST_PAT}


@pytest.mark.parametrize("test_case", list(TEST_CASES.keys()))
def test_dataset_retrieve(test_case: str, dev_env_proc_api_url: str) -> None:
    process_id = TEST_CASES[test_case]["process_id"]
    request_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"processes/{process_id}/execution"
    )
    response = requests.post(
        request_url,
        json=TEST_CASES[test_case]["execution_content"],
        headers=TEST_AUTH_HEADERS,
    )
    assert response.status_code == 201

    response_body = response.json()
    job_id = response_body["jobID"]
    job_request_url = urllib.parse.urljoin(dev_env_proc_api_url, f"jobs/{job_id}")
    while response_body["status"] not in ("successful", "failed"):
        time.sleep(3)
        response = requests.get(job_request_url, headers=TEST_AUTH_HEADERS)
        response_body = response.json()
    results_request_url = urllib.parse.urljoin(
        dev_env_proc_api_url, f"jobs/{job_id}/results"
    )
    results_response = requests.get(results_request_url, headers=TEST_AUTH_HEADERS)
    assert response_body["status"] == "successful", results_response.json()["detail"]

    response = requests.delete(job_request_url, headers=TEST_AUTH_HEADERS)
