"""User requests to system requests adapters."""

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

from typing import Any

import cads_catalogue.database
import ogc_api_processes_fastapi.models

FALLBACK_SETUP_CODE = """
import cacholote
import cadsapi
import cdscdm
import xarray as xr


@cacholote.cacheable
def adapter(request, config, metadata):

    # parse input options
    collection_id = request.pop("collection_id", None)
    if collection_id:
        raise ValueError(f"collection_id is required in request")
    data_format = request.pop("format", "grib")
    if data_format not in {"netcdf", "grib"}:
        raise ValueError(f"{data_format=} is not supported")

    # retrieve data
    client = cdsapi.Client()
    client.retrieve(collection_id, request, "download.grib")  # TODO
    data = xr.open_dataset("download.grib")

    # post-process data
    if data_format == "netcdf":
        data = cdscdm.open_dataset("download.grib")

    return data
"""

FALLBACK_ENTRY_POINT = "adapter"

FALLBACK_CONFIG: dict[str, str] = {}


def make_system_request(
    process_id: str,
    execution_content: ogc_api_processes_fastapi.models.Execute,
    job_id: str,
    resource: cads_catalogue.database.Resource,
) -> dict[str, Any]:

    compute_request: dict[str, Any] = {}

    try:
        setup_code = resource.adapter_code
    except AttributeError:
        setup_code = FALLBACK_SETUP_CODE

    try:
        entry_point = resource.entry_point
    except AttributeError:
        entry_point = FALLBACK_ENTRY_POINT

    try:
        config = resource.config
    except AttributeError:
        config = FALLBACK_CONFIG

    inputs = execution_content.dict()["inputs"]
    kwargs = {"request": inputs, "config": config}

    compute_request["inputs"] = {
        "setup_code": setup_code,
        "entry_point": entry_point,
        "kwargs": {
            "value": kwargs,
        },
    }
    compute_request["metadata"] = {
        "X-Forward-Job-ID": job_id,
        "X-Forward-Process-ID": process_id,
    }

    return compute_request
