"""User requests to system requests adaptors."""

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

from typing import Any, Dict

import cads_catalogue.database

FALLBACK_SETUP_CODE = """
import cacholote
import cdsapi


@cacholote.cacheable
def adaptor(request, config, metadata):

    # parse input options
    collection_id = config.pop('collection_id', None)
    if not collection_id:
        raise ValueError(f'collection_id is required in request')

    # retrieve data
    client = cdsapi.Client(config["url"], config["key"])
    result_path = client.retrieve(collection_id, request).download()
    return open(result_path, "rb")
"""

FALLBACK_ENTRY_POINT = "adaptor"

FALLBACK_CONFIG: dict[str, str] = {
    "url": "https://cds.climate.copernicus.eu/api/v2",
    "key": "155265:cd60cf87-5f89-4ef4-8350-3817254b3884",
}


def make_system_job_kwargs(
    process_id: str,
    execution_content: Dict[str, Any],
    resource: cads_catalogue.database.Resource,
) -> dict[str, Any]:

    config: dict[str, Any] = resource.adaptor_configuration  # type: ignore
    if config is None:
        config = FALLBACK_CONFIG.copy()

    entry_point = config.pop("entry_point", FALLBACK_ENTRY_POINT)

    setup_code = resource.adaptor
    if setup_code is None:
        setup_code = FALLBACK_SETUP_CODE
        config["collection_id"] = process_id

    mapping = resource.mapping
    if resource.mapping is not None:
        config["mapping"] = mapping

    kwargs = {
        "request": execution_content["inputs"],
        "config": config,
    }

    job_kwargs: dict[str, Any] = {
        "setup_code": setup_code,
        "entry_point": entry_point,
        "kwargs": kwargs,
    }

    return job_kwargs
