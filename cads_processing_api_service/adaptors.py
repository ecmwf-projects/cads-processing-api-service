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

import requests
from typing import Any, Dict
import urllib

import cads_catalogue.database
from . import clients, config


def retrieve_from_storage(relative_path):
    settings = config.ensure_settings()
    storage_url = settings.document_storage_url
    url = urllib.parse.urljoin(storage_url, relative_path)
    return requests.get(url).json()


FALLBACK_SETUP_CODE = """
import cacholote
import cdsapi


@cacholote.cacheable
def cds_adaptor(request, config, metadata):

    # parse input options
    collection_id = config.pop('collection_id', None)
    if not collection_id:
        raise ValueError(f'collection_id is required in request')

    # retrieve data
    client = cdsapi.Client(config["url"], config["key"])
    result_path = client.retrieve(collection_id, request).download()
    return open(result_path, "rb")
"""


FALLBACK_CONFIG: dict[str, str] = {
    "entry_point": "cds_adaptor",
    "url": "https://cds.climate.copernicus.eu/api/v2",
    "key": "155265:cd60cf87-5f89-4ef4-8350-3817254b3884",
}


def make_system_job_kwargs(
    process_id: str,
    execution_content: Dict[str, Any],
    resource: cads_catalogue.database.Resource,
) -> dict[str, Any]:

    setup_code = resource.adaptor
    if setup_code is None:
        setup_code = FALLBACK_SETUP_CODE

    config = resource.adaptor_configuration
    if config is None:
        config = FALLBACK_CONFIG.copy()

    config["collection_id"] = process_id
    config["mapping"] = retrieve_from_storage(resource.mapping)

    entry_point = config.pop("entry_point")

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
