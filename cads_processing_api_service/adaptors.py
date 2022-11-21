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

TEST_URL_SETUP_CODE = """
import cacholote
import requests

from cads_processing_api_service import cads_common

@cacholote.cacheable
def url_adapter(request, config, metadata):

    # parse input options
    # request, format = cads_common.extract_format_options(request)
    # request, reduce = cads_common.extract_reduce_options(request)
    collection_id = config.pop('collection_id', None)
    
    dataset = cads_common.retrieve_from_db(catalogue_id)
    mapping = cads_common.retrieve_from_storage(dataset.mapping)    
    request_mapped = cads_common.apply_mapping(request, mapping)
    
    adaptor_url = dataset.adaptor["adaptor.url"]    
    urls = requests_to_urls(request_mapped, adaptor_url['pattern'])
    
    data =  requests.get(urls[0]["url"])

    # retrieve data
    
    # with cads.add_step_metrics("download data", metadata):
    # data = (mars_request)

    # post-process data
    # if reduce is not None:
    # with cads.add_step_metrics("reduce data", metadata):
    #     data = cads.apply_reduce(data, reduce)

    # if format is not None:
    # with cads.add_step_metrics("reformat data", metadata):
    #    data = cads.translate(data, format)

    return data
"""


FALLBACK_ENTRY_POINT = "cds_adaptor"

FALLBACK_CONFIG: dict[str, str] = {
    "url": "https://cds.climate.copernicus.eu/api/v2",
    "key": "155265:cd60cf87-5f89-4ef4-8350-3817254b3884",
}


def make_system_job_kwargs(
    process_id: str,
    execution_content: Dict[str, Any],
    resource: cads_catalogue.database.Resource,
) -> dict[str, Any]:

    job_kwargs: dict[str, Any] = {}

    try:
        setup_code = resource.adaptor_code
    except AttributeError:
        setup_code = TEST_URL_SETUP_CODE

    try:
        entry_point = resource.entry_point
    except AttributeError:
        entry_point = FALLBACK_ENTRY_POINT

    config = getattr(resource, "adaptor_configuration", None)
    if config is None:
        config = FALLBACK_CONFIG.copy()
        config["collection_id"] = process_id

    inputs = execution_content["inputs"]
    kwargs = {
        "request": inputs,
        "config": config,
    }

    job_kwargs = {
        "setup_code": setup_code,
        "entry_point": entry_point,
        "kwargs": kwargs,
    }

    return job_kwargs
