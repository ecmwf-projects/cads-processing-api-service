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

from typing import Any

import cachetools
import cads_adaptors.adaptor
import cads_adaptors.adaptor_utils
import cads_catalogue.database

DEFAULT_ENTRY_POINT = "cads_adaptors:UrlCdsAdaptor"


@cachetools.cached(
    cache=cachetools.TTLCache(maxsize=1024, ttl=60),
)
def get_adaptor_parameters(
    dataset: type[cads_catalogue.database.Resource],
) -> dict[str, Any]:
    config: dict[str, Any] = dataset.adaptor_configuration
    if config:
        config = config.copy()
    else:
        config = {}

    entry_point = config.pop("entry_point", DEFAULT_ENTRY_POINT)
    setup_code = dataset.adaptor
    form = dataset.form_data

    constraints = dataset.constraints_data
    if constraints is not None:
        config["constraints"] = constraints
    mapping = dataset.mapping
    if mapping is not None:
        config["mapping"] = mapping
    licences = dataset.licences
    if licences is not None:
        config["licences"] = licences

    adaptor_params: dict[str, Any] = {
        "entry_point": entry_point,
        "setup_code": setup_code,
        "form": form,
        "config": config,
    }

    return adaptor_params


@cachetools.cached(
    cache=cachetools.TTLCache(maxsize=1024, ttl=60),
)
def instantiate_adaptor(
    dataset: type[cads_catalogue.database.Resource],
) -> cads_adaptors.adaptor.AbstractAdaptor:
    adaptor_params = get_adaptor_parameters(dataset)
    adaptor_class = cads_adaptors.adaptor_utils.get_adaptor_class(
        entry_point=adaptor_params["entry_point"],
        setup_code=adaptor_params["setup_code"],
    )
    adaptor = adaptor_class(form=adaptor_params["form"], **adaptor_params["config"])

    return adaptor
