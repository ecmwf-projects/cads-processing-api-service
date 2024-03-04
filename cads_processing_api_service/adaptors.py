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

import cads_adaptors
import cads_catalogue.database

DEFAULT_ENTRY_POINT = "cads_adaptors:UrlCdsAdaptor"


def get_adaptor_properties(
    dataset: cads_catalogue.database.Resource,
) -> dict[str, Any]:
    config: dict[str, Any] = dataset.resource_data.adaptor_configuration  # type: ignore
    if config:
        config = config.copy()
    else:
        config = {}

    entry_point = config.pop("entry_point", DEFAULT_ENTRY_POINT)
    setup_code = dataset.adaptor
    resources = config.pop("resources", {})

    constraints = dataset.resource_data.constraints_data  # type: ignore
    if constraints is not None:
        config["constraints"] = constraints
    mapping = dataset.resource_data.mapping  # type: ignore
    if mapping is not None:
        config["mapping"] = mapping
    licences: list[cads_catalogue.database.Licence] = dataset.licences
    if licences is not None:
        config["licences"] = [
            (licence.licence_uid, licence.revision) for licence in licences
        ]
    form = dataset.resource_data.form_data  # type: ignore
    hash = dataset.adaptor_properties_hash

    adaptor_properties: dict[str, Any] = {
        "entry_point": entry_point,
        "setup_code": setup_code,
        "resources": resources,
        "form": form,
        "config": config,
        "hash": hash,
    }

    return adaptor_properties


def make_system_job_kwargs(
    dataset: cads_catalogue.database.Resource,
    request: dict[str, Any],
    adaptor_resources: dict[str, int],
) -> dict[str, Any]:
    adaptor_properties = get_adaptor_properties(dataset)
    # merge adaptor and dataset resources
    resources = dict(adaptor_resources, **adaptor_properties["resources"])
    system_job_kwargs = {
        "entry_point": adaptor_properties["entry_point"],
        "setup_code": adaptor_properties["setup_code"],
        "resources": resources,
        "adaptor_form": adaptor_properties["form"],
        "adaptor_config": adaptor_properties["config"],
        "adaptor_properties_hash": adaptor_properties["hash"],
        "request": request["inputs"],
    }
    return system_job_kwargs


def instantiate_adaptor(
    dataset: cads_catalogue.database.Resource | None = None,
    adaptor_properties: dict[str, Any] | None = None,
) -> cads_adaptors.AbstractAdaptor:
    if not adaptor_properties:
        if dataset is None:
            raise ValueError("Either adaptor_properties or dataset must be provided")
        adaptor_properties = get_adaptor_properties(dataset)
    adaptor_class = cads_adaptors.get_adaptor_class(
        entry_point=adaptor_properties["entry_point"],
        setup_code=adaptor_properties["setup_code"],
    )
    adaptor = adaptor_class(
        form=adaptor_properties["form"], **adaptor_properties["config"]
    )

    return adaptor
