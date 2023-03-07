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

import cads_adaptors.adaptor
import cads_adaptors.adaptor_utils
import cads_catalogue.database

from . import translators

DEFAULT_ENTRY_POINT = "cads_adaptors:UrlCdsAdaptor"


def get_adaptor_properties(
    dataset: cads_catalogue.database.Resource,
) -> dict[str, Any]:
    config: dict[str, Any] = dataset.adaptor_configuration
    if config:
        config = config.copy()
    else:
        config = {}

    # entry_point = config.pop("entry_point", DEFAULT_ENTRY_POINT)
    # setup_code = dataset.adaptor
    entry_point = DEFAULT_ENTRY_POINT
    setup_code = None
    resources = config.pop("resources", {})

    constraints = dataset.constraints_data
    if constraints is not None:
        config["constraints"] = constraints
    mapping = dataset.mapping
    if mapping is not None:
        config["mapping"] = mapping
    licences: list[cads_catalogue.database.Licence] = dataset.licences
    if licences is not None:
        config["licences"] = [
            (licence.licence_uid, licence.revision) for licence in licences
        ]
    raw_form = dataset.form_data
    form = translators.translate_cds_form(raw_form)

    adaptor_properties: dict[str, Any] = {
        "entry_point": entry_point,
        "setup_code": setup_code,
        "resources": resources,
        "form": form,
        "config": config,
    }

    return adaptor_properties


def make_system_job_kwargs(
    dataset: cads_catalogue.database.Resource, request: dict[str, Any]
) -> dict[str, Any]:
    adaptor_properties = get_adaptor_properties(dataset)
    system_job_kwargs = {
        "entry_point": adaptor_properties["entry_point"],
        "setup_code": adaptor_properties["setup_code"],
        "resources": adaptor_properties["resources"],
        "kwargs": {
            "config": adaptor_properties["config"],
            "request": request["inputs"],
        },
    }
    return system_job_kwargs


def instantiate_adaptor(
    dataset: cads_catalogue.database.Resource,
) -> cads_adaptors.adaptor.AbstractAdaptor:
    adaptor_properties = get_adaptor_properties(dataset)
    adaptor_class = cads_adaptors.adaptor_utils.get_adaptor_class(
        entry_point=adaptor_properties["entry_point"],
        setup_code=adaptor_properties["setup_code"],
    )
    adaptor = adaptor_class(
        form=adaptor_properties["form"], **adaptor_properties["config"]
    )

    return adaptor
