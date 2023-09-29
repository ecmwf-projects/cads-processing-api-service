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

import unittest.mock
from typing import Any

import cads_adaptors.adaptors.url
import cads_catalogue.database

from cads_processing_api_service import adaptors


def test_get_adaptor_properties() -> None:
    adaptor_configuration = {
        "entry_point": "test_entry_point",
        "resources": {"test_resource_key": "test_resource_value"},
        "test_configuration_key": "test_configuration_value",
    }
    constraints_data = {"constraints_key": "constraints_value"}
    mapping = {"mapping_key": "mapping_value"}
    licences = [cads_catalogue.database.Licence(licence_uid="licence_uid", revision=1)]
    form_data: dict[str, Any] = {}
    setup_code = "test_setup_code"
    adaptor_properties_hash = "test_adaptor_properties_hash"
    dataset = cads_catalogue.database.Resource(
        adaptor_configuration=adaptor_configuration,
        constraints_data=constraints_data,
        mapping=mapping,
        licences=licences,  # type: ignore
        form_data=form_data,
        adaptor=setup_code,
        adaptor_properties_hash=adaptor_properties_hash,
    )
    adaptor_properties = adaptors.get_adaptor_properties(dataset)

    exp_adaptor_properties = {
        "entry_point": adaptor_configuration["entry_point"],
        "setup_code": setup_code,
        "resources": adaptor_configuration["resources"],
        "form": form_data,
        "config": {
            "test_configuration_key": "test_configuration_value",
            "licences": [("licence_uid", 1)],
            "constraints": constraints_data,
            "mapping": mapping,
        },
        "hash": adaptor_properties_hash,
    }
    assert adaptor_properties == exp_adaptor_properties

    adaptor_configuration = {
        "resources": {"test_resource_key": "test_resource_value"},
        "test_configuration_key": "test_configuration_value",
    }
    dataset = cads_catalogue.database.Resource(
        adaptor_configuration=adaptor_configuration,
        constraints_data=constraints_data,
        mapping=mapping,
        licences=licences,  # type: ignore
        form_data=form_data,
    )
    adaptor_properties = adaptors.get_adaptor_properties(dataset)
    assert adaptor_properties["entry_point"] == adaptors.DEFAULT_ENTRY_POINT
    assert adaptor_properties["setup_code"] is None

    adaptor_configuration = {}
    dataset = cads_catalogue.database.Resource(
        adaptor_configuration=adaptor_configuration,
        form_data=form_data,
    )
    adaptor_properties = adaptors.get_adaptor_properties(dataset)
    assert "constraints" not in adaptor_properties["config"]
    assert "mapping" not in adaptor_properties["config"]
    assert "licences" not in adaptor_properties["config"]


def test_make_system_job_kwargs() -> None:
    test_dataset = cads_catalogue.database.Resource()
    test_request = {"inputs": {"test_inputs_key": "test_inputs_value"}}
    test_adaptor_resources = {"test_adaptor_resources_key": 1}
    with unittest.mock.patch(
        "cads_processing_api_service.adaptors.get_adaptor_properties"
    ) as mock_get_adaptor_properties:
        mock_get_adaptor_properties.return_value = {
            "entry_point": "test_entry_point",
            "setup_code": "test_setup_code",
            "resources": {"test_resource_key": 1},
            "form": {},
            "config": {"test_configuration_key": "test_configuration_value"},
            "hash": "test_adaptor_properties_hash",
        }
        system_job_kwargs = adaptors.make_system_job_kwargs(
            test_dataset, test_request, test_adaptor_resources
        )
    exp_system_job_kwargs = {
        "entry_point": "test_entry_point",
        "setup_code": "test_setup_code",
        "resources": {"test_adaptor_resources_key": 1, "test_resource_key": 1},
        "adaptor_form": {},
        "adaptor_config": {"test_configuration_key": "test_configuration_value"},
        "adaptor_properties_hash": "test_adaptor_properties_hash",
        "request": {"test_inputs_key": "test_inputs_value"},
    }
    assert system_job_kwargs == exp_system_job_kwargs


def test_instantiate_adaptor() -> None:
    adaptors_configuration = {
        "resources": {"test_resource_key": "test_resource_value"},
        "test_configuration_key": "test_configuration_value",
    }
    constraints_data = {"constraints_key": "constraints_value"}
    mapping = {"mapping_key": "mapping_value"}
    licences = [cads_catalogue.database.Licence(licence_uid="licence_uid", revision=1)]
    form_data: dict[str, Any] = {}
    dataset = cads_catalogue.database.Resource(
        adaptor_configuration=adaptors_configuration,
        constraints_data=constraints_data,
        mapping=mapping,
        licences=licences,  # type: ignore
        form_data=form_data,
        adaptor_properties_hash="test_adaptor_properties_hash",
    )
    adaptor = adaptors.instantiate_adaptor(dataset)

    assert isinstance(adaptor, cads_adaptors.adaptors.url.UrlCdsAdaptor)
