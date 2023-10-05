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

import cads_processing_api_service.translators

TEST_INPUT: dict[str, Any] = {
    "string_list": {
        "details": {"labels": {"val1": "Val1", "val2": "Val2", "val3": "Val3"}},
        "type": "StringListWidget",
    },
    "string_list_array": {
        "details": {
            "groups": [
                {"labels": {"val1": "Val1", "val2": "Val2"}},
                {"labels": {"val2": "Val2", "val3": "Val3"}},
            ]
        },
        "type": "StringListArrayWidget",
    },
    "string_choice": {
        "details": {
            "labels": {"val1": "Val1", "val2": "Val2", "val3": "Val3"},
            "default": "val1",
        },
        "type": "StringChoiceWidget",
    },
    "geographic_extent_map": {
        "details": {"default": [1, 2, 3, 4]},
        "type": "GeographicExtentMapWidget",
    },
    "string_list_array_groups": {
        "details": {
            "groups": [
                {
                    "groups": [
                        {"labels": {"val1": "Val1", "val2": "Val2"}},
                        {"labels": {"val2": "Val2", "val3": "Val3"}},
                    ]
                },
                {
                    "groups": [
                        {"labels": {"val4": "Val4", "val5": "Val5"}},
                        {"labels": {"val5": "Val5", "val6": "Val6"}},
                    ]
                },
            ]
        },
        "type": "StringListArrayWidget",
    },
}


def test_extract_groups_labels() -> None:
    test_groups = TEST_INPUT["string_list_array"]["details"]["groups"]
    test_values = {"test_value": "Test Value"}
    exp_output = {
        "test_value": "Test Value",
        "val1": "Val1",
        "val2": "Val2",
        "val3": "Val3",
    }
    res_output = cads_processing_api_service.translators.extract_groups_labels(
        test_groups, test_values
    )
    assert res_output == exp_output

    test_groups = TEST_INPUT["string_list_array"]["details"]["groups"]
    exp_output = {"val1": "Val1", "val2": "Val2", "val3": "Val3"}
    res_output = cads_processing_api_service.translators.extract_groups_labels(
        test_groups
    )
    assert res_output == exp_output

    test_groups = TEST_INPUT["string_list_array_groups"]["details"]["groups"]
    exp_output = {
        "val1": "Val1",
        "val2": "Val2",
        "val3": "Val3",
        "val4": "Val4",
        "val5": "Val5",
        "val6": "Val6",
    }
    res_output = cads_processing_api_service.translators.extract_groups_labels(
        test_groups
    )
    assert res_output == exp_output


def test_extract_labels() -> None:
    test_inputs_cds_schema = TEST_INPUT["string_list_array"]
    exp_output = {"val1": "Val1", "val2": "Val2", "val3": "Val3"}
    res_output = cads_processing_api_service.translators.extract_labels(
        test_inputs_cds_schema
    )
    assert res_output == exp_output

    test_inputs_cds_schema = TEST_INPUT["string_list"]
    exp_output = {"val1": "Val1", "val2": "Val2", "val3": "Val3"}
    res_output = cads_processing_api_service.translators.extract_labels(
        test_inputs_cds_schema
    )
    assert res_output == exp_output


def test_translate_string_list() -> None:
    test_input = TEST_INPUT["string_list"]
    exp_ouput = {
        "type": "array",
        "items": {"type": "string", "enum": ["val1", "val2", "val3"]},
    }
    res_output = cads_processing_api_service.translators.translate_string_list(
        test_input
    )

    assert res_output == exp_ouput


def test_translate_string_list_array() -> None:
    test_input = TEST_INPUT["string_list_array"]
    exp_ouput = {
        "type": "array",
        "items": {"type": "string", "enum": ["val1", "val2", "val3"]},
    }
    res_output = cads_processing_api_service.translators.translate_string_list_array(
        test_input
    )

    assert res_output == exp_ouput

    test_input = TEST_INPUT["string_list_array_groups"]
    exp_ouput = {
        "type": "array",
        "items": {
            "type": "string",
            "enum": ["val1", "val2", "val3", "val4", "val5", "val6"],
        },
    }
    res_output = cads_processing_api_service.translators.translate_string_list_array(
        test_input
    )

    assert res_output == exp_ouput


def test_translate_string_choice() -> None:
    test_input = TEST_INPUT["string_choice"]
    exp_ouput = {"type": "string", "enum": ["val1", "val2", "val3"], "default": "val1"}
    res_output = cads_processing_api_service.translators.translate_string_choice(
        test_input
    )

    assert res_output == exp_ouput


def test_translate_geographic_extent_map() -> None:
    test_input = TEST_INPUT["geographic_extent_map"]
    exp_ouput = {
        "type": "array",
        "minItems": 4,
        "maxItems": 4,
        "items": {"type": "number"},
        "default": [1, 2, 3, 4],
    }
    res_output = (
        cads_processing_api_service.translators.translate_geographic_extent_map(
            test_input
        )
    )

    assert res_output == exp_ouput


def test_format_request_value() -> None:
    test_value_1 = "test_value"
    exp_output_1 = "'test_value'"
    res_output_1 = cads_processing_api_service.translators.format_request_value(
        test_value_1
    )
    assert res_output_1 == exp_output_1

    test_value_2 = ["test_value_1", "test_value_2"]
    exp_output_2 = "['test_value_1', 'test_value_2']"
    res_output_2 = cads_processing_api_service.translators.format_request_value(
        test_value_2
    )
    assert res_output_2 == exp_output_2


def test_format_api_request() -> None:
    test_api_request_template = (
        "import cads_api_client\n\n"
        "request = {api_request_kwargs}\n\n"
        "client = cads_api_client.ApiClient()\n"
        "client.retrieve(\n\t"
        "collection_id='{process_id}',\n\t"
        "**request\n"
        ")\n"
    )
    test_process_id = "test_process_id"
    test_request = {
        "inputs": {
            "variable": "test_variable_1",
            "year": ["2000", "2001"],
        }
    }
    exp_output = (
        "import cads_api_client\n\n"
        "request = {\n\t"
        "'variable': 'test_variable_1',\n\t"
        "'year': ['2000', '2001']\n"
        "}\n\n"
        "client = cads_api_client.ApiClient()\n"
        "client.retrieve(\n\t"
        "collection_id='test_process_id',\n\t"
        "**request\n"
        ")\n"
    )
    res_output = cads_processing_api_service.translators.format_api_request(
        test_api_request_template, test_process_id, test_request
    )
    assert res_output == exp_output
