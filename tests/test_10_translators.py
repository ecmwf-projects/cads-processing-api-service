# type: ignore

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

# mypy: ignore-errors

from typing import Any

from cads_processing_api_service import config, translators

TEST_INPUT_CDS_SCHEMAS: dict[str, Any] = {
    "string_list": {
        "name": "string_list",
        "label": "String List",
        "details": {"labels": {"val1": "Val1", "val2": "Val2", "val3": "Val3"}},
        "type": "StringListWidget",
    },
    "string_list_array": {
        "name": "string_list_array",
        "label": "String List Array",
        "details": {
            "groups": [
                {"labels": {"val1": "Val1", "val2": "Val2"}},
                {"labels": {"val2": "Val2", "val3": "Val3"}},
            ]
        },
        "type": "StringListArrayWidget",
    },
    "string_choice": {
        "name": "string_choice",
        "label": "String Choice",
        "details": {
            "labels": {"val1": "Val1", "val2": "Val2", "val3": "Val3"},
            "default": "val1",
        },
        "type": "StringChoiceWidget",
    },
    "geographic_extent_map": {
        "name": "geographic_extent_map",
        "label": "Geographic Extent Map",
        "details": {"default": [1, 2, 3, 4]},
        "type": "GeographicExtentMapWidget",
    },
    "geographic_location": {
        "name": "geographic_location",
        "label": "Geographic Location",
        "details": {},
        "type": "GeographicLocationWidget",
    },
    "string_list_array_groups": {
        "name": "string_list_array_groups",
        "label": "String List Array Groups",
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
    "free_edition_widget": {
        "name": "free_edition_widget",
        "label": "Free Edition Widget",
        "type": "FreeEditionWidget",
        "details": {},
    },
    "exclusive_group_widget": {
        "name": "exclusive_group_widget",
        "label": "Exclusive Group Widget",
        "type": "ExclusiveGroupWidget",
        "children": ["child_1", "child_2"],
        "details": {"default": "child_1"},
    },
    "child_1": {
        "name": "child_1",
        "label": "Child 1",
        "type": "Child1Widget",
        "details": {},
    },
    "child_2": {
        "name": "child_2",
        "label": "Child 2",
        "type": "Child2Widget",
        "details": {},
    },
}


def test_extract_groups_labels() -> None:
    test_groups = TEST_INPUT_CDS_SCHEMAS["string_list_array"]["details"]["groups"]
    test_values = {"test_value": "Test Value"}
    exp_output = {
        "test_value": "Test Value",
        "val1": "Val1",
        "val2": "Val2",
        "val3": "Val3",
    }
    res_output = translators.extract_groups_labels(test_groups, test_values)
    assert res_output == exp_output

    test_groups = TEST_INPUT_CDS_SCHEMAS["string_list_array"]["details"]["groups"]
    exp_output = {"val1": "Val1", "val2": "Val2", "val3": "Val3"}
    res_output = translators.extract_groups_labels(test_groups)
    assert res_output == exp_output

    test_groups = TEST_INPUT_CDS_SCHEMAS["string_list_array_groups"]["details"][
        "groups"
    ]
    exp_output = {
        "val1": "Val1",
        "val2": "Val2",
        "val3": "Val3",
        "val4": "Val4",
        "val5": "Val5",
        "val6": "Val6",
    }
    res_output = translators.extract_groups_labels(test_groups)
    assert res_output == exp_output


def test_extract_labels() -> None:
    test_inputs_cds_schema = TEST_INPUT_CDS_SCHEMAS["string_list_array"]
    exp_output = {"val1": "Val1", "val2": "Val2", "val3": "Val3"}
    res_output = translators.extract_labels(test_inputs_cds_schema)
    assert res_output == exp_output

    test_inputs_cds_schema = TEST_INPUT_CDS_SCHEMAS["string_list"]
    exp_output = {"val1": "Val1", "val2": "Val2", "val3": "Val3"}
    res_output = translators.extract_labels(test_inputs_cds_schema)
    assert res_output == exp_output

    test_inputs_cds_schema = TEST_INPUT_CDS_SCHEMAS["free_edition_widget"]
    exp_output = {}
    res_output = translators.extract_labels(test_inputs_cds_schema)
    assert res_output == exp_output


def test_translate_string_list() -> None:
    test_input = TEST_INPUT_CDS_SCHEMAS["string_list"]
    exp_ouput = {
        "type": "array",
        "items": {"type": "string", "enum": ["val1", "val2", "val3"]},
    }
    res_output = translators.translate_string_list(test_input)
    assert res_output == exp_ouput


def test_translate_string_list_array() -> None:
    test_input = TEST_INPUT_CDS_SCHEMAS["string_list_array"]
    exp_ouput = {
        "type": "array",
        "items": {"type": "string", "enum": ["val1", "val2", "val3"]},
    }
    res_output = translators.translate_string_list_array(test_input)
    assert res_output == exp_ouput

    test_input = TEST_INPUT_CDS_SCHEMAS["string_list_array_groups"]
    exp_ouput = {
        "type": "array",
        "items": {
            "type": "string",
            "enum": ["val1", "val2", "val3", "val4", "val5", "val6"],
        },
    }
    res_output = translators.translate_string_list_array(test_input)
    assert res_output == exp_ouput


def test_translate_string_choice() -> None:
    test_input = TEST_INPUT_CDS_SCHEMAS["string_choice"]
    exp_ouput = {"type": "string", "enum": ["val1", "val2", "val3"], "default": "val1"}
    res_output = translators.translate_string_choice(test_input)
    assert res_output == exp_ouput


def test_translate_geographic_extent_map() -> None:
    test_input = TEST_INPUT_CDS_SCHEMAS["geographic_extent_map"]
    exp_ouput = {
        "type": "array",
        "minItems": 4,
        "maxItems": 4,
        "items": {"type": "number"},
        "default": [1, 2, 3, 4],
    }
    res_output = translators.translate_geographic_extent_map(test_input)
    assert res_output == exp_ouput


def test_make_request_labels() -> None:
    test_input_value_ids = ["1", "1", "1", "1"]
    test_input_cds_schema = TEST_INPUT_CDS_SCHEMAS["geographic_extent_map"]
    exp_output = ["North: 1°", "West: 1°", "South: 1°", "East: 1°"]
    res_output = translators.make_request_labels(
        test_input_value_ids, test_input_cds_schema
    )
    assert res_output == exp_output

    test_input_value_ids = [{"latitude": 10, "longitude": 10}]
    test_input_cds_schema = TEST_INPUT_CDS_SCHEMAS["geographic_location"]
    exp_output = ["Latitude: 10°", "Longitude: 10°"]
    res_output = translators.make_request_labels(
        test_input_value_ids, test_input_cds_schema
    )
    assert res_output == exp_output

    test_input_value_ids = ["val1", "val2"]
    test_input_cds_schema = TEST_INPUT_CDS_SCHEMAS["string_list"]
    exp_output = ["Val1", "Val2"]
    res_output = translators.make_request_labels(
        test_input_value_ids, test_input_cds_schema
    )
    assert res_output == exp_output

    test_input_value_ids = ["val1", "val4"]
    test_input_cds_schema = TEST_INPUT_CDS_SCHEMAS["string_list"]
    exp_output = ["Val1", "val4"]
    res_output = translators.make_request_labels(
        test_input_value_ids, test_input_cds_schema
    )
    assert res_output == exp_output


def test_translate_request_ids_into_labels() -> None:
    request = {"key1": "val1", "key2": "val2"}
    cds_schema = None
    exp_output = {"key1": "val1", "key2": "val2"}
    res_output = translators.translate_request_ids_into_labels(request, cds_schema)
    assert res_output == exp_output

    request = {
        "string_list": ["val1", "val2"],
        "string_choice": "val1",
        "unknown_key": "unknown_value",
    }
    cds_schema = [
        TEST_INPUT_CDS_SCHEMAS["string_list"],
        TEST_INPUT_CDS_SCHEMAS["string_choice"],
    ]
    exp_output = {
        "String List": ["Val1", "Val2"],
        "String Choice": ["Val1"],
        "unknown_key": "unknown_value",
    }
    res_output = translators.translate_request_ids_into_labels(request, cds_schema)
    assert res_output == exp_output

    request = {}
    cds_schema = [
        TEST_INPUT_CDS_SCHEMAS["string_choice"],
        TEST_INPUT_CDS_SCHEMAS["exclusive_group_widget"],
        TEST_INPUT_CDS_SCHEMAS["child_1"],
        TEST_INPUT_CDS_SCHEMAS["child_2"],
    ]
    exp_output = {
        "String Choice": ["Val1"],
        "Exclusive Group Widget": ["Child 1"],
    }


def test_format_list() -> None:
    value_list = ["test_value_1", "test_value_2"]
    max_items_per_line = 1
    exp_output = "[\n        'test_value_1',\n        'test_value_2'\n    ]"
    res_output = translators.format_list(value_list, max_items_per_line)
    assert res_output == exp_output

    max_items_per_line = 2
    exp_output = "['test_value_1', 'test_value_2']"
    res_output = translators.format_list(value_list, max_items_per_line)
    assert res_output == exp_output


def test_format_request_value() -> None:
    test_value = "test_value"
    exp_output = '"test_value"'
    res_output = translators.format_request_value(test_value)
    assert res_output == exp_output

    test_value = 1
    exp_output = "1"
    res_output = translators.format_request_value(test_value)
    assert res_output == exp_output

    test_value = ["test_value_1", "test_value_2"]
    exp_output = "[\n        'test_value_1',\n        'test_value_2'\n    ]"
    res_output = translators.format_request_value(test_value)
    assert res_output == exp_output


def test_format_api_request() -> None:
    test_api_request_template = config.API_REQUEST_TEMPLATE
    test_process_id = "test_process_id"
    test_request = {
        "inputs": {
            "variable_1": "value_1",
            "variable_2": ["value_1", "value_2"],
            "variable_3": 1,
        }
    }
    exp_output = (
        "import logging\n\n"
        "import datapi\n\n"
        'logging.basicConfig(level="INFO")\n\n'
        'collection_id = "test_process_id"\n'
        "request = {\n"
        '    "variable_1": "value_1",\n'
        '    "variable_2": [\n        "value_1",\n        "value_2"\n    ],\n'
        '    "variable_3": 1\n'
        "}\n\n"
        "client = datapi.ApiClient()\n"
        "client.retrieve(collection_id, **request)\n"
    )
    res_output = translators.format_api_request(
        test_api_request_template, test_process_id, test_request
    )
    print(res_output)
    aaaa
    assert res_output == exp_output
