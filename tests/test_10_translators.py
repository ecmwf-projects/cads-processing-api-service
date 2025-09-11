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

import pytest

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
    "string_choice_default_list": {
        "name": "string_choice",
        "label": "String Choice",
        "details": {
            "labels": {"val1": "Val1", "val2": "Val2", "val3": "Val3"},
            "default": ["val1"],
        },
        "type": "StringChoiceWidget",
    },
    "geographic_extent_map": {
        "name": "geographic_extent_map",
        "label": "Geographic Extent Map",
        "details": {"default": [1, 2, 3, 4]},
        "type": "GeographicExtentMapWidget",
    },
    "geographic_extent_map_default_dict": {
        "name": "geographic_extent_map",
        "label": "Geographic Extent Map",
        "details": {"default": {"n": 1, "w": 2, "s": 3, "e": 4}},
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
    "inclusive_group_widget_group_output_false": {
        "name": "inclusive_group_widget",
        "label": "Inclusive Group Widget",
        "type": "InclusiveGroupWidget",
        "children": ["child_3", "child_4"],
        "details": {"group_output": False},
    },
    "child_3": {
        "name": "child_3",
        "label": "Child 3",
        "type": "Child3Widget",
        "details": {},
    },
    "child_4": {
        "name": "child_4",
        "label": "Child 4",
        "type": "Child4Widget",
        "details": {},
    },
    "inclusive_group_widget_group_output_true": {
        "name": "inclusive_group_widget",
        "label": "Inclusive Group Widget",
        "type": "InclusiveGroupWidget",
        "children": ["child_5", "child_6"],
        "details": {"group_output": True},
    },
    "child_5": {
        "name": "child_5",
        "label": "Child 5",
        "type": "Child5Widget",
        "details": {},
    },
    "child_6": {
        "name": "child_6",
        "label": "Child 6",
        "type": "Child6Widget",
        "details": {},
    },
}


@pytest.mark.parametrize(
    "groups, labels, expected_output",
    [
        (
            TEST_INPUT_CDS_SCHEMAS["string_list_array"]["details"]["groups"],
            None,
            {"val1": "Val1", "val2": "Val2", "val3": "Val3"},
        ),
        (
            TEST_INPUT_CDS_SCHEMAS["string_list_array"]["details"]["groups"],
            {"test_value": "Test Label"},
            {
                "test_value": "Test Label",
                "val1": "Val1",
                "val2": "Val2",
                "val3": "Val3",
            },
        ),
        (
            TEST_INPUT_CDS_SCHEMAS["string_list_array_groups"]["details"]["groups"],
            None,
            {
                "val1": "Val1",
                "val2": "Val2",
                "val3": "Val3",
                "val4": "Val4",
                "val5": "Val5",
                "val6": "Val6",
            },
        ),
    ],
    ids=[
        "StringListArrayWidget without labels",
        "StringListArrayWidget with labels",
        "StringListArrayWidget with bested groups",
    ],
)
def test_extract_groups_labels(groups, labels, expected_output) -> None:
    output = translators.extract_groups_labels(groups, labels)
    assert output == expected_output


@pytest.mark.parametrize(
    "cds_schema, expected_output",
    [
        (
            TEST_INPUT_CDS_SCHEMAS["string_list_array"],
            {"val1": "Val1", "val2": "Val2", "val3": "Val3"},
        ),
        (
            TEST_INPUT_CDS_SCHEMAS["string_list"],
            {"val1": "Val1", "val2": "Val2", "val3": "Val3"},
        ),
        (TEST_INPUT_CDS_SCHEMAS["free_edition_widget"], {}),
    ],
    ids=["StringListArrayWidget", "StringListWidget", "FreeEditionWidget"],
)
def test_extract_labels(cds_schema, expected_output) -> None:
    output = translators.extract_labels(cds_schema)
    assert output == expected_output


def test_translate_string_list() -> None:
    test_input = TEST_INPUT_CDS_SCHEMAS["string_list"]
    exp_ouput = {
        "type": "array",
        "items": {"type": "string", "enum": ["val1", "val2", "val3"]},
    }
    res_output = translators.translate_string_list(test_input)
    assert res_output == exp_ouput


@pytest.mark.parametrize(
    "cds_schema, expected_output",
    [
        (
            TEST_INPUT_CDS_SCHEMAS["string_list_array"],
            {
                "type": "array",
                "items": {"type": "string", "enum": ["val1", "val2", "val3"]},
            },
        ),
        (
            TEST_INPUT_CDS_SCHEMAS["string_list_array_groups"],
            {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": ["val1", "val2", "val3", "val4", "val5", "val6"],
                },
            },
        ),
    ],
    ids=["shallow groups", "nested groups"],
)
def test_translate_string_list_array(cds_schema, expected_output) -> None:
    output = translators.translate_string_list_array(cds_schema)
    assert output == expected_output


@pytest.mark.parametrize(
    "cds_schema, expected_output",
    [
        (
            TEST_INPUT_CDS_SCHEMAS["string_choice"],
            {"type": "string", "enum": ["val1", "val2", "val3"], "default": "val1"},
        ),
        (
            TEST_INPUT_CDS_SCHEMAS["string_choice_default_list"],
            {"type": "string", "enum": ["val1", "val2", "val3"], "default": "val1"},
        ),
    ],
    ids=["default as string", "default as list"],
)
def test_translate_string_choice(cds_schema, expected_output) -> None:
    output = translators.translate_string_choice(cds_schema)
    assert output == expected_output


@pytest.mark.parametrize(
    "cds_schema, expected_output",
    [
        (
            TEST_INPUT_CDS_SCHEMAS["geographic_extent_map"],
            {
                "type": "array",
                "minItems": 4,
                "maxItems": 4,
                "items": {"type": "number"},
                "default": [1, 2, 3, 4],
            },
        ),
        (
            TEST_INPUT_CDS_SCHEMAS["geographic_extent_map_default_dict"],
            {
                "type": "array",
                "minItems": 4,
                "maxItems": 4,
                "items": {"type": "number"},
                "default": [1, 2, 3, 4],
            },
        ),
    ],
    ids=["default as list", "default as dict"],
)
def test_translate_geographic_extent_map(cds_schema, expected_output) -> None:
    output = translators.translate_geographic_extent_map(cds_schema)
    assert output == expected_output


@pytest.mark.parametrize(
    "input_value_ids, cds_schema, expected_output",
    [
        (
            ["1", "1", "1", "1"],
            TEST_INPUT_CDS_SCHEMAS["geographic_extent_map"],
            ["North: 1°", "West: 1°", "South: 1°", "East: 1°"],
        ),
        (
            [{"latitude": 10, "longitude": 10}],
            TEST_INPUT_CDS_SCHEMAS["geographic_location"],
            ["Latitude: 10°", "Longitude: 10°"],
        ),
        (
            ["val1", "val2"],
            TEST_INPUT_CDS_SCHEMAS["string_list"],
            ["Val1", "Val2"],
        ),
        (
            ["val1", "val4"],
            TEST_INPUT_CDS_SCHEMAS["string_list"],
            ["Val1", "val4"],
        ),
    ],
    ids=[
        "GeographicExtentMapWidget",
        "GeographicLocationWidget",
        "StringListWidget with known values",
        "StringListWidget with unknown value",
    ],
)
def test_make_labels_from_ids(input_value_ids, cds_schema, expected_output) -> None:
    output = translators.make_labels_from_ids(input_value_ids, cds_schema)
    assert output == expected_output


@pytest.mark.parametrize(
    "request_ids, cds_schema, expected_output",
    [
        (
            {"key1": "val1", "key2": "val2"},
            None,
            {"key1": "val1", "key2": "val2"},
        ),
        (
            {
                "string_list": ["val1", "val2"],
                "string_choice": "val1",
                "unknown_key": "unknown_value",
            },
            [
                TEST_INPUT_CDS_SCHEMAS["string_list"],
                TEST_INPUT_CDS_SCHEMAS["string_choice"],
            ],
            {
                "String List": ["Val1", "Val2"],
                "String Choice": ["Val1"],
                "unknown_key": "unknown_value",
            },
        ),
        (
            {},
            [
                TEST_INPUT_CDS_SCHEMAS["string_choice"],
                TEST_INPUT_CDS_SCHEMAS["exclusive_group_widget"],
                TEST_INPUT_CDS_SCHEMAS["child_1"],
                TEST_INPUT_CDS_SCHEMAS["child_2"],
            ],
            {
                "String Choice": ["Val1"],
                "Exclusive Group Widget": ["Child 1"],
            },
        ),
    ],
    ids=["no cds_schema", "request with unknown key", "empty request with defaults"],
)
def test_translate_request_ids_into_labels(
    request_ids, cds_schema, expected_output
) -> None:
    output = translators.translate_request_ids_into_labels(request_ids, cds_schema)
    assert output == expected_output


@pytest.mark.parametrize(
    "value_list, max_items_per_line, expected_output",
    [
        (
            ["test_value_1", "test_value_2"],
            1,
            "[\n        'test_value_1',\n        'test_value_2'\n    ]",
        ),
        (["test_value_1", "test_value_2"], 2, "['test_value_1', 'test_value_2']"),
    ],
    ids=["max_items_per_line = 1", "max_items_per_line = 2"],
)
def test_format_list(value_list, max_items_per_line, expected_output) -> None:
    output = translators.format_list(value_list, max_items_per_line)
    assert output == expected_output


@pytest.mark.parametrize(
    "value, key, expected_output",
    [
        ("test_value", None, '"test_value"'),
        (1, None, "1"),
        (
            ["test_value_1", "test_value_2"],
            None,
            "[\n        'test_value_1',\n        'test_value_2'\n    ]",
        ),
    ],
    ids=["string value", "integer value", "list value"],
)
def test_format_request_value(value, key, expected_output) -> None:
    output = translators.format_request_value(value, key)
    assert output == expected_output


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
        "import cdsapi\n\n"
        'dataset = "test_process_id"\n'
        "request = {\n"
        '    "variable_1": "value_1",\n'
        '    "variable_2": [\n        "value_1",\n        "value_2"\n    ],\n'
        '    "variable_3": 1\n'
        "}\n\n"
        "client = cdsapi.Client()\n"
        "client.retrieve(dataset, request).download()\n"
    )
    res_output = translators.format_api_request(
        test_api_request_template, test_process_id, test_request
    )
    assert res_output == exp_output
