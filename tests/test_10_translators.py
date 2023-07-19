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

import cads_processing_api_service.translators

TEST_INPUT = {
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
