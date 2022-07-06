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

import itertools
import json
import pathlib
from typing import Any

from ogc_api_processes_fastapi import models

ACCEPTED_INPUTS = [
    "product_type",
    "variable",
    "year",
    "month",
    "time",
    "area",
    "format",
]


def string_array_to_string_array(input_cds_schema: dict[str, Any]) -> dict[str, Any]:
    input_ogc_schema: dict[str, Any] = {"type": "array", "items": {"type": "string"}}
    input_ogc_schema["items"]["enum"] = sorted(input_cds_schema["details"]["values"])
    return input_ogc_schema


def string_list_to_string_array(input_cds_schema: dict[str, Any]) -> dict[str, Any]:
    input_ogc_schema: dict[str, Any] = {"type": "array", "items": {"type": "string"}}
    values = []
    for group in input_cds_schema["details"]["groups"]:
        values.append(group["values"])
    input_ogc_schema["items"]["enum"] = sorted(
        list(set(itertools.chain.from_iterable(values)))
    )
    return input_ogc_schema


def string_choice_to_string_value(input_cds_schema: dict[str, Any]) -> dict[str, Any]:
    input_ogc_schema = {
        "type": "string",
        "enum": input_cds_schema["details"]["values"],
        "default": input_cds_schema["details"]["default"],
    }
    return input_ogc_schema


def extent_to_area(input_cds_schema: dict[str, Any]) -> dict[str, Any]:
    input_ogc_schema = {
        "type": "array",
        "minItems": 4,
        "maxItems": 4,
        "items": {"type": "number"},
        "default": input_cds_schema["details"]["default"],
    }
    return input_ogc_schema


SCHEMA_TRANSLATORS = {
    "StringListWidget": string_array_to_string_array,
    "StringListArrayWidget": string_list_to_string_array,
    "StringChoiceWidget": string_choice_to_string_value,
    "GeographicExtentMapWidget": extent_to_area,
}


def build_input_ogc_schema(input_cds_schema: dict[str, Any]) -> dict[str, Any]:
    input_cds_type = input_cds_schema["type"]
    input_ogc_schema = SCHEMA_TRANSLATORS[input_cds_type](input_cds_schema)
    return input_ogc_schema


def translate_cds_into_ogc_inputs(
    cds_form_file: str | pathlib.Path,
) -> list[dict[str, models.InputDescription]]:

    with open(cds_form_file, "r") as f:
        cds_form = json.load(f)

    inputs_ogc = []
    for input_cds_schema in cds_form:
        if input_cds_schema["name"] in ACCEPTED_INPUTS:
            input_ogc = {
                input_cds_schema["name"]: models.InputDescription(
                    title=input_cds_schema["label"],
                    schema_=models.SchemaItem(  # type: ignore
                        **build_input_ogc_schema(input_cds_schema)
                    ),
                )
            }
            inputs_ogc.append(input_ogc)

    return inputs_ogc
