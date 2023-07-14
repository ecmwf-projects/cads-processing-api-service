"""Catalogue CDS forms to OGC API Processes compliant inputs translators."""

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


def extract_labels(input_cds_schema: dict[str, Any]) -> dict[str, str]:
    details = input_cds_schema["details"]
    values = {}
    if "groups" in details:
        values = extract_groups_labels(details["groups"])
    else:
        values = details["labels"]
    return values


def extract_groups_labels(
    groups: list[Any], values: dict[str, str] | None = None
) -> list[Any]:
    if values is None:
        values = {}
    for group in groups:
        if "labels" in group:
            values.update(group["labels"])
        elif "groups" in group:
            values = extract_groups_labels(group["groups"], values)
    return values


def translate_string_list(input_cds_schema: dict[str, Any]) -> dict[str, Any]:
    labels = extract_labels(input_cds_schema)
    input_ogc_schema: dict[str, Any] = {
        "type": "array",
        "items": {"type": "string", "enum": sorted(list(labels.keys()))},
    }
    return input_ogc_schema


def translate_string_list_array(
    input_cds_schema: dict[str, Any],
) -> dict[str, Any]:
    labels = extract_labels(input_cds_schema)
    input_ogc_schema: dict[str, Any] = {
        "type": "array",
        "items": {"type": "string", "enum": sorted(list(set(labels.keys())))},
    }
    return input_ogc_schema


def translate_string_choice(input_cds_schema: dict[str, Any]) -> dict[str, Any]:
    labels = extract_labels(input_cds_schema)
    input_ogc_schema = {
        "type": "string",
        "enum": list(labels.keys()),
        "default": input_cds_schema["details"].get("default", None),
    }
    return input_ogc_schema


def translate_geographic_extent_map(input_cds_schema: dict[str, Any]) -> dict[str, Any]:
    input_ogc_schema = {
        "type": "array",
        "minItems": 4,
        "maxItems": 4,
        "items": {"type": "number"},
        "default": input_cds_schema["details"].get("default", None),
    }
    return input_ogc_schema


SCHEMA_TRANSLATORS = {
    "StringListWidget": translate_string_list,
    "StringListArrayWidget": translate_string_list_array,
    "StringChoiceWidget": translate_string_choice,
    "GeographicExtentMapWidget": translate_geographic_extent_map,
    "GeographicExtentWidget": translate_geographic_extent_map,
}


def make_ogc_input_schema(cds_input_schema: dict[str, Any]) -> dict[str, Any]:
    cds_input_type = cds_input_schema["type"]
    ogc_input_schema = SCHEMA_TRANSLATORS[cds_input_type](cds_input_schema)
    return ogc_input_schema


def translate_cds_form(
    cds_form: list[Any] | dict[str, Any],
) -> dict[str, Any]:
    """Translate CDS forms inputs into OGC API compliants ones.

    Convert inputs information contained in the provided CDS form file
    into a Python object compatible with the OGC API - Processes standard.

    Parameters
    ----------
    cds_form : list[Any] | dict[str, Any]
        CDS form.

    Returns
    -------
    dict[str, Any]
        Python object containing translated inputs information.
    """
    if not isinstance(cds_form, list):
        cds_form = [
            cds_form,
        ]
    ogc_inputs = {}
    for cds_input_schema in cds_form:
        if cds_input_schema["type"] in SCHEMA_TRANSLATORS:
            ogc_inputs[cds_input_schema["name"]] = {
                "title": cds_input_schema["label"],
                "schema_": {**make_ogc_input_schema(cds_input_schema)},
            }

    return ogc_inputs


def translate_request_ids_into_labels(
    request: dict[str, Any], cds_form: list[Any] | dict[str, Any]
) -> dict[str, Any]:
    if not isinstance(cds_form, list):
        cds_form = [
            cds_form,
        ]
    request_labels = {}
    for cds_input_schema in cds_form:
        input_name = cds_input_schema["name"]
        if input_name in request:
            input_ids = request[input_name]
            if not isinstance(input_ids, list):
                input_ids = [
                    input_ids,
                ]
            input_labels = extract_labels(cds_input_schema)
            request_labels[cds_input_schema["label"]] = [
                input_labels[input_id] for input_id in input_ids
            ]
    return request_labels
