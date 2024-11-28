"""Catalogue CDS forms requests translators."""

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

import copy
from typing import Any

import fastapi
import structlog

from . import config

SETTINGS = config.settings

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


def extract_groups_labels(
    groups: list[Any], values: dict[str, str] | None = None
) -> dict[str, str]:
    if values is None:
        values = {}
    for group in groups:
        if "labels" in group:
            values.update(group["labels"])
        elif "groups" in group:
            values = extract_groups_labels(group["groups"], values)
    return values


def extract_labels(input_cds_schema: dict[str, Any]) -> dict[str, str]:
    details: dict[str, Any] = input_cds_schema["details"]
    if "groups" in details:
        values = extract_groups_labels(details["groups"])
    else:
        values = details.get("labels", {})
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


def make_request_labels(
    input_value_ids: Any,
    cds_input_schema: dict[str, Any],
) -> list[str]:
    if not isinstance(input_value_ids, list):
        input_value_ids = [input_value_ids]
    if cds_input_schema["type"] in (
        "GeographicExtentWidget",
        "GeographicExtentMapWidget",
    ):
        request_labels = [
            f"{label}: {value}°"
            for label, value in zip(
                ["North", "West", "South", "East"],
                input_value_ids,
            )
        ]
    elif cds_input_schema["type"] == "GeographicLocationWidget":
        location = input_value_ids[0]
        try:
            latitude = f"{location['latitude']}°"
            longitude = f"{location['longitude']}°"
        except Exception as e:
            logger.error(
                "Error extracting latitude and longitude from geographic location",
                error=e,
            )
            latitude = longitude = "Unknown"
        request_labels = [
            f"Latitude: {latitude}",
            f"Longitude: {longitude}",
        ]
    else:
        input_value_label = extract_labels(cds_input_schema)
        request_labels = []
        for input_value_id in input_value_ids:
            if input_value_id in input_value_label:
                request_labels.append(input_value_label[input_value_id])
            else:
                request_labels.append(input_value_id)
    return request_labels


def make_request_labels_group(
    request: dict[str, Any],
    children: list[str],
    default: str,
    cds_form: list[dict[str, Any]],
) -> list[str]:
    for cds_input_schema in cds_form:
        if cds_input_schema["name"] in children:
            input_key_id = cds_input_schema["name"]
            if input_key_id in request:
                input_value_ids = request[input_key_id]
                request_labels = make_request_labels(input_value_ids, cds_input_schema)
                cds_form.remove(cds_input_schema)
                return request_labels
            elif input_key_id == default:
                request_labels = [cds_input_schema["label"]]
    return request_labels


def translate_request_ids_into_labels(
    request: dict[str, Any], cds_form: list[Any] | dict[str, Any] | None
) -> dict[str, Any]:
    """Translate request input values into labels."""
    cds_form = copy.deepcopy(cds_form)
    if cds_form is None:
        cds_form = {}
    if not isinstance(cds_form, list):
        cds_form = [cds_form]
    # This will include in the labels the input keys that are not associated with
    # any cds_input_schema in the cds_form
    request_labels: dict[str, Any] = {
        input_key_id: str(input_value_id)
        for input_key_id, input_value_id in request.items()
    }
    exclusive_group_widgets_children = []
    for cds_input_schema in cds_form:
        if cds_input_schema.get("type", None) == "ExclusiveGroupWidget":
            exclusive_group_widgets_children.extend(cds_input_schema["children"])
    for cds_input_schema in cds_form:
        cds_input_schema_name = cds_input_schema.get("name", None)
        if cds_input_schema_name in exclusive_group_widgets_children:
            continue
        if cds_input_schema.get("type", None) == "ExclusiveGroupWidget":
            input_key_label = cds_input_schema["label"]
            children = cds_input_schema["children"]
            if keys_to_remove := list(set(request_labels.keys()) & set(children)):
                for key_to_remove in keys_to_remove:
                    del request_labels[key_to_remove]
            default = cds_input_schema.get("details", {}).get("default", None)
            request_labels[input_key_label] = make_request_labels_group(
                request, children, default, cds_form
            )
        else:
            input_key_id = cds_input_schema.get("name", None)
            input_key_label = cds_input_schema.get("label", None)
            if input_key_id in request_labels:
                del request_labels[input_key_id]
                input_value_ids = request[input_key_id]
            elif default_value_ids := cds_input_schema.get("details", {}).get(
                "default", None
            ):
                input_value_ids = default_value_ids
            else:
                continue
            if not isinstance(input_value_ids, list):
                input_value_ids = [input_value_ids]
            request_labels[input_key_label] = make_request_labels(
                input_value_ids, cds_input_schema
            )
    return request_labels


def format_list(
    value_list: list[int | float | str], max_items_per_line: int = 1
) -> str:
    if len(value_list) > max_items_per_line:
        formatted = "[\n"
        for i in range(0, len(value_list), max_items_per_line):
            line = ", ".join(
                f"'{item}'" if isinstance(item, str) else f"{item}"
                for item in value_list[i : i + max_items_per_line]
            )
            formatted += f"        {line},\n"
        formatted = formatted.rstrip(",\n") + "\n    ]"
    else:
        formatted = str(value_list)
    return formatted


def format_request_value(
    request_value: int | float | str | list[int | float | str],
    key: str | None = None,
) -> str:
    if isinstance(request_value, list):
        if key is None:
            formatted_request_value = format_list(request_value)
        else:
            api_request_max_list_length = SETTINGS.api_request_max_list_length
            max_items_per_line = api_request_max_list_length.get(key, 1)
            formatted_request_value = format_list(request_value, max_items_per_line)
    elif isinstance(request_value, str):
        formatted_request_value = f'"{request_value}"'
    else:
        formatted_request_value = str(request_value)
    return formatted_request_value


def format_api_request(
    api_request_template: str,
    process_id: str,
    request: dict[str, Any],
) -> str:
    """Format processing request into a CADS API request kwargs.

    Parameters
    ----------
    api_request_template: str,
        CADS API request template.
    process_id : str
        Process identifier.
    request : dict[str, Any]
        Request.

    Returns
    -------
    str
        CADS API request.
    """
    request_inputs: dict[str, Any] = request["inputs"]
    api_request_kwargs = (
        "{"
        + ",".join(
            [
                f'\n    "{key}": {format_request_value(value, key)}'
                for key, value in request_inputs.items()
            ]
        )
        + "\n}"
    )
    api_request = api_request_template.format(
        process_id=process_id, api_request_kwargs=api_request_kwargs
    ).replace("'", '"')

    return api_request


def get_api_request(
    process_id: str = fastapi.Path(...),
    request: dict[str, Any] = fastapi.Body(...),
) -> dict[str, str]:
    """Get CADS API request equivalent to the provided processing request.

    Parameters
    ----------
    process_id : str, optional
        Process identifier, by default fastapi.Path(...)
    request : dict[str, Any], optional
        Request, by default fastapi.Body(...)

    Returns
    -------
    dict[str, str]
        CDS API request.
    """
    api_request_template = SETTINGS.api_request_template
    api_request = format_api_request(api_request_template, process_id, request)
    return {"api_request": api_request}
