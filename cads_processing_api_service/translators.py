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

import structlog

from . import config

SETTINGS = config.settings

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


def extract_groups_labels(
    groups: list[Any], labels: dict[str, str] | None = None
) -> dict[str, str]:
    """Extract labels from groups.

    Parameters
    ----------
    groups : list[Any]
        List of groups.
    values : dict[str, str] | None, optional
        Dictionary to populate, by default None

    Returns
    -------
    dict[str, str]
        Extracted labels, with keys as label ids and values as label names.
    """
    if labels is None:
        labels = {}
    for group in groups:
        if "labels" in group:
            labels.update(group["labels"])
        elif "groups" in group:
            labels = extract_groups_labels(group["groups"], labels)
    return labels


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
    default = input_cds_schema["details"].get("default", None)
    if type(default) is not str:
        if type(default) is list:
            default = default[0]
    input_ogc_schema = {
        "type": "string",
        "enum": list(labels.keys()),
        "default": default,
    }
    return input_ogc_schema


def translate_geographic_extent_map(input_cds_schema: dict[str, Any]) -> dict[str, Any]:
    default = input_cds_schema["details"].get("default", None)
    if type(default) is not list:
        if type(default) is dict and "e" in default:
            default = [default["n"], default["w"], default["s"], default["e"]]
    input_ogc_schema = {
        "type": "array",
        "minItems": 4,
        "maxItems": 4,
        "items": {"type": "number"},
        "default": default,
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


def make_labels_from_geographic_extent_widget_ids(
    input_value_ids: list[str | float],
) -> list[str]:
    """Translate geographic extent input value ids into labels.

    Parameters
    ----------
    input_value_ids : list[str | float]
        Input value ids.

    Returns
    -------
    list[str]
        List of input value labels.
    """
    request_labels = [
        f"{label}: {value}°"
        for label, value in zip(
            ["North", "West", "South", "East"],
            input_value_ids,
        )
    ]
    return request_labels


def make_labels_from_geographic_location_widget_ids(
    input_value_ids: list[dict[str, str | float]],
) -> list[str]:
    """Translate geographic location input value ids into labels.

    Parameters
    ----------
    input_value_ids : list[dict[str, str | float]]
        Input value ids.

    Returns
    -------
    list[str]
        List of input value labels.
    """
    location = input_value_ids[0]
    try:
        latitude = f"{location['latitude']}°"
        longitude = f"{location['longitude']}°"
    except Exception as e:
        logger.error(
            "Error extracting latitude and longitude from geographic location", error=e
        )
        latitude = longitude = "Unknown"
    request_labels = [
        f"Latitude: {latitude}",
        f"Longitude: {longitude}",
    ]
    return request_labels


def make_labels_from_generic_widget_ids(
    input_value_ids: list[Any], cds_input_schema: dict[str, Any]
) -> list[str]:
    """Translate generic input value ids into labels.

    Parameters
    ----------
    input_value_ids : list[Any]
        Input value ids.

    Returns
    -------
    list[str]
        List of input value labels.
    """
    input_value_label = extract_labels(cds_input_schema)
    request_labels = []
    for input_value_id in input_value_ids:
        if not isinstance(input_value_id, str):
            input_value_id = str(input_value_id)
        if input_value_id in input_value_label:
            request_labels.append(input_value_label[input_value_id])
        else:
            request_labels.append(input_value_id)
    return request_labels


LABELS_GENERATORS = {
    "GeographicExtentWidget": make_labels_from_geographic_extent_widget_ids,
    "GeographicExtentMapWidget": make_labels_from_geographic_extent_widget_ids,
    "GeographicLocationWidget": make_labels_from_geographic_location_widget_ids,
}


def make_labels_from_ids(
    input_value_ids: list[str | dict[str, str | float]],
    cds_input_schema: dict[str, Any],
) -> list[str]:
    """Translate request's input value ids into labels.

    Parameters
    ----------
    input_value_ids : list[str]
        Input value ids.
    cds_input_schema : dict[str, Any]
        CDS input schema.

    Returns
    -------
    list[str]
        List of input value labels.
    """
    if cds_input_schema.get("type", None) in LABELS_GENERATORS:
        input_value_label_generator = LABELS_GENERATORS[cds_input_schema["type"]]
        request_labels: list[str] = input_value_label_generator(input_value_ids)  # type: ignore
    else:
        request_labels = make_labels_from_generic_widget_ids(
            input_value_ids,
            cds_input_schema,
        )
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
                request_labels = make_labels_from_ids(input_value_ids, cds_input_schema)
                cds_form.remove(cds_input_schema)
                return request_labels
            elif input_key_id == default:
                request_labels = [cds_input_schema["label"]]
    return request_labels


def translate_request_ids_into_labels(
    request: dict[str, Any], cds_form: list[Any] | dict[str, Any] | None
) -> dict[str, Any]:
    """Translate request input values into labels.

    Parameters
    ----------
    request : dict[str, Any]
        Request.
    cds_form : list[Any] | dict[str, Any] | None
        CDS form.

    Returns
    -------
    dict[str, Any]
        Request with input values translated into labels.
    """
    if cds_form is None:
        return request
    elif not isinstance(cds_form, list):
        cds_form = [cds_form]
    cds_form_names_map = {
        cds_input_schema["name"]: cds_input_schema for cds_input_schema in cds_form
    }
    request_labels = {}
    for input_key_id, input_value_ids in request.items():
        if input_key_id not in cds_form_names_map:
            request_labels[input_key_id] = copy.deepcopy(input_value_ids)
        else:
            input_key_label = cds_form_names_map[input_key_id]["label"]
            if not isinstance(input_value_ids, dict):
                if not isinstance(input_value_ids, list):
                    input_value_ids = [input_value_ids]
                input_value_labels = make_labels_from_ids(
                    input_value_ids, cds_form_names_map[input_key_id]
                )
                request_labels[input_key_label] = (
                    input_value_labels
                    if len(input_value_labels) > 1
                    else input_value_labels[0]
                )
            else:
                request_labels[input_key_label] = translate_request_ids_into_labels(
                    input_value_ids, cds_form
                )
    return request_labels


def format_list(
    value_list: list[int | float | str], max_items_per_line: int = 1
) -> str:
    """Format a list into a string representation.

    Parameters
    ----------
    value_list : list[int | float | str]
        List of values to format.
    max_items_per_line : int, optional
        Maximum number of items per line, by default 1.

    Returns
    -------
    str
        Formatted string representation of the list.
    """
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
    request_value: int | float | str | list[int | float | str] | dict[str, Any],
    key: str | None = None,
) -> str:
    """Format a request value into a string representation.

    Parameters
    ----------
    request_value : int | float | str | list[int | float | str] | dict[str, Any]
        Request value to format.
    key : str | None, optional
        Request key, by default None.

    Returns
    -------
    str
        Formatted string representation of the request value.
    """
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
