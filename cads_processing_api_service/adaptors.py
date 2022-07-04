import itertools
import json
import pathlib
from typing import Any

ACCEPTED_INPUTS = [
    "product_type",
    "variable",
    "year",
    "month",
    "time",
    "area",
    "format",
]


def _string_array_to_string_array(input_cds_schema: dict[str, Any]) -> dict[str, Any]:
    input_ogc_schema = {"type": "array", "items": {"type": "string"}}
    input_ogc_schema["enum"] = input_cds_schema["details"]["values"]
    return input_ogc_schema


def _string_list_to_string_array(input_cds_schema: dict[str, Any]) -> dict[str, Any]:
    input_ogc_schema = {"type": "array", "items": {"type": "string"}}
    values = []
    for group in input_cds_schema["details"]["groups"]:
        values.append(group["values"])
    input_ogc_schema["enum"] = list(set(itertools.chain.from_iterable(values)))
    return input_ogc_schema


def _string_choice_to_string_value(input_cds_schema: dict[str, Any]) -> dict[str, Any]:
    input_ogc_schema = {
        "type": "string",
        "enum": input_cds_schema["details"]["values"],
        "default": input_cds_schema["details"]["default"],
    }
    return input_ogc_schema


def _extent_to_area(input_cds_schema: dict[str, Any]) -> dict[str, Any]:
    input_ogc_schema = {
        "type": "array",
        "minItems": 4,
        "maxItems": 4,
        "items": {"type": "number"},
        "default": input_cds_schema["details"]["default"],
    }
    return input_ogc_schema


SCHEMA_TRANSLATORS = {
    "StringListWidget": _string_array_to_string_array,
    "StringListArrayWidget": _string_list_to_string_array,
    "StringChoiceWidget": _string_choice_to_string_value,
    "GeographicExtentMapWidget": _extent_to_area,
}


def _build_input_ogc_schema(input_cds_schema: dict[str, Any]) -> dict[str, Any]:
    input_cds_type = input_cds_schema["type"]
    input_ogc_schema = SCHEMA_TRANSLATORS[input_cds_type](input_cds_schema)
    return input_ogc_schema


def translate_cds_into_ogc_inputs(
    cds_form_file: str | pathlib.Path,
) -> list[dict[str, Any]]:

    with open(cds_form_file, "r") as f:
        cds_form = json.load(f)

    inputs_ogc = []
    for input_cds_schema in cds_form:
        if input_cds_schema["name"] in ACCEPTED_INPUTS:
            input_ogc: dict[str, Any] = {
                input_cds_schema["name"]: {
                    "title": input_cds_schema["label"],
                    "schema": _build_input_ogc_schema(input_cds_schema),
                }
            }
            inputs_ogc.append(input_ogc)

    return inputs_ogc
