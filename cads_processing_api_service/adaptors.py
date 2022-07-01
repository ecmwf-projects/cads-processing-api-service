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


def translate_cds_into_ogc_inputs(
    cds_form_file: str | pathlib.Path,
) -> list[dict[str, Any]]:

    with open(cds_form_file, "r") as f:
        cds_form = json.load(f)

    inputs_ogc = []
    for input_cds in cds_form:
        if input_cds["name"] in ACCEPTED_INPUTS:
            input_ogc: dict[str, Any] = {input_cds["name"]: {}}
            inputs_ogc.append(input_ogc)

    return inputs_ogc
