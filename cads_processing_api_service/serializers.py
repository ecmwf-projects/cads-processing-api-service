"""Catalogue entries to OGC API Processes models serializers."""

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

import urllib.parse
from typing import Any, Dict

import cads_catalogue.database
import ogc_api_processes_fastapi.responses
import requests

from . import config, translators


def get_cds_form(cds_form_url: str) -> list[Any]:
    """Get CDS form from URL.

    Parameters
    ----------
    cds_form_url : str
        URL to the CDS form, relative to the Document Storage URL.

    Returns
    -------
    list[Any]
        CDS form.
    """
    settings = config.ensure_settings()
    cds_form_complete_url = urllib.parse.urljoin(
        settings.document_storage_url, cds_form_url
    )
    cds_form: list[Any] = requests.get(cds_form_complete_url).json()
    return cds_form


def serialize_process_summary(
    db_model: cads_catalogue.database.Resource,
) -> ogc_api_processes_fastapi.responses.schema["ProcessSummary"]:  # noqa
    """Convert provided database entry into a representation of a process summary.

    Parameters
    ----------
    db_model : cads_catalogue.database.Resource
        Database entry.

    Returns
    -------
    ogc_api_processes_fastapi.models.ProcessSummary
        Process summary representation.
    """
    retval = ogc_api_processes_fastapi.responses.schema["ProcessSummary"](
        title=f"{db_model.title}",
        description=db_model.abstract,
        keywords=db_model.keywords,
        id=db_model.resource_uid,
        version="1.0.0",
        jobControlOptions=[
            "async-execute",
        ],
        outputTransmission=[
            "reference",
        ],
    )

    return retval


def serialize_process_inputs(
    db_model: cads_catalogue.database.Resource,
) -> Dict[str, Any]:
    """Convert provided database entry into a representation of a process inputs.

    Returns
    -------
    dict[str, Any]
        Process inputs representation.
    """
    form_url = db_model.form
    cds_form = get_cds_form(cds_form_url=form_url)
    inputs = translators.translate_cds_into_ogc_inputs(cds_form)
    return inputs


def serialize_process_description(
    db_model: cads_catalogue.database.Resource,
) -> ogc_api_processes_fastapi.responses.schema["ProcessDescription"]:  # noqa
    """Convert provided database entry into a representation of a process description.

    Parameters
    ----------
    db_model : cads_catalogue.database.Resource
        Database entry.

    Returns
    -------
    ogc_api_processes_fastapi.responses.schema["ProcessDescription"]
        Process description representation.
    """
    process_summary = serialize_process_summary(db_model)
    retval = ogc_api_processes_fastapi.responses.schema["ProcessDescription"](
        **process_summary.dict(),
        inputs=serialize_process_inputs(db_model),
    )
    return retval
