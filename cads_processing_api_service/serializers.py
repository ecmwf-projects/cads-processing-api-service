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

import cads_catalogue.database
import ogc_api_processes_fastapi.models

from . import translators


def serialize_process_summary(
    db_model: cads_catalogue.database.Resource,
) -> ogc_api_processes_fastapi.models.ProcessSummary:
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
    retval = ogc_api_processes_fastapi.models.ProcessSummary(
        title=db_model.title,
        description=db_model.abstract,
        id=db_model.resource_uid,  # type: ignore
        version="1.0.0",
        jobControlOptions=[
            ogc_api_processes_fastapi.models.JobControlOptions.async_execute,
        ],
        outputTransmission=[
            ogc_api_processes_fastapi.models.TransmissionMode.reference,
        ],
    )

    return retval


def serialize_process_description(
    db_model: cads_catalogue.database.Resource,
) -> ogc_api_processes_fastapi.models.ProcessDescription:
    """Convert provided database entry into a representation of the related process description.

    Parameters
    ----------
    db_model : cads_catalogue.database.Resource
        Database entry.

    Returns
    -------
    ogc_api_processes_fastapi.models.ProcessDescription
        Process description representation.
    """
    process_summary = serialize_process_summary(db_model)
    cds_form = db_model.resource_data.form_data  # type: ignore
    process_inputs = {}
    if cds_form:
        process_inputs = translators.translate_cds_form(cds_form)  # type: ignore
    retval = ogc_api_processes_fastapi.models.ProcessDescription(
        **process_summary.model_dump(),
        inputs=process_inputs,
    )
    return retval
