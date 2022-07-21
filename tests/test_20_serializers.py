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

import json

import cads_catalogue.database
import ogc_api_processes_fastapi.models

import cads_processing_api_service.clients


def get_record(id: str) -> cads_catalogue.database.Resource:
    return cads_catalogue.database.Resource(
        resource_uid=id,
        title="ERA5",
        description={"name": "dataset"},
        abstract="Lorem ipsum dolor",
        form="form",
        keywords=["label 1", "label 2"],
        version="2.0.0",
        variables=["var1", "var2"],
        extent=[[-180, 180], [-90, 90]],
    )


def test_process_summary_serializer() -> None:
    record = get_record("era5-something")
    oap_record = cads_processing_api_service.clients.process_summary_serializer(record)
    expected = ogc_api_processes_fastapi.models.ProcessSummary(
        title="Retrieve of ERA5",
        description=json.dumps({"name": "dataset"}),
        keywords=["label 1", "label 2"],
        id="retrieve-era5-something",
        version="1.0.0",
        jobControlOptions=["async-execute"],
        outputTransmission=["reference"],
    )
    assert oap_record == expected


# TODO: update test when retrievable forms enabled
def test_process_description_serializer() -> None:
    record = get_record("era5-something")
    oap_record = cads_processing_api_service.clients.process_description_serializer(
        record
    )
    expected = ogc_api_processes_fastapi.models.ProcessDescription(
        title="Retrieve of ERA5",
        description=json.dumps({"name": "dataset"}),
        keywords=["label 1", "label 2"],
        id="retrieve-era5-something",
        version="1.0.0",
        jobControlOptions=["async-execute"],
        outputTransmission=["reference"],
        inputs=cads_processing_api_service.clients.process_inputs_serializer(),
    )
    assert oap_record == expected
