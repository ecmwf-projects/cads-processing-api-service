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

import ogc_api_processes_fastapi.models

import cads_processing_api_service.serializers

from . import testing


def test_process_summary_serializer() -> None:
    record = testing.get_record("era5-something")
    oap_record = cads_processing_api_service.serializers.ProcessSerializer.process_summary_db_to_oap(
        record
    )
    expected = ogc_api_processes_fastapi.models.ProcessSummary(
        title="Retrieve of ERA5",
        description="description",
        keywords=["label 1", "label 2"],
        id="retrieve-era5-something",
        version="1.0.0",
        jobControlOptions=["async-execute"],
        outputTransmission=["reference"],
    )
    assert oap_record == expected


def test_process_description_serializer() -> None:
    record = testing.get_record("era5-something")
    oap_record = cads_processing_api_service.serializers.ProcessSerializer.process_description_db_to_oap(
        record
    )
    expected = ogc_api_processes_fastapi.models.Process(
        title="Retrieve of ERA5",
        description="description",
        keywords=["label 1", "label 2"],
        id="retrieve-era5-something",
        version="1.0.0",
        jobControlOptions=["async-execute"],
        outputTransmission=["reference"],
    )
    assert oap_record == expected
