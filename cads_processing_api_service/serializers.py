# Copyright 2022, European Union.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

import attrs
from cads_catalogue import database
from ogc_api_processes_fastapi import models


@attrs.define
class ProcessSerializer:
    """Serialization methods for OGC API - Processes processes."""

    @classmethod
    def process_summary_db_to_oap(
        cls, db_model: database.Resource
    ) -> models.ProcessSummary:

        retval = models.ProcessSummary(
            title=f"Retrieve of {db_model.title}",
            description=db_model.description,
            keywords=db_model.keywords,
            id=f"retrieve-{db_model.resource_id}",
            version="1.0.0",
            jobControlOptions=[
                "async-execute",
            ],
            outputTransmission=[
                "reference",
            ],
        )

        return retval

    @classmethod
    def process_description_db_to_oap(
        cls, db_model: database.Resource
    ) -> models.Process:

        process_summary = cls.process_summary_db_to_oap(db_model)
        retval = models.Process(**process_summary.dict())

        return retval
