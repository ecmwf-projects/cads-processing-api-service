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

import logging
import urllib.parse
from typing import Any, Type

import attrs
import cads_catalogue.database
import fastapi
import fastapi_utils.session
import ogc_api_processes_fastapi
import ogc_api_processes_fastapi.clients
import ogc_api_processes_fastapi.models
import sqlalchemy.orm
import sqlalchemy.orm.exc

from . import adaptors, config, errors

settings = config.SqlalchemySettings()

logger = logging.getLogger(__name__)


def lookup_id(
    id: str,
    record: Type[cads_catalogue.database.BaseModel],
    session: sqlalchemy.orm.Session,
) -> cads_catalogue.database.BaseModel:
    """Lookup row by id."""
    try:
        row = session.query(record).filter(record.resource_uid == id).one()
    except sqlalchemy.orm.exc.NoResultFound:
        raise errors.NotFoundError(f"{record.__name__} {id} not found")
    return row


def process_summary_serializer(
    db_model: cads_catalogue.database.Resource,
) -> ogc_api_processes_fastapi.models.ProcessSummary:

    retval = ogc_api_processes_fastapi.models.ProcessSummary(
        title=f"Retrieve of {db_model.title}",
        description=db_model.description,
        keywords=db_model.keywords,
        id=f"retrieve-{db_model.resource_uid}",
        version="1.0.0",
        jobControlOptions=[
            "async-execute",
        ],
        outputTransmission=[
            "reference",
        ],
    )

    return retval


# TODO: this is a mock implementation. Change it when database is ready.
def process_inputs_serializer() -> list[
    dict[str, ogc_api_processes_fastapi.models.InputDescription]
]:
    inputs = adaptors.translate_cds_into_ogc_inputs(
        urllib.parse.urljoin(__file__, "../tests/data/form.json")
    )
    return inputs


def process_description_serializer(
    db_model: cads_catalogue.database.Resource,
) -> ogc_api_processes_fastapi.models.ProcessDescription:

    process_summary = process_summary_serializer(db_model)
    retval = ogc_api_processes_fastapi.models.ProcessDescription(
        **process_summary.dict(),
        inputs=process_inputs_serializer(),
    )

    return retval


@attrs.define
class DatabaseClient(ogc_api_processes_fastapi.clients.BaseClient):
    """
    Database implementation of the OGC API - Processes endpoints.
    """

    reader: fastapi_utils.session.FastAPISessionMaker = attrs.field(
        default=fastapi_utils.session.FastAPISessionMaker(settings.connection_string),
        init=False,
    )
    process_table: Type[cads_catalogue.database.Resource] = attrs.field(
        default=cads_catalogue.database.Resource
    )

    def get_processes_list(
        self, limit: int | None = None, offset: int = 0
    ) -> list[ogc_api_processes_fastapi.models.ProcessSummary]:
        with self.reader.context_session() as session:
            if limit:
                processes = (
                    session.query(self.process_table).offset(offset).limit(limit).all()
                )
            else:
                processes = session.query(self.process_table).offset(offset).all()
            processes_list = [
                process_summary_serializer(process) for process in processes
            ]

        return processes_list

    def get_process_description(
        self, process_id: str
    ) -> ogc_api_processes_fastapi.models.ProcessDescription:
        with self.reader.context_session() as session:
            id = process_id[len("retrieve-") :]
            process = lookup_id(id=id, record=self.process_table, session=session)
            process_description = process_description_serializer(process)
            process_description.outputs = [
                {
                    "download_url": ogc_api_processes_fastapi.models.OutputDescription(
                        title="Download URL",
                        description="URL to download process result",
                        schema_=ogc_api_processes_fastapi.models.SchemaItem(  # type: ignore
                            type="string", format="url"
                        ),
                    )
                }
            ]

        return process_description

    def post_process_execute(
        self,
        process_id: str,
        execution_content: ogc_api_processes_fastapi.models.Execute,
    ) -> Any:
        retval = {
            "message": f"requested execution of process {process_id}",
            "request_content": execution_content,
            "process_description": self.get_process_description(process_id),
        }
        return retval


app = fastapi.FastAPI()
app = ogc_api_processes_fastapi.include_routers(app=app, client=DatabaseClient())
