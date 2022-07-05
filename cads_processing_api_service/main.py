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
from typing import Any, Type

import attrs
import cads_catalogue.database
import fastapi
import fastapi_utils.session
import sqlalchemy.orm
import sqlalchemy.orm.exc
from cads_catalogue import database
from ogc_api_processes_fastapi import clients, main, models

from . import adaptors, config, errors

settings = config.SqlalchemySettings()

logger = logging.getLogger(__name__)


def lookup_id(
    id: str,
    record: Type[cads_catalogue.database.BaseModel],
    session: sqlalchemy.orm.Session,
) -> database.BaseModel:
    """Lookup row by id."""
    try:
        row = session.query(record).filter(record.resource_uid == id).one()
    except sqlalchemy.orm.exc.NoResultFound:
        raise errors.NotFoundError(f"{record.__name__} {id} not found")
    return row


def process_summary_serializer(
    db_model: cads_catalogue.database.Resource,
) -> models.ProcessSummary:

    retval = models.ProcessSummary(
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


def process_description_serializer(db_model: database.Resource) -> models.Process:

    process_summary = process_summary_serializer(db_model)
    retval = models.Process(**process_summary.dict())

    return retval


@attrs.define
class DatabaseClient(clients.BaseClient):
    """
    Database implementation of the OGC API - Processes endpoints.
    """

    reader: fastapi_utils.session.FastAPISessionMaker = attrs.field(
        default=fastapi_utils.session.FastAPISessionMaker(settings.connection_string),
        init=False,
    )
    process_table: Type[database.Resource] = attrs.field(default=database.Resource)

    def get_processes_list(
        self, limit: int | None = None, offset: int = 0
    ) -> list[models.ProcessSummary]:
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

    def get_process_description(self, process_id: str) -> models.Process:
        with self.reader.context_session() as session:
            id = process_id[len("retrieve-") :]
            process = lookup_id(id=id, record=self.process_table, session=session)
            process_description = process_description_serializer(process)

        return process_description

    def post_process_execution(
        self, process_id: str, execution_content: models.Execute
    ) -> Any:
        inputs_schema = adaptors.translate_cds_into_ogc_inputs("./tests/form.json")
        return inputs_schema


app = fastapi.FastAPI()
app = main.include_ogc_api_processes_routers(app=app, client=DatabaseClient())
