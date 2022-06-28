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

from typing import Type

import attrs
import fastapi
from cads_catalogue import database
from ogc_api_processes_fastapi import clients, main, models

from . import config, dbsession, serializers

settings = config.SqlalchemySettings()


@attrs.define
class DatabaseClient(clients.BaseClient):
    """
    Database implementation of the OGC API - Processes endpoints.
    """

    session: dbsession.Session = attrs.field(
        default=dbsession.Session.create_from_settings(settings)
    )
    process_table: Type[database.Resource] = attrs.field(default=database.Resource)
    process_serializer: Type[serializers.ProcessSerializer] = attrs.field(
        default=serializers.ProcessSerializer
    )

    def get_processes_list(
        self, limit: int, offset: int
    ) -> list[models.ProcessSummary]:
        with self.session.reader.context_session() as session:
            processes = (
                session.query(self.process_table).offset(offset).limit(limit).all()
            )
            processes_list = [
                self.process_serializer.db_to_oap(process) for process in processes
            ]

        return processes_list


app = fastapi.FastAPI()
app = main.include_ogc_api_processes_routers(app=app, client=DatabaseClient())
