from typing import Type

import attrs
import fastapi
from cads_catalogue import database
from ogc_api_processes_fastapi import clients, main, models

from . import config, dbsession, serializers

settings = config.SqlalchemySettings()


@attrs.define
class DatabaseClient(clients.BaseClient):  # type: ignore
    """
    Database implementation of the OGC API - Processes endpoints.
    """

    session: dbsession.Session = attrs.field(
        default=dbsession.Session.create_from_settings(settings)
    )
    process_table: database.Resource = attrs.field(default=database.Resource)
    process_serializer: Type[serializers.Serializer] = attrs.field(
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
