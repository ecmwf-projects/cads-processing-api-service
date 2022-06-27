import abc
from typing import Any

import attrs
from cads_catalogue import database
from ogc_api_processes_fastapi import models


@attrs.define
class Serializer(abc.ABC):
    """Defines serialization methods between the API and the data model."""

    @classmethod
    @abc.abstractmethod
    def db_to_oap(cls, db_model: database.BaseModel) -> dict[str, Any]:
        """Transform database model to OGC API - Processes schema."""
        ...

    @classmethod
    @abc.abstractmethod
    def oap_to_db(cls, stac_data: dict[str, Any]) -> database.BaseModel:
        """Transform OGC API - Processes schema to database model."""
        ...

    @classmethod
    def row_to_dict(cls, db_model: database.BaseModel) -> dict[str, Any]:
        """Transform a database model to it's dictionary representation."""
        d = {}
        for column in db_model.__table__.columns:
            value = getattr(db_model, column.name)
            if value:
                d[column.name] = value
        return d


class ProcessSerializer(Serializer):
    """Serialization methods for OGC API - Processes processes."""

    @classmethod
    def db_to_oap(cls, db_model: database.Resource) -> models.ProcessSummary:

        return models.ProcessSummary(
            id="-".join(["retrieve", db_model.resource_id]),
            version="1.0.0",
        )

    @classmethod
    def oap_to_db(cls, opa_data: dict[str, Any]) -> database.Resource:

        return database.Resource(**dict(opa_data))
