from cads_catalogue import database
from ogc_api_processes_fastapi import models


class ProcessSerializer:
    """Serialization methods for OGC API - Processes processes."""

    @classmethod
    def db_to_oap(cls, db_model: database.Resource) -> models.ProcessSummary:

        return models.ProcessSummary(
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
