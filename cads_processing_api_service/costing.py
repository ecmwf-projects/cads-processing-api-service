from typing import Any

import cads_adaptors
import cads_adaptors.constraints
import cads_catalogue
import fastapi
import ogc_api_processes_fastapi.models

from . import adaptors, db_utils, exceptions, utils


def estimate_costing(
    process_id: str = fastapi.Path(...),
    request: ogc_api_processes_fastapi.models.Execute = fastapi.Body(...),
) -> dict[str, Any]:
    table = cads_catalogue.database.Resource
    catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker(
        db_utils.ConnectionMode.read
    )
    with catalogue_sessionmaker() as catalogue_session:
        dataset = utils.lookup_resource_by_id(
            resource_id=process_id, table=table, session=catalogue_session
        )
    adaptor: cads_adaptors.AbstractAdaptor = adaptors.instantiate_adaptor(dataset)
    try:
        costing: dict[str, Any] = adaptor.estimate_costing(request=request.model_dump())
    except cads_adaptors.constraints.ParameterError as exc:
        raise exceptions.InvalidParameter(detail=str(exc))

    return costing
