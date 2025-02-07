from typing import Any

import cads_adaptors
import cads_adaptors.constraints
import cads_adaptors.exceptions
import cads_catalogue
import fastapi

from . import adaptors, db_utils, exceptions, models, utils


@exceptions.exception_logger
def apply_constraints(
    process_id: str = fastapi.Path(...),
    execution_content: models.Execute = fastapi.Body(...),
    portals: list[str] | None = fastapi.Depends(utils.get_portals),
) -> dict[str, Any]:
    request = execution_content.model_dump()
    table = cads_catalogue.database.Resource
    catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker(
        db_utils.ConnectionMode.read
    )
    with catalogue_sessionmaker() as catalogue_session:
        dataset = utils.lookup_resource_by_id(
            resource_id=process_id,
            table=table,
            session=catalogue_session,
            portals=portals,
        )
    adaptor: cads_adaptors.AbstractAdaptor = adaptors.instantiate_adaptor(dataset)
    try:
        constraints: dict[str, Any] = adaptor.apply_constraints(
            request.get("inputs", {})
        )
    except (
        cads_adaptors.exceptions.ParameterError,
        cads_adaptors.exceptions.InvalidRequest,
    ) as exc:
        raise exceptions.InvalidParameter(detail=str(exc)) from exc

    return constraints
