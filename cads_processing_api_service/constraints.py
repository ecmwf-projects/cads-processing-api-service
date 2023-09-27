from typing import Any

import cads_adaptors
import cads_adaptors.constraints
import cads_catalogue
import fastapi

from . import adaptors, db_utils, exceptions, utils


def apply_constraints(
    process_id: str = fastapi.Path(...),
    request: dict[str, Any] = fastapi.Body(...),
) -> dict[str, Any]:
    record = cads_catalogue.database.Resource
    catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker()
    with catalogue_sessionmaker() as catalogue_session:
        dataset = utils.lookup_resource_by_id(process_id, record, catalogue_session)
    adaptor: cads_adaptors.AbstractAdaptor = adaptors.instantiate_adaptor(dataset)
    try:
        constraints: dict[str, Any] = adaptor.apply_constraints(request=request)
    except cads_adaptors.constraints.ParameterError as exc:
        raise exceptions.InvalidParameter(detail=str(exc))

    return constraints
