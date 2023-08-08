from typing import Any

import cads_catalogue
import fastapi

# import cads_adaptors
from cads_adaptors.constraints import ParameterError, validate_constraints

from . import adaptors, db_utils, exceptions, utils


def apply_constraints(
    process_id: str = fastapi.Path(...),
    request: dict[str, Any] = fastapi.Body(...),
) -> dict[str, list[str]]:
    record = cads_catalogue.database.Resource
    catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker()
    with catalogue_sessionmaker() as catalogue_session:
        dataset = utils.lookup_resource_by_id(process_id, record, catalogue_session)

    adaptor_properties = adaptors.get_adaptor_properties(dataset)
    # Why are we catching this error here and not letting it fail where it fails?
    try:
        constraints = validate_constraints(
            adaptor_properties.get("form", []),
            request,
            adaptor_properties.get("config", {}).get("constraints", []),
        )
    except ParameterError as exc:
        raise exceptions.InvalidParameter(detail=str(exc))

    return constraints
