from typing import Any

import cads_adaptors
import cads_adaptors.constraints
import cads_catalogue
import fastapi
import ogc_api_processes_fastapi.models

from . import adaptors, db_utils, models, utils


def estimate_costing(
    process_id: str = fastapi.Path(...),
    request: ogc_api_processes_fastapi.models.Execute = fastapi.Body(...),
) -> models.Costing:
    table = cads_catalogue.database.Resource
    catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker(
        db_utils.ConnectionMode.read
    )
    with catalogue_sessionmaker() as catalogue_session:
        dataset = utils.lookup_resource_by_id(
            resource_id=process_id, table=table, session=catalogue_session
        )
        adaptor_configuration: dict[
            str, Any
        ] = dataset.resource_data.adaptor_configuration
    costing_config: dict[str, Any] = adaptor_configuration.get("costing", {})
    max_costs: dict[str, Any] = costing_config.get("max_costs", {})
    adaptor: cads_adaptors.AbstractAdaptor = adaptors.instantiate_adaptor(dataset)
    costs: dict[str, float] = adaptor.estimate_costing(request=request.model_dump())
    max_costs_exceeded = {}
    for max_cost_id, max_cost_value in max_costs.items():
        if max_cost_id in costs.keys():
            if costs[max_cost_id] > max_cost_value:
                max_costs_exceeded[max_cost_id] = max_cost_value
    costing = models.Costing(costs=costs, max_costs_exceeded=max_costs_exceeded)

    return costing
