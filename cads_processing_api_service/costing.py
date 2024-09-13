"""Requests' cost estimation endpoint."""

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

from typing import Any

import cads_adaptors
import cads_catalogue
import fastapi

from . import adaptors, costing, db_utils, models, utils


def estimate_costs(
    process_id: str = fastapi.Path(...),
    execution_content: models.Execute = fastapi.Body(...),
) -> models.CostingInfo:
    request = execution_content.model_dump()
    table = cads_catalogue.database.Resource
    catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker(
        db_utils.ConnectionMode.read
    )
    with catalogue_sessionmaker() as catalogue_session:
        dataset = utils.lookup_resource_by_id(
            resource_id=process_id, table=table, session=catalogue_session
        )
    adaptor_properties = adaptors.get_adaptor_properties(dataset)
    costing_info = costing.compute_costing(
        request.get("inputs", {}), adaptor_properties
    )
    return costing_info


def compute_costing(
    request: dict[str, Any],
    adaptor_properties: dict[str, Any],
) -> models.CostingInfo:
    """
    Compute the costs of the request.

    Parameters
    ----------
    request : dict[str, Any]
        Request to be processed.
    adaptor_properties : dict[str, Any]
        Adaptor properties.

    Returns
    -------
    models.CostingInfo
        Costs of the request.
    """
    adaptor: cads_adaptors.AbstractAdaptor = adaptors.instantiate_adaptor(
        adaptor_properties=adaptor_properties
    )
    costs: dict[str, float] = adaptor.estimate_costs(request=request)
    costing_config: dict[str, Any] = adaptor_properties["config"].get("costing", {})
    limits: dict[str, Any] = costing_config.get("max_costs", {})
    limits_exceeded = {}
    for limit_id, limit in limits.items():
        limit = float(limit)
        if limit_id in costs.keys():
            if costs[limit_id] > limit:
                limits_exceeded[limit_id] = limit
    costing_info = models.CostingInfo(
        costs=costs, limits=limits, max_costs_exceeded=limits_exceeded
    )
    return costing_info
