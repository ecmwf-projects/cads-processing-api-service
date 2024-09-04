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

COST_THRESHOLDS = {"api": "max_costs", "ui": "max_costs_portal"}


def estimate_costs(
    process_id: str = fastapi.Path(...),
    request_origin: str | None = fastapi.Query("api"),
    execution_content: models.Execute = fastapi.Body(...),
) -> models.Costing:
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
        request.get("inputs", {}), adaptor_properties, request_origin
    )
    return costing_info


def compute_costing(
    request: dict[str, Any],
    adaptor_properties: dict[str, Any],
    request_origin: str,
) -> models.Costing:
    adaptor: cads_adaptors.AbstractAdaptor = adaptors.instantiate_adaptor(
        adaptor_properties=adaptor_properties
    )
    cost_threshold = COST_THRESHOLDS.get(request_origin)
    costs: dict[str, float] = adaptor.estimate_costs(
        request=request, cost_threshold=cost_threshold
    )
    costing_config: dict[str, Any] = adaptor_properties["config"].get("costing", {})
    max_costs: dict[str, Any] = costing_config.get(cost_threshold, {})
    max_costs_exceeded = {}
    for max_cost_id, max_cost_value in max_costs.items():
        max_cost_value = float(max_cost_value)
        if max_cost_id in costs.keys():
            if costs[max_cost_id] > max_cost_value:
                max_costs_exceeded[max_cost_id] = max_cost_value
    costing_info = models.Costing(
        costs=costs, max_costs=max_costs, max_costs_exceeded=max_costs_exceeded
    )
    return costing_info
