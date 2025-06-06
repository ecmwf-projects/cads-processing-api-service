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

import enum
from typing import Any

import cads_adaptors
import cads_adaptors.exceptions

from . import adaptors, exceptions, models

COST_THRESHOLDS = {"api": "max_costs", "ui": "max_costs_portal"}


class RequestOrigin(str, enum.Enum):
    api = "api"
    ui = "ui"


def check_request_validity(
    request: dict[str, Any],
    request_origin: RequestOrigin,
    mandatory_inputs: bool,
    adaptor_properties: dict[str, Any],
) -> None:
    """
    Check if the request is valid.

    Parameters
    ----------
    request : dict[str, Any]
        Request to be processed.
    request_origin : RequestOrigin
        Origin of the request.
    mandatory_inputs : bool
        Whether mandatory inputs have been provided.
    adaptor_properties : dict[str, Any]
        Adaptor properties.

    Returns
    -------
    None

    Raises
    ------
    exceptions.InvalidRequest
        If the request is invalid.
    """
    if not mandatory_inputs and request_origin == RequestOrigin.ui:
        raise exceptions.InvalidRequest("missing mandatory inputs")
    try:
        adaptor = adaptors.instantiate_adaptor(adaptor_properties=adaptor_properties)
        _ = adaptor.check_validity(request.get("inputs", {}))
        return
    except cads_adaptors.exceptions.InvalidRequest as exc:
        raise exceptions.InvalidRequest(str(exc))


def compute_highest_cost_limit_ratio(
    costing_info: models.CostingInfo,
) -> models.RequestCost:
    """
    Compute the highest cost/limit ratio of the request.

    Parameters
    ----------
    costing_info : models.CostingInfo
        Costs of the request.

    Returns
    -------
    models.RequestCost
        Info on the cost with the highest cost/limit ratio.
    """
    costs = costing_info.costs
    costs_numeric = {
        cost_id: cost
        for cost_id, cost in costs.items()
        if isinstance(cost, (int, float))
    }
    limits = costing_info.limits
    highest_cost_limit_ratio = 0.0
    highest_cost = models.RequestCost()
    for limit_id, limit in limits.items():
        cost = costs_numeric.get(limit_id, 0.0)
        cost_limit_ratio = cost / limit if limit > 0 else 1.1
        if cost_limit_ratio > highest_cost_limit_ratio:
            highest_cost_limit_ratio = cost_limit_ratio
            highest_cost = models.RequestCost(cost=cost, limit=limit, id=limit_id)
    return highest_cost


def compute_costing(
    request: dict[str, Any],
    adaptor_properties: dict[str, Any],
    request_origin: str,
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
    if request_origin not in COST_THRESHOLDS:
        raise ValueError(f"Invalid request origin: {request_origin}")
    cost_threshold = COST_THRESHOLDS[request_origin]
    costs: dict[str, Any] = adaptor.estimate_costs(
        request=request, cost_threshold=cost_threshold
    )
    costing_config: dict[str, Any] = adaptor_properties["config"].get("costing", {})
    limits: dict[str, Any] = costing_config.get("max_costs", {})
    cost_bar_steps = (
        costing_config.get("cost_bar_steps", None) if request_origin == "ui" else None
    )
    costing_info = models.CostingInfo(
        costs=costs, limits=limits, cost_bar_steps=cost_bar_steps
    )
    return costing_info
