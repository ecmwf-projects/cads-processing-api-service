# Copyright 2022, European Union.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# mypy: ignore-errors

from cads_processing_api_service import costing, models


def test_compute_highest_cost_limit_ratio() -> None:
    costing_info = models.CostingInfo(
        costs={
            "cost_id_1": 10.0,
            "cost_id_2": 10.0,
        },
        limits={
            "cost_id_1": 20.0,
            "cost_id_2": 20.0,
        },
    )
    cost = costing.compute_highest_cost_limit_ratio(costing_info)
    exp_cost = models.RequestCost(id="cost_id_1", cost=10.0, limit=20.0)
    assert cost == exp_cost

    costing_info = models.CostingInfo(
        costs={
            "cost_id_1": 10.0,
            "cost_id_2": 30.0,
        },
        limits={
            "cost_id_1": 20.0,
            "cost_id_2": 20.0,
        },
    )
    cost = costing.compute_highest_cost_limit_ratio(costing_info)
    exp_cost = models.RequestCost(id="cost_id_2", cost=30.0, limit=20.0)
    assert cost == exp_cost

    costing_info = models.CostingInfo(
        costs={
            "cost_id_1": 10.0,
            "cost_id_2": 10.0,
        },
        limits={
            "cost_id_1": 20.0,
            "cost_id_2": 0.0,
        },
    )
    cost = costing.compute_highest_cost_limit_ratio(costing_info)
    exp_cost = models.RequestCost(id="cost_id_2", cost=10.0, limit=0.0)
    assert cost == exp_cost
