"""CADS Processing API service metrics."""

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

from typing import Callable

import cads_broker
import fastapi
import prometheus_client
import starlette.requests
import starlette.responses
import starlette_exporter

from . import clients

GAUGE = prometheus_client.Gauge(
    "broker_queue", "Number of accepted requests", labelnames=("queue",)
)


def create_handle_metrics_endpoint(
    client: clients.DatabaseClient,  # type:ignore
) -> Callable[[fastapi.Request], starlette.responses.Response]:
    def handle_metrics_endpoint(
        request: starlette.requests.Request,
    ) -> starlette.responses.Response:
        compute_db_session_maker = client.compute_db_session_maker
        with compute_db_session_maker() as compute_session:
            GAUGE.labels("queue").set(
                cads_broker.database.count_accepted_requests_in_session(compute_session)
            )
        return starlette_exporter.handle_metrics(request)

    return handle_metrics_endpoint
