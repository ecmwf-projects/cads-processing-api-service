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

import cads_broker
import fastapi
import prometheus_client
import sqlalchemy
import starlette.requests
import starlette.responses
import starlette_exporter

from . import crud

GAUGE = prometheus_client.Gauge(
    "broker_queue", "Number of accepted requests", labelnames=("queue",)
)


def handle_metrics(
    request: starlette.requests.Request,
    compute_session: sqlalchemy.orm.Session = fastapi.Depends(crud.get_compute_session),
) -> starlette.responses.Response:
    GAUGE.labels("queue").set(
        cads_broker.database.count_accepted_requests_in_session(compute_session)
    )
    return starlette_exporter.handle_metrics(request)
