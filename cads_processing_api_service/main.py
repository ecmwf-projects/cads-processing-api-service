"""CADS Processing API service instantiation."""

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

import fastapi
import fastapi.middleware.cors
import ogc_api_processes_fastapi
import starlette_exporter
import structlog

from . import clients, constraints, exceptions, metrics, tracing

tracing.configure_logger()
logger = structlog.get_logger(__name__)

app = ogc_api_processes_fastapi.instantiate_app(
    clients.DatabaseClient()  # type: ignore
)

app = ogc_api_processes_fastapi.include_exception_handlers(app=app)
app = exceptions.include_exception_handlers(app=app)

app.router.add_api_route(
    "/processes/{process_id}/constraints",
    constraints.validate_constraints,
    methods=["POST"],
)

app.router.add_api_route("/metrics", metrics.handle_metrics)
app.add_middleware(starlette_exporter.middleware.PrometheusMiddleware)

app.add_middleware(tracing.LoggerInitializationMiddleware)

# FIXME: temporary workaround
app.add_middleware(
    fastapi.middleware.cors.CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
