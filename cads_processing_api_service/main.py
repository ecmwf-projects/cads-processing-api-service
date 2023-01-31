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

import uuid
from typing import Any, Awaitable, Callable

import fastapi
import fastapi.middleware.cors
import ogc_api_processes_fastapi
import starlette_exporter
import structlog

from . import clients, config, constraints, exceptions, metrics

config.configure_logger()
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


@app.middleware("http")
async def initialize_logger(
    request: fastapi.Request, call_next: Callable[[fastapi.Request], Awaitable[Any]]
) -> Any:
    structlog.contextvars.clear_contextvars()
    request_id = request.headers.get("X-Request-ID", None)
    if not request_id:
        request_id = str(uuid.uuid4())
    structlog.contextvars.bind_contextvars(request_id=request_id)
    response = await call_next(request)
    return response


# FIXME: temporary workaround
app.add_middleware(
    fastapi.middleware.cors.CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
