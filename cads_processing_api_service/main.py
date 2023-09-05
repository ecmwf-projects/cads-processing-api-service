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

import logging
import uuid
from contextlib import asynccontextmanager
from typing import Any, Awaitable, Callable, Mapping, MutableMapping

import cads_common.logging
import fastapi
import fastapi.middleware.cors
import ogc_api_processes_fastapi
import structlog

from . import clients, constraints, exceptions, metrics, middlewares, translators


def add_user_request_flag(
    logger: logging.Logger, method_name: str, event_dict: MutableMapping[str, Any]
) -> Mapping[str, Any]:
    """Add user_request flag to log message."""
    if "user_id" in event_dict:
        event_dict["user_request"] = True
    return event_dict


@asynccontextmanager
async def lifespan(application: fastapi.FastAPI):
    cads_common.logging.structlog_configure([add_user_request_flag])
    cads_common.logging.logging_configure()
    yield


logger = structlog.get_logger(__name__)

app = ogc_api_processes_fastapi.instantiate_app(
    client=clients.DatabaseClient(),  # type: ignore
    exception_handler=exceptions.exception_handler,
)
app = exceptions.include_exception_handlers(app)
# FIXME : "app.router.lifespan_context" is not officially supported and would likely break
app.router.lifespan_context = lifespan
app.router.add_api_route(
    "/processes/{process_id}/constraints",
    constraints.apply_constraints,
    methods=["POST"],
)
app.router.add_api_route(
    "/processes/{process_id}/api-request",
    translators.get_api_request,
    methods=["POST"],
)

app.router.add_api_route("/metrics", metrics.handle_metrics)
app.add_middleware(middlewares.ProcessingPrometheusMiddleware, group_paths=True)


@app.middleware("http")
async def initialize_logger(
    request: fastapi.Request, call_next: Callable[[fastapi.Request], Awaitable[Any]]
) -> Any:
    structlog.contextvars.clear_contextvars()
    trace_id = str(uuid.uuid4())
    structlog.contextvars.bind_contextvars(trace_id=trace_id)
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
