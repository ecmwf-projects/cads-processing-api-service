"""CADS Processing API specific exceptions."""

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

import attrs
import fastapi
import ogc_api_processes_fastapi.exceptions
import ogc_api_processes_fastapi.models
import requests
import structlog


@attrs.define
class PermissionDenied(ogc_api_processes_fastapi.exceptions.OGCAPIException):
    type: str = "permission denied"
    status_code: int = fastapi.status.HTTP_403_FORBIDDEN
    title: str = "operation not permitted"


@attrs.define
class ParameterError(ogc_api_processes_fastapi.exceptions.OGCAPIException):
    type: str = "parameter error"
    status_code: int = fastapi.status.HTTP_422_UNPROCESSABLE_ENTITY
    title: str = "invalid parameters"


def request_readtimeout_handler(
    request: fastapi.Request, exc: requests.exceptions.ReadTimeout
) -> fastapi.responses.JSONResponse:
    """Catch ReadTimeout exceptions to properly trigger an HTTP 504."""
    out = fastapi.responses.JSONResponse(
        status_code=504, content={"message": str(exc), "title": "ReadTimeout error"}
    )
    return out


def parameter_error_handler(
    request: fastapi.Request, exc: ParameterError
) -> fastapi.Response:
    return fastapi.responses.JSONResponse(
        status_code=422, content={"message": str(exc), "title": "invalid parameters"}
    )


def exception_handler(
    request: fastapi.Request, exc: ogc_api_processes_fastapi.exceptions.OGCAPIException
) -> fastapi.responses.JSONResponse:
    return fastapi.responses.JSONResponse(
        status_code=exc.status_code,
        content=ogc_api_processes_fastapi.models.Exception(
            type=exc.type,
            title=exc.title,
            status=exc.status_code,
            detail=exc.detail,
            instance=str(request.url),
            trace_id=structlog.contextvars.get_contextvars().get("trace_id", None),
        ).dict(exclude_none=True),
    )


def include_exception_handlers(app: fastapi.FastAPI) -> fastapi.FastAPI:
    """Add CADS Processes API exceptions handlers to a FastAPI application.

    Parameters
    ----------
    app : fastapi.FastAPI
        FastAPI application to which CADS Processes API exceptions handlers
        should be added.

    Returns
    -------
    fastapi.FastAPI
        FastAPI application including CADS Processes API exceptions handlers.
    """
    app.add_exception_handler(PermissionDenied, exception_handler)
    app.add_exception_handler(requests.exceptions.ReadTimeout, exception_handler)
    app.add_exception_handler(ParameterError, exception_handler)
    return app
