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

import traceback

import attrs
import fastapi
import ogc_api_processes_fastapi.exceptions
import requests
import structlog

from . import models

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


@attrs.define
class PermissionDenied(ogc_api_processes_fastapi.exceptions.OGCAPIException):
    type: str = "permission denied"
    status_code: int = fastapi.status.HTTP_403_FORBIDDEN
    title: str = "permission denied"


@attrs.define
class InvalidParameter(ogc_api_processes_fastapi.exceptions.OGCAPIException):
    type: str = "invalid parameter"
    status_code: int = fastapi.status.HTTP_422_UNPROCESSABLE_ENTITY
    title: str = "invalid parameter"


@attrs.define
class JobResultsExpired(ogc_api_processes_fastapi.exceptions.OGCAPIException):
    type: str = "results expired"
    status_code: int = fastapi.status.HTTP_404_NOT_FOUND
    title: str = "results expired"


@attrs.define
class InvalidRequest(ogc_api_processes_fastapi.exceptions.OGCAPIException):
    type: str = "invalid request"
    status_code: int = fastapi.status.HTTP_400_BAD_REQUEST
    title: str = "invalid request"


def format_exception_content(
    exc: ogc_api_processes_fastapi.exceptions.OGCAPIException,
    request: fastapi.Request | None = None,
) -> dict[str, str | int | None]:
    """Format an OGC API exception as a JSON serializable pytthon dictionary.

    Parameters
    ----------
    exc : ogc_api_processes_fastapi.exceptions.OGCAPIException
        Exception to be formatted.
    request: fastapi.Request
        HTTP request rasing the exception.

    Returns
    -------
    dict[str, str | int | None]
        Formatted exception.
    """
    instance = str(request.url) if request else None
    exception_content = models.Exception(
        type=exc.type,
        title=exc.title,
        status=exc.status_code,
        detail=exc.detail,
        instance=instance,
        trace_id=structlog.contextvars.get_contextvars().get("trace_id", "unset"),
    ).model_dump(exclude_none=True)
    if exc is ogc_api_processes_fastapi.exceptions.JobResultsFailed:
        exception_content["traceback"] = exc.traceback

    return exception_content


def exception_handler(
    request: fastapi.Request, exc: ogc_api_processes_fastapi.exceptions.OGCAPIException
) -> fastapi.responses.JSONResponse:
    """Handle all exceptions defined as OGC API Exceptions.

    Parameters
    ----------
    request : fastapi.Request
        HTTP request object.
    exc : ogc_api_processes_fastapi.exceptions.OGCAPIException
        Exception to be handled.

    Returns
    -------
    fastapi.responses.JSONResponse
        JSON response.
    """
    logger.error(
        exc.title,
        exception="".join(traceback.TracebackException.from_exception(exc).format()),
        url=str(request.url),
    )
    out = fastapi.responses.JSONResponse(
        status_code=exc.status_code,
        content=format_exception_content(exc=exc, request=request),
    )
    return out


def request_readtimeout_handler(
    request: fastapi.Request, exc: requests.exceptions.ReadTimeout
) -> fastapi.responses.JSONResponse:
    """Handle ReadTimeout exceptions to properly trigger a 504 HTTP response.

    Parameters
    ----------
    request : fastapi.Request
        HTTP request object.
    exc : requests.exceptions.ReadTimeout
        Exception to be handled.

    Returns
    -------
    fastapi.responses.JSONResponse
        JSON response.
    """
    out = fastapi.responses.JSONResponse(
        status_code=fastapi.status.HTTP_504_GATEWAY_TIMEOUT,
        content=models.Exception(
            type="read timeout error",
            title="read timeout error",
            trace_id=structlog.contextvars.get_contextvars().get("trace_id", "unset"),
        ).model_dump(exclude_none=True),
    )
    return out


def general_exception_handler(
    request: fastapi.Request, exc: Exception
) -> fastapi.responses.JSONResponse:
    """Handle all uncaught exceptions to trigger a 500 HTTP response.

    The function also add an ERROR message to the log with information on the raisd exception.

    Parameters
    ----------
    request : fastapi.Request
        HTTP request object.
    exc : Exception
        Exception to be handled.

    Returns
    -------
    fastapi.responses.JSONResponse
    """
    logger.error(
        "internal server error",
        exception="".join(traceback.TracebackException.from_exception(exc).format()),
    )
    out = fastapi.responses.JSONResponse(
        status_code=fastapi.status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=models.Exception(
            type="internal server error",
            title="internal server error",
            trace_id=structlog.contextvars.get_contextvars().get("trace_id", "unset"),
        ).model_dump(exclude_none=True),
    )
    return out


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
    app.add_exception_handler(PermissionDenied, exception_handler)  # type: ignore
    app.add_exception_handler(InvalidParameter, exception_handler)  # type: ignore
    app.add_exception_handler(InvalidRequest, exception_handler)  # type: ignore
    app.add_exception_handler(JobResultsExpired, exception_handler)  # type: ignore
    app.add_exception_handler(
        requests.exceptions.ReadTimeout, request_readtimeout_handler
    )  # type: ignore
    app.add_exception_handler(Exception, general_exception_handler)
    return app
