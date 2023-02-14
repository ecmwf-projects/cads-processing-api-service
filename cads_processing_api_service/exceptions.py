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
import requests


@attrs.define
class PermissionDenied(ogc_api_processes_fastapi.exceptions.OGCAPIException):
    type: str = "permission denied"
    status_code: int = fastapi.status.HTTP_403_FORBIDDEN
    title: str = "operation not permitted"


class ParameterError(KeyError):
    pass


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
    app.add_exception_handler(
        PermissionDenied, ogc_api_processes_fastapi.exceptions.ogc_api_exception_handler
    )
    app.add_exception_handler(
        requests.exceptions.ReadTimeout, request_readtimeout_handler
    )
    app.add_exception_handler(ParameterError, parameter_error_handler)
    return app
