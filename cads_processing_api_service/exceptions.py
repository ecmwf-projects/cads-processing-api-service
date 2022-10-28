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


@attrs.define
class AuthenticationError(Exception):

    type: str = "authentication error"
    status_code: int = fastapi.status.HTTP_403_FORBIDDEN
    title: str = "authentication error"
    detail: str = "authentication failed"


def authentication_error_exception_handler(
    request: fastapi.Request, exc: AuthenticationError
) -> fastapi.responses.JSONResponse:
    return fastapi.responses.JSONResponse(
        status_code=exc.status_code,
        content={
            "type": exc.type,
            "title": exc.title,
            "detail": exc.detail,
            "instance": str(request.url),
        },
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
        AuthenticationError, authentication_error_exception_handler
    )
    return app
