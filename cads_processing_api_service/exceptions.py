"""Exceptions definitions."""

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
import ogc_api_processes_fastapi.models


class OGCProcessesApiError(Exception):
    """Generic API error."""

    pass


class DatabaseError(OGCProcessesApiError):
    """Generic database errors."""

    pass


class NotFoundError(OGCProcessesApiError):
    """Resource not found."""

    pass


@attrs.define
class NotValidJobId(Exception):
    detail: str | None = None


def not_valid_job_id_exception_handler(
    request: fastapi.Request, exc: Exception
) -> fastapi.responses.JSONResponse:
    return fastapi.responses.JSONResponse(
        status_code=fastapi.status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=ogc_api_processes_fastapi.models.Exception(
            type="not-valid-job-id",
            title="not valid job id",
            detail=exc.detail,
            instance=str(request.url),
        ).dict(exclude_unset=True),
    )
