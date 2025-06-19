"""CADS Processing API specific models."""

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

import datetime
import enum
from typing import Any

import ogc_api_processes_fastapi.models
import pydantic


class AuthInfo(pydantic.BaseModel):
    user_uid: str
    user_role: str | None = None
    email: str | None = None
    request_origin: str
    auth_header: tuple[str, str]
    portals: tuple[str, ...] | None = None


class StatusCode(str, enum.Enum):
    accepted = "accepted"
    running = "running"
    successful = "successful"
    failed = "failed"
    rejected = "rejected"


class StatusInfoMetadata(pydantic.BaseModel):
    request: dict[str, Any] | None = None
    results: dict[str, Any] | None = None
    datasetMetadata: dict[str, Any] | None = None
    qos: dict[str, Any] | None = None
    log: list[tuple[str, str]] | None = None
    origin: str | None = None


class StatusInfo(ogc_api_processes_fastapi.models.StatusInfo):
    status: StatusCode
    metadata: StatusInfoMetadata | None = None


class JobListMetadata(pydantic.BaseModel):
    totalCount: int | None = None


class DatasetMessage(pydantic.BaseModel):
    date: datetime.datetime | None = None
    severity: str | None = None
    content: str | None = None


class DatasetMetadata(pydantic.BaseModel):
    title: str | None = None
    messages: list[DatasetMessage] | None = None


class JobList(ogc_api_processes_fastapi.models.JobList):
    jobs: list[StatusInfo]  # type: ignore
    metadata: JobListMetadata | None = None


class Exception(ogc_api_processes_fastapi.models.Exception):
    trace_id: str | None = None
    traceback: str | None = None
    messages: list[tuple[str, str]] | None = None


class CostingInfo(pydantic.BaseModel):
    costs: dict[str, float | str] = {}
    limits: dict[str, float] = {}
    cost_bar_steps: list[int] | None = None


class RequestCost(pydantic.BaseModel):
    id: str | None = None
    cost: float = 0.0
    limit: float = 1.0
    cost_bar_steps: list[int] | None = None
    request_is_valid: bool = True
    invalid_reason: str | None = None


class Execute(ogc_api_processes_fastapi.models.Execute):
    inputs: dict[str, Any] | None = None


class DeleteJobs(pydantic.BaseModel):
    """Request body for DELETE /jobs."""

    job_ids: list[str] = pydantic.Field(
        ..., description="Identifiers of the jobs to delete."
    )
