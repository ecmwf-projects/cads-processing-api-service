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
from typing import Any

import ogc_api_processes_fastapi.models
import pydantic


class StatusInfoMetadata(pydantic.BaseModel):
    request: dict[str, Any] | None = None
    results: dict[str, Any] | None = None
    datasetMetadata: dict[str, Any] | None = None
    qos: dict[str, Any] | None = None
    log: list[tuple[datetime.datetime, str]] | None = None


class StatusInfo(ogc_api_processes_fastapi.models.StatusInfo):
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
