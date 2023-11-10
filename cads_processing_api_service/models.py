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

from typing import Any

import ogc_api_processes_fastapi.models


class StatusInfo(ogc_api_processes_fastapi.models.StatusInfo):
    request: dict[str, Any] | None = None
    results: dict[str, Any] | None = None
    processDescription: dict[str, Any] | None = None
    statistics: dict[str, Any] | None = None
    log: list[str] | None = None


class JobList(ogc_api_processes_fastapi.models.JobList):
    jobs: list[StatusInfo]  # type: ignore


class Exception(ogc_api_processes_fastapi.models.Exception):
    trace_id: str | None = None
    traceback: str | None = None
