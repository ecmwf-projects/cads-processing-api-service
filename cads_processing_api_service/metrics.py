"""CADS Processing API service metrics."""

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

import prometheus_client
import starlette.requests
import starlette.responses
import starlette_exporter
import structlog

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


DOWNLOAD_BYTES = prometheus_client.Summary(
    "download_bytes",
    "Download bytes requests",
    labelnames=("dataset_id",),
)


def handle_metrics(
    request: starlette.requests.Request,
) -> starlette.responses.Response:
    """Control for the metrics endpoint."""
    return starlette_exporter.handle_metrics(request)


def handle_download_metrics(job: dict[str, Any], results: dict[str, Any]) -> None:
    """Update the download metrics when a user downloads a dataset."""
    try:
        dataset_id = job["process_id"]
        result_size = int(results["asset"]["value"]["file:size"])
        DOWNLOAD_BYTES.labels(dataset_id).observe(result_size)
    except Exception as e:
        logger.error("Error updating download metrics", error=e)
    return
