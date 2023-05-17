"""CADS Processing API service metrics."""
import os

import cachetools

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
import cads_broker
import prometheus_client
import starlette.requests
import starlette.responses
import starlette_exporter
from sqlalchemy.orm import session

from . import db_utils

TOTAL_REQ_NUM_GAUGE = prometheus_client.Gauge(
    "total_request_number",
    "Total number of requests",
    labelnames=("status", "dataset_id"),
)

TOTAL_REQ_DURATION_GAUGE = prometheus_client.Gauge(
    "total_request_duration",
    "Total request duration (in microseconds)",
    labelnames=("status", "dataset_id"),
)


TOTAL_USERS_GAUGE = prometheus_client.Gauge(
    "total_users", "Total users", labelnames=("status", "dataset_id")
)


@cachetools.cached(  # type: ignore
    cache=cachetools.TTLCache(
        maxsize=1024, ttl=int(os.getenv("METRIC_QUERIES_CACHE_TIME", 300))
    ),
    info=True,
)
def handle_metrics(
    request: starlette.requests.Request,
) -> starlette.responses.Response:
    compute_sessionmaker = db_utils.get_compute_sessionmaker_sync()
    with compute_sessionmaker() as compute_session:
        set_request_metrics(compute_session)
        set_user_metrics(compute_session)
    return starlette_exporter.handle_metrics(request)


def set_request_metrics(compute_session: session):
    count_results = cads_broker.database.count_requests_per_dataset_status(
        compute_session
    )
    for dataset, status, count in count_results:
        TOTAL_REQ_NUM_GAUGE.labels(status=status, dataset_id=dataset).set(count)

    total_dur = cads_broker.database.total_request_time_per_dataset_status(
        compute_session
    )
    for dataset, status, duration in total_dur:
        TOTAL_REQ_DURATION_GAUGE.labels(status=status, dataset_id=dataset).set(
            duration.microseconds
        )


def set_user_metrics(compute_session: session):
    active_users = cads_broker.database.count_active_users(compute_session)
    for dataset, count in active_users:
        TOTAL_USERS_GAUGE.labels(status="active", dataset_id=dataset).set(count)
    queued_users = cads_broker.database.count_queued_users(compute_session)
    for dataset, count in queued_users:
        TOTAL_USERS_GAUGE.labels(status="queued", dataset_id=dataset).set(count)
    waiting_behind_themselves = (
        cads_broker.database.count_waiting_users_queued_behind_themselves(
            compute_session
        )
    )
    for dataset, count in waiting_behind_themselves:
        TOTAL_USERS_GAUGE.labels(
            status="queued_behind_themselves", dataset_id=dataset
        ).set(count)
    waiting_users_queued = cads_broker.database.count_waiting_users_queued(
        compute_session
    )
    for dataset, count in waiting_users_queued:
        TOTAL_USERS_GAUGE.labels(status="waiting_queued", dataset_id=dataset).set(count)
    running_users = cads_broker.database.count_running_users(compute_session)
    for dataset, count in running_users:
        TOTAL_USERS_GAUGE.labels(status="running_users", dataset_id=dataset).set(count)
