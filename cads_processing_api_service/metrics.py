# type: ignore

import cads_broker
import fastapi
import prometheus_client
import sqlalchemy
import starlette
import starlette_exporter

from . import dependencies

GAUGE = prometheus_client.Gauge(
    "broker_queue", "Number of accepted requests", labelnames=("queue",)
)


def handle_metrics(
    request: starlette.requests.Request,
    compute_session: sqlalchemy.orm.Session = fastapi.Depends(
        dependencies.get_compute_session
    ),
) -> starlette.responses.Response:
    GAUGE.labels("queue").set(
        cads_broker.database.count_accepted_requests(compute_session)
    )
    return starlette_exporter.handle_metrics(request)
