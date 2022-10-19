# type: ignore

import cads_broker
import prometheus_client
import starlette.requests

GAUGE = prometheus_client.Gauge(
    "broker_queue", "Number of accepted requests", labelnames=("queue",)
)


def add_metrics_middleware(app):
    @app.middleware("http")
    async def add_accepted_requests(request: starlette.requests.Request, call_next):
        if request.scope["path"] == "/metrics":
            GAUGE.labels("queue").set(cads_broker.database.count_accepted_requests())
        response = await call_next(request)
        return response
