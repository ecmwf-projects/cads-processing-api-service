from cads_broker import database
from prometheus_client import Gauge
from starlette.requests import Request

GAUGE = Gauge("broker_queue", "Number of accepted requests", labelnames=("queue",))


def add_metrics_middleware(app):
    @app.middleware("http")
    async def add_accepted_requests(request: Request, call_next):
        if request.scope["path"] == "/metrics":
            GAUGE.labels("queue").set(database.count_accepted_requests())
        response = await call_next(request)
        return response
