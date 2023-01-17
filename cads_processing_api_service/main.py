"""CADS Processing API service instantiation."""

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

import ogc_api_processes_fastapi
import starlette_exporter.middleware

from . import clients, config, constraints, exceptions, metrics

config.configure_logger()
app = ogc_api_processes_fastapi.instantiate_app(
    clients.DatabaseClient()  # type: ignore
)
app = ogc_api_processes_fastapi.include_exception_handlers(app=app)
app = exceptions.include_exception_handlers(app=app)
app.router.add_api_route(
    "/processes/{process_id}/constraints",
    constraints.validate_constraints,
    methods=["POST"],
)
app.add_route("/metrics", starlette_exporter.handle_metrics)
app.add_middleware(starlette_exporter.middleware.PrometheusMiddleware)
metrics.add_metrics_middleware(app)  # type: ignore
