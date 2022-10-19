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

import cads_catalogue.config
import fastapi
import fastapi_utils.session
import ogc_api_processes_fastapi
import starlette_exporter

from . import clients, metrics

app = fastapi.FastAPI()
app.add_middleware(starlette_exporter.PrometheusMiddleware)
metrics.add_metrics_middleware(app)
connection_string = cads_catalogue.config.ensure_settings().connection_string
sql_session_reader = fastapi_utils.session.FastAPISessionMaker(connection_string)
api = ogc_api_processes_fastapi.OGCProcessesAPI(
    app=app,
    client=clients.DatabaseClient(reader=sql_session_reader),
)
app = api.app
app = ogc_api_processes_fastapi.include_exception_handlers(app=app)
app.add_route("/metrics", starlette_exporter.handle_metrics)
