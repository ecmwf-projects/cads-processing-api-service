"""Middlewares."""

# Copyright 2023, European Union.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import fastapi
import starlette
import starlette_exporter

from . import config

CACHEABLE_HTTP_METHODS = ["GET", "HEAD"]


class CacheControlMiddleware(starlette.middleware.base.BaseHTTPMiddleware):
    """Set Cache-Control header for any GET and HEAD requests.

    If header is set already by route handler or other middleware, not set by it.
    """

    async def dispatch(
        self,
        request: fastapi.Request,
        call_next: starlette.middleware.base.RequestResponseEndpoint,
    ) -> fastapi.Response:
        response = await call_next(request)
        if (
            "cache-control" not in response.headers
            and request.method in CACHEABLE_HTTP_METHODS
        ):
            settings = config.ensure_settings()
            if settings.default_cache_control:
                response.headers["cache-control"] = settings.default_cache_control
            if settings.default_vary:
                response.headers["vary"] = settings.default_vary
        return response


class ProcessingPrometheusMiddleware(starlette_exporter.PrometheusMiddleware):
    @staticmethod
    def _get_router_path(scope: starlette.types.Scope) -> str | None:
        path = scope.get("path", "")
        if path.startswith("/jobs/") and path != "/jobs/":
            return "/jobs/job_id"
        else:
            return None
