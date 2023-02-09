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

import uuid

import fastapi
import starlette
import structlog

from . import config


# See https://github.com/snok/asgi-correlation-id/blob/5a7be6337f3b33b84a00d03baae3da999bb722d5/asgi_correlation_id/middleware.py  # noqa: E501
class LoggerInitializationMiddleware:
    def __init__(self, app: starlette.types.ASGIApp):
        self.app = app

    async def __call__(
        self,
        scope: starlette.types.Scope,
        receive: starlette.types.Receive,
        send: starlette.types.Send,
    ):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        # request = fastapi.Request(scope, receive=receive)
        # trace_id = request.headers.get("X-Trace-ID", None)

        async def send_with_trace_id(message):
            if message["type"] == "http.response.start":
                headers = starlette.datastructures.MutableHeaders(scope=message)
                headers.append("X-Trace-Id", trace_id)

            await send(message)

        structlog.contextvars.clear_contextvars()
        trace_id = str(uuid.uuid4())
        structlog.contextvars.bind_contextvars(trace_id=trace_id)
        await self.app(scope, receive, send_with_trace_id)


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
            response.headers["cache-control"] = settings.private_cache_control
            response.headers["vary"] = settings.private_vary
        return response
