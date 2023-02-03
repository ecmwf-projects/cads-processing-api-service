"""Logging related functions."""

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

import json
import logging
import uuid
from typing import Any, Callable, Mapping, MutableMapping

import structlog
from starlette.types import ASGIApp, Receive, Scope, Send


def add_user_request_flag(
    logger: logging.Logger, method_name: str, event_dict: MutableMapping[str, Any]
) -> Mapping[str, Any]:
    """Add user_request flag to log message."""
    if "user_id" in event_dict:
        event_dict["user_request"] = True
    return event_dict


def sorting_serializer_factory(
    sorted_keys: list[str],
) -> Callable[[MutableMapping[str, Any]], str]:
    def sorting_serializer(event_dict: MutableMapping[str, Any], **kw: Any) -> str:
        sorted_dict = {}
        for key in sorted_keys:
            if key in event_dict:
                sorted_dict[key] = event_dict[key]
                event_dict.pop(key)
        for key in event_dict:
            sorted_dict[key] = event_dict[key]
        return json.dumps(sorted_dict, **kw)

    return sorting_serializer


def configure_logger() -> None:
    """
    Configure the logging module.

    This function configures the logging module to log in rfc5424 format.
    """
    logging.basicConfig(
        level=logging.INFO,
    )

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            add_user_request_flag,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M.%S"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(
                serializer=sorting_serializer_factory(["event", "user_id"])
            ),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


class LoggerInitializationMiddleware:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        structlog.contextvars.clear_contextvars()
        trace_id = str(uuid.uuid4())
        structlog.contextvars.bind_contextvars(trace_id=trace_id)
        await self.app(scope, receive, send)
        return


def add_request_id_header(headers: Mapping[str, str]) -> Mapping[str, str]:
    structlog_contextvars = structlog.contextvars.get_contextvars()
    request_id = structlog_contextvars.get("trace_id", None)
    enriched_headers = {
        **headers,
        "X-Trace-ID": request_id,
    }
    return enriched_headers
