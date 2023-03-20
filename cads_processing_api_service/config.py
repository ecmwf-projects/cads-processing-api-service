"""Configuration of the service.

Options are based on pydantic.BaseSettings, so they automatically get values from the environment.
"""

# Copyright 2022, European Union.
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

import json
import logging
import sys
from typing import Any, Callable, Mapping, MutableMapping

import pydantic
import structlog

general_settings = None


class Settings(pydantic.BaseSettings):
    """General settings."""

    internal_proxy_url: str = "http://proxy"
    profiles_base_url: str = "/api/profiles/"

    default_cache_control: str = "max-age=2"
    default_vary: str = "PRIVATE-TOKEN, Authorization"
    public_cache_control: str = "public, max-age=60"

    cache_users_maxsize: int = 2000
    cache_users_ttl: int = 600
    cache_resources_maxsize: int = 1000
    cache_resources_ttl: int = 10


def ensure_settings(
    settings: Settings | None = None,
) -> Settings:
    """If `settings` is None, create a new Settings object.

    Parameters
    ----------
    settings: an optional Settings object to be set as general settings.

    Returns
    -------
    Settings:
        General settings.
    """
    global general_settings
    if settings and isinstance(settings, pydantic.BaseSettings):
        general_settings = settings
    else:
        general_settings = Settings()
    return general_settings


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
    """Configure the logger."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        stream=sys.stdout,
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
                serializer=sorting_serializer_factory(["event", "user_id"]),
            ),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
