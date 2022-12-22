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

import logging
import logging.handlers

import pydantic
from syslog_rfc5424_formatter import RFC5424Formatter

general_settings = None


class Settings(pydantic.BaseSettings):
    """General settings."""

    internal_proxy_url: str = "http://proxy"
    profiles_base_url: str = "/api/profiles/"


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


def configure_logger() -> None:
    """
    Configure the logging module.

    This function configures the logging module to log in rfc5424 format.
    """
    fmt = RFC5424Formatter(
        sd_id="cads_processing_api_service",
    )
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    handler.setFormatter(fmt)
    logger.addHandler(handler)
