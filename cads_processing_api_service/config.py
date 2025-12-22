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

import functools
import os
import pathlib
import random
from typing import Annotated

import limits
import pydantic
import pydantic_settings
import structlog
import yaml

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

API_TITLE = "ECMWF Data Stores Processing API"
API_DESCRIPTION = (
    "This REST API service enables the submission of processing tasks (data retrieval) to the "
    "ECMWF Data Stores system, and their consequent monitoring and management. "
    "The service is based on the [OGC API - Processes standard](https://ogcapi.ogc.org/processes/).\n\n"
    "Being based on the OGC API - Processes standard, some terminology is inherited from it. "
    "In the context of this specific API, each _process_ is associated with a specific dataset "
    "and enables the retrieval of data from that dataset: as such, each _process_ identifier "
    "corresponds to a specific dataset identifier.\n"
    "A _job_, instead, is a specific data retrieval task that has been submitted for execution."
)

API_REQUEST_TEMPLATE = """import cdsapi

dataset = "{process_id}"
request = {api_request_kwargs}

client = cdsapi.Client()
client.retrieve(dataset, request).download()
"""

API_REQUEST_MAX_LIST_LENGTH: dict[str, int] = {
    "year": 3,
    "month": 3,
    "day": 3,
    "time": 3,
    "area": 4,
    "pressure_level": 3,
}

ANONYMOUS_LICENCES_MESSAGE = (
    "The job has been submitted as an anonymous user. "
    "Please consider the following licences implicitly accepted: "
    "{licences}"
)

DEPRECATION_WARNING_MESSAGE = (
    "You are using a deprecated API endpoint. "
    "If you are using cdsapi, please upgrade to the latest version."
)

MISSING_LICENCES_MESSAGE = (
    "Not all the required licences have been accepted; "
    "please visit {dataset_licences_url} "
    "to accept the required licence(s)."
)

DATASET_LICENCES_URL = "{base_url}/datasets/{process_id}?tab=download#manage-licences"

RATE_LIMITS_STORAGE = limits.storage.MemoryStorage()
RATE_LIMITS_LIMITER = limits.strategies.FixedWindowRateLimiter(RATE_LIMITS_STORAGE)


def validate_rate_limits(rate_limits: list[str]) -> list[str]:
    """Validate rate limits configuration."""
    for rate_limit in rate_limits:
        limits.parse(rate_limit)
    return rate_limits


class RateLimitsMethodConfig(pydantic.BaseModel):
    """Rate limits configuration for a specific origin."""

    api: Annotated[list[str], pydantic.AfterValidator(validate_rate_limits)] = (
        pydantic.Field(default=[])
    )
    ui: Annotated[list[str], pydantic.AfterValidator(validate_rate_limits)] = (
        pydantic.Field(default=[])
    )


class RateLimitsRouteConfig(pydantic.BaseModel):
    post: RateLimitsMethodConfig = pydantic.Field(default=RateLimitsMethodConfig())
    get: RateLimitsMethodConfig = pydantic.Field(default=RateLimitsMethodConfig())
    delete: RateLimitsMethodConfig = pydantic.Field(default=RateLimitsMethodConfig())


class RateLimitsRouteParamConfig(pydantic.BaseModel):
    __pydantic_extra__: dict[str, RateLimitsRouteConfig] = pydantic.Field(init=False)

    default: RateLimitsRouteConfig = pydantic.Field(default=RateLimitsRouteConfig())

    model_config = pydantic.ConfigDict(extra="allow")


class RateLimitsUserConfig(pydantic.BaseModel):
    default: RateLimitsRouteConfig = pydantic.Field(
        default=RateLimitsRouteConfig(), validate_default=True
    )
    processes_processid_execution: RateLimitsRouteParamConfig = pydantic.Field(
        alias="/processes/{process_id}/execution",
        default=RateLimitsRouteParamConfig(),
        validate_default=True,
    )
    processes_processid_constraints: RateLimitsRouteParamConfig = pydantic.Field(
        alias="/processes/{process_id}/constraints",
        default=RateLimitsRouteParamConfig(),
        validate_default=True,
    )
    processes_processid_costing: RateLimitsRouteParamConfig = pydantic.Field(
        alias="/processes/{process_id}/costing",
        default=RateLimitsRouteParamConfig(),
        validate_default=True,
    )
    jobs: RateLimitsRouteConfig = pydantic.Field(
        default=RateLimitsRouteConfig(), alias="/jobs", validate_default=True
    )
    jobs_jobsid: RateLimitsRouteConfig = pydantic.Field(
        default=RateLimitsRouteConfig(), alias="/jobs/{job_id}", validate_default=True
    )
    jobs_jobsid_results: RateLimitsRouteConfig = pydantic.Field(
        default=RateLimitsRouteConfig(),
        alias="/jobs/{job_id}/results",
        validate_default=True,
    )
    jobs_delete: RateLimitsRouteConfig = pydantic.Field(
        default=RateLimitsRouteConfig(), alias="/jobs/delete", validate_default=True
    )


class RateLimitsConfig(RateLimitsUserConfig):
    """Rate limits configuration for the service."""

    unauthenticated: RateLimitsUserConfig = pydantic.Field(
        default=RateLimitsUserConfig(), validate_default=True
    )


def load_rate_limits(rate_limits_file: str | None) -> RateLimitsConfig:
    rate_limits = RateLimitsConfig()
    if rate_limits_file is not None:
        try:
            with open(rate_limits_file, "r") as file:
                loaded_rate_limits = yaml.safe_load(file)
            rate_limits = RateLimitsConfig(**loaded_rate_limits)
        except OSError:
            logger.exception(
                "Failed to read rate limits file", rate_limits_file=rate_limits_file
            )
        except pydantic.ValidationError:
            logger.exception(
                "Failed to validate rate limits file", rate_limits_file=rate_limits_file
            )
    return rate_limits


def load_portals(portals_file: str | None) -> dict[str, str]:
    portals = {}
    if portals_file is not None:
        try:
            with open(portals_file, "r") as file:
                loaded_portals = yaml.safe_load(file)
            portals = loaded_portals
        except OSError:
            logger.exception("Failed to read portals file", portals_file=portals_file)
    return portals


class Settings(pydantic_settings.BaseSettings):
    """General API settings."""

    profiles_service: str = "profiles-api"
    profiles_api_service_port: int = 8000

    @property
    def profiles_api_url(self) -> str:
        return f"http://{self.profiles_service}:{self.profiles_api_service_port}"

    allow_cors: bool = True

    default_cache_control: str = "max-age=2"
    default_vary: str = "PRIVATE-TOKEN, Authorization"
    public_cache_control: str = "public, max-age=60"
    portal_header_name: str = "X-CADS-PORTAL"

    cache_users_maxsize: int = 2000
    cache_users_ttl: int = 60
    cache_resources_maxsize: int = 1000
    cache_resources_ttl: int = 10

    api_title: str = API_TITLE
    api_description: str = API_DESCRIPTION

    api_request_template: str = API_REQUEST_TEMPLATE
    api_request_max_list_length: dict[str, int] = API_REQUEST_MAX_LIST_LENGTH
    missing_dataset_title: str = "Dataset not available"
    anonymous_licences_message: str = ANONYMOUS_LICENCES_MESSAGE
    deprecation_warning_message: str = DEPRECATION_WARNING_MESSAGE
    missing_licences_message: str = MISSING_LICENCES_MESSAGE
    dataset_licences_url: str = DATASET_LICENCES_URL

    retrieve_api_broker_pool_timeout: float = 1.0
    retrieve_api_broker_pool_recycle: int = 60
    retrieve_api_broker_pool_size: int = 5
    retrieve_api_broker_max_overflow: int = 15

    retrieve_api_catalogue_pool_timeout: float = 0.1
    retrieve_api_catalogue_pool_recycle: int = 60
    retrieve_api_catalogue_pool_size: int = 5
    retrieve_api_catalogue_max_overflow: int = 15

    rate_limits_file: str | None = None
    rate_limits: RateLimitsConfig = pydantic.Field(default=RateLimitsConfig())

    portals_file: str | None = None
    portals: dict[str, str] = pydantic.Field(default={})

    @pydantic.model_validator(mode="after")  # type: ignore
    def load_rate_limits(self) -> pydantic_settings.BaseSettings:
        self.rate_limits: RateLimitsConfig = load_rate_limits(self.rate_limits_file)
        return self

    @pydantic.model_validator(mode="after")  # type: ignore
    def load_portals(self) -> pydantic_settings.BaseSettings:
        self.portals: dict[str, str] = load_portals(self.portals_file)
        return self


settings = Settings()


def validate_download_nodes_file(download_nodes_file: str) -> pathlib.Path:
    download_nodes_file_path = pathlib.Path(download_nodes_file)
    _ = load_download_nodes(download_nodes_file_path)
    return download_nodes_file_path


@functools.lru_cache
def load_download_nodes(download_nodes_file: pathlib.Path) -> list[str]:
    download_nodes = []
    with open(download_nodes_file, "r") as file:
        for line in file:
            if download_node := os.path.expandvars(line.rstrip("\n")):
                download_nodes.append(download_node)
    if not download_nodes:
        raise ValueError(
            f"No download nodes found in download nodes file: {download_nodes_file}"
        )
    return download_nodes


class DownloadNodesSettings(pydantic_settings.BaseSettings):
    """Settings for download nodes."""

    download_nodes_file: Annotated[
        str, pydantic.AfterValidator(validate_download_nodes_file)
    ] = "/etc/retrieve-api/download-nodes.config"

    @property
    def download_node(self) -> str:
        download_nodes = load_download_nodes(self.download_nodes_file)
        return random.choice(download_nodes)
