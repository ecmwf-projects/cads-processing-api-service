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

import pydantic
import pydantic_settings
import yaml

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


class Settings(pydantic_settings.BaseSettings):
    """General API settings."""

    profiles_service: str = "profiles-api"
    profiles_api_service_port: int = 8000

    @property
    def profiles_api_url(self) -> str:
        return f"http://{self.profiles_service}:{self.profiles_api_service_port}"

    allow_cors: bool = True

    default__control: str = "max-age=2"
    default_vary: str = "PRIVATE-TOKEN, Authorization"
    public_cache_control: str = "public, max-age=60"
    portal_header_name: str = "X-CADS-PORTAL"

    cache_users_maxsize: int = 2000
    cache_users_ttl: int = 60
    cache_resources_maxsize: int = 1000
    # cache_resources_ttl: int = 10
    cache_resources_ttl: int = 10

    api_request_template: str = API_REQUEST_TEMPLATE
    api_request_max_list_length: dict[str, int] = API_REQUEST_MAX_LIST_LENGTH
    missing_dataset_title: str = "Dataset not available"
    anonymous_licences_message: str = ANONYMOUS_LICENCES_MESSAGE
    deprecation_warning_message: str = DEPRECATION_WARNING_MESSAGE
    missing_licences_message: str = MISSING_LICENCES_MESSAGE
    dataset_licences_url: str = (
        "{base_url}/datasets/{process_id}?tab=download#manage-licences"
    )

    rate_limits_file: str = "/etc/retrieve-api/rate-limits.yaml"

    @property
    def rate_limits(self) -> dict[str, dict[str, str]]:
        rate_limits = {}
        if os.path.exists(self.rate_limits_file):
            with open(self.rate_limits_file) as fp:
                rate_limits = yaml.safe_load(fp) or {}
        return rate_limits


settings = Settings()


def validate_download_nodes_file(download_nodes_file: str) -> pathlib.Path:
    download_nodes_file_path = pathlib.Path(download_nodes_file)
    if not download_nodes_file_path.exists():
        raise FileNotFoundError(
            f"Download nodes file not found: {download_nodes_file_path}"
        )
    try:
        with open(download_nodes_file_path, "r") as file:
            lines = file.readlines()
            line_count = len(lines)
            if line_count == 0:
                raise ValueError("Download nodes file is empty")
    except Exception as e:
        raise ValueError(
            f"Failed to read download nodes file: {download_nodes_file_path}"
        ) from e
    return download_nodes_file_path


@functools.lru_cache
def load_download_nodes(download_nodes_file: pathlib.Path) -> list[str]:
    download_nodes = []
    with open(download_nodes_file, "r") as file:
        for line in file:
            if download_node := os.path.expandvars(line.rstrip("\n")):
                download_nodes.append(download_node)
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
