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

import os
import random

import pydantic_settings

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


general_settings = None


class Settings(pydantic_settings.BaseSettings):
    """General settings."""

    profiles_service: str = "profiles-api"
    profiles_api_service_port: int = 8000

    allow_cors: bool = True

    default_cache_control: str = "max-age=2"
    default_vary: str = "PRIVATE-TOKEN, Authorization"
    public_cache_control: str = "public, max-age=60"

    cache_users_maxsize: int = 2000
    cache_users_ttl: int = 60
    cache_resources_maxsize: int = 1000
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

    download_nodes_config: str = "/etc/retrieve-api/download-nodes.config"

    @property
    def profiles_api_url(self) -> str:
        return f"http://{self.profiles_service}:{self.profiles_api_service_port}"

    @property
    def download_node(self) -> str:
        download_nodes = []
        with open(self.download_nodes_config) as fp:
            for line in fp:
                if download_node := os.path.expandvars(line.rstrip("\n")):
                    download_nodes.append(download_node)
        return random.choice(download_nodes)


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
    if settings and isinstance(settings, pydantic_settings.BaseSettings):
        general_settings = settings
    else:
        general_settings = Settings()
    return general_settings


PORTAL_HEADER_NAME = "X-CADS-PORTAL"
