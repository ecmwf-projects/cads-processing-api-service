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


import pydantic

general_settings = None


class Settings(pydantic.BaseSettings):
    """General settings."""

    profiles_service: str = "profiles-api"
    profiles_api_service_port: int = 8000

    default_cache_control: str = "max-age=2"
    default_vary: str = "PRIVATE-TOKEN, Authorization"
    public_cache_control: str = "public, max-age=60"

    cache_users_maxsize: int = 2000
    cache_users_ttl: int = 600
    cache_resources_maxsize: int = 1000
    cache_resources_ttl: int = 10

    api_request_template: str = (
        "import cads_api_client\n\nclient = cads_api_client.ApiClient()\n\nclient.retrieve("
        "\n\tcollection_id='{process_id}',"
        "\n\t{api_request_kwargs}\n)\n"
    )

    @property
    def profiles_api_url(self) -> str:
        return f"http://{self.profiles_service}:{self.profiles_api_service_port}"


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


PORTAL_HEADER_NAME = "X-CADS-PORTAL"
