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

import contextlib
import os
from typing import Generator

import cads_processing_api_service.config


@contextlib.contextmanager
def set_env(**environ: str) -> Generator[None, None, None]:
    """
    Temporarily set the process environment variables.
    """
    old_environ = dict(os.environ)
    os.environ.update(environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


def test_settings_default() -> None:
    """Test that the default settings are correct."""
    settings = cads_processing_api_service.config.SqlalchemySettings()
    exp_connection_string = "postgresql://catalogue:password@localhost:5432/catalogue"

    assert settings.connection_string == exp_connection_string


def test_settings_custom() -> None:
    """Test that the default settings can be changed."""
    settings = cads_processing_api_service.config.SqlalchemySettings()
    settings.postgres_dbname = "foo"
    exp_connection_string = "postgresql://catalogue:password@localhost:5432/foo"

    assert settings.connection_string == exp_connection_string


def test_settings_env() -> None:
    """Test that the default settings can be taken from env vars."""
    env_var = {
        "postgres_user": "test_user",
        "postgres_password": "test_password",
        "postgres_host": "test_host",
        "postgres_port": "1234",
        "postgres_dbname": "test_dbname",
    }
    with set_env(**env_var):
        settings = cads_processing_api_service.config.SqlalchemySettings()
    exp_connection_string = (
        f"postgresql://{env_var['postgres_user']}"
        f":{env_var['postgres_password']}@{env_var['postgres_host']}"
        f":{env_var['postgres_port']}/{env_var['postgres_dbname']}"
    )

    assert settings.connection_string == exp_connection_string
