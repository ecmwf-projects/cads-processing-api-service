"""CADS Processing client dependencies."""

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

import functools
import urllib.parse
from typing import Iterator, Mapping

import cads_broker.config
import cads_catalogue.config
import fastapi
import requests
import sqlalchemy
import structlog

from . import config, exceptions, utils

logger = structlog.get_logger(__name__)


@functools.lru_cache()
def get_compute_session_maker() -> sqlalchemy.orm.sessionmaker:
    broker_settings = cads_broker.config.ensure_settings()
    broker_engine = sqlalchemy.create_engine(broker_settings.connection_string)
    return sqlalchemy.orm.sessionmaker(broker_engine)


def get_compute_session() -> Iterator[sqlalchemy.orm.Session]:
    session_maker = get_compute_session_maker()
    session: sqlalchemy.orm.Session = session_maker()
    try:
        yield session
    finally:
        session.close()


@functools.lru_cache()
def get_catalogue_session_maker() -> sqlalchemy.orm.sessionmaker:
    catalogue_settings = cads_catalogue.config.ensure_settings()
    catalogue_engine = sqlalchemy.create_engine(catalogue_settings.connection_string)
    return sqlalchemy.orm.sessionmaker(catalogue_engine)


def get_catalogue_session() -> Iterator[sqlalchemy.orm.Session]:
    session_maker = get_catalogue_session_maker()
    session: sqlalchemy.orm.Session = session_maker()
    try:
        yield session
    finally:
        session.close()


def validate_token(
    pat: str
    | None = fastapi.Header(
        None, description="Personal Access Token", alias="PRIVATE-TOKEN"
    ),
    jwt: str
    | None = fastapi.Header(None, description="JSON Web Token", alias="Authorization"),
) -> dict[str, str | int | Mapping[str, str | int]]:
    verification_endpoint, auth_header = utils.check_token(pat=pat, jwt=jwt)
    settings = config.ensure_settings()
    request_url = urllib.parse.urljoin(
        settings.internal_proxy_url,
        f"{settings.profiles_base_url}{verification_endpoint}",
    )
    headers = auth_header
    headers["X-Request-ID"] = structlog.contextvars.get_contextvars()["request_id"]
    response = requests.post(request_url, headers=headers)
    if response.status_code == fastapi.status.HTTP_401_UNAUTHORIZED:
        raise exceptions.PermissionDenied(
            status_code=response.status_code, detail=response.json()["detail"]
        )
    response.raise_for_status()
    user: dict[str, str | int | Mapping[str, str | int]] = response.json()
    user["auth_header"] = auth_header
    return user
