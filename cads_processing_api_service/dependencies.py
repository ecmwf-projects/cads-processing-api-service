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
from typing import Iterator, Mapping

import cads_broker.database
import cads_catalogue.database
import fastapi
import sqlalchemy.orm

from . import utils


@functools.lru_cache()
def get_compute_session_maker() -> sqlalchemy.orm.sessionmaker:
    session_maker = cads_broker.database.ensure_session_obj(None)
    return session_maker


def get_compute_session() -> Iterator[sqlalchemy.orm.Session]:
    session_maker = get_compute_session_maker()
    session: sqlalchemy.orm.Session = session_maker()
    try:
        yield session
    finally:
        session.close()


@functools.lru_cache()
def get_catalogue_session_maker() -> sqlalchemy.orm.sessionmaker:
    session_maker = cads_catalogue.database.ensure_session_obj(None)
    return session_maker


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
    user = utils.validate_token_timed(pat, jwt)
    return user
