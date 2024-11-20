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

import enum
import functools

import cads_broker.config
import cads_catalogue.config
import sqlalchemy
import sqlalchemy.orm

from . import config

SETTINGS = config.settings


class ConnectionMode(str, enum.Enum):
    """Database connection mode."""

    read = "read"
    write = "write"


@functools.lru_cache()
def get_compute_sessionmaker(
    mode: ConnectionMode = ConnectionMode.write,
) -> sqlalchemy.orm.sessionmaker[sqlalchemy.orm.Session]:
    """Get an sqlalchemy.orm.sessionmaker object bound to the Broker database.

    Parameters
    ----------
    mode: ConnectionMode
        Connection mode to the database. If ConnectionMode.read, the sessionmaker
        will open a connection to a read-only hostname.

    Returns
    -------
    sqlalchemy.orm.sessionmaker
        sqlalchemy.orm.sessionmaker object bound to the Broker database.
    """
    broker_settings = cads_broker.config.ensure_settings()
    if mode == ConnectionMode.write:
        connection_string = broker_settings.connection_string
    elif mode == ConnectionMode.read:
        connection_string = broker_settings.connection_string_read
    else:
        raise ValueError(f"Invalid connection mode: {str(mode)}")
    broker_engine = sqlalchemy.create_engine(
        connection_string,
        pool_timeout=SETTINGS.retrieve_api_broker_pool_timeout,
        pool_recycle=SETTINGS.retrieve_api_broker_pool_recycle,
        pool_size=SETTINGS.retrieve_api_broker_pool_size,
        max_overflow=SETTINGS.retrieve_api_broker_max_overflow,
    )
    return sqlalchemy.orm.sessionmaker(broker_engine)


@functools.lru_cache()
def get_catalogue_sessionmaker(
    mode: ConnectionMode = ConnectionMode.read,
) -> sqlalchemy.orm.sessionmaker[sqlalchemy.orm.Session]:
    """Get an sqlalchemy.orm.sessionmaker object bound to the Catalogue database.

    Parameters
    ----------
    mode: ConnectionMode
        Connection mode to the database. If ConnectionMode.read, the sessionmaker
        will open a connection to a read-only hostname.

    Returns
    -------
    sqlalchemy.orm.sessionmaker
        sqlalchemy.orm.sessionmaker object bound to the Catalogue database.
    """
    catalogue_settings = cads_catalogue.config.ensure_settings()
    if mode == ConnectionMode.write:
        connection_string = catalogue_settings.connection_string
    elif mode == ConnectionMode.read:
        connection_string = catalogue_settings.connection_string_read
    else:
        raise ValueError(f"Invalid connection mode: {str(mode)}")
    catalogue_engine = sqlalchemy.create_engine(
        connection_string,
        pool_timeout=0.1,
        pool_recycle=catalogue_settings.pool_recycle,
    )
    return sqlalchemy.orm.sessionmaker(catalogue_engine)
