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

import cads_broker.config
import cads_catalogue.config
import sqlalchemy
import sqlalchemy.ext.asyncio


@functools.lru_cache()
def get_compute_sessionmaker() -> sqlalchemy.orm.sessionmaker:
    """Get a sync sqlalchemy.orm.sessionmaker object bound to the Broker database.

    Returns
    -------
    sqlalchemy.orm.sessionmaker
        sqlalchemy.orm.sessionmaker object bound to the Broker database.
    """
    broker_settings = cads_broker.config.ensure_settings()
    broker_engine = sqlalchemy.create_engine(
        broker_settings.connection_string,
        pool_timeout=broker_settings.pool_timeout,
        pool_recycle=broker_settings.pool_recycle,
    )
    return sqlalchemy.orm.sessionmaker(broker_engine)


@functools.lru_cache()
def get_compute_async_sessionmaker() -> sqlalchemy.orm.sessionmaker:
    """Get an async sqlalchemy.orm.sessionmaker object bound to the Broker database.

    Returns
    -------
    sqlalchemy.orm.sessionmaker
        sqlalchemy.orm.sessionmaker object bound to the Broker database.
    """
    broker_settings = cads_broker.config.ensure_settings()
    connection_string = broker_settings.connection_string.replace(
        "postgresql", "postgresql+asyncpg"
    )
    broker_engine = sqlalchemy.ext.asyncio.create_async_engine(
        connection_string,
        pool_timeout=broker_settings.pool_timeout,
        pool_recycle=broker_settings.pool_recycle,
    )
    sessionmaker = sqlalchemy.orm.sessionmaker(
        broker_engine,
        expire_on_commit=False,
        class_=sqlalchemy.ext.asyncio.AsyncSession,
    )
    return sessionmaker


@functools.lru_cache()
def get_catalogue_sessionmaker() -> sqlalchemy.orm.sessionmaker:
    """Get a sync sqlalchemy.orm.sessionmaker object bound to the Catalogue database.

    Returns
    -------
    sqlalchemy.orm.sessionmaker
        sqlalchemy.orm.sessionmaker object bound to the Catalogue database.
    """
    catalogue_settings = cads_catalogue.config.ensure_settings()
    catalogue_engine = sqlalchemy.create_engine(
        catalogue_settings.connection_string,
        pool_timeout=0.1,
        pool_recycle=catalogue_settings.pool_recycle,
    )
    return sqlalchemy.orm.sessionmaker(catalogue_engine)


@functools.lru_cache()
def get_catalogue_async_sessionmaker() -> sqlalchemy.orm.sessionmaker:
    """Get an async sqlalchemy.orm.sessionmaker object bound to the Catalogue database.

    Returns
    -------
    sqlalchemy.orm.sessionmaker
        sqlalchemy.orm.sessionmaker object bound to the Catalogue database.
    """
    catalogue_settings = cads_catalogue.config.ensure_settings()
    connection_string = catalogue_settings.connection_string.replace(
        "postgresql", "postgresql+asyncpg"
    )
    catalogue_engine = sqlalchemy.ext.asyncio.create_async_engine(
        connection_string,
        pool_timeout=0.1,
        pool_recycle=catalogue_settings.pool_recycle,
    )
    sessionmaker = sqlalchemy.orm.sessionmaker(
        catalogue_engine,
        expire_on_commit=False,
        class_=sqlalchemy.ext.asyncio.AsyncSession,
    )
    return sessionmaker
