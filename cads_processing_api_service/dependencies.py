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


import cads_broker.config
import cads_catalogue.config
import sqlalchemy


def get_compute_db_session_maker() -> sqlalchemy.orm.sessionmaker:
    broker_settings = cads_broker.config.ensure_settings()
    broker_engine = sqlalchemy.create_engine(
        broker_settings.connection_string,
        pool_timeout=0.1,
        pool_recycle=broker_settings.pool_recycle,
    )
    return sqlalchemy.orm.sessionmaker(broker_engine)


def get_catalogue_db_session_maker() -> sqlalchemy.orm.sessionmaker:
    catalogue_settings = cads_catalogue.config.ensure_settings()
    catalogue_engine = sqlalchemy.create_engine(
        catalogue_settings.connection_string,
        pool_timeout=0.1,
        pool_recycle=catalogue_settings.pool_recycle,
    )
    return sqlalchemy.orm.sessionmaker(catalogue_engine)
