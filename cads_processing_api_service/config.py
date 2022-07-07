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

import pydantic


class SqlalchemySettings(pydantic.BaseSettings):
    """Postgres-specific API settings.

    Attributes
    ----------
    postgres_user: str
        postgres username.
    postgres_password: str
        postgres password.
    postgres_host: str
        hostname for the connection.
    postgres_port: str
        database port.
    postgres_dbname: str
        database name.
    connection_string: str
        reader psql connection string.
    """

    postgres_user: str = "catalogue"
    postgres_password: str = "password"
    postgres_host: str = "localhost"
    postgres_port: str = "5432"
    postgres_dbname: str = "catalogue"

    @property
    def connection_string(self) -> str:
        """Create reader psql connection string.

        Returns
        -------
        str
            Reader psql connection string.
        """
        return (
            f"postgresql://{self.postgres_user}"
            f":{self.postgres_password}@{self.postgres_host}"
            f":{self.postgres_port}/{self.postgres_dbname}"
        )
