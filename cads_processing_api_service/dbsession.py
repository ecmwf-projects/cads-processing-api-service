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

import contextlib
import logging
from typing import Iterator

import attrs
import fastapi_utils.session
import ogc_api_processes_fastapi.errors
import sqlalchemy as sa

from . import config

logger = logging.getLogger(__name__)


class FastAPISessionMaker(fastapi_utils.session.FastAPISessionMaker):
    """FastAPISessionMaker."""

    @contextlib.contextmanager
    def context_session(self) -> Iterator[sa.orm.Session]:
        """Override base method to include exception handling."""
        try:
            yield from self.get_db()
        except sa.exc.StatementError as e:
            logger.error(e, exc_info=True)
            raise ogc_api_processes_fastapi.errors.DatabaseError(
                "unhandled database error"
            )


@attrs.define
class Session:
    """Database session management."""

    conn_string: str = attrs.field()
    reader: FastAPISessionMaker = attrs.field(init=False)

    @classmethod
    def create_from_settings(cls, settings: config.SqlalchemySettings) -> "Session":
        """Create a Session object from settings."""
        return cls(
            conn_string=settings.connection_string,
        )

    def __attrs_post_init__(self) -> None:
        """Post init handler."""
        self.reader = FastAPISessionMaker(self.conn_string)
