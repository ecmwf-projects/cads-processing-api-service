import contextlib
import logging
from typing import Iterator

import attrs
import fastapi_utils.session
import ogc_api_processes_fastapi.errors
import psycopg2
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
            if isinstance(e.orig, psycopg2.errors.UniqueViolation):
                raise ogc_api_processes_fastapi.errors.ConflictError(
                    "resource already exists"
                ) from e
            elif isinstance(e.orig, psycopg2.errors.ForeignKeyViolation):
                raise ogc_api_processes_fastapi.errors.ForeignKeyError(
                    "collection does not exist"
                ) from e
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
