import contextlib
import datetime
import os
from typing import Generator

import cads_catalogue.database


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


def get_record(id: str) -> cads_catalogue.database.Resource:
    return cads_catalogue.database.Resource(
        resource_id=id,
        title="ERA5",
        description="description",
        abstract="Lorem ipsum dolor",
        contact=["aaaa", "bbbb"],
        form="form",
        citation="",
        keywords=["label 1", "label 2"],
        version="2.0.0",
        variables=["var1", "var2"],
        providers=["provider 1", "provider 2"],
        extent=[[-180, 180], [-90, 90]],
        links=[{"rel": "foo", "href": "http://foo.com"}],
        documentation="documentation",
        previewimage="img",
        publication_date=datetime.datetime.strptime(
            "2020-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"
        ),
        record_update=datetime.datetime.strptime(
            "2020-02-03T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"
        ),
        resource_update=datetime.datetime.strptime(
            "2020-02-05T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"
        ),
    )
