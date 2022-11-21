import jinja2
from itertools import product
import urllib
import requests

import cads_catalogue

from .date_tools import expand_dates_list
from .. import clients, config

def _ensure_list(x):
    return x if isinstance(x, (list, tuple)) else [x]


def extract_format_options(request, config):
    pass


def extract_reduce_options(request, config):
    pass


def unfactorise(hcubes, date_field='date'):
    """Generator function that, for a list of hypercubes, yields each individual
       field as a dict in order."""

    expanders = {date_field: expand_dates_list}

    for hcube in _ensure_list(hcubes):
        value_lists = [expanders.get(k, _ensure_list)(v)
                       for k, v in hcube.items()]
        for values in product(*value_lists):
            yield {k: v for k, v in zip(hcube.keys(), values)}


def requests_to_urls(requests, patterns):
    """Given a list of requests and a list of URL patterns with Jinja2
       formatting, yield the associated URLs to download."""

    templates = [jinja2.Template(p) for p in patterns]

    for req in unfactorise(requests):
        for url in [t.render(req).strip() for t in templates]:
            if url:
                yield {'url': url, 'req': req}


def retrieve_dataset(catalogue_id: str):
    session_obj = cads_catalogue.database.ensure_session_obj(None)
    with session_obj() as session:
        resource = clients.lookup_resource_by_id(
            id=catalogue_id,
            record=cads_catalogue.database.Resource,
            session=session
        )
    return resource


def retrieve_from_storage(relative_path):
    settings = config.ensure_settings()
    storage_url = settings.document_storage_url
    url = urllib.parse.urljoin(storage_url, relative_path)
    return requests.get(url).json()


