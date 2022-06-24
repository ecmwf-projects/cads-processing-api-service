import urllib.parse
from typing import Any

import attrs
import fastapi
from ogc_api_processes_fastapi import clients, main

PROCESS_LIST: list[dict[str, str]] = [
    {"id": "retrieve-reanalysis-era5-single-levels", "version": "0.1"},
    {"id": "retrieve-reanalysis-era5-pressure-levels", "version": "0.1"},
    {"id": "retrieve-reanalysis-era5-land", "version": "0.1"},
    {"id": "retrieve-reanalysis-era5-land-monthly-means", "version": "0.2"},
]


@attrs.define
class DummyClient(clients.BaseClient):  # type: ignore
    """
    Dummy implementation of the OGC API - Processes endpoints.
    """

    def get_processes(
        self, request: fastapi.Request
    ) -> dict[str, list[dict[str, Any]]]:

        links = [
            {
                "href": urllib.parse.urljoin(str(request.base_url), "processes"),
                "rel": "self",
            }
        ]
        processes_list = PROCESS_LIST

        retval = {"processes": processes_list, "links": links}

        return retval


app = main.instantiate_app(client=DummyClient())
