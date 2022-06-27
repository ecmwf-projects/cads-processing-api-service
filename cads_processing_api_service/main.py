import attrs
import fastapi
from ogc_api_processes_fastapi import clients, main, models

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

    def get_processes_list(
        self, limit: int, offset: int
    ) -> list[models.ProcessSummary]:
        retval = PROCESS_LIST[offset : (offset + limit)]

        return retval


app = fastapi.FastAPI()
app = main.include_ogc_api_processes_routers(app=app, client=DummyClient())
