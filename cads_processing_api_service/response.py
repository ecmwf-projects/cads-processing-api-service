"""API responses editing."""

from typing import Any

import ogc_api_processes_fastapi.exceptions
import ogc_api_processes_fastapi.models
import sqlalchemy

from . import crud, models, utils


def make_cursor(
    entries: list[ogc_api_processes_fastapi.models.StatusInfo]
    | list[ogc_api_processes_fastapi.models.ProcessSummary],
    sort_key: str,
    page: str,
) -> str:
    if page not in ("next", "prev"):
        raise ValueError(f"page: {page} not valid")
    if page == "next":
        bookmark_element = entries[-1]
    else:
        bookmark_element = entries[0]
    cursor = utils.encode_base64(str(getattr(bookmark_element, sort_key)))
    return cursor


def make_pagination_qs(
    entries: list[ogc_api_processes_fastapi.models.StatusInfo]
    | list[ogc_api_processes_fastapi.models.ProcessSummary],
    sort_key: str,
) -> ogc_api_processes_fastapi.models.PaginationQueryParameters:
    pagination_qs = ogc_api_processes_fastapi.models.PaginationQueryParameters(
        next={}, prev={}
    )
    if len(entries) != 0:
        cursor_next = make_cursor(entries, sort_key, "next")
        pagination_qs.next = {"cursor": cursor_next, "back": "False"}
        cursor_prev = make_cursor(entries, sort_key, "prev")
        pagination_qs.prev = {"cursor": cursor_prev, "back": "True"}
    return pagination_qs


def make_status_info(
    job: dict[str, Any],
    session: sqlalchemy.orm.Session,
    add_results: bool = True,
) -> models.StatusInfo:
    job_status = job["status"]
    request_uid = job["request_uid"]
    status_info = models.StatusInfo(
        type="process",
        jobID=request_uid,
        processID=job["process_id"],
        status=job_status,
        created=job["created_at"],
        started=job["started_at"],
        finished=job["finished_at"],
        updated=job["updated_at"],
        request=job["request_body"]["kwargs"]["request"],
    )
    if add_results:
        results = None
        try:
            results = crud.get_results_from_broker_db(job=job, session=session)
        except ogc_api_processes_fastapi.exceptions.JobResultsFailed as exc:
            results = {
                "type": exc.type,
                "title": exc.title,
                "detail": exc.detail,
            }
        except ogc_api_processes_fastapi.exceptions.ResultsNotReady:
            results = None
        status_info.results = results
    return status_info
