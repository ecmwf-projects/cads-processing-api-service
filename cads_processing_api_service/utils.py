"""Client utility functions."""

# Copyright 2022, European Union.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import datetime
import enum
import threading
import urllib.parse
from typing import Any, Callable, Mapping

import cachetools
import cachetools.keys
import cads_broker.database
import cads_catalogue.database
import fastapi
import ogc_api_processes_fastapi.exceptions
import ogc_api_processes_fastapi.models
import sqlalchemy as sa
import sqlalchemy.exc
import sqlalchemy.orm
import sqlalchemy.orm.attributes
import sqlalchemy.sql.selectable
import structlog

from . import config, exceptions, models

SETTINGS = config.settings

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


class ProcessSortCriterion(str, enum.Enum):
    resource_uid_asc = "id"
    resource_uid_desc = "-id"


class JobSortCriterion(str, enum.Enum):
    created_at_asc = "created"
    created_at_desc = "-created"


@cachetools.cached(
    cache=cachetools.TTLCache(
        maxsize=SETTINGS.cache_resources_maxsize,
        ttl=SETTINGS.cache_resources_ttl,
    ),
    lock=threading.Lock(),
    key=lambda resource_id,
    table,
    session,
    load_messages=False,
    portals=None: cachetools.keys.hashkey(resource_id, table, load_messages, portals),
)
def lookup_resource_by_id(
    resource_id: str,
    table: type[cads_catalogue.database.Resource],
    session: sqlalchemy.orm.Session,
    load_messages: bool = False,
    portals: tuple[str] | None = None,
) -> cads_catalogue.database.Resource:
    """Look for the resource identified by `id` into the Catalogue database.

    Parameters
    ----------
    resource_id : str
        Resource identifier.
    table : type[cads_catalogue.database.Resource]
        Catalogue database table.
    session : sqlalchemy.orm.Session
        Catalogue database session.
    load_messages : bool, optional
        If True, load resource messages, by default False.
    portals: tuple[str] | None, optional
        Portals to filter resources by, by default None.

    Returns
    -------
    cads_catalogue.database.Resource
        Found resource.

    Raises
    ------
    ogc_api_processes_fastapi.exceptions.NoSuchProcess
        Raised if no resource corresponding to the provided `id` is found.
    """
    statement = (
        sa.select(table)
        .options(sqlalchemy.orm.joinedload(table.resource_data))
        .options(sqlalchemy.orm.joinedload(table.licences))
    )
    if load_messages:
        statement = statement.options(sqlalchemy.orm.joinedload(table.messages))
    if portals:
        statement = statement.filter(table.portal.in_(portals))
    statement = statement.filter(table.resource_uid == resource_id)
    try:
        row: cads_catalogue.database.Resource = (
            session.execute(statement).unique().scalar_one()
        )
    except sqlalchemy.exc.NoResultFound:
        raise ogc_api_processes_fastapi.exceptions.NoSuchProcess(
            detail=f"dataset {resource_id} not found"
        )
    session.expunge(row)
    return row


@cachetools.cached(
    cache=cachetools.TTLCache(
        maxsize=SETTINGS.cache_resources_maxsize,
        ttl=SETTINGS.cache_resources_ttl,
    ),
    key=lambda resource_id, table, properties, session: cachetools.keys.hashkey(
        resource_id, table, properties
    ),
)
def get_resource_properties(
    resource_id: str,
    properties: str | tuple[str],
    table: (
        type[cads_catalogue.database.Resource]
        | type[cads_catalogue.database.ResourceData]
    ),
    session: sqlalchemy.orm.Session,
) -> tuple[Any, ...]:
    """Look for the resource identified by `id` into the Catalogue database.

    Parameters
    ----------
    resource_id : str
        Resource identifier.
    properties : str | tuple[str]
        Resource properties to be retrieved.
    table : type[cads_catalogue.database.Resource]
        Catalogue database table.
    session : sqlalchemy.orm.Session
        Catalogue database session.

    Returns
    -------
    sqlalchemy.Row[Any]
        Found properties.

    Raises
    ------
    ogc_api_processes_fastapi.exceptions.NoSuchProcess
        Raised if no resource corresponding to the provided `resource_id` is found.
    """
    if isinstance(properties, str):
        properties = (properties,)
    properties_values = tuple(getattr(table, property) for property in properties)
    statement = sa.select(*properties_values).filter(table.resource_uid == resource_id)
    try:
        resource_properties = tuple(session.execute(statement).one())
    except sqlalchemy.exc.NoResultFound:
        raise ogc_api_processes_fastapi.exceptions.NoSuchProcess(
            detail=f"dataset {resource_id} not found"
        )
    return resource_properties


def parse_sortby(sortby: str) -> tuple[str, str]:
    sort_params = sortby.split("_")
    sort_key = "_".join(sort_params[:-1])
    sort_dir = sort_params[-1]
    return (sort_key, sort_dir)


def apply_metadata_filters(
    statement: sqlalchemy.sql.selectable.Select[
        tuple[cads_broker.database.SystemRequest]
    ],
    resource: type[cads_broker.database.SystemRequest],
    filters: dict[str, list[str]],
) -> sqlalchemy.sql.selectable.Select[tuple[cads_broker.database.SystemRequest]]:
    """Apply search filters to the provided select statement.

    Parameters
    ----------
        statement: sqlalchemy.sql.selectable.Select
            Select statement.
        resource: type[cads_broker.database.SystemRequest]
            Broker database table.
        filters: dict[str, list[str]],
            Filters as key-value pairs.


    Returns
    -------
        sqlalchemy.sql.selectable.Select
            Updated select statement.
    """
    for filter_key, filter_values in filters.items():
        if filter_values:
            statement = statement.where(
                (resource.request_metadata[filter_key].astext).in_(filter_values)
            )
    return statement


def apply_job_filters(
    statement: sqlalchemy.sql.selectable.Select[
        tuple[cads_broker.database.SystemRequest]
    ],
    resource: type[cads_broker.database.SystemRequest],
    filters: Mapping[str, list[str] | None],
) -> sqlalchemy.sql.selectable.Select[tuple[cads_broker.database.SystemRequest]]:
    """Apply search filters related to the job status to the provided select statement.

    Parameters
    ----------
        statement: sqlalchemy.sql.selectable.Select
            Select statement.
        resource: type[ads_broker.database.SystemRequest]
            Broker database table.
        filters: Mapping[str, list[str] | None]
            Filters as key-value pairs.


    Returns
    -------
        sqlalchemy.sql.selectable.Select
            updated select statement
    """
    for filter_key, filter_values in filters.items():
        if filter_values:
            statement = statement.where(
                getattr(resource, filter_key).in_(filter_values)
            )
    return statement


def get_compare_and_sort_method_name(
    sort_dir: str, back: bool | None = False
) -> dict[str, str]:
    if (sort_dir == "asc" and back) or (sort_dir == "desc" and not back):
        compare_method_name = "__lt__"
        sort_method_name = "desc"
    elif (sort_dir == "asc" and not back) or (sort_dir == "desc" and back):
        compare_method_name = "__gt__"
        sort_method_name = "asc"
    else:
        raise ValueError(
            f"sort_dir={sort_dir} and back={back} are not a valid combination"
        )
    compare_and_sort_method_name = {
        "compare_method_name": compare_method_name,
        "sort_method_name": sort_method_name,
    }
    return compare_and_sort_method_name


def decode_base64(encoded: str) -> str:
    encoded_bytes = encoded.encode("ascii")
    decoded_bytes = base64.b64decode(encoded_bytes)
    decoded_str = decoded_bytes.decode("ascii")
    return decoded_str


def encode_base64(decoded: str) -> str:
    decoded_bytes = decoded.encode("ascii")
    encoded_bytes = base64.b64encode(decoded_bytes)
    encoded_str = encoded_bytes.decode("ascii")
    return encoded_str


def apply_bookmark(
    statement: sqlalchemy.sql.selectable.Select[
        tuple[cads_broker.database.SystemRequest]
    ],
    resource: (
        type[cads_catalogue.database.Resource]
        | type[cads_broker.database.SystemRequest]
    ),
    cursor: str,
    back: bool,
    sort_key: str,
    sort_dir: str,
) -> sqlalchemy.sql.selectable.Select[tuple[cads_broker.database.SystemRequest]]:
    """Apply pagination bookmark to the provided select statement.

    Parameters
    ----------
        statement: sqlalchemy.sql.selectable.Select
            Select statement.
        resource: type[cads_catalogue.database.Resource] | type[cads_broker.database.SystemRequest],
            Catalogue or Broker database table.
        cursor: str
            Bookmark cursor.
        back: bool
            If True set bookmark for previous page, else set bookmark for next page.
        sort_key: str
            Key for sorting results.
        sort_dir: str
            Sorting direction.

    Returns
    -------
        sqlalchemy.sql.selectable.Select
            Updated select statement.
    """
    resource_attribute: sqlalchemy.orm.attributes.InstrumentedAttribute[Any] = getattr(
        resource, sort_key
    )
    compare_method_name: str = get_compare_and_sort_method_name(sort_dir, back)[
        "compare_method_name"
    ]
    compare_method: Callable = getattr(resource_attribute, compare_method_name)  # type: ignore
    bookmark_value: str = decode_base64(cursor)
    statement = statement.where(compare_method(bookmark_value))

    return statement


def apply_sorting(
    statement: sqlalchemy.sql.selectable.Select[
        tuple[cads_broker.database.SystemRequest]
    ],
    resource: (
        type[cads_catalogue.database.Resource]
        | type[cads_broker.database.SystemRequest]
    ),
    back: bool,
    sort_key: str,
    sort_dir: str,
) -> sqlalchemy.sql.selectable.Select[tuple[cads_broker.database.SystemRequest]]:
    """Apply sorting to the provided select statement.

    Parameters
    ----------
        statement: sqlalchemy.sql.selectable.Select
            Select statement.
        resource: type[cads_catalogue.database.Resource] | type[cads_broker.database.SystemRequest],
            Catalogue or Broker database table.
        back: bool
            If True set bookmark for previous page, else set bookmark for next page.
        sort_key: str
            Key for sorting results.
        sort_dir: str
            Sorting direction.

    Returns
    -------
        sqlalchemy.sql.selectable.Select
            Updated select statement.
    """
    resource_attribute: sqlalchemy.orm.attributes.InstrumentedAttribute[Any] = getattr(
        resource, sort_key
    )
    sort_method_name: str = get_compare_and_sort_method_name(sort_dir, back)[
        "sort_method_name"
    ]
    sort_method: Callable = getattr(resource_attribute, sort_method_name)  # type: ignore
    statement = statement.order_by(sort_method())

    return statement


def apply_limit(
    statement: sqlalchemy.sql.selectable.Select[
        tuple[cads_broker.database.SystemRequest]
    ],
    limit: int | None,
) -> sqlalchemy.sql.selectable.Select[tuple[cads_broker.database.SystemRequest]]:
    """Apply limit to the provided select statement.

    Parameters
    ----------
        statement: sqlalchemy.sql.selectable.Select
            Select statement.
        limit: int | None
            Requested number of results to be shown.

    Returns
    -------
        sqlalchemy.sql.selectable.Select
            Updated select statement.
    """
    statement = statement.limit(limit)
    return statement


def make_cursor(
    entries: (
        list[models.StatusInfo] | list[ogc_api_processes_fastapi.models.ProcessSummary]
    ),
    sort_key: str,
    page: str,
) -> str:
    if page not in ("next", "prev"):
        raise ValueError(f"page: {page} not valid")
    if page == "next":
        bookmark_element = entries[-1]
    else:
        bookmark_element = entries[0]
    cursor = encode_base64(str(getattr(bookmark_element, sort_key)))
    return cursor


def make_pagination_query_params(
    entries: (
        list[models.StatusInfo] | list[ogc_api_processes_fastapi.models.ProcessSummary]
    ),
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


def dictify_job(request: cads_broker.database.SystemRequest) -> dict[str, Any]:
    job: dict[str, Any] = {
        column.key: getattr(request, column.key)
        for column in sqlalchemy.inspect(request).mapper.column_attrs  # type: ignore
    }
    return job


def get_job_from_broker_db(
    job_id: str,
    session: sqlalchemy.orm.Session,
) -> cads_broker.SystemRequest:
    """Get job description from the Broker database.

    Parameters
    ----------
    job_id : str
        Job identifer.
    session : sqlalchemy.orm.Session
        Broker database session.

    Returns
    -------
    dict[str, Any]
        Job description.

    Raises
    ------
    ogc_api_processes_fastapi.exceptions.NoSuchJob
        Raised if no job corresponding to the provided identifier is found.
    """
    try:
        job = cads_broker.database.get_request(request_uid=job_id, session=session)
        if job.status in ("dismissed", "deleted"):
            logger.error("job status is dismissed or deleted", job_status=job.status)
            raise ogc_api_processes_fastapi.exceptions.NoSuchJob(
                detail=f"job {job_id} {job.status}"
            )
    except cads_broker.database.NoResultFound:
        logger.error("job not found")
        raise ogc_api_processes_fastapi.exceptions.NoSuchJob(
            detail=f"job {job_id} not found"
        )
    except cads_broker.database.InvalidRequestID:
        logger.error("invalid job id")
        raise ogc_api_processes_fastapi.exceptions.NoSuchJob(
            detail=f"invalid job id {job_id}"
        )
    return job


def update_results_href(local_path: str, download_node: str) -> str:
    file_path = local_path.split("://", 1)[-1]
    results_href = urllib.parse.urljoin(download_node, file_path)
    return results_href


def get_results_from_job(
    job: cads_broker.SystemRequest, session: sqlalchemy.orm.Session
) -> dict[str, Any]:
    """Get job results description from SystemRequest instance.

    Parameters
    ----------
    job : cads_broker.SystemRequest
        Job status description.

    Returns
    -------
    dict[str, Any]
        Job results description.

    Raises
    ------
    ogc_api_processes_fastapi.exceptions.JobResultsFailed
        Raised if the job for which results have been requested has status `failed`.
    results_not_ready_exc
        Raised if job's results are not yet ready.
    """
    job_status = job.status
    job_id = job.request_uid
    if job_status == "successful":
        try:
            asset_value = job.cache_entry.result["args"][0]  # type: ignore
        except Exception:
            raise exceptions.JobResultsExpired(
                detail=f"results of job {job_id} expired"
            )
        asset_value["href"] = update_results_href(
            asset_value["file:local_path"], config.DownloadNodesSettings().download_node
        )
        results = {"asset": {"value": asset_value}}
    elif job_status in ("failed", "rejected"):
        error_messages = get_job_events(
            job=job, session=session, event_type="user_visible_error"
        )
        traceback = "\n".join([message[1] for message in error_messages])
        match job_status:
            case "failed":
                exc_title = "The job has failed"
            case "rejected":
                exc_title = "The job has been rejected"
        raise ogc_api_processes_fastapi.exceptions.JobResultsFailed(
            title=exc_title,
            status_code=fastapi.status.HTTP_400_BAD_REQUEST,
            traceback=traceback,
        )
    elif job_status in ("accepted", "running"):
        raise ogc_api_processes_fastapi.exceptions.ResultsNotReady(
            detail=f"status of {job_id} is '{job_status}'"
        )
    return results


def parse_results_from_broker_db(
    job: cads_broker.SystemRequest, session: sqlalchemy.orm.Session
) -> dict[str, Any]:
    try:
        results = get_results_from_job(job=job, session=session)
    except ogc_api_processes_fastapi.exceptions.OGCAPIException as exc:
        results = exceptions.format_exception_content(exc=exc)
    return results


def get_job_qos_info(
    job: cads_broker.SystemRequest, session: sqlalchemy.orm.Session
) -> dict[str, Any]:
    entry_point = str(job.entry_point)
    user_uid = str(job.user_uid)
    qos = {
        "adaptor_entry_point": entry_point,
        "running_requests_per_user_adaptor": cads_broker.database.count_requests(
            session=session,
            status="running",
            entry_point=entry_point,
            user_uid=user_uid,
        ),
        "queued_requests_per_user_adaptor": cads_broker.database.count_requests(
            session=session,
            status="accepted",
            entry_point=entry_point,
            user_uid=user_uid,
        ),
        "running_requests_per_adaptor": cads_broker.database.count_requests(
            session=session,
            status="running",
            entry_point=entry_point,
        ),
        "queued_requests_per_adaptor": cads_broker.database.count_requests(
            session=session,
            status="accepted",
            entry_point=entry_point,
        ),
        "active_users_per_adaptor": cads_broker.database.count_users(
            session=session,
            status="running",
            entry_point=entry_point,
        ),
        "waiting_users_per_adaptor": cads_broker.database.count_users(
            session=session,
            status="accepted",
            entry_point=entry_point,
        ),
    }
    return qos


def get_job_events(
    job: cads_broker.SystemRequest,
    session: sqlalchemy.orm.Session,
    event_type: str | None = None,
    start_time: datetime.datetime | None = None,
) -> list[tuple[datetime.datetime, str]]:
    events = []
    request_uid = str(job.request_uid)
    request_events: list[cads_broker.database.Events] = (
        cads_broker.database.get_events_from_request(
            request_uid, session, event_type, start_time
        )
    )
    for request_event in request_events:
        events.append((request_event.timestamp, request_event.message))
    return events  # type: ignore


def make_status_info(
    job: cads_broker.SystemRequest | dict[str, Any],
    request: dict[str, Any] | None = None,
    results: dict[str, Any] | None = None,
    dataset_metadata: dict[str, Any] | None = None,
    qos: dict[str, Any] | None = None,
    log: list[tuple[str, str]] | None = None,
) -> models.StatusInfo:
    """Compose job's status information.

    Parameters
    ----------
    job : cads_broker.SystemRequest | dict[str, Any]
        Job description.
    results : dict[str, Any] | None, optional
        Results description, by default None
    dataset_metadata : dict[str, Any] | None, optional
        Dataset metadata, by default None
    qos : dict[str, Any] | None, optional
        Job qos info, by default None
    log : list[str] | None, optional
        Job log, by default None

    Returns
    -------
    models.StatusInfo
        Job status information.
    """
    if isinstance(job, cads_broker.SystemRequest):
        job = dictify_job(request=job)
    status_info = models.StatusInfo(
        type=ogc_api_processes_fastapi.models.JobType.process,
        jobID=job["request_uid"],
        processID=job["process_id"],
        status=job["status"],
        created=job["created_at"],
        started=job["started_at"],
        finished=job["finished_at"],
        updated=job["updated_at"],
    )
    status_info.metadata = models.StatusInfoMetadata(
        origin=job.get("origin", None),
        request=request,
        results=results,
        datasetMetadata=dataset_metadata,
        qos=qos,
        log=log,
    )
    return status_info


def get_portals(
    portal_header: str | None = fastapi.Header(
        None, alias=SETTINGS.portal_header_name, include_in_schema=False
    ),
) -> tuple[str, ...] | None:
    """Get the list of portals from the incoming HTTP request's header.

    Parameters
    ----------
    portal_header : str | None, optional
        Portal header

    Returns
    -------
    tuple[str] | None
        List of portals.
    """
    portals = (
        tuple([p.strip() for p in portal_header.split(",")]) if portal_header else None
    )
    return portals
