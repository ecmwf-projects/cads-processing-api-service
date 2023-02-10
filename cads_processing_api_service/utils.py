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
import enum
import uuid
from typing import Any, Callable, Mapping

import cachetools
import cachetools.keys
import cads_broker.database
import cads_catalogue.database
import fastapi
import ogc_api_processes_fastapi.exceptions
import ogc_api_processes_fastapi.models
import sqlalchemy.exc
import sqlalchemy.orm
import sqlalchemy.orm.attributes
import sqlalchemy.orm.exc
import sqlalchemy.sql.selectable
import structlog

from . import adaptors, models

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


class ProcessSortCriterion(str, enum.Enum):
    resource_uid_asc: str = "id"
    resource_uid_desc: str = "-id"


class JobSortCriterion(str, enum.Enum):
    created_at_asc: str = "created"
    created_at_desc: str = "-created"


@cachetools.cached(  # type: ignore
    cache=cachetools.TTLCache(maxsize=1024, ttl=60),
    key=lambda id, record, session: cachetools.keys.hashkey(id, record),
    info=True,
)
def lookup_resource_by_id(
    id: str,
    record: type[cads_catalogue.database.Resource],
    session: sqlalchemy.orm.Session,
) -> cads_catalogue.database.Resource:
    try:
        row: cads_catalogue.database.Resource = (
            session.query(record)  # type: ignore
            .options(sqlalchemy.orm.joinedload(record.licences))
            .filter(record.resource_uid == id)
            .one()
        )
    except sqlalchemy.orm.exc.NoResultFound as exc:
        logger.exception(repr(exc))
        raise ogc_api_processes_fastapi.exceptions.NoSuchProcess()
    session.expunge(row)  # type:ignore
    return row


def parse_sortby(sortby: str) -> tuple[str, str]:
    sort_params = sortby.split("_")
    sort_key = "_".join(sort_params[:-1])
    sort_dir = sort_params[-1]
    return (sort_key, sort_dir)


def apply_metadata_filters(
    statement: sqlalchemy.sql.selectable.Select,
    resource: type[cads_broker.database.SystemRequest],
    filters: dict[str, list[str]],
) -> sqlalchemy.sql.selectable.Select:
    """Apply search filters to the running query.

    Parameters
    ----------
        statement: sqlalchemy.sql.selectable.Select
            select statement
        resource: cads_broker.database.SystemRequest
            sqlalchemy declarative base
        filters: dict[str, Optional[list[str]]],
            filters as key-value pairs


    Returns
    -------
        sqlalchemy.sql.selectable.Select
            updated select statement
    """
    for filter_key, filter_values in filters.items():
        if filter_values:
            statement = statement.where(
                (resource.request_metadata[filter_key].astext).in_(filter_values)
            )
    return statement


def apply_job_filters(
    statement: sqlalchemy.sql.selectable.Select,
    resource: type[cads_broker.database.SystemRequest],
    filters: Mapping[str, list[str] | None],
) -> sqlalchemy.sql.selectable.Select:
    """Apply search filters related to the job status to the running query.

    Parameters
    ----------
        statement: sqlalchemy.sql.selectable.Select
            select statement
        resource: cads_broker.database.SystemRequest
            sqlalchemy declarative base
        filters: dict[str, Optional[list[str]]]
            filters as key-value pairs


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
    statement: sqlalchemy.sql.selectable.Select,
    resource: type[cads_catalogue.database.Resource]
    | type[cads_broker.database.SystemRequest],
    cursor: str,
    back: bool,
    sort_key: str,
    sort_dir: str,
) -> sqlalchemy.sql.selectable.Select:
    """Apply pagination bookmark to the running query.

    Parameters
    ----------
        statement: sqlalchemy.sql.selectable.Select
            select statement
        resource: Type[cads_catalogue.database.Resource] | Type[cads_broker.database.SystemRequest],
            sqlalchemy declarative base
        cursor: str
            bookmark cursor
        back: bool
            if True set bookmark for previous page, else set bookmark for next page
        sort_key: str
            key for sorting results
        sort_dir: str
            sorting direction

    Returns
    -------
        sqlalchemy.sql.selectable.Select
            updated select statement
    """
    resource_attribute: sqlalchemy.orm.attributes.InstrumentedAttribute = getattr(
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
    statement: sqlalchemy.sql.selectable.Select,
    resource: type[cads_catalogue.database.Resource]
    | type[cads_broker.database.SystemRequest],
    back: bool,
    sort_key: str,
    sort_dir: str,
) -> sqlalchemy.sql.selectable.Select:
    """Apply sorting to the running query.

    Parameters
    ----------
        statement: sqlalchemy.sql.selectable.Select
            select statement
        resource: Type[cads_catalogue.database.Resource] | Type[cads_broker.database.SystemRequest],
            sqlalchemy declarative base
        back: bool
            if True set bookmark for previous page, else set bookmark for next page
        sort_key: str
            key for sorting results
        sort_dir: str
            sorting direction

    Returns
    -------
        sqlalchemy.sql.selectable.Select
            updated select statement
    """
    resource_attribute: sqlalchemy.orm.attributes.InstrumentedAttribute = getattr(
        resource, sort_key
    )
    sort_method_name: str = get_compare_and_sort_method_name(sort_dir, back)[
        "sort_method_name"
    ]
    sort_method: Callable = getattr(resource_attribute, sort_method_name)  # type: ignore
    statement = statement.order_by(sort_method())

    return statement


def apply_limit(
    statement: sqlalchemy.sql.selectable.Select,
    limit: int | None,
) -> sqlalchemy.sql.selectable.Select:
    """Apply limit to the running query.

    Parameters
    ----------
        statement: sqlalchemy.sql.selectable.Select
            select statement
        limit: Optional[int]
            requested number of results to be shown

    Returns
    -------
        sqlalchemy.sql.selectable.Select
            updated select statement
    """
    statement = statement.limit(limit)
    return statement


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
    cursor = encode_base64(str(getattr(bookmark_element, sort_key)))
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


def submit_job(
    user_id: int,
    process_id: str,
    execution_content: dict[str, Any],
    resource: cads_catalogue.database.Resource,
    compute_session: sqlalchemy.orm.Session,
) -> ogc_api_processes_fastapi.models.StatusInfo:
    """Submit new job.

    Parameters
    ----------
    user_id: int,
        User identifier.
    process_id: str
        Process ID.
    execution_content: ogc_api_processes_fastapi.models.Execute
        Body of the process execution request.
    resource: cads_catalogue.database.Resource,
        Catalogue resource corresponding to the requested retrieve process.


    Returns
    -------
    ogc_api_processes_fastapi.models.StatusInfo
        Sumbitted job status info.
    """
    job_id = str(uuid.uuid4())
    structlog.contextvars.bind_contextvars(job_id=job_id)
    job_kwargs = adaptors.make_system_job_kwargs(
        process_id, execution_content, resource
    )
    logger.info("Submitting job")
    job = cads_broker.database.create_request_in_session(
        session=compute_session,
        request_uid=job_id,
        user_id=user_id,
        process_id=process_id,
        **job_kwargs,
    )
    logger.info("Job submitted")
    status_info = models.StatusInfo(
        processID=job["process_id"],
        type="process",
        jobID=job["request_uid"],
        status=job["status"],
        created=job["created_at"],
        started=job["started_at"],
        finished=job["finished_at"],
        updated=job["updated_at"],
        request=job["request_body"]["kwargs"]["request"],
    )

    return status_info


def dictify_job(request: cads_broker.database.SystemRequest) -> dict[str, Any]:
    job: dict[str, Any] = {
        column.key: getattr(request, column.key)
        for column in sqlalchemy.inspect(request).mapper.column_attrs
    }
    return job


def get_job_from_broker_db(
    job_id: str, session: sqlalchemy.orm.Session
) -> dict[str, Any]:
    try:
        request = cads_broker.database.get_request_in_session(
            request_uid=job_id, session=session
        )
    except (
        sqlalchemy.exc.StatementError,
        sqlalchemy.orm.exc.NoResultFound,
    ) as exc:
        logger.exception(repr(exc))
        raise ogc_api_processes_fastapi.exceptions.NoSuchJob(
            f"Can't find the job {job_id}."
        )
    job = dictify_job(request)
    return job


def get_results_from_broker_db(
    job: dict[str, Any], session: sqlalchemy.orm.Session
) -> dict[str, Any]:
    job_status = job["status"]
    job_id = job["request_uid"]
    if job_status == "successful":
        try:
            asset_value = cads_broker.database.get_request_result_in_session(
                request_uid=job_id, session=session
            )["args"][0]
            results = {"asset": {"value": asset_value}}
        except Exception:
            results = {}
    elif job_status == "failed":
        job_results_failed_exc = ogc_api_processes_fastapi.exceptions.JobResultsFailed(
            type="RuntimeError",
            detail=job["response_traceback"],
            status_code=fastapi.status.HTTP_400_BAD_REQUEST,
        )
        raise job_results_failed_exc
    elif job_status in ("accepted", "running"):
        results_not_ready_exc = ogc_api_processes_fastapi.exceptions.ResultsNotReady(
            f"Status of {job_id} is {job_status}."
        )
        raise results_not_ready_exc
    return results


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
            results = get_results_from_broker_db(job=job, session=session)
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
