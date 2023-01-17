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
import urllib.parse
from typing import Any, Callable, Optional, Type

import cads_broker.database
import cads_catalogue.database
import fastapi
import ogc_api_processes_fastapi.exceptions
import ogc_api_processes_fastapi.models
import requests
import sqlalchemy.orm
import sqlalchemy.orm.attributes
import sqlalchemy.orm.decl_api
import sqlalchemy.orm.exc
import sqlalchemy.sql.selectable

from . import adaptors, config, exceptions


class ProcessSortCriterion(str, enum.Enum):
    resource_uid_asc: str = "id"
    resource_uid_desc: str = "-id"


class JobSortCriterion(str, enum.Enum):
    created_at_asc: str = "created"
    created_at_desc: str = "-created"


def lookup_resource_by_id(
    id: str,
    record: Type[cads_catalogue.database.BaseModel],
    session: sqlalchemy.orm.Session,
) -> cads_catalogue.database.Resource:
    try:
        row: cads_catalogue.database.Resource = (
            session.query(record).filter(record.resource_uid == id).one()
        )
    except sqlalchemy.orm.exc.NoResultFound:
        raise ogc_api_processes_fastapi.exceptions.NoSuchProcess()
    return row


def parse_sortby(sortby: str) -> tuple[str, str]:
    sort_params = sortby.split("_")
    sort_key = "_".join(sort_params[:-1])
    sort_dir = sort_params[-1]
    return (sort_key, sort_dir)


def apply_metadata_filters(
    statement: sqlalchemy.sql.selectable.Select,
    resource: cads_broker.database.SystemRequest,
    filters: dict[str, Optional[list[str]]],
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
    resource: cads_broker.database.SystemRequest,
    filters: dict[str, Optional[list[str]]],
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


def get_compare_and_sort_method_name(sort_dir: str, back: bool) -> dict[str, str]:
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
    resource: sqlalchemy.orm.decl_api.DeclarativeMeta,
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
        resource: sqlalchemy.orm.decl_api.DeclarativeMeta
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
    resource: sqlalchemy.orm.decl_api.DeclarativeMeta,
    back: bool,
    sort_key: str,
    sort_dir: str,
) -> sqlalchemy.sql.selectable.Select:
    """Apply sorting to the running query.

    Parameters
    ----------
        statement: sqlalchemy.sql.selectable.Select
            select statement
        resource: sqlalchemy.orm.decl_api.DeclarativeMeta
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
    limit: Optional[int],
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
    entries: list[
        ogc_api_processes_fastapi.models.StatusInfo
        | ogc_api_processes_fastapi.models.ProcessSummary
    ],
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
    entries: list[
        ogc_api_processes_fastapi.models.StatusInfo
        | ogc_api_processes_fastapi.models.ProcessSummary
    ],
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


def get_contextual_accepted_licences(
    execution_content: dict[str, Any]
) -> set[tuple[str, int]]:
    licences = execution_content.get("acceptedLicences")
    if not licences:
        licences = []
    accepted_licences = set(
        [(licence["id"], licence["revision"]) for licence in licences]
    )
    return accepted_licences


def get_stored_accepted_licences(auth_header: dict[str, str]) -> set[tuple[str, int]]:
    settings = config.ensure_settings()
    request_url = urllib.parse.urljoin(
        settings.internal_proxy_url,
        f"{settings.profiles_base_url}/account/licences",
    )
    response = requests.get(request_url, headers=auth_header)
    response.raise_for_status()
    licences = response.json()["licences"]
    accepted_licences = set(
        [(licence["id"], licence["revision"]) for licence in licences]
    )
    return accepted_licences


def check_licences(
    required_licences: set[tuple[str, int]], accepted_licences: set[tuple[str, int]]
) -> set[tuple[str, int]]:
    missing_licences = required_licences - accepted_licences
    if not len(missing_licences) == 0:
        missing_licences_detail = [
            {"id": licence[0], "revision": licence[1]} for licence in missing_licences
        ]
        raise exceptions.PermissionDenied(
            title="required licences not accepted",
            detail=(
                "please accept the following licences to proceed: "
                f"{missing_licences_detail}"
            ),
        )
    return missing_licences


def validate_request(
    process_id: str,
    execution_content: dict[str, Any],
    auth_header: dict[str, str],
    session: sqlalchemy.orm.Session,
    process_table: Type[cads_catalogue.database.Resource],
) -> cads_catalogue.database.Resource:
    """Validate retrieve process execution request.

    Check if requested dataset exists and if execution content is valid.
    In case the check is successful, returns the resource (dataset)
    associated to the process request.

    Parameters
    ----------
    process_id : str
        Process ID
    execution_content: dict[str, Any]
        Content of the process execution request
    auth_header: dict[str, str]
        Authorization header sent with the request
    session : sqlalchemy.orm.Session
        SQLAlchemy ORM session
    process_table: Type[cads_catalogue.database.Resource]
        Resources table

    Returns
    -------
    cads_catalogue.database.BaseModel
        Resource (dataset) associated to the process request.
    """
    resource = lookup_resource_by_id(
        id=process_id, record=process_table, session=session
    )
    required_licences = set(
        (licence.licence_uid, licence.revision) for licence in resource.licences
    )
    contextual_accepted_licences = get_contextual_accepted_licences(execution_content)
    stored_accepted_licences = get_stored_accepted_licences(auth_header)
    accepted_licences = contextual_accepted_licences.union(stored_accepted_licences)
    check_licences(required_licences, accepted_licences)

    return resource


def submit_job(
    user_id: int,
    process_id: str,
    execution_content: dict[str, Any],
    resource: cads_catalogue.database.Resource,
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
    job_kwargs = adaptors.make_system_job_kwargs(
        process_id, execution_content, resource
    )
    job = cads_broker.database.create_request(
        user_id=user_id,
        process_id=process_id,
        **job_kwargs,
    )

    status_info = ogc_api_processes_fastapi.models.StatusInfo(
        processID=job["process_id"],
        type="process",
        jobID=job["request_uid"],
        status=job["status"],
        created=job["created_at"],
        started=job["started_at"],
        finished=job["finished_at"],
        updated=job["updated_at"],
    )

    return status_info


def check_token(
    pat: Optional[str] = None, jwt: Optional[str] = None
) -> tuple[str, dict[str, str]]:
    if pat:
        verification_endpoint = "/account/verification/pat"
        auth_header = {"PRIVATE-TOKEN": pat}
    elif jwt:
        verification_endpoint = "/account/verification/oidc"
        auth_header = {"Authorization": jwt}
    else:
        raise exceptions.PermissionDenied(
            status_code=fastapi.status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
        )
    return (verification_endpoint, auth_header)


def validate_token(
    pat: Optional[str] = fastapi.Header(
        None, description="Personal Access Token", alias="PRIVATE-TOKEN"
    ),
    jwt: Optional[str] = fastapi.Header(
        None, description="JSON Web Token", alias="Authorization"
    ),
) -> dict[str, str]:
    verification_endpoint, auth_header = check_token(pat=pat, jwt=jwt)
    settings = config.ensure_settings()
    request_url = urllib.parse.urljoin(
        settings.internal_proxy_url,
        f"{settings.profiles_base_url}{verification_endpoint}",
    )
    response = requests.post(request_url, headers=auth_header)
    if response.status_code == fastapi.status.HTTP_401_UNAUTHORIZED:
        raise exceptions.PermissionDenied(
            status_code=response.status_code, detail=response.json()["detail"]
        )
    response.raise_for_status()
    user: dict[str, Any] = response.json()
    user["auth_header"] = auth_header
    return user


def dictify_job(job: cads_broker.database.SystemRequest) -> dict[str, Any]:
    job = {
        column.key: getattr(job, column.key)
        for column in sqlalchemy.inspect(job).mapper.column_attrs
    }
    return job


def get_job_from_broker_db(job_id: str) -> dict[str, Any]:
    try:
        request = cads_broker.database.get_request(request_uid=job_id)
    except (
        sqlalchemy.exc.StatementError,
        sqlalchemy.orm.exc.NoResultFound,
    ):
        raise ogc_api_processes_fastapi.exceptions.NoSuchJob(
            f"Can't find the job {job_id}."
        )
    job = dictify_job(request)
    return job


def verify_permission(user: dict[str, str], job: dict[str, Any]) -> None:
    user_id = user.get("id", None)
    if job["request_metadata"]["user_id"] != user_id:
        raise exceptions.PermissionDenied(detail="Operation not permitted")


def get_results_from_broker_db(job: dict[str, Any]) -> dict[str, Any]:
    job_status = job["status"]
    job_id = job["request_uid"]
    if job_status == "successful":
        asset_value = cads_broker.database.get_request_result(request_uid=job_id)[
            "args"
        ][0]
        results = {"asset": {"value": asset_value}}
    elif job_status == "failed":
        raise ogc_api_processes_fastapi.exceptions.JobResultsFailed(
            type="RuntimeError",
            detail=job["response_traceback"],
            status_code=fastapi.status.HTTP_400_BAD_REQUEST,
        )
    elif job_status in ("accepted", "running"):
        raise ogc_api_processes_fastapi.exceptions.ResultsNotReady(
            f"Status of {job_id} is {job_status}."
        )
    return results


def make_status_info(
    job: dict[str, Any], add_results=True
) -> ogc_api_processes_fastapi.models.StatusInfo:
    job_status = job["status"]
    request_uid = job["request_uid"]
    status_info = ogc_api_processes_fastapi.models.StatusInfo(
        type="process",
        jobID=request_uid,
        processID=job["process_id"],
        status=job_status,
        created=job["created_at"],
        started=job["started_at"],
        finished=job["finished_at"],
        updated=job["updated_at"],
    )
    if add_results:
        results = None
        try:
            results = get_results_from_broker_db(job)
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
