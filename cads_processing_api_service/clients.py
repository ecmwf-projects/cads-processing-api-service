# type: ignore

"""CADS Processing client, implementing the OGC API Processes standard."""

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

import base64
import enum
import logging
import urllib.parse
from typing import Any, Callable, Optional, Type

import attrs
import cacholote.extra_encoders
import cads_broker.database
import cads_catalogue.config
import cads_catalogue.database
import fastapi
import fastapi_utils.session
import ogc_api_processes_fastapi
import ogc_api_processes_fastapi.clients
import ogc_api_processes_fastapi.exceptions
import ogc_api_processes_fastapi.models
import requests
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.orm.attributes
import sqlalchemy.orm.decl_api
import sqlalchemy.orm.exc
import sqlalchemy.sql.selectable

from . import adaptors, config, exceptions, models, serializers

logger = logging.getLogger(__name__)


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
        row = session.query(record).filter(record.resource_uid == id).one()
    except sqlalchemy.orm.exc.NoResultFound:
        raise ogc_api_processes_fastapi.exceptions.NoSuchProcess()
    return row


def parse_sortby(sortby: str) -> tuple[str]:
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
    cursor: Optional[str],
    back: Optional[str],
    sort_key: Optional[str],
    sort_dir: Optional[str],
) -> sqlalchemy.sql.selectable.Select:
    """Apply pagination bookmark to the running query.

    Parameters
    ----------
        statement: sqlalchemy.sql.selectable.Select
            select statement
        resource: sqlalchemy.orm.decl_api.DeclarativeMeta
            sqlalchemy declarative base
        cursor: Optional[str]
            bookmark cursor
        back: Optional[str]
            if True set bookmark for previous page, else set bookmark for next page
        sort_key: Optional[str]
            key for sorting results
        sort_dir: Optional[str]
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
    compare_method: Callable = getattr(resource_attribute, compare_method_name)
    bookmark_value: str = decode_base64(cursor)
    statement = statement.where(compare_method(bookmark_value))

    return statement


def apply_sorting(
    statement: sqlalchemy.sql.selectable.Select,
    resource: sqlalchemy.orm.decl_api.DeclarativeMeta,
    back: Optional[str],
    sort_key: Optional[str],
    sort_dir: Optional[str],
) -> sqlalchemy.sql.selectable.Select:
    """Apply sorting to the running query.

    Parameters
    ----------
        statement: sqlalchemy.sql.selectable.Select
            select statement
        resource: sqlalchemy.orm.decl_api.DeclarativeMeta
            sqlalchemy declarative base
        back: Optional[str]
            if True set bookmark for previous page, else set bookmark for next page
        sort_key: Optional[str]
            key for sorting results
        sort_dir: Optional[str]
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
    sort_method: Callable = getattr(resource_attribute, sort_method_name)
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
    entries: list[models.StatusInfo | ogc_api_processes_fastapi.models.ProcessSummary],
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
) -> models.StatusInfo:
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
    models.StatusInfo
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
    status_info = make_status_info(job)

    return status_info


def check_token(
    pat: Optional[str] = None, jwt: Optional[str] = None
) -> tuple[str, dict[str, str]]:
    print(pat)
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
    user = response.json()
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


def make_status_info(job: dict[str, Any]) -> models.StatusInfo:
    status_info = models.StatusInfo(
        type="process",
        jobID=job["request_uid"],
        processID=job["process_id"],
        status=job["status"],
        created=job["created_at"],
        started=job["started_at"],
        finished=job["finished_at"],
        updated=job["updated_at"],
    )
    return status_info


@attrs.define
class DatabaseClient(ogc_api_processes_fastapi.clients.BaseClient):
    """Database implementation of the OGC API - Processes endpoints.

    Attributes
    ----------
    process_table: Type[cads_catalogue.database.Resource]
        Processes record/table.
    """

    process_table: Type[cads_catalogue.database.Resource] = attrs.field(
        default=cads_catalogue.database.Resource
    )
    job_table: Type[cads_broker.database.SystemRequest] = attrs.field(
        default=cads_broker.database.SystemRequest
    )

    @property
    def reader(self) -> fastapi_utils.session.FastAPISessionMaker:
        """Return the reader for the catalogue database."""
        connection_string = cads_catalogue.config.ensure_settings().connection_string
        sql_session_reader = fastapi_utils.session.FastAPISessionMaker(
            connection_string
        )
        return sql_session_reader

    def get_processes(
        self,
        limit: Optional[int] = fastapi.Query(10, ge=1, le=10000),
        sortby: Optional[ProcessSortCriterion] = fastapi.Query(
            ProcessSortCriterion.resource_uid_asc
        ),
        cursor: Optional[str] = fastapi.Query(None, include_in_schema=False),
        back: Optional[bool] = fastapi.Query(None, include_in_schema=False),
    ) -> ogc_api_processes_fastapi.models.ProcessList:
        """Implement OGC API - Processes `GET /processes` endpoint.

        Get the list of available processes.

        Parameters
        ----------
        limit : Optional[int]
            Number of processes summaries to be returned.
        sortby: Optional[ProcessSortCriterion]
            Sorting criterion for request's results.
        cursor: Optional[str]
            Hash string used for pagination.
        back: Optional[bool]
            Boolean parameter used for pagination.

        Returns
        -------
        ogc_api_processes_fastapi.models.ProcessList
            List of available processes.
        """
        with self.reader.context_session() as session:
            statement = sqlalchemy.select(self.process_table)
            sort_key, sort_dir = parse_sortby(sortby.name)
            if cursor:
                statement = apply_bookmark(
                    statement, self.process_table, cursor, back, sort_key, sort_dir
                )
            statement = apply_sorting(
                statement, self.process_table, back, sort_key, sort_dir
            )
            statement = apply_limit(statement, limit)
            processes_entries = session.scalars(statement).all()
            processes = [
                serializers.serialize_process_summary(process)
                for process in processes_entries
            ]
        if back:
            processes = list(reversed(processes))
        process_list = ogc_api_processes_fastapi.models.ProcessList(processes=processes)
        pagination_qs = make_pagination_qs(processes, sort_key=sortby.lstrip("-"))
        process_list._pagination_qs = pagination_qs

        return process_list

    def get_process(
        self,
        process_id: str = fastapi.Path(...),
    ) -> ogc_api_processes_fastapi.models.ProcessDescription:
        """Implement OGC API - Processes `GET /processes/{process_id}` endpoint.

        Get the description of the process identified by `process_id`.

        Parameters
        ----------
        process_id : str
            Process identifier.

        Returns
        -------
        ogc_api_processes_fastapi.models.ProcessDescription
            Process description.

        Raises
        ------
        ogc_api_processes_fastapi.exceptions.NoSuchProcess
            If the process `process_id` is not found.
        """
        with self.reader.context_session() as session:
            resource = lookup_resource_by_id(
                id=process_id, record=self.process_table, session=session
            )
            process_description = serializers.serialize_process_description(resource)
            process_description.outputs = {
                "asset": ogc_api_processes_fastapi.models.OutputDescription(
                    title="Asset",
                    description="Downloadable asset description",
                    schema_=ogc_api_processes_fastapi.models.SchemaItem(
                        type="object",
                        properties={
                            "value": cacholote.extra_encoders.FileInfoModel.schema()
                        },
                    ),
                ),
            }

        return process_description

    def post_process_execution(
        self,
        process_id: str = fastapi.Path(...),
        execution_content: models.Execute = fastapi.Body(...),
        user: dict[str, str] = fastapi.Depends(validate_token),
    ) -> models.StatusInfo:
        """Implement OGC API - Processes `POST /processes/{process_id}/execution` endpoint.

        Request execution of the process identified by `process_id`.

        Parameters
        ----------
        process_id : str
            Process identifier.
        execution_content : models.Execute
            Process execution details (e.g. inputs).
        user: dict[str, str]
            Authenticated user credentials.

        Returns
        -------
        models.StatusInfo
            Information on the status of the job.

        Raises
        ------
        ogc_api_processes_fastapi.exceptions.NoSuchProcess
            If the process `process_id` is not found.
        """
        user_id = user.get("id", None)
        execution_content = execution_content.dict()
        logger.info(
            "post_process_execution",
            {
                "structured_data": {
                    "user_id": user_id,
                    "process_id": process_id,
                    **execution_content,
                }
            },
        )
        with self.reader.context_session() as session:
            resource = validate_request(
                process_id,
                execution_content,
                user.get("auth_header", None),
                session,
                self.process_table,
            )
            status_info = submit_job(user_id, process_id, execution_content, resource)
        return status_info

    def get_jobs(
        self,
        processID: Optional[list[str]] = fastapi.Query(None),
        status: Optional[list[str]] = fastapi.Query(None),
        limit: Optional[int] = fastapi.Query(10, ge=1, le=10000),
        sortby: Optional[JobSortCriterion] = fastapi.Query(
            JobSortCriterion.created_at_desc
        ),
        cursor: Optional[str] = fastapi.Query(None, include_in_schema=False),
        back: Optional[bool] = fastapi.Query(None, include_in_schema=False),
        user: dict[str, str] = fastapi.Depends(validate_token),
    ) -> models.JobList:
        """Implement OGC API - Processes `GET /jobs` endpoint.

        Get jobs' status information list.

        Parameters
        ----------
        processID: Optional[List[str]]
            If the parameter is specified with the operation, only jobs that have a value for
            the processID property that matches one of the values specified for the processID
            parameter shall be included in the response.
        status: Optional[List[str]]
            If the parameter is specified with the operation, only jobs that have a value for
            the status property that matches one of the specified values of the status parameter
            shall be included in the response.
        limit: Optional[int]
            The response shall not contain more jobs than specified by the optional ``limit``
            parameter.
        sortby: Optional[JobSortCriterion]
            Sorting criterion for request's results.
        cursor: Optional[str] = fastapi.Query(None)
            Hash string used for pagination.
        back: Optional[bool] = fastapi.Query(None),
            Boolean parameter used for pagination.
        user: dict[str, str]
            Authenticated user credentials.

        Returns
        -------
        models.JobList
            Information on the status of the job.
        """
        session_obj = cads_broker.database.ensure_session_obj(None)
        user_id = user.get("id", None)
        metadata_filters = {"user_id": [str(user_id)] if user_id else []}
        job_filters = {"process_id": processID, "status": status}
        sort_key, sort_dir = parse_sortby(sortby.name)
        with session_obj() as session:
            statement = sqlalchemy.select(self.job_table)
            statement = apply_metadata_filters(
                statement, self.job_table, metadata_filters
            )
            statement = apply_job_filters(statement, self.job_table, job_filters)
            if cursor:
                statement = apply_bookmark(
                    statement,
                    self.job_table,
                    cursor,
                    back,
                    sort_key,
                    sort_dir,
                )
            statement = apply_sorting(
                statement, self.job_table, back, sort_key, sort_dir
            )
            statement = apply_limit(statement, limit)
            job_entries = session.scalars(statement).all()
        if back:
            job_entries = reversed(job_entries)
        jobs = [make_status_info(dictify_job(job)) for job in job_entries]
        job_list = models.JobList(jobs=jobs)
        pagination_qs = make_pagination_qs(jobs, sort_key=sortby.lstrip("-"))
        job_list._pagination_qs = pagination_qs

        return job_list

    def get_job(
        self,
        job_id: str = fastapi.Path(...),
        user: dict[str, str] = fastapi.Depends(validate_token),
    ) -> models.StatusInfo:
        """Implement OGC API - Processes `GET /jobs/{job_id}` endpoint.

        Get status information for the job identifed by `job_id`.

        Parameters
        ----------
        job_id : str
            Identifier of the job.
        user: dict[str, str]
            Authenticated user credentials.

        Returns
        -------
        models.StatusInfo
            Information on the status of the job.

        Raises
        ------
        ogc_api_processes_fastapi.exceptions.NoSuchJob
            If the job `job_id` is not found.
        """
        job = get_job_from_broker_db(job_id=job_id)
        verify_permission(user, job)
        status_info = make_status_info(job)
        return status_info

    def get_job_results(
        self,
        job_id: str = fastapi.Path(...),
        user: dict[str, str] = fastapi.Depends(validate_token),
    ) -> ogc_api_processes_fastapi.models.Results:
        """Implement OGC API - Processes `GET /jobs/{job_id}/results` endpoint.

        Get results for the job identifed by `job_id`.

        Parameters
        ----------
        job_id : str
            Identifier of the job.
        user: dict[str, str]
            Authenticated user credentials.

        Returns
        -------
        ogc_api_processes_fastapi.models.Results
            Job results.

        Raises
        ------
        ogc_api_processes_fastapi.exceptions.NoSuchJob
            If the job `job_id` is not found.
        ogc_api_processes_fastapi.exceptions.ResultsNotReady
            If job `job_id` results are not yet ready.
        ogc_api_processes_fastapi.exceptions.JobResultsFailed
            If job `job_id` results preparation failed.
        """
        job = get_job_from_broker_db(job_id=job_id)
        verify_permission(user, job)
        job_status = job["status"]
        if job_status == "successful":
            asset_value = cads_broker.database.get_request_result(
                request_uid=job["request_uid"]
            )["args"][0]
            return {"asset": {"value": asset_value}}
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

    def delete_job(
        self,
        job_id: str = fastapi.Path(...),
        user: dict[str, str] = fastapi.Depends(validate_token),
    ) -> models.StatusInfo:
        """Implement OGC API - Processes `DELETE /jobs/{job_id}` endpoint.

        Dismiss the job identifed by `job_id`.

        Parameters
        ----------
        job_id : str
            Identifier of the job.
        user: dict[str, str]
            Authenticated user credentials.

        Returns
        -------
        models.StatusInfo
            Information on the status of the job.

        Raises
        ------
        ogc_api_processes_fastapi.exceptions.NoSuchJob
            If the job `job_id` is not found.
        """
        job = get_job_from_broker_db(job_id=job_id)
        verify_permission(user, job)
        job = cads_broker.database.delete_request(request_uid=job_id)
        job = dictify_job(job)
        status_info = make_status_info(job)
        return status_info
