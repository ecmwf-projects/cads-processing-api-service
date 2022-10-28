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
from typing import Any, Callable, Optional, Type

import attrs
import cads_broker.database
import cads_catalogue.config
import cads_catalogue.database
import fastapi
import fastapi_utils.session
import ogc_api_processes_fastapi
import ogc_api_processes_fastapi.clients
import ogc_api_processes_fastapi.exceptions
import ogc_api_processes_fastapi.responses
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.orm.attributes
import sqlalchemy.orm.decl_api
import sqlalchemy.orm.exc
import sqlalchemy.sql.selectable

from . import adaptors, serializers

logger = logging.getLogger(__name__)


SYSTEM_REQUEST_KEYS = {"created": "created_at"}


class SortCriterion(str, enum.Enum):
    created: str = "created"


class SortDirection(str, enum.Enum):
    asc: str = "asc"
    desc: str = "desc"


def lookup_resource_by_id(
    id: str,
    record: Type[cads_catalogue.database.BaseModel],
    session: sqlalchemy.orm.Session,
) -> cads_catalogue.database.BaseModel:

    try:
        row = session.query(record).filter(record.resource_uid == id).one()
    except sqlalchemy.orm.exc.NoResultFound:
        raise ogc_api_processes_fastapi.exceptions.NoSuchProcess()
    return row


def apply_jobs_filters(
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
        filters: dict[str, Optional[list[str]]]
            filters as key-value pairs


    Returns
    -------
        sqlalchemy.sql.selectable.Select
            updated select statement
    """
    for filter_key, filter_value in filters.items():
        if filter_value:
            statement = statement.where(getattr(resource, filter_key).in_(filter_value))
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
    bookmark: dict[str, Optional[str]],
    sorting: dict[str, Optional[str]],
) -> sqlalchemy.sql.selectable.Select:
    """Apply pagination bookmark to the running query.

    Parameters
    ----------
        statement: sqlalchemy.sql.selectable.Select
            select statement
        resource: sqlalchemy.orm.decl_api.DeclarativeMeta
            sqlalchemy declarative base
        bookmark: dict[str, Optional[str]]
            page bookmark as key-value pairs
        sorting: dict[str, Optional[str]]
            sorting instructions as key-value pairs

    Returns
    -------
        sqlalchemy.sql.selectable.Select
            updated select statement
    """
    resource_attribute: sqlalchemy.orm.attributes.InstrumentedAttribute = getattr(
        resource, SYSTEM_REQUEST_KEYS[sorting["sort_key"]]
    )
    compare_method_name: str = get_compare_and_sort_method_name(
        sorting["sort_dir"], bookmark["back"]
    )["compare_method_name"]
    compare_method: Callable = getattr(resource_attribute, compare_method_name)
    bookmark_value: str = decode_base64(bookmark["cursor"])
    statement = statement.where(compare_method(bookmark_value))

    return statement


def apply_sorting(
    statement: sqlalchemy.sql.selectable.Select,
    resource: sqlalchemy.orm.decl_api.DeclarativeMeta,
    bookmark: dict[str, Optional[str]],
    sorting: dict[str, Optional[str]],
) -> sqlalchemy.sql.selectable.Select:
    """Apply sorting to the running query.

    Parameters
    ----------
        statement: sqlalchemy.sql.selectable.Select
            select statement
        resource: sqlalchemy.orm.decl_api.DeclarativeMeta
            sqlalchemy declarative base
        bookmark: dict[str, Optional[str]]
            page bookmark as key-value pairs
        sorting: dict[str, Optional[str]]
            sorting instructions as key-value pairs

    Returns
    -------
        sqlalchemy.sql.selectable.Select
            updated select statement
    """
    resource_attribute: sqlalchemy.orm.attributes.InstrumentedAttribute = getattr(
        resource, SYSTEM_REQUEST_KEYS[sorting["sort_key"]]
    )
    sort_method_name: str = get_compare_and_sort_method_name(
        sorting["sort_dir"], bookmark["back"]
    )["sort_method_name"]
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


def make_jobs_query_statement(
    job_table: Type[
        cads_broker.database.SystemRequest,
    ],
    filters: dict[str, Optional[list[str]]],
    sorting: dict[str, Optional[str]],
    bookmark: dict[str, Optional[str]],
    limit: Optional[int],
) -> sqlalchemy.sql.selectable.Select:
    statement = sqlalchemy.select(job_table)
    statement = apply_jobs_filters(statement, job_table, filters)
    if bookmark["cursor"]:
        statement = apply_bookmark(statement, job_table, bookmark, sorting)
    statement = apply_sorting(statement, job_table, bookmark, sorting)
    statement = apply_limit(statement, limit)

    return statement


def make_cursor(
    jobs: list[ogc_api_processes_fastapi.responses.StatusInfo], sort_key: str, page: str
) -> str:
    if page not in ("next", "prev"):
        raise ValueError(f"page: {page} not valid")
    if page == "next":
        bookmark_element = jobs[-1]
    else:
        bookmark_element = jobs[0]
    cursor = encode_base64(str(getattr(bookmark_element, sort_key)))
    return cursor


def make_pagination_qs(
    jobs: list[ogc_api_processes_fastapi.responses.StatusInfo], sort_key: str
) -> ogc_api_processes_fastapi.responses.PaginationQueryParameters:
    pagination_qs = ogc_api_processes_fastapi.responses.PaginationQueryParameters(
        next={}, prev={}
    )
    if len(jobs) != 0:
        cursor_next = make_cursor(jobs, sort_key, "next")
        pagination_qs.next = {"cursor": cursor_next, "back": "False"}
        cursor_prev = make_cursor(jobs, sort_key, "prev")
        pagination_qs.prev = {"cursor": cursor_prev, "back": "True"}
    return pagination_qs


def validate_request(
    process_id: str,
    session: sqlalchemy.orm.Session,
    process_table: Type[cads_catalogue.database.Resource],
) -> cads_catalogue.database.BaseModel:
    """Validate retrieve process execution request.

    Check if requested dataset exists and if execution content is valid.
    In case the check is successful, returns the resource (dataset)
    associated to the process request.

    Parameters
    ----------
    process_id : str
        Process ID.
    session : sqlalchemy.orm.Session
        SQLAlchemy ORM session

    Returns
    -------
    cads_catalogue.database.BaseModel
        Resource (dataset) associated to the process request.
    """
    # TODO: implement inputs validation
    resource = lookup_resource_by_id(
        id=process_id, record=process_table, session=session
    )
    return resource


def submit_job(
    process_id: str,
    execution_content: dict[str, Any],
    resource: cads_catalogue.database.Resource,
) -> ogc_api_processes_fastapi.responses.StatusInfo:
    """Submit new job.

    Parameters
    ----------
    process_id: str
        Process ID.
    execution_content: ogc_api_processes_fastapi.models.Execute
        Body of the process execution request.
    resource: cads_catalogue.database.Resource,
        Catalogue resource corresponding to the requested retrieve process


    Returns
    -------
    ogc_api_processes_fastapi.responses.schema["StatusInfo"]
        Sumbitted job status info.
    """
    job_kwargs = adaptors.make_system_job_kwargs(
        process_id, execution_content, resource
    )
    job = cads_broker.database.create_request(
        process_id=process_id,
        **job_kwargs,
    )
    status_info = ogc_api_processes_fastapi.responses.StatusInfo(
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
        self, limit: int | None = fastapi.Query(None)
    ) -> ogc_api_processes_fastapi.responses.ProcessList:
        """Implement OGC API - Processes `GET /processes` endpoint.

        Get the list of available processes.

        Parameters
        ----------
        limit : int | None, optional
            Number of processes summaries to be returned.

        Returns
        -------
        ogc_api_processes_fastapi.responses.ProcessList
            List of available processes.
        """
        with self.reader.context_session() as session:
            if limit:
                processes_entries = session.query(self.process_table).limit(limit).all()
            else:
                processes_entries = session.query(self.process_table).all()
            processes = [
                serializers.serialize_process_summary(process)
                for process in processes_entries
            ]
        process_list = ogc_api_processes_fastapi.responses.ProcessList(
            processes=processes
        )

        return process_list

    def get_process(
        self, process_id: str = fastapi.Path(...)
    ) -> ogc_api_processes_fastapi.responses.ProcessDescription:
        """Implement OGC API - Processes `GET /processes/{process_id}` endpoint.

        Get the description of the process identified by `process_id`.

        Parameters
        ----------
        process_id : str
            Process identifier.

        Returns
        -------
        ogc_api_processes_fastapi.responses.ProcessDescription
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
                "asset": ogc_api_processes_fastapi.responses.OutputDescription(
                    title="Asset",
                    description="Downloadable asset description",
                    schema_=ogc_api_processes_fastapi.responses.SchemaItem(
                        type="object",
                        properties={
                            "value": ogc_api_processes_fastapi.responses.SchemaItem(
                                type="object",
                                properties={
                                    "type": ogc_api_processes_fastapi.responses.SchemaItem(
                                        type="string"
                                    ),
                                    "href": ogc_api_processes_fastapi.responses.SchemaItem(
                                        type="string"
                                    ),
                                    "file:checksum": ogc_api_processes_fastapi.responses.SchemaItem(
                                        type="integer"
                                    ),
                                    "file:size": ogc_api_processes_fastapi.responses.SchemaItem(
                                        type="integer"
                                    ),
                                    "file:local_path": ogc_api_processes_fastapi.responses.SchemaItem(
                                        type="string"
                                    ),
                                    "tmp:storage_option": ogc_api_processes_fastapi.responses.SchemaItem(
                                        type="object"
                                    ),
                                    "tmp:open_kwargs": ogc_api_processes_fastapi.responses.SchemaItem(
                                        type="object"
                                    ),
                                },
                            ),
                        },
                    ),
                ),
            }

        return process_description

    def post_process_execute(
        self,
        process_id: str = fastapi.Path(...),
        execution_content: dict[str, Any] = fastapi.Body(...),
    ) -> ogc_api_processes_fastapi.responses.StatusInfo:
        """Implement OGC API - Processes `POST /processes/{process_id}/execute` endpoint.

        Request execution of the process identified by `process_id`.

        Parameters
        ----------
        process_id : str
            Process identifier.
        execution_content : ogc_api_processes_fastapi.models.Execute
            Process execution details (e.g. inputs).

        Returns
        -------
        ogc_api_processes_fastapi.responses.StatusInfo
            Information on the status of the job.

        Raises
        ------
        ogc_api_processes_fastapi.exceptions.NoSuchProcess
            If the process `process_id` is not found.
        """
        with self.reader.context_session() as session:
            resource = validate_request(process_id, session, self.process_table)
            status_info = submit_job(process_id, execution_content, resource)
        return status_info

    def get_jobs(
        self,
        processID: Optional[list[str]] = fastapi.Query(None),
        status: Optional[list[str]] = fastapi.Query(None),
        limit: Optional[int] = fastapi.Query(10, ge=1, le=10000),
        sort: Optional[SortCriterion] = fastapi.Query("created"),
        dir: Optional[SortDirection] = fastapi.Query("desc"),
        cursor: Optional[str] = fastapi.Query(None, include_in_schema=False),
        back: Optional[bool] = fastapi.Query(None, include_in_schema=False),
    ) -> ogc_api_processes_fastapi.responses.JobList:
        """Implement OGC API - Processes `GET /jobs` endpoint.

        Get jobs' status information list.

        Parameters
        ----------
        processID: Optional[List[str]] = fastapi.Query(None)
            If the parameter is specified with the operation, only jobs that have a value for
            the processID property that matches one of the values specified for the processID
            parameter shall be included in the response.
        status: Optional[List[str]] = fastapi.Query(None)
            If the parameter is specified with the operation, only jobs that have a value for
            the status property that matches one of the specified values of the status parameter
            shall be included in the response.
        limit: Optional[int] = fastapi.Query(10, ge=1, le=10000)
            The response shall not contain more jobs than specified by the optional ``limit``
            parameter.
        cursor: Optional[str] = fastapi.Query(None)
            Hash string used for pagination
        back: Optional[bool] = fastapi.Query(None),
            Boolean parameter used for pagination

        Returns
        -------
        ogc_api_processes_fastapi.responses.JobList
            Information on the status of the job.
        """
        print(cursor)
        session_obj = cads_broker.database.ensure_session_obj(None)
        with session_obj() as session:
            statement = make_jobs_query_statement(
                self.job_table,
                filters={"process_id": processID, "status": status},
                sorting={"sort_key": sort, "sort_dir": dir},
                bookmark={"cursor": cursor, "back": back},
                limit=limit,
            )
            job_entries = session.scalars(statement).all()
        if back:
            job_entries = reversed(job_entries)
        jobs = [
            ogc_api_processes_fastapi.responses.StatusInfo(
                type="process",
                jobID=job.request_uid,
                processID=job.process_id,
                status=job.status,
                created=job.created_at,
                started=job.started_at,
                finished=job.finished_at,
                updated=job.updated_at,
            )
            for job in job_entries
        ]
        job_list = ogc_api_processes_fastapi.responses.JobList(jobs=jobs)
        pagination_qs = make_pagination_qs(jobs, sort_key=sort)
        job_list._pagination_qs = pagination_qs

        return job_list

    def get_job(
        self, job_id: str = fastapi.Path(...)
    ) -> ogc_api_processes_fastapi.responses.StatusInfo:
        """Implement OGC API - Processes `GET /jobs/{job_id}` endpoint.

        Get status information for the job identifed by `job_id`.

        Parameters
        ----------
        job_id : str
            Identifier of the job.

        Returns
        -------
        ogc_api_processes_fastapi.responses.StatusInfo
            Information on the status of the job.

        Raises
        ------
        ogc_api_processes_fastapi.exceptions.NoSuchJob
            If the job `job_id` is not found.
        """
        try:
            job = cads_broker.database.get_request(request_uid=job_id)
        except (
            sqlalchemy.exc.StatementError,
            sqlalchemy.orm.exc.NoResultFound,
        ):
            raise ogc_api_processes_fastapi.exceptions.NoSuchJob(
                f"Can't find the job {job_id}."
            )
        status_info = ogc_api_processes_fastapi.responses.StatusInfo(
            processID=job.process_id,
            type="process",
            jobID=job.request_uid,
            status=job.status,
            created=job.created_at,
            started=job.started_at,
            finished=job.finished_at,
            updated=job.updated_at,
        )
        return status_info

    def get_job_results(
        self, job_id: str = fastapi.Path(...)
    ) -> ogc_api_processes_fastapi.responses.Results:
        """Implement OGC API - Processes `GET /jobs/{job_id}/results` endpoint.

        Get results for the job identifed by `job_id`.

        Parameters
        ----------
        job_id : str
            Identifier of the job.

        Returns
        -------
        ogc_api_processes_fastapi.responses.Results
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
        try:
            job = cads_broker.database.get_request(request_uid=job_id)
        except (
            sqlalchemy.exc.StatementError,
            sqlalchemy.orm.exc.NoResultFound,
        ):
            raise ogc_api_processes_fastapi.exceptions.NoSuchJob(
                f"Can't find the job {job_id}."
            )
        if job.status == "successful":
            asset_value = cads_broker.database.get_request_result(
                request_uid=job.request_uid
            )["args"][0]
            return {"asset": {"value": asset_value}}
        elif job.status == "failed":
            raise ogc_api_processes_fastapi.exceptions.JobResultsFailed(
                type="RuntimeError",
                detail=job.response_traceback,
                status_code=fastapi.status.HTTP_400_BAD_REQUEST,
            )
        elif job.status in ("accepted", "running"):
            raise ogc_api_processes_fastapi.exceptions.ResultsNotReady(
                f"Status of {job_id} is {job.status}."
            )
