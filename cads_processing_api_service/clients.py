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

import logging
from typing import Any, Optional, Type

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
import sqlalchemy.orm.exc
import sqlalchemy.sql.selectable

from . import adaptors, exceptions, rfc5424_log, serializers

logger = logging.getLogger(__name__)


def lookup_id(
    id: str,
    record: Type[cads_catalogue.database.BaseModel],
    session: sqlalchemy.orm.Session,
) -> cads_catalogue.database.BaseModel:

    try:
        row = session.query(record).filter(record.resource_uid == id).one()
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NotFoundError(f"{record.__name__} {id} not found")
    return row


def lookup_resource_by_id(
    id: str,
    session: sqlalchemy.orm.Session,
    process_table: Type[cads_catalogue.database.Resource],
) -> cads_catalogue.database.BaseModel:

    try:
        process = lookup_id(id=id, record=process_table, session=session)
    except exceptions.NotFoundError:
        raise ogc_api_processes_fastapi.exceptions.NoSuchProcess()

    return process


def apply_jobs_filters(
    statement: sqlalchemy.sql.selectable.Select,
    resource: Type[cads_broker.database.SystemRequest],
    filters: dict[str, Optional[list[str]]],
):
    """Apply search filters to the running query.

    Parameters
    ----------
        statement: sqlalchemy.sql.selectable.Select
            select statement
        resource: Type[cads_broker.database.SystemRequest]
            sqlalchemy declarative base
        filters: dict[str, Optional[list[str]]]
            filters as key-value pairs
    """
    for filter_key, filter_value in filters.items():
        if filter_value:
            statement = statement.where(getattr(resource, filter_key).in_(filter_value))
    return statement


def apply_limit(
    statement: sqlalchemy.sql.selectable.Select,
    limit: Optional[int],
):
    """Apply limit to the running query.

    Parameters
    ----------
        statement: sqlalchemy.sql.selectable.Select
            select statement
        limit: Optional[int]
            requested number of results to be shown
    """
    statement = statement.limit(limit)
    return statement


def make_jobs_query_statement(
    job_table: Type[
        cads_broker.database.SystemRequest,
    ],
    filters: dict[str, Optional[list[str]]],
    limit: Optional[int],
) -> sqlalchemy.sql.selectable.Select:
    statement = sqlalchemy.select(job_table)
    statement = apply_jobs_filters(statement, job_table, filters)
    statement = apply_limit(statement, limit)

    return statement


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
    resource = lookup_resource_by_id(process_id, session, process_table)
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

    # Log job submission info
    rfc5424_log.log_job_submission(logger, job_kwargs)

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
        list[ogc_api_processes_fastapi.responses.schema["ProcessSummary"]]
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
        ogc_api_processes_fastapi.responses.schema["ProcessDescription"]
            Process description.

        Raises
        ------
        ogc_api_processes_fastapi.exceptions.NoSuchProcess
            If the process `process_id` is not found.
        """
        with self.reader.context_session() as session:
            resource = lookup_resource_by_id(process_id, session, self.process_table)
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
        ogc_api_processes_fastapi.responses.schema["StatusInfo"]
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

        Returns
        -------
        list[ogc_api_processes_fastapi.responses.schema["StatusInfo"]]
            Information on the status of the job.
        """
        session_obj = cads_broker.database.ensure_session_obj(None)
        with session_obj() as session:
            statement = make_jobs_query_statement(
                self.job_table,
                filters={"process_id": processID, "status": status},
                limit=limit,
            )
            jobs_entries = session.scalars(statement).all()
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
            for job in jobs_entries
        ]
        job_list = ogc_api_processes_fastapi.responses.JobList(jobs=jobs)

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
        ogc_api_processes_fastapi.responses.schema["StatusInfo"]
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
        ogc_api_processes_fastapi.responses.schema["Results"]
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
