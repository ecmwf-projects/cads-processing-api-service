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

import uuid

import attrs
import cacholote.extra_encoders
import cads_broker.database
import cads_catalogue.config
import cads_catalogue.database
import fastapi
import ogc_api_processes_fastapi
import ogc_api_processes_fastapi.clients
import ogc_api_processes_fastapi.exceptions
import ogc_api_processes_fastapi.models
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.orm.attributes
import sqlalchemy.orm.decl_api
import sqlalchemy.orm.exc
import sqlalchemy.sql.selectable
import structlog

from . import adaptors, auth, config, db_utils, models, serializers, utils

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


@attrs.define
class DatabaseClient(ogc_api_processes_fastapi.clients.BaseClient):
    """Database implementation of the OGC API - Processes endpoints.

    Attributes
    ----------
    process_table: Type[cads_catalogue.database.Resource]
        Processes record/table.
    """

    process_table: type[cads_catalogue.database.Resource] = attrs.field(
        default=cads_catalogue.database.Resource
    )
    job_table: type[cads_broker.database.SystemRequest] = attrs.field(
        default=cads_broker.database.SystemRequest
    )

    def get_processes(
        self,
        limit: int | None = fastapi.Query(10, ge=1, le=10000),
        sortby: utils.ProcessSortCriterion
        | None = fastapi.Query(utils.ProcessSortCriterion.resource_uid_asc),
        cursor: str | None = fastapi.Query(None, include_in_schema=False),
        back: bool | None = fastapi.Query(None, include_in_schema=False),
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
        statement = sqlalchemy.select(self.process_table)
        sort_key, sort_dir = utils.parse_sortby(sortby.name)
        if cursor:
            statement = utils.apply_bookmark(
                statement, self.process_table, cursor, back, sort_key, sort_dir
            )
        statement = utils.apply_sorting(
            statement, self.process_table, back, sort_key, sort_dir
        )
        statement = utils.apply_limit(statement, limit)
        catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker()
        with catalogue_sessionmaker() as catalogue_session:
            processes_entries = catalogue_session.scalars(statement).all()
        processes = [
            serializers.serialize_process_summary(process)
            for process in processes_entries
        ]
        if back:
            processes = list(reversed(processes))
        process_list = ogc_api_processes_fastapi.models.ProcessList(processes=processes)
        pagination_qs = utils.make_pagination_qs(processes, sort_key=sortby.lstrip("-"))
        process_list._pagination_qs = pagination_qs
        return process_list

    def get_process(
        self,
        response: fastapi.Response,
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
        catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker()
        with catalogue_sessionmaker() as catalogue_session:
            resource = utils.lookup_resource_by_id(
                id=process_id, record=self.process_table, session=catalogue_session
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

        response.headers[
            "cache-control"
        ] = config.ensure_settings().public_cache_control

        return process_description

    def post_process_execution(
        self,
        process_id: str = fastapi.Path(...),
        execution_content: models.Execute = fastapi.Body(...),
        auth_header: tuple[str, str] = fastapi.Depends(auth.get_auth_header),
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
        user_uid = auth.authenticate_user(auth_header)
        structlog.contextvars.bind_contextvars(user_uid=user_uid)
        logger.info("User authenticated")
        stored_accepted_licences = auth.get_stored_accepted_licences(auth_header)
        execution_content = execution_content.dict()
        catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker()
        with catalogue_sessionmaker() as catalogue_session:
            resource = utils.lookup_resource_by_id(
                id=process_id, record=self.process_table, session=catalogue_session
            )
        auth.validate_licences(
            execution_content, stored_accepted_licences, resource.licences
        )
        job_id = str(uuid.uuid4())
        structlog.contextvars.bind_contextvars(job_id=job_id)
        job_kwargs = adaptors.make_system_job_kwargs(
            process_id, execution_content, resource
        )
        compute_sessionmaker = db_utils.get_compute_sessionmaker()
        with compute_sessionmaker() as compute_session:
            job = cads_broker.database.create_request(
                session=compute_session,
                request_uid=job_id,
                user_uid=user_uid,
                process_id=process_id,
                **job_kwargs,
            )
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

    def get_jobs(
        self,
        processID: list[str] | None = fastapi.Query(None),
        status: list[str] | None = fastapi.Query(None),
        limit: int | None = fastapi.Query(10, ge=1, le=10000),
        sortby: utils.JobSortCriterion
        | None = fastapi.Query(utils.JobSortCriterion.created_at_desc),
        cursor: str | None = fastapi.Query(None, include_in_schema=False),
        back: bool | None = fastapi.Query(None, include_in_schema=False),
        auth_header: tuple[str, str] = fastapi.Depends(auth.get_auth_header),
    ) -> models.JobList:
        """Implement OGC API - Processes `GET /jobs` endpoint.

        Get jobs' status information list.

        Parameters
        ----------
        processID: Optional[List[str]]
            If the parameter is specified with the operation, only jobs that have a
            value for the processID property that matches one of the values specified
            for the processID nparameter shall be included in the response.
        status: Optional[List[str]]
            If the parameter is specified with the operation, only jobs that have a
            value for the status property that matches one of the specified values of
            the status parameter shall be included in the response.
        limit: Optional[int]
            The response shall not contain more jobs than specified by the optional
            ``limit`` parameter.
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
        user_uid = auth.authenticate_user(auth_header)
        metadata_filters = {"user_uid": [str(user_uid)] if user_uid else []}
        job_filters = {"process_id": processID, "status": status}
        sort_key, sort_dir = utils.parse_sortby(sortby.name)
        statement = sqlalchemy.select(self.job_table)
        statement = utils.apply_metadata_filters(
            statement, self.job_table, metadata_filters
        )
        statement = utils.apply_job_filters(statement, self.job_table, job_filters)
        if cursor:
            statement = utils.apply_bookmark(
                statement,
                self.job_table,
                cursor,
                back,
                sort_key,
                sort_dir,
            )
        statement = utils.apply_sorting(
            statement, self.job_table, back, sort_key, sort_dir
        )
        statement = utils.apply_limit(statement, limit)
        compute_sessionmaker = db_utils.get_compute_sessionmaker()
        with compute_sessionmaker() as compute_session:
            job_entries = compute_session.scalars(statement).all()
            if back:
                job_entries = reversed(job_entries)
            jobs = [
                utils.make_status_info(
                    job=utils.dictify_job(job), session=compute_session
                )
                for job in job_entries
            ]
        job_list = models.JobList(jobs=jobs)
        pagination_qs = utils.make_pagination_qs(jobs, sort_key=sortby.lstrip("-"))
        job_list._pagination_qs = pagination_qs

        return job_list

    def get_job(
        self,
        job_id: str = fastapi.Path(...),
        auth_header: tuple[str, str] = fastapi.Depends(auth.get_auth_header),
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
        user_uid = auth.authenticate_user(auth_header)
        compute_sessionmaker = db_utils.get_compute_sessionmaker()
        with compute_sessionmaker() as compute_session:
            job = utils.get_job_from_broker_db(job_id=job_id, session=compute_session)
            status_info = utils.make_status_info(job=job, session=compute_session)
        auth.verify_permission(user_uid, job)
        return status_info

    def get_job_results(
        self,
        job_id: str = fastapi.Path(...),
        auth_header: tuple[str, str] = fastapi.Depends(auth.get_auth_header),
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
        user_uid = auth.authenticate_user(auth_header)
        compute_sessionmaker = db_utils.get_compute_sessionmaker()
        with compute_sessionmaker() as compute_session:
            job = utils.get_job_from_broker_db(job_id=job_id, session=compute_session)
            auth.verify_permission(user_uid, job)
            results = utils.get_results_from_broker_db(job=job, session=compute_session)
        return results

    def delete_job(
        self,
        job_id: str = fastapi.Path(...),
        auth_header: tuple[str, str] = fastapi.Depends(auth.get_auth_header),
    ) -> ogc_api_processes_fastapi.models.StatusInfo:
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
        ogc_api_processes_fastapi.models.StatusInfo
            Information on the status of the job.

        Raises
        ------
        ogc_api_processes_fastapi.exceptions.NoSuchJob
            If the job `job_id` is not found.
        """
        structlog.contextvars.bind_contextvars(job_id=job_id)
        user_uid = auth.authenticate_user(auth_header)
        structlog.contextvars.bind_contextvars(user_id=user_uid)
        logger.info("User authenticated")
        compute_sessionmaker = db_utils.get_compute_sessionmaker()
        with compute_sessionmaker() as compute_session:
            job = utils.get_job_from_broker_db(job_id=job_id, session=compute_session)
            auth.verify_permission(user_uid, job)
            job = cads_broker.database.delete_request(
                request_uid=job_id, session=compute_session
            )
            job = utils.dictify_job(job)
            status_info = utils.make_status_info(
                job, session=compute_session, add_results=False
            )
        return status_info
