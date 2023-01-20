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

from . import dependencies, models, serializers, utils

logger = logging.getLogger(__name__)


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
        catalogue_session: sqlalchemy.orm.Session = fastapi.Depends(
            dependencies.get_catalogue_session
        ),
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
        process_id: str = fastapi.Path(...),
        catalgoue_session: sqlalchemy.orm.Session = fastapi.Depends(
            dependencies.get_catalogue_session
        ),
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
        resource = utils.lookup_resource_by_id(
            id=process_id, record=self.process_table, session=catalgoue_session
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
        user: dict[str, str] = fastapi.Depends(dependencies.validate_token),
        catalogue_session: sqlalchemy.orm.Session = fastapi.Depends(
            dependencies.get_catalogue_session
        ),
        compute_session: sqlalchemy.orm.Session = fastapi.Depends(
            dependencies.get_compute_session
        ),
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
        resource = utils.validate_request(
            process_id,
            execution_content,
            user.get("auth_header", None),
            catalogue_session,
            self.process_table,
        )
        status_info = utils.submit_job(
            user_id, process_id, execution_content, resource, compute_session
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
        user: dict[str, str] = fastapi.Depends(dependencies.validate_token),
        compute_session: sqlalchemy.orm.Session = fastapi.Depends(
            dependencies.get_compute_session
        ),
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
        user_id = user.get("id", None)
        metadata_filters = {"user_id": [str(user_id)] if user_id else []}
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
        job_entries = compute_session.scalars(statement).all()
        if back:
            job_entries = reversed(job_entries)
        jobs = [
            utils.make_status_info(job=utils.dictify_job(job), session=compute_session)
            for job in job_entries
        ]
        job_list = models.JobList(jobs=jobs)
        pagination_qs = utils.make_pagination_qs(jobs, sort_key=sortby.lstrip("-"))
        job_list._pagination_qs = pagination_qs

        return job_list

    def get_job(
        self,
        job_id: str = fastapi.Path(...),
        user: dict[str, str] = fastapi.Depends(dependencies.validate_token),
        compute_session: sqlalchemy.orm.Session = fastapi.Depends(
            dependencies.get_compute_session
        ),
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
        job = utils.get_job_from_broker_db(job_id=job_id, session=compute_session)
        utils.verify_permission(user, job)
        status_info = utils.make_status_info(job=job, session=compute_session)
        return status_info

    def get_job_results(
        self,
        job_id: str = fastapi.Path(...),
        user: dict[str, str] = fastapi.Depends(dependencies.validate_token),
        compute_session: sqlalchemy.orm.Session = fastapi.Depends(
            dependencies.get_compute_session
        ),
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
        job = utils.get_job_from_broker_db(job_id=job_id, session=compute_session)
        utils.verify_permission(user, job)
        results = utils.get_results_from_broker_db(job=job, session=compute_session)
        return results

    def delete_job(
        self,
        job_id: str = fastapi.Path(...),
        user: dict[str, str] = fastapi.Depends(dependencies.validate_token),
        compute_session: sqlalchemy.orm.Session = fastapi.Depends(
            dependencies.get_compute_session
        ),
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
        job = utils.get_job_from_broker_db(job_id=job_id, session=compute_session)
        utils.verify_permission(user, job)
        job = cads_broker.database.delete_request(request_uid=job_id)
        job = utils.dictify_job(job)
        status_info = utils.make_status_info(
            job, session=compute_session, add_results=False
        )
        return status_info
