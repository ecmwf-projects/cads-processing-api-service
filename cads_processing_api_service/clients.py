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

from . import adaptors, auth, config, db_utils, models, serializers, translators, utils
from .metrics import handle_download_metrics

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


@attrs.define
class DatabaseClient(ogc_api_processes_fastapi.clients.BaseClient):
    """Database implementation of the OGC API - Processes endpoints.

    Attributes
    ----------
    process_table: type[cads_catalogue.database.Resource]
        Process record/table.
    job_table: type[cads_broker.database.SystemRequest]
        Job record/table.
    """

    process_table = cads_catalogue.database.Resource
    process_data_table = cads_catalogue.database.ResourceData
    job_table = cads_broker.database.SystemRequest

    def get_processes(
        self,
        limit: int | None = fastapi.Query(10, ge=1, le=10000),
        sortby: utils.ProcessSortCriterion
        | None = fastapi.Query(utils.ProcessSortCriterion.resource_uid_asc),
        cursor: str | None = fastapi.Query(None, include_in_schema=False),
        back: bool | None = fastapi.Query(None, include_in_schema=False),
    ) -> ogc_api_processes_fastapi.models.ProcessList:
        """Implement OGC API - Processes `GET /processes` endpoint.

        Get the list of available processes' summaries.

        Parameters
        ----------
        limit : int | None, optional
            Specifies the number of process summaries to be included in the response.
        sortby : utils.ProcessSortCriterion | None, optional
            Specifies the sorting criterion for results. By default
            results are sorted by resource (process) uid in ascending alphabetical order.
        cursor : str | None, optional
            Hash string representing the reference to a particular process, used for pagination.
        back : bool | None, optional
            Specifies in which sense the list of processes should be traversed, used for pagination.
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
        catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker(
            db_utils.ConnectionMode.read
        )
        with catalogue_sessionmaker() as catalogue_session:
            processes_entries = catalogue_session.scalars(statement).all()
        processes = [
            serializers.serialize_process_summary(process)
            for process in processes_entries
        ]
        if back:
            processes = list(reversed(processes))
        process_list = ogc_api_processes_fastapi.models.ProcessList(
            processes=processes, links=[ogc_api_processes_fastapi.models.Link(href="")]
        )
        pagination_query_params = utils.make_pagination_query_params(
            processes, sort_key=sortby.lstrip("-")
        )
        process_list._pagination_query_params = pagination_query_params
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
        response : fastapi.Response
            fastapi.Response object.
        process_id : str
            Process identifier.

        Returns
        -------
        ogc_api_processes_fastapi.models.ProcessDescription
            Process description.
        """
        catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker(
            db_utils.ConnectionMode.read
        )
        with catalogue_sessionmaker() as catalogue_session:
            resource = utils.lookup_resource_by_id(
                resource_id=process_id,
                table=self.process_table,
                session=catalogue_session,
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
        execution_content: ogc_api_processes_fastapi.models.Execute = fastapi.Body(...),
        auth_header: tuple[str, str] = fastapi.Depends(auth.get_auth_header),
        portal_header: str
        | None = fastapi.Header(None, alias=config.PORTAL_HEADER_NAME),
    ) -> models.StatusInfo:
        """Implement OGC API - Processes `POST /processes/{process_id}/execution` endpoint.

        Request execution of a process.

        Parameters
        ----------
        process_id : str
            Process identifier.
        execution_content : models.Execute
            Details needed for the process execution.
        auth_header : tuple[str, str], optional
            Authorization header.

        Returns
        -------
        models.StatusInfo
            Submitted job's status information.
        """
        user_uid = auth.authenticate_user(auth_header, portal_header)
        structlog.contextvars.bind_contextvars(user_uid=user_uid)
        accepted_licences = auth.get_accepted_licences(auth_header)
        execution_content = execution_content.model_dump()
        catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker(
            db_utils.ConnectionMode.read
        )
        with catalogue_sessionmaker() as catalogue_session:
            resource: cads_catalogue.database.Resource = utils.lookup_resource_by_id(
                resource_id=process_id,
                table=self.process_table,
                session=catalogue_session,
            )
        adaptor = adaptors.instantiate_adaptor(resource)
        licences = adaptor.get_licences(execution_content)
        auth.validate_licences(accepted_licences, licences)
        job_id = str(uuid.uuid4())
        structlog.contextvars.bind_contextvars(job_id=job_id)
        job_kwargs = adaptors.make_system_job_kwargs(
            resource, execution_content, adaptor.resources
        )
        compute_sessionmaker = db_utils.get_compute_sessionmaker(
            mode=db_utils.ConnectionMode.write
        )
        with compute_sessionmaker() as compute_session:
            job = cads_broker.database.create_request(
                session=compute_session,
                request_uid=job_id,
                origin=auth.REQUEST_ORIGIN[auth_header[0]],
                user_uid=user_uid,
                process_id=process_id,
                portal=resource.portal,
                qos_tags=resource.qos_tags,
                **job_kwargs,
            )
        status_info = utils.make_status_info(job)
        return status_info

    def get_jobs(
        self,
        processID: list[str] | None = fastapi.Query(None),
        status: list[ogc_api_processes_fastapi.models.StatusCode]
        | None = fastapi.Query(None),
        limit: int | None = fastapi.Query(10, ge=1, le=10000),
        sortby: utils.JobSortCriterion
        | None = fastapi.Query(utils.JobSortCriterion.created_at_desc),
        cursor: str | None = fastapi.Query(None, include_in_schema=False),
        back: bool | None = fastapi.Query(None, include_in_schema=False),
        auth_header: tuple[str, str] = fastapi.Depends(auth.get_auth_header),
        portal_header: str
        | None = fastapi.Header(None, alias=config.PORTAL_HEADER_NAME),
    ) -> models.JobList:
        """Implement OGC API - Processes `GET /jobs` endpoint.

        Get the list of submitted jobs, alongside information on their status.

        Parameters
        ----------
        processID : list[str] | None, optional
            If specified, only jobs that have a value for the processID property
            matching one of the specified values shall be included in the response.
        status : list[ogc_api_processes_fastapi.models.StatusCode] | None, optional
            If specified, only jobs that have a value for the status property matching
            one of the specified values of shall be included in the response.
        limit : int | None, optional
            Specifies the number of jobs to be included in the response.
        sortby : utils.JobSortCriterion | None, optional
            Specifies the sorting criterion for results. By default results are sorted
            by the job's creation date in descending order.
        cursor : str | None, optional
            Hash string representing the reference to a particular job, used for pagination.
        back : bool | None, optional
            Specifies in which sense the list of processes should be traversed, used for pagination.
        auth_header : tuple[str, str], optional
            Authorization header

        Returns
        -------
        models.JobList
            List of jobs status information.
        """
        user_uid = auth.authenticate_user(auth_header, portal_header)
        portals = [p.strip() for p in portal_header.split(",")]
        job_filters = {
            "process_id": processID,
            "status": status,
            "user_uid": [user_uid],
            "portal": portals,
        }
        sort_key, sort_dir = utils.parse_sortby(sortby.name)
        statement = sqlalchemy.select(self.job_table)
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
        compute_sessionmaker = db_utils.get_compute_sessionmaker(
            mode=db_utils.ConnectionMode.read
        )
        with compute_sessionmaker() as compute_session:
            job_entries = compute_session.scalars(statement).all()
        if back:
            job_entries = reversed(job_entries)
        jobs = []
        catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker(
            db_utils.ConnectionMode.read
        )
        for job in job_entries:
            with catalogue_sessionmaker() as catalogue_session:
                (dataset_title,) = utils.get_resource_properties(
                    resource_id=job.process_id,
                    properties="title",
                    table=self.process_table,
                    session=catalogue_session,
                )
            results = utils.parse_results_from_broker_db(job)
            jobs.append(
                utils.make_status_info(
                    job=job,
                    results=results,
                    dataset_metadata={"title": dataset_title},
                )
            )
        job_list = models.JobList(
            jobs=jobs, links=[ogc_api_processes_fastapi.models.Link(href="")]
        )
        pagination_query_params = utils.make_pagination_query_params(
            jobs, sort_key=sortby.lstrip("-")
        )
        job_list._pagination_query_params = pagination_query_params

        return job_list

    def get_job(
        self,
        job_id: str = fastapi.Path(...),
        auth_header: tuple[str, str] = fastapi.Depends(auth.get_auth_header),
        portal_header: str
        | None = fastapi.Header(None, alias=config.PORTAL_HEADER_NAME),
        statistics: bool = fastapi.Query(False),
        request: bool = fastapi.Query(False),
        log: bool = fastapi.Query(False),
    ) -> models.StatusInfo:
        """Implement OGC API - Processes `GET /jobs/{job_id}` endpoint.

        Get status information for the job identifed by `job_id`.

        Parameters
        ----------
        job_id : str
            Job identifer.
        auth_header : tuple[str, str], optional
            Authorization header
        portal_header : str | None, optional
            Portal header
        statistics : bool, optional
            Whether to include job statistics in the response
        request : bool, optional
            Whether to include the request in the response
        log : bool, optional
            Whether to include the job's log in the response

        Returns
        -------
        models.StatusInfo
            Job status information.
        """
        user_uid = auth.authenticate_user(auth_header, portal_header)
        portals = [p.strip() for p in portal_header.split(",")]
        try:
            compute_sessionmaker = db_utils.get_compute_sessionmaker(
                mode=db_utils.ConnectionMode.read
            )
            with compute_sessionmaker() as compute_session:
                job = utils.get_job_from_broker_db(
                    job_id=job_id, session=compute_session
                )
                if statistics:
                    job_statistics = utils.collect_job_statistics(job, compute_session)
        except ogc_api_processes_fastapi.exceptions.NoSuchJob:
            compute_sessionmaker = db_utils.get_compute_sessionmaker(
                mode=db_utils.ConnectionMode.write
            )
            with compute_sessionmaker() as compute_session:
                job = utils.get_job_from_broker_db(
                    job_id=job_id, session=compute_session
                )
                if statistics:
                    job_statistics = utils.collect_job_statistics(job, compute_session)
        if job.portal not in portals:
            raise ogc_api_processes_fastapi.exceptions.NoSuchJob()
        auth.verify_permission(user_uid, job)
        kwargs = {}
        if request:
            request_ids = job.request_body["request"]
            catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker(
                db_utils.ConnectionMode.read
            )
            with catalogue_sessionmaker() as catalogue_session:
                (form_data,) = utils.get_resource_properties(
                    resource_id=job.process_id,
                    properties="form_data",
                    table=self.process_data_table,
                    session=catalogue_session,
                )
            kwargs["request"] = {
                "ids": request_ids,
                "labels": translators.translate_request_ids_into_labels(
                    request_ids, form_data
                ),
            }
        if statistics:
            kwargs["statistics"] = {
                **job_statistics,
                "qos_status": cads_broker.database.get_qos_status_from_request(job),
            }
        if log:
            kwargs["log"] = utils.extract_job_log(job)
        status_info = utils.make_status_info(job=job, **kwargs)
        return status_info

    def get_job_results(
        self,
        job_id: str = fastapi.Path(...),
        auth_header: tuple[str, str] = fastapi.Depends(auth.get_auth_header),
        portal_header: str
        | None = fastapi.Header(None, alias=config.PORTAL_HEADER_NAME),
    ) -> ogc_api_processes_fastapi.models.Results:
        """Implement OGC API - Processes `GET /jobs/{job_id}/results` endpoint.

        Get results for the job identifed by `job_id`.

        Parameters
        ----------
        job_id : str
            Identifier of the job.
        auth_header : tuple[str, str], optional
            Authorization header

        Returns
        -------
        ogc_api_processes_fastapi.models.Results
            Job results.
        """
        structlog.contextvars.bind_contextvars(job_id=job_id)
        user_uid = auth.authenticate_user(auth_header, portal_header)
        structlog.contextvars.bind_contextvars(user_id=user_uid)
        try:
            compute_sessionmaker = db_utils.get_compute_sessionmaker(
                mode=db_utils.ConnectionMode.read
            )
            with compute_sessionmaker() as compute_session:
                job = utils.get_job_from_broker_db(
                    job_id=job_id, session=compute_session
                )
            auth.verify_permission(user_uid, job)
            results = utils.get_results_from_job(job=job)
        except (
            ogc_api_processes_fastapi.exceptions.NoSuchJob,
            ogc_api_processes_fastapi.exceptions.ResultsNotReady,
        ):
            compute_sessionmaker = db_utils.get_compute_sessionmaker(
                mode=db_utils.ConnectionMode.write
            )
            with compute_sessionmaker() as compute_session:
                job = utils.get_job_from_broker_db(
                    job_id=job_id, session=compute_session
                )
            auth.verify_permission(user_uid, job)
            results = utils.get_results_from_job(job=job)
        handle_download_metrics(job.process_id, results)
        return results

    def delete_job(
        self,
        job_id: str = fastapi.Path(...),
        auth_header: tuple[str, str] = fastapi.Depends(auth.get_auth_header),
        portal_header: str
        | None = fastapi.Header(None, alias=config.PORTAL_HEADER_NAME),
    ) -> ogc_api_processes_fastapi.models.StatusInfo:
        """Implement OGC API - Processes `DELETE /jobs/{job_id}` endpoint.

        Dismiss the job identifed by `job_id`.

        Parameters
        ----------
        job_id : str
            Identifier of the job.
        auth_header : tuple[str, str], optional
            Authorization header.

        Returns
        -------
        ogc_api_processes_fastapi.models.StatusInfo
            Job status information
        """
        structlog.contextvars.bind_contextvars(job_id=job_id)
        user_uid = auth.authenticate_user(auth_header, portal_header)
        structlog.contextvars.bind_contextvars(user_id=user_uid)
        compute_sessionmaker = db_utils.get_compute_sessionmaker(
            mode=db_utils.ConnectionMode.write
        )
        with compute_sessionmaker() as compute_session:
            job = utils.get_job_from_broker_db(job_id=job_id, session=compute_session)
            auth.verify_permission(user_uid, job)
            job = cads_broker.database.set_request_status(
                request_uid=job_id, status="dismissed", session=compute_session
            )
            job = utils.dictify_job(job)
        status_info = utils.make_status_info(job)
        return status_info
