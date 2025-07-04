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

import datetime
import uuid

import attrs
import cacholote.extra_encoders
import cads_adaptors.exceptions
import cads_broker.database
import cads_catalogue.database
import fastapi
import ogc_api_processes_fastapi
import ogc_api_processes_fastapi.clients
import ogc_api_processes_fastapi.exceptions
import ogc_api_processes_fastapi.models
import sqlalchemy
import structlog

from . import (
    adaptors,
    auth,
    config,
    db_utils,
    exceptions,
    limits,
    models,
    serializers,
    translators,
    utils,
)
from .metrics import handle_download_metrics

SETTINGS = config.settings

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

    endpoints_description = {
        "GetProcesses": "Get the list of available processes' summaries.",
        "GetProcess": "Get the description of the process identified by `process_id`.",
        "PostProcessExecution": "Request execution of a process.",
        "GetJobs": "Get the list of submitted jobs, alongside information on their status.",
        "GetJob": "Get status information for the job identifed by `job_id`.",
        "GetJobResults": "Get results for the job identifed by `job_id`.",
        "DeleteJob": "Dismiss the job identifed by `job_id`.",
    }

    @exceptions.exception_logger
    def get_processes(
        self,
        limit: int | None = fastapi.Query(
            10, ge=1, le=10000, description="Maximum number of results to return."
        ),
        sortby: utils.ProcessSortCriterion | None = fastapi.Query(
            utils.ProcessSortCriterion.resource_uid_asc,
            description="Sorting criterion.",
        ),
        cursor: str | None = fastapi.Query(None, include_in_schema=False),
        back: bool | None = fastapi.Query(None, include_in_schema=False),
        portals: tuple[str] | None = fastapi.Depends(utils.get_portals),
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
        portals: tuple[str] | None
            Portals
        """
        statement = sqlalchemy.select(self.process_table)
        sort_key, sort_dir = utils.parse_sortby(sortby.name)
        if portals:
            statement = statement.filter(self.process_table.portal.in_(portals))
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

    @exceptions.exception_logger
    def get_process(
        self,
        response: fastapi.Response,
        process_id: str = fastapi.Path(..., description="Process identifier."),
        portals: tuple[str] | None = fastapi.Depends(utils.get_portals),
    ) -> ogc_api_processes_fastapi.models.ProcessDescription:
        """Implement OGC API - Processes `GET /processes/{process_id}` endpoint.

        Get the description of the process identified by `process_id`.

        Parameters
        ----------
        response : fastapi.Response
            fastapi.Response object.
        process_id : str
            Process identifier.
        portals: tuple[str] | None
            Portals

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
                portals=portals,
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

        response.headers["cache-control"] = SETTINGS.public_cache_control

        return process_description

    @exceptions.exception_logger
    def post_process_execution(
        self,
        request: fastapi.Request,
        process_id: str = fastapi.Path(..., description="Process identifier."),
        execution_content: models.Execute = fastapi.Body(...),
        auth_info: models.AuthInfo = fastapi.Depends(auth.get_auth_info),
    ) -> models.StatusInfo:
        """Implement OGC API - Processes `POST /processes/{process_id}/execution` endpoint.

        Request execution of a process.

        Parameters
        ----------
        request: fastapi.Request
            API Request object.
        process_id : str
            Process identifier.
        execution_content : models.Execute
            Details needed for the process execution.
        auth_info : models.AuthInfo
            Authentication information.

        Returns
        -------
        models.StatusInfo
            Submitted job's status information.
        """
        structlog.contextvars.bind_contextvars(user_uid=auth_info.user_uid)
        _ = limits.check_rate_limits(
            SETTINGS.rate_limits.process_execution.post,
            auth_info,
        )
        request_body = execution_content.model_dump()
        catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker(
            db_utils.ConnectionMode.read
        )
        with catalogue_sessionmaker() as catalogue_session:
            dataset: cads_catalogue.database.Resource = utils.lookup_resource_by_id(
                resource_id=process_id,
                table=self.process_table,
                session=catalogue_session,
                load_messages=True,
                portals=auth_info.portals,
            )
        auth.verify_if_disabled(dataset.disabled_reason, auth_info.user_role)
        adaptor_properties = adaptors.get_adaptor_properties(dataset)
        adaptor = adaptors.instantiate_adaptor(adaptor_properties=adaptor_properties)
        try:
            request_inputs = adaptor.normalise_request(request_body.get("inputs", {}))
            _ = adaptor.check_validity(request_inputs)
        except cads_adaptors.exceptions.InvalidRequest as exc:
            raise exceptions.InvalidRequest(detail=str(exc)) from exc
        if dataset.api_enforce_constraints:
            try:
                _ = adaptor.apply_constraints(request_inputs)
            except (
                cads_adaptors.exceptions.ParameterError,
                cads_adaptors.exceptions.InvalidRequest,
            ) as exc:
                raise exceptions.InvalidRequest(detail=str(exc)) from exc
        costs = auth.verify_cost(
            request_inputs, adaptor_properties, auth_info.request_origin
        )
        required_licences = adaptor.get_licences(request_inputs)
        if auth_info.user_uid != "anonymous":
            accepted_licences = auth.get_accepted_licences(auth_info.auth_header)
            request_url = str(request.url)
            _ = auth.verify_licences(
                accepted_licences,
                required_licences,
                request_url,
                process_id,
                dataset.portal,
            )
            job_message = None
        else:
            job_message = SETTINGS.anonymous_licences_message.format(
                licences="; ".join(
                    [
                        f"{licence[0]} (rev: {licence[1]})"
                        for licence in required_licences
                    ]
                )
            )
        job_id = str(uuid.uuid4())
        job_kwargs = adaptors.make_system_job_kwargs(
            dataset, request_inputs, adaptor.resources
        )
        compute_sessionmaker = db_utils.get_compute_sessionmaker(
            mode=db_utils.ConnectionMode.write
        )
        with compute_sessionmaker() as compute_session:
            job = cads_broker.database.create_request(
                session=compute_session,
                request_uid=job_id,
                origin=auth_info.request_origin,
                user_uid=auth_info.user_uid,
                process_id=process_id,
                portal=dataset.portal,
                qos_tags=dataset.qos_tags,
                metadata={
                    "costs": costs,
                    "user_data": {"email": auth_info.email},
                },
                **job_kwargs,
            )
        dataset_messages = [
            models.DatasetMessage(
                date=message.date,
                severity=message.severity,
                content=message.content,
            )
            for message in dataset.messages
            if message.live
        ]
        url = str(request.url)
        if url.rstrip("/").endswith("execute"):
            message = models.DatasetMessage(
                date=datetime.datetime.now(),
                severity="WARNING",
                content=SETTINGS.deprecation_warning_message,
            )
            dataset_messages.append(message)
        status_info = utils.make_status_info(
            job, dataset_metadata={"messages": dataset_messages}
        )
        status_info.message = job_message
        return status_info

    @exceptions.exception_logger
    def get_jobs(
        self,
        processID: list[str] | None = fastapi.Query(
            None,
            description=(
                "Processes identifiers. Only jobs associated to the specified "
                "processes shall be included in the response."
            ),
        ),
        status: list[models.SearchableStatusCode] | None = fastapi.Query(
            [
                models.StatusCode.accepted,
                models.StatusCode.running,
                models.StatusCode.successful,
                models.StatusCode.failed,
                models.StatusCode.rejected,
            ],
            description=(
                "Job statuses. Only jobs with the specified statuses shall be included in "
                "the response."
            ),
        ),
        limit: int | None = fastapi.Query(
            10, ge=1, le=10000, description="Maximum number of results to return."
        ),
        sortby: utils.JobSortCriterion | None = fastapi.Query(
            utils.JobSortCriterion.created_at_desc, description="Sorting criterion."
        ),
        cursor: str | None = fastapi.Query(None, include_in_schema=False),
        back: bool | None = fastapi.Query(None, include_in_schema=False),
        auth_info: models.AuthInfo = fastapi.Depends(auth.get_auth_info),
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
        auth_info: models.AuthInfo
            Authentication information.

        Returns
        -------
        models.JobList
            List of jobs status information.
        """
        structlog.contextvars.bind_contextvars(user_uid=auth_info.user_uid)
        _ = limits.check_rate_limits(
            SETTINGS.rate_limits.jobs.get,
            auth_info,
        )
        job_filters = {
            "process_id": processID,
            "status": status,
            "user_uid": [auth_info.user_uid],
            "portal": auth_info.portals,
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
            mode=db_utils.ConnectionMode.write
        )
        with compute_sessionmaker() as compute_session:
            jobs_count = cads_broker.database.count_requests(
                session=compute_session,
                **job_filters,
            )
            job_entries = compute_session.scalars(statement).all()
            if back:
                job_entries = reversed(job_entries)
            jobs = []
            catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker(
                db_utils.ConnectionMode.read
            )
            for job in job_entries:
                with catalogue_sessionmaker() as catalogue_session:
                    try:
                        (dataset_title,) = utils.get_resource_properties(
                            resource_id=job.process_id,
                            properties="title",
                            table=self.process_table,
                            session=catalogue_session,
                        )
                    except ogc_api_processes_fastapi.exceptions.NoSuchProcess:
                        dataset_title = SETTINGS.missing_dataset_title
                results = utils.parse_results_from_broker_db(
                    job, session=compute_session
                )
                jobs.append(
                    utils.make_status_info(
                        job=job,
                        results=results,
                        dataset_metadata={
                            "title": dataset_title,
                            "catalogue": job.portal,
                        },
                        qos={
                            "status": cads_broker.database.get_qos_status_from_request(
                                job
                            )
                        },
                    )
                )
        job_list = models.JobList(
            jobs=jobs,
            links=[ogc_api_processes_fastapi.models.Link(href="")],
            metadata=models.JobListMetadata(totalCount=jobs_count),
        )
        pagination_query_params = utils.make_pagination_query_params(
            jobs, sort_key=sortby.lstrip("-")
        )
        job_list._pagination_query_params = pagination_query_params

        return job_list

    @exceptions.exception_logger
    def get_job(
        self,
        job_id: str = fastapi.Path(..., description="Job identifier."),
        qos: bool = fastapi.Query(
            False, description="Whether to include job qos info in the response."
        ),
        request: bool = fastapi.Query(
            False,
            description="Whether to include the sumbitted request in the response.",
        ),
        log: bool = fastapi.Query(
            False, description="Whether to include the job's log in the response."
        ),
        log_start_time: datetime.datetime | None = fastapi.Query(
            None,
            alias="logStartTime",
            description="Datetime of the first log message to be returned.",
        ),
        auth_info: models.AuthInfo = fastapi.Depends(auth.get_auth_info),
    ) -> models.StatusInfo:
        """Implement OGC API - Processes `GET /jobs/{job_id}` endpoint.

        Get status information for the job identifed by `job_id`.

        Parameters
        ----------
        job_id : str
            Job identifer.
        qos : bool, optional
            Whether to include job qos info in the response
        request : bool, optional
            Whether to include the request in the response
        log : bool, optional
            Whether to include the job's log in the response
        log_start_time: datetime.datetime, optional
            Datetime of the first log message to be returned
        auth_info: models.AuthInfo
            Authentication Information.

        Returns
        -------
        models.StatusInfo
            Job status information.
        """
        structlog.contextvars.bind_contextvars(user_uid=auth_info.user_uid)
        _ = limits.check_rate_limits(
            SETTINGS.rate_limits.job.get,
            auth_info,
        )
        compute_connection_mode = (
            db_utils.ConnectionMode.write
            if auth_info.request_origin == "ui"
            else db_utils.ConnectionMode.read
        )
        try:
            compute_sessionmaker = db_utils.get_compute_sessionmaker(
                mode=compute_connection_mode
            )
            with compute_sessionmaker() as compute_session:
                job = utils.get_job_from_broker_db(
                    job_id=job_id, session=compute_session
                )
                if qos:
                    job_qos_info = {
                        **utils.get_job_qos_info(job, compute_session),
                        "status": cads_broker.database.get_qos_status_from_request(job),
                    }
                # These lines are inside the session context because the related fields
                # are lazy loaded
                if log:
                    job_log = utils.get_job_events(
                        job,
                        compute_session,
                        "user_visible_log",
                        log_start_time,
                    )
        except ogc_api_processes_fastapi.exceptions.NoSuchJob:
            if compute_connection_mode == db_utils.ConnectionMode.write:
                raise
            else:
                compute_sessionmaker = db_utils.get_compute_sessionmaker(
                    mode=db_utils.ConnectionMode.write
                )
                with compute_sessionmaker() as compute_session:
                    job = utils.get_job_from_broker_db(
                        job_id=job_id, session=compute_session
                    )
                    if qos:
                        job_qos_info = {
                            **utils.get_job_qos_info(job, compute_session),
                            "status": cads_broker.database.get_qos_status_from_request(
                                job
                            ),
                        }
                    # These lines are inside the session context because the related fields
                    # are lazy loaded
                    if log:
                        job_log = utils.get_job_events(
                            job,
                            compute_session,
                            "user_visible_log",
                            log_start_time,
                        )
        if job.portal not in auth_info.portals:
            raise ogc_api_processes_fastapi.exceptions.NoSuchJob(
                detail=f"job {job_id} not found"
            )
        auth.verify_permission(auth_info.user_uid, job)
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
        if log:
            kwargs["log"] = [
                (message[0].isoformat(), message[1]) for message in job_log
            ]
        if qos:
            kwargs["qos"] = {
                **job_qos_info,
            }
        status_info = utils.make_status_info(
            job=job, dataset_metadata={"catalogue": job.portal}, **kwargs
        )
        return status_info

    @exceptions.exception_logger
    def get_job_results(
        self,
        job_id: str = fastapi.Path(..., description="Job identifier."),
        auth_info: models.AuthInfo = fastapi.Depends(auth.get_auth_info),
    ) -> ogc_api_processes_fastapi.models.Results:
        """Implement OGC API - Processes `GET /jobs/{job_id}/results` endpoint.

        Get results for the job identifed by `job_id`.

        Parameters
        ----------
        job_id : str
            Identifier of the job.
        auth_info: models.AuthInfo
            Authentication Information.

        Returns
        -------
        ogc_api_processes_fastapi.models.Results
            Job results.
        """
        structlog.contextvars.bind_contextvars(user_uid=auth_info.user_uid)
        _ = limits.check_rate_limits(
            SETTINGS.rate_limits.job_results.get,
            auth_info,
        )
        compute_connection_mode = (
            db_utils.ConnectionMode.write
            if auth_info.request_origin == "ui"
            else db_utils.ConnectionMode.read
        )
        try:
            compute_sessionmaker = db_utils.get_compute_sessionmaker(
                mode=compute_connection_mode
            )
            with compute_sessionmaker() as compute_session:
                job = utils.get_job_from_broker_db(
                    job_id=job_id, session=compute_session
                )
                results = utils.get_results_from_job(job=job, session=compute_session)
            auth.verify_permission(auth_info.user_uid, job)
        except (
            ogc_api_processes_fastapi.exceptions.NoSuchJob,
            ogc_api_processes_fastapi.exceptions.ResultsNotReady,
        ):
            if compute_connection_mode == db_utils.ConnectionMode.write:
                raise
            else:
                compute_sessionmaker = db_utils.get_compute_sessionmaker(
                    mode=db_utils.ConnectionMode.write
                )
                with compute_sessionmaker() as compute_session:
                    job = utils.get_job_from_broker_db(
                        job_id=job_id, session=compute_session
                    )
                    results = utils.get_results_from_job(
                        job=job, session=compute_session
                    )
                auth.verify_permission(auth_info.user_uid, job)
        handle_download_metrics(job.process_id, results)
        return results

    @exceptions.exception_logger
    def delete_job(
        self,
        job_id: str = fastapi.Path(..., description="Job identifier."),
        auth_info: models.AuthInfo = fastapi.Depends(auth.get_auth_info),
    ) -> models.StatusInfo:
        """Implement OGC API - Processes `DELETE /jobs/{job_id}` endpoint.

        Dismiss the job identifed by `job_id`.

        Parameters
        ----------
        job_id : str
            Identifier of the job.
        auth_info: models.AuthInfo
            Authentication Information.

        Returns
        -------
        models.StatusInfo
            Job status information
        """
        structlog.contextvars.bind_contextvars(user_uid=auth_info.user_uid)
        _ = limits.check_rate_limits(
            SETTINGS.rate_limits.job.delete,
            auth_info,
        )
        compute_sessionmaker = db_utils.get_compute_sessionmaker(
            mode=db_utils.ConnectionMode.write
        )
        with compute_sessionmaker() as compute_session:
            job = utils.get_job_from_broker_db(job_id=job_id, session=compute_session)
            auth.verify_permission(auth_info.user_uid, job)
            job = cads_broker.database.set_dismissed_request(
                request_uid=job_id, session=compute_session
            )
            job = utils.dictify_job(job)
        status_info = utils.make_status_info(job)
        return status_info
