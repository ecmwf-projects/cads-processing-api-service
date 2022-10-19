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

import json
import logging
from typing import Any, Type

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

from . import adaptors, exceptions, serializers

logger = logging.getLogger(__name__)


@attrs.define
class DatabaseClient(ogc_api_processes_fastapi.clients.BaseClient):
    """Database implementation of the OGC API - Processes endpoints.

    Attributes
    ----------
    reader : fastapi_utils.session.FastAPISessionMaker
        SQLAlchemy ORM session reader.
    process_table: Type[cads_catalogue.database.Resource]
        Processes record/table.
    """

    reader: fastapi_utils.session.FastAPISessionMaker
    process_table: Type[cads_catalogue.database.Resource] = attrs.field(
        default=cads_catalogue.database.Resource
    )

    def _lookup_id(
        self,
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
        self,
        id: str,
        session: sqlalchemy.orm.Session,
    ) -> cads_catalogue.database.BaseModel:

        try:
            process = self._lookup_id(id=id, record=self.process_table, session=session)
        except exceptions.NotFoundError:
            raise ogc_api_processes_fastapi.exceptions.NoSuchProcess()

        return process

    def validate_request(
        self,
        process_id: str,
        execution_content: dict[str, Any],
        session: sqlalchemy.orm.Session,
    ) -> cads_catalogue.database.BaseModel:
        """Validate retrieve process execution request.

        Check if requested dataset exists and if execution content is valid.
        In case the check is successful, returns the resource (dataset)
        associated to the process request.

        Parameters
        ----------
        process_id : str
            Process ID.
        execution_content : Dict[str, Any]
            Body of the process execution request.
        session : sqlalchemy.orm.Session
            SQLAlchemy ORM session

        Returns
        -------
        cads_catalogue.database.BaseModel
            Resource (dataset) associated to the process request.
        """
        # TODO: implement inputs validation
        resource = self.lookup_resource_by_id(process_id, session)
        return resource

    def submit_job(
        self,
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
            resource = self.lookup_resource_by_id(process_id, session)
            process_description = serializers.serialize_process_description(resource)
            process_description.outputs = {
                "asset": {
                    "title": "Asset",
                    "description": "Downloadable asset description",
                    "schema_": {
                        "type": "object",
                        "properties": {
                            "value": {
                                "type": "object",
                                "properties": {
                                    "type": {"type": "string"},
                                    "href": {"type": "string"},
                                    "file:checksum": {"type": "integer"},
                                    "file:size": {"type": "integer"},
                                    "file:local_path": {"type": "string"},
                                    "tmp:storage_option": {"type": "object"},
                                    "tmp:open_kwargs": {"type": "object"},
                                },
                            },
                        },
                    },
                },
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
            resource = self.validate_request(process_id, execution_content, session)
            status_info = self.submit_job(process_id, execution_content, resource)
        return status_info

    def get_jobs(
        self,
    ) -> ogc_api_processes_fastapi.responses.JobList:
        """Implement OGC API - Processes `GET /jobs` endpoint.

        Get jobs' status information list.

        Parameters
        ----------
        ...

        Returns
        -------
        list[ogc_api_processes_fastapi.responses.schema["StatusInfo"]]
            Information on the status of the job.
        """
        session_obj = cads_broker.database.ensure_session_obj(None)
        with session_obj() as session:
            statement = sqlalchemy.select(cads_broker.database.SystemRequest).order_by(
                cads_broker.database.SystemRequest.created_at.desc()
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
            sqlalchemy.exc.NoResultFound,
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
            sqlalchemy.exc.NoResultFound,
        ):
            raise ogc_api_processes_fastapi.exceptions.NoSuchJob(
                f"Can't find the job {job_id}."
            )
        if job.status == "successful":
            job_results = {
                "asset": {"value": json.loads(job.response_body.get("result"))}
            }
            return job_results
        elif job.status == "failed":
            raise ogc_api_processes_fastapi.exceptions.JobResultsFailed(
                type="RuntimeError",
                detail=job.response_body.get("traceback"),
                status_code=fastapi.status.HTTP_400_BAD_REQUEST,
            )
        elif job.status in ("accepted", "running"):
            raise ogc_api_processes_fastapi.exceptions.ResultsNotReady(
                f"Status of {job_id} is {job.status}."
            )
