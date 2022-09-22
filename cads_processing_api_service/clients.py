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
import uuid
from typing import Any, Type

import attrs
import cads_api_client
import cads_catalogue.config
import cads_catalogue.database
import fastapi
import fastapi_utils.session
import ogc_api_processes_fastapi
import ogc_api_processes_fastapi.clients
import ogc_api_processes_fastapi.exceptions
import ogc_api_processes_fastapi.models
import sqlalchemy.orm
import sqlalchemy.orm.exc

from . import adaptors, config, exceptions, serializers

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
    ) -> Any:

        try:
            process = self._lookup_id(id=id, record=self.process_table, session=session)
        except exceptions.NotFoundError:
            raise ogc_api_processes_fastapi.exceptions.NoSuchProcess()

        return process

    def validate_request(
        self,
        process_id: str,
        execution_content: ogc_api_processes_fastapi.models.Execute,
        session: sqlalchemy.orm.Session,
    ) -> tuple[str, cads_catalogue.database.Resource]:
        """Validate retrieve process execution request.

        Check if requested dataset exists and if execution content is valid.
        In case the check is successful, creates and returns the job ID and the
        resource (dataset) associated to the process request.

        Parameters
        ----------
        process_id : str
            Process ID.
        execution_content : ogc_api_processes_fastapi.models.Execute
            Body of the process execution request.
        session : sqlalchemy.orm.Session
            SQLAlchemy ORM session

        Returns
        -------
        tuple[str, cads_catalogue.database.Resource]
            Job ID and resource (dataset) associated to the process request.
        """
        # TODO: implement inputs validation
        resource = self.lookup_resource_by_id(process_id, session)
        job_id = str(uuid.uuid4())
        print(execution_content, resource)
        return job_id, resource

    def submit_job(
        self,
        process_id: str,
        execution_content: ogc_api_processes_fastapi.models.Execute,
        job_id: str,
        resource: cads_catalogue.database.Resource,
    ) -> ogc_api_processes_fastapi.models.StatusInfo:
        """Submit new job.

        Parameters
        ----------
        process_id: str
            Process ID.
        execution_content: ogc_api_processes_fastapi.models.Execute
            Body of the process execution request.
        job_id : str
            Job ID.
        resource: cads_catalogue.database.Resource,
            Catalogue resource corresponding to the requested retrieve process


        Returns
        -------
        ogc_api_processes_fastapi.models.StatusInfo
            Sumbitted job status info.
        """
        request = adaptors.make_system_request(
            process_id, execution_content, job_id, resource
        )
        settings = config.ensure_settings()
        response = cads_api_client.Processing(
            url=settings.compute_api_url, force_exact_url=True
        ).process_execute(
            "submit-workflow", request["inputs"], headers=request["metadata"]
        )
        status_info = ogc_api_processes_fastapi.models.StatusInfo(**response.json)
        status_info.processID = status_info.metadata.pop("apiProcessID")

        return status_info

    def get_processes(
        self, limit: int | None = None, offset: int = 0
    ) -> list[ogc_api_processes_fastapi.models.ProcessSummary]:
        """Implement OGC API - Processes `GET /processes` endpoint.

        Get the list of available processes from the database.

        Parameters
        ----------
        limit : int | None, optional
            Number of processes summaries to be returned.
        offset : int, optional
            Index (starting from 0) of the first process summary
            to be returned, by default 0.

        Returns
        -------
        list[ogc_api_processes_fastapi.models.ProcessSummary]
            List of available processes.
        """
        with self.reader.context_session() as session:
            if limit:
                processes = (
                    session.query(self.process_table).offset(offset).limit(limit).all()
                )
            else:
                processes = session.query(self.process_table).offset(offset).all()
            processes_list = [
                serializers.serialize_process_summary(process) for process in processes
            ]

        return processes_list

    def get_process(
        self, process_id: str
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
            resource = self.lookup_resource_by_id(process_id, session)
            process_description = serializers.serialize_process_description(resource)
            process_description.outputs = {
                "download_url": ogc_api_processes_fastapi.models.OutputDescription(
                    title="Download URL",
                    description="URL to download process result",
                    schema_=ogc_api_processes_fastapi.models.SchemaItem(  # type: ignore
                        type="string", format="url"
                    ),
                )
            }

        return process_description

    def post_process_execute(
        self,
        process_id: str,
        execution_content: ogc_api_processes_fastapi.models.Execute,
        request: fastapi.Request,
        response: fastapi.Response,
    ) -> ogc_api_processes_fastapi.models.StatusInfo:
        """Implement OGC API - Processes `POST /processes/{process_id}/execute` endpoint.

        Request execution of the process identified by `process_id`.

        Parameters
        ----------
        process_id : str
            Process identifier.
        execution_content : ogc_api_processes_fastapi.models.Execute
            Process execution details (e.g. inputs).
        request: fastapi.Request
            Request.
        response: fastapi.Response
            Response.

        Returns
        -------
        ogc_api_processes_fastapi.models.StatusInfo
            Information on the status of the job.

        Raises
        ------
        ogc_api_processes_fastapi.exceptions.NoSuchProcess
            If the process `process_id` is not found.
        """
        with self.reader.context_session() as session:
            job_id, resource = self.validate_request(
                process_id, execution_content, session
            )
        status_info = self.submit_job(process_id, execution_content, job_id, resource)
        return status_info

    def get_jobs(self) -> list[ogc_api_processes_fastapi.models.StatusInfo]:
        """Implement OGC API - Processes `GET /jobs` endpoint.

        Get jobs' status information list.

        Parameters
        ----------
        ...

        Returns
        -------
        list[ogc_api_processes_fastapi.models.StatusInfo]
            Information on the status of the job.
        """
        settings = config.ensure_settings()
        response = cads_api_client.Processing(
            url=settings.compute_api_url, force_exact_url=True
        ).jobs()
        status_info_list = [
            ogc_api_processes_fastapi.models.StatusInfo(**job)
            for job in response.json["jobs"]
        ]
        for status_info in status_info_list:
            status_info.processID = status_info.metadata.pop("apiProcessID")

        return status_info_list

    def get_job(
        self, job_id: str, response: fastapi.Response
    ) -> ogc_api_processes_fastapi.models.StatusInfo:
        """Implement OGC API - Processes `GET /jobs/{job_id}` endpoint.

        Get status information for the job identifed by `job_id`.

        Parameters
        ----------
        job_id : str
            Identifier of the job.
        response: fastapi.Response
            Response.

        Returns
        -------
        ogc_api_processes_fastapi.models.StatusInfo
            Information on the status of the job.

        Raises
        ------
        ogc_api_processes_fastapi.exceptions.NoSuchJob
            If the job `job_id` is not found.
        """
        settings = config.ensure_settings()
        response = cads_api_client.Processing(
            url=settings.compute_api_url, force_exact_url=True
        ).job(job_id)
        status_info = ogc_api_processes_fastapi.models.StatusInfo(**response.json)
        status_info.processID = status_info.metadata.pop("apiProcessID")

        return status_info

    def get_job_results(self, job_id: str) -> dict[str, Any]:
        """Implement OGC API - Processes `GET /jobs/{job_id}/results` endpoint.

        Get results for the job identifed by `job_id`.

        Parameters
        ----------
        job_id : str
            Identifier of the job.

        Returns
        -------
        dict[str, Any]
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
        settings = config.ensure_settings()
        response = cads_api_client.Processing(
            url=settings.compute_api_url, force_exact_url=True
        ).job_results(job_id)
        results = dict(**response.json)
        return results
