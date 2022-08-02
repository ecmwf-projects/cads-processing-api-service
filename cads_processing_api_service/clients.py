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
import logging
import random
import uuid
from typing import Any, Type

import attrs
import cads_catalogue.config
import cads_catalogue.database
import fastapi_utils.session
import ogc_api_processes_fastapi
import ogc_api_processes_fastapi.clients
import ogc_api_processes_fastapi.exceptions
import ogc_api_processes_fastapi.models
import sqlalchemy.orm
import sqlalchemy.orm.exc

from . import adapters, exceptions, serializers

logger = logging.getLogger(__name__)

JOBS: dict[str, dict[str, str | datetime.datetime | None]] = {}


def update_job_status(job_id: str) -> ogc_api_processes_fastapi.models.StatusInfo:
    """Randomly update status of job `job_id`.

    Parameters
    ----------
    job_id : str
        Job ID.
    """
    if JOBS[job_id]["status"] == "accepted":
        random_number = random.randint(1, 10)
        if random_number >= 5:
            JOBS[job_id]["status"] = "running"
            JOBS[job_id]["updated"] = datetime.datetime.now()
            JOBS[job_id]["started"] = datetime.datetime.now()
        elif random_number <= 1:
            JOBS[job_id]["status"] = "failed"
            JOBS[job_id]["updated"] = datetime.datetime.now()
            JOBS[job_id]["finished"] = datetime.datetime.now()
    elif JOBS[job_id]["status"] == "running":
        random_number = random.randint(1, 10)
        if random_number >= 7:
            JOBS[job_id]["status"] = "successful"
            JOBS[job_id]["updated"] = datetime.datetime.now()
            JOBS[job_id]["finished"] = datetime.datetime.now()
        elif random_number <= 1:
            JOBS[job_id]["status"] = "failed"
            JOBS[job_id]["updated"] = datetime.datetime.now()
            JOBS[job_id]["finished"] = datetime.datetime.now()
    status_info = ogc_api_processes_fastapi.models.StatusInfo(**JOBS[job_id])

    return status_info


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
        resource_id = process_id[len("retrieve-") :]
        resource = self.lookup_resource_by_id(resource_id, session)
        job_id = str(uuid.uuid4())
        print(execution_content, resource)
        return job_id, resource

    def submit_job_mock(
        self,
        job_id: str,
        process_id: str,
        execution_content: ogc_api_processes_fastapi.models.Execute,
    ) -> ogc_api_processes_fastapi.models.StatusInfo:

        JOBS[job_id] = {
            "jobID": job_id,
            "status": "accepted",
            "type": "process",
            "created": datetime.datetime.now(),
            "started": None,
            "finished": None,
            "updated": datetime.datetime.now(),
            "processID": process_id,
        }
        status_info = ogc_api_processes_fastapi.models.StatusInfo(**JOBS[job_id])

        return status_info

    def submit_job(
        self,
        job_id: str,
        process_id: str,
        execution_content: ogc_api_processes_fastapi.models.Execute,
    ) -> ogc_api_processes_fastapi.models.StatusInfo | None:
        """Submit new job.

        Parameters
        ----------
        job_id : str
            Job ID.
        process_id: str
            Process ID.
        execution_content: ogc_api_processes_fastapi.models.Execute
            Body of the process execution request.

        Returns
        -------
        ogc_api_processes_fastapi.models.StatusInfo
            Sumbitted job status info.
        """
        # TODO: request adaptation
        setup_code, entry_point, kwargs, metadata = adapters.adapt_user_request(
            job_id, process_id, execution_content
        )
        # TODO: submit request to broker

        return None

    def request_job_status_mock(
        self, job_id: str
    ) -> ogc_api_processes_fastapi.models.StatusInfo:

        if JOBS[job_id]["status"] == "accepted":
            random_number = random.randint(1, 10)
            if random_number >= 5:
                JOBS[job_id]["status"] = "running"
                JOBS[job_id]["updated"] = datetime.datetime.now()
                JOBS[job_id]["started"] = datetime.datetime.now()
            elif random_number <= 1:
                JOBS[job_id]["status"] = "failed"
                JOBS[job_id]["updated"] = datetime.datetime.now()
                JOBS[job_id]["finished"] = datetime.datetime.now()
        elif JOBS[job_id]["status"] == "running":
            random_number = random.randint(1, 10)
            if random_number >= 7:
                JOBS[job_id]["status"] = "successful"
                JOBS[job_id]["updated"] = datetime.datetime.now()
                JOBS[job_id]["finished"] = datetime.datetime.now()
            elif random_number <= 1:
                JOBS[job_id]["status"] = "failed"
                JOBS[job_id]["updated"] = datetime.datetime.now()
                JOBS[job_id]["finished"] = datetime.datetime.now()
        status_info = ogc_api_processes_fastapi.models.StatusInfo(**JOBS[job_id])

        return status_info

    def get_processes(
        self, limit: int | None = None, offset: int = 0
    ) -> list[ogc_api_processes_fastapi.models.ProcessSummary]:
        """Implement OGC API - Processes `GET /processes` endpoint.

        Get the list of available processes from the database.

        Returns
        -------
        list[ogc_api_processes_fastapi.models.ProcessSummary]
            List of available processes.
        """
        with self.reader.context_session() as session:
            processes = session.query(self.process_table).all()
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
            resource_id = process_id[len("retrieve-") :]
            resource = self.lookup_resource_by_id(resource_id, session)
            process_description = serializers.serialize_process_description(resource)
            process_description.outputs = [
                {
                    "download_url": ogc_api_processes_fastapi.models.OutputDescription(
                        title="Download URL",
                        description="URL to download process result",
                        schema_=ogc_api_processes_fastapi.models.SchemaItem(  # type: ignore
                            type="string", format="url"
                        ),
                    )
                }
            ]

        return process_description

    def post_process_execute(
        self,
        process_id: str,
        execution_content: ogc_api_processes_fastapi.models.Execute,
    ) -> ogc_api_processes_fastapi.models.StatusInfo:
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
        status_info = self.submit_job_mock(job_id, process_id, execution_content)
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
        jobs_list = [self.request_job_status_mock(job_id) for job_id in JOBS]
        return jobs_list

    def get_job(self, job_id: str) -> ogc_api_processes_fastapi.models.StatusInfo:
        """Implement OGC API - Processes `GET /jobs/{job_id}` endpoint.

        Get status information for the job identifed by `job_id`.

        Parameters
        ----------
        job_id : str
            Identifier of the job.

        Returns
        -------
        ogc_api_processes_fastapi.models.StatusInfo
            Information on the status of the job.

        Raises
        ------
        ogc_api_processes_fastapi.exceptions.NoSuchJob
            If the job `job_id` is not found.
        """
        if job_id not in JOBS.keys():
            raise ogc_api_processes_fastapi.exceptions.NoSuchJob()
        status_info = self.request_job_status_mock(job_id)

        return status_info

    def get_job_results(self, job_id: str) -> ogc_api_processes_fastapi.models.Link:
        """Implement OGC API - Processes `GET /jobs/{job_id}/results` endpoint.

        Get results for the job identifed by `job_id`.

        Parameters
        ----------
        job_id : str
            Identifier of the job.

        Returns
        -------
        ogc_api_processes_fastapi.models.Link
            Link to the job results.

        Raises
        ------
        ogc_api_processes_fastapi.exceptions.NoSuchJob
            If the job `job_id` is not found.

        ogc_api_processes_fastapi.exceptions.ResultsNotReady
            If job `job_id` results are not yet ready.

        ogc_api_processes_fastapi.exceptions.JobResultsFailed
            If job `job_id` results preparation failed.
        """
        if job_id not in JOBS.keys():
            raise ogc_api_processes_fastapi.exceptions.NoSuchJob()
        if JOBS[job_id]["status"] in ("accepted", "running"):
            raise ogc_api_processes_fastapi.exceptions.ResultsNotReady()
        elif JOBS[job_id]["status"] == "failed":
            raise ogc_api_processes_fastapi.exceptions.JobResultsFailed()
        results_link = ogc_api_processes_fastapi.models.Link(
            href=f"https://example.org/{job_id}-results.nc",
            title=f"Download link for the result of job {job_id}",
        )
        return results_link
