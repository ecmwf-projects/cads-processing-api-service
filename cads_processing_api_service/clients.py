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
import urllib.parse
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
import requests  # type: ignore
import sqlalchemy.orm
import sqlalchemy.orm.exc

from . import adaptors, config, exceptions

logger = logging.getLogger(__name__)

JOBS: dict[str, dict[str, str | datetime.datetime | None]] = {}


def lookup_id(
    id: str,
    record: Type[cads_catalogue.database.BaseModel],
    session: sqlalchemy.orm.Session,
) -> cads_catalogue.database.BaseModel:
    """Search database record by id.

    Lookup `record` instance containing identifier `id` in the provided SQLAlchemy `session`.

    Parameters
    ----------
    id : str
        Identifier to look up.
    record : Type[cads_catalogue.database.BaseModel]
        Record for which to look for identifier `id`.
    session : sqlalchemy.orm.Session
        SQLAlchemy ORM session.

    Returns
    -------
    cads_catalogue.database.BaseModel
        Record instance containing identifier `id`.

    Raises
    ------
    errors.NotFoundError
        If not `record` instance is found containing identifier `id`.
    """
    try:
        row = session.query(record).filter(record.resource_uid == id).one()
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NotFoundError(f"{record.__name__} {id} not found")
    return row


def get_cds_form(cds_form_url: str) -> list[Any]:
    """Get CDS form from URL.

    Parameters
    ----------
    cds_form_url : str
        URL to the CDS form, relative to the Document Storage URL.

    Returns
    -------
    list[Any]
        CDS form.
    """
    settings = config.ensure_settings()
    cds_form_complete_url = urllib.parse.urljoin(
        settings.document_storage_url, cds_form_url
    )
    cds_form: list[Any] = requests.get(cds_form_complete_url).json()
    return cds_form


def serialize_process_summary(
    db_model: cads_catalogue.database.Resource,
) -> ogc_api_processes_fastapi.models.ProcessSummary:
    """Convert provided database entry into a representation of a process summary.

    Parameters
    ----------
    db_model : cads_catalogue.database.Resource
        Database entry.

    Returns
    -------
    ogc_api_processes_fastapi.models.ProcessSummary
        Process summary representation.
    """
    retval = ogc_api_processes_fastapi.models.ProcessSummary(
        title=f"Retrieve of {db_model.title}",
        description=db_model.abstract,
        keywords=db_model.keywords,
        id=f"retrieve-{db_model.resource_uid}",
        version="1.0.0",
        jobControlOptions=[
            "async-execute",
        ],
        outputTransmission=[
            "reference",
        ],
    )

    return retval


def serialize_process_inputs(
    db_model: cads_catalogue.database.Resource,
) -> list[dict[str, ogc_api_processes_fastapi.models.InputDescription]]:
    """Convert provided database entry into a representation of a process inputs.

    Returns
    -------
    list[ dict[str, ogc_api_processes_fastapi.models.InputDescription] ]
        Process inputs representation.
    """
    form_url = db_model.form
    cds_form = get_cds_form(cds_form_url=form_url)
    inputs = adaptors.translate_cds_into_ogc_inputs(cds_form)
    return inputs


def serialize_process_description(
    db_model: cads_catalogue.database.Resource,
) -> ogc_api_processes_fastapi.models.ProcessDescription:
    """Convert provided database entry into a representation of a process description.

    Parameters
    ----------
    db_model : cads_catalogue.database.Resource
        Database entry.

    Returns
    -------
    ogc_api_processes_fastapi.models.ProcessDescription
        Process description representation.
    """
    process_summary = serialize_process_summary(db_model)
    retval = ogc_api_processes_fastapi.models.ProcessDescription(
        **process_summary.dict(),
        inputs=serialize_process_inputs(db_model),
    )

    return retval


def update_job_status(job_id: str) -> None:
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
                serialize_process_summary(process) for process in processes
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
            id = process_id[len("retrieve-") :]
            try:
                process = lookup_id(id=id, record=self.process_table, session=session)
            except exceptions.NotFoundError:
                raise ogc_api_processes_fastapi.exceptions.NoSuchProcess()
            process_description = serialize_process_description(process)
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
        process_description = self.get_process(process_id)
        # TODO: inputs validation
        print(process_description)
        job_id = str(uuid.uuid4())
        while job_id in JOBS.keys():
            job_id = str(uuid.uuid4())
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
        for job_id in JOBS:
            update_job_status(job_id)
        jobs_list = [
            ogc_api_processes_fastapi.models.StatusInfo(**JOBS[job_id])
            for job_id in JOBS
        ]
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
        update_job_status(job_id)
        status_info = ogc_api_processes_fastapi.models.StatusInfo(**JOBS[job_id])

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
        update_job_status(job_id)
        if JOBS[job_id]["status"] in ("accepted", "running"):
            raise ogc_api_processes_fastapi.exceptions.ResultsNotReady()
        elif JOBS[job_id]["status"] == "failed":
            raise ogc_api_processes_fastapi.exceptions.JobResultsFailed()
        results_link = ogc_api_processes_fastapi.models.Link(
            href=f"https://example.org/{job_id}-results.nc",
            title=f"Download link for the result of job {job_id}",
        )
        return results_link
