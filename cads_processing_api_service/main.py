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
import json
import logging
import random
import urllib.parse
from typing import Type

import attrs
import cads_catalogue.database
import fastapi
import fastapi_utils.session
import ogc_api_processes_fastapi
import ogc_api_processes_fastapi.clients
import ogc_api_processes_fastapi.models
import sqlalchemy.orm
import sqlalchemy.orm.exc

from . import adaptors, config, errors

settings = config.SqlalchemySettings()

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
        raise errors.NotFoundError(f"{record.__name__} {id} not found")
    return row


def process_summary_serializer(
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
        description=json.dumps(db_model.description),
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


# TODO: this is a mock implementation. Change it when database is ready.
def process_inputs_serializer() -> list[
    dict[str, ogc_api_processes_fastapi.models.InputDescription]
]:
    """Convert provided database entry into a representation of a process inputs.

    Returns
    -------
    list[ dict[str, ogc_api_processes_fastapi.models.InputDescription] ]
        Process inputs representation.
    """
    inputs = adaptors.translate_cds_into_ogc_inputs(
        urllib.parse.urljoin(__file__, "../tests/data/form.json")
    )
    return inputs


def process_description_serializer(
    db_model: cads_catalogue.database.Resource,
) -> ogc_api_processes_fastapi.models.ProcessDescription:
    """Convert provided database entry into a representation of a process description.

    Returns
    -------
    ogc_api_processes_fastapi.models.ProcessDescription
        Process description representation.
    """
    process_summary = process_summary_serializer(db_model)
    retval = ogc_api_processes_fastapi.models.ProcessDescription(
        **process_summary.dict(),
        inputs=process_inputs_serializer(),
    )

    return retval


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

    reader: fastapi_utils.session.FastAPISessionMaker = attrs.field(
        default=fastapi_utils.session.FastAPISessionMaker(settings.connection_string),
        init=False,
    )
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
                process_summary_serializer(process) for process in processes
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
        """
        with self.reader.context_session() as session:
            id = process_id[len("retrieve-") :]
            process = lookup_id(id=id, record=self.process_table, session=session)
            process_description = process_description_serializer(process)
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
        """
        job_id = f"{random.randint(1,1000):04}"
        while job_id in JOBS.keys():
            job_id = f"{random.randint(1,1000):04}"
        JOBS[job_id] = {"status": "accepted"}
        JOBS[job_id]["created"] = datetime.datetime.now()
        JOBS[job_id]["started"] = None
        JOBS[job_id]["finished"] = None
        JOBS[job_id]["updated"] = datetime.datetime.now()
        JOBS[job_id]["processID"] = process_id
        status_info = ogc_api_processes_fastapi.models.StatusInfo(
            jobID=job_id,
            status=JOBS[job_id]["status"],
            type=ogc_api_processes_fastapi.models.JobType.process,
            processID=JOBS[job_id]["processID"],
            created=JOBS[job_id]["created"],
            started=JOBS[job_id]["started"],
            finished=JOBS[job_id]["finished"],
            updated=JOBS[job_id]["updated"],
        )
        return status_info

    def get_job(self, job_id: str) -> ogc_api_processes_fastapi.models.StatusInfo:
        """Implement OGC API - Processes `GET /jobs/{job_id}` endpoint.

        Get status information for the job identifed by `job_id`.

        Parameters
        ----------
        job_id : str
            Identifier of the job.

        Returns
        -------
        models.StatusInfo
            Information on the status of the job.
        """
        if job_id not in JOBS.keys():
            raise fastapi.HTTPException(
                status_code=404, detail=f"Job {job_id} not found"
            )
        elif JOBS[job_id]["status"] == "accepted":
            random_number = random.randint(1, 10)
            if random_number >= 5:
                JOBS[job_id]["status"] = "running"
                JOBS[job_id]["updated"] = datetime.datetime.now()
                JOBS[job_id]["started"] = datetime.datetime.now()
        elif JOBS[job_id]["status"] == "running":
            random_number = random.randint(1, 10)
            if random_number >= 9:
                JOBS[job_id]["status"] = "successful"
                JOBS[job_id]["updated"] = datetime.datetime.now()
                JOBS[job_id]["finished"] = datetime.datetime.now()
        status_info = ogc_api_processes_fastapi.models.StatusInfo(
            jobID=job_id,
            status=JOBS[job_id]["status"],
            type=ogc_api_processes_fastapi.models.JobType.process,
            processID=JOBS[job_id]["processID"],
            created=JOBS[job_id]["created"],
            started=JOBS[job_id]["started"],
            finished=JOBS[job_id]["finished"],
            updated=JOBS[job_id]["updated"],
        )

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
        models.Link
            Link to the job results.
        """
        if job_id not in JOBS.keys():
            raise fastapi.HTTPException(
                status_code=404, detail=f"Job {job_id} not found."
            )
        elif JOBS[job_id]["status"] != "successful":
            raise fastapi.HTTPException(
                status_code=404, detail=f"Job {job_id} is not finished yet."
            )
        results_link = ogc_api_processes_fastapi.models.Link(
            href=f"https://example.org/{job_id}-results.nc",
            title=f"Download link for the result of job {job_id}",
        )
        return results_link


app = fastapi.FastAPI()
app = ogc_api_processes_fastapi.include_routers(app=app, client=DatabaseClient())
