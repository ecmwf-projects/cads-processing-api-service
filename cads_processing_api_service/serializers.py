import urllib.parse
from typing import Any, Type

import cads_catalogue.database
import ogc_api_processes_fastapi.models
import requests  # type: ignore
import sqlalchemy.orm
import sqlalchemy.orm.exc

from . import config, exceptions, translators


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
    inputs = translators.translate_cds_into_ogc_inputs(cds_form)
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
