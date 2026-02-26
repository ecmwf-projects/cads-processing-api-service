"""Additional endpoints for the CADS Processing API Service."""

import functools
from typing import Any

import cads_adaptors
import cads_adaptors.exceptions
import cads_adaptors.models
import cads_broker
import cads_catalogue
import fastapi
import ogc_api_processes_fastapi.exceptions
import structlog

from . import (
    adaptors,
    auth,
    config,
    costing,
    db_utils,
    exceptions,
    limits,
    models,
    translators,
    utils,
)

SETTINGS = config.settings


@exceptions.exception_logger
def apply_constraints(
    process_id: str = fastapi.Path(..., description="Process identifier."),
    execution_content: models.Execute = fastapi.Body(...),
    auth_info: models.AuthInfo = fastapi.Depends(
        exceptions.exception_logger(
            functools.partial(auth.get_auth_info, allow_unauthenticated=True)
        )
    ),
) -> dict[str, Any]:
    limits.check_rate_limits(
        SETTINGS.rate_limits,
        "processes_processid_constraints",
        "post",
        auth_info,
        process_id,
    )
    request = execution_content.model_dump()
    table = cads_catalogue.database.Resource
    catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker(
        db_utils.ConnectionMode.read
    )
    with catalogue_sessionmaker() as catalogue_session:
        dataset = utils.lookup_resource_by_id(
            resource_id=process_id,
            table=table,
            session=catalogue_session,
            portals=auth_info.portals,
        )
    adaptor: cads_adaptors.AbstractAdaptor = adaptors.instantiate_adaptor(dataset)
    try:
        constraints: dict[str, Any] = adaptor.apply_constraints(
            request.get("inputs", {})
        )
    except (
        cads_adaptors.exceptions.ParameterError,
        cads_adaptors.exceptions.InvalidRequest,
    ) as exc:
        raise exceptions.InvalidParameter(detail=str(exc)) from exc
    return constraints


@exceptions.exception_logger
def estimate_cost(
    process_id: str = fastapi.Path(..., description="Process identifier."),
    request_origin: costing.RequestOrigin = fastapi.Query(
        "api", include_in_schema=False
    ),
    mandatory_inputs: bool = fastapi.Query(False, include_in_schema=False),
    execution_content: models.Execute = fastapi.Body(...),
    auth_info: models.AuthInfo = fastapi.Depends(
        exceptions.exception_logger(
            functools.partial(auth.get_auth_info, allow_unauthenticated=True)
        )
    ),
) -> models.RequestCost:
    """
    Estimate the cost with the highest cost/limit ratio of the request.

    Parameters
    ----------
    process_id : str
        Process ID.
    execution_content : models.Execute
        Request content.

    Returns
    -------
    models.RequestCost
        Info on the cost with the highest cost/limit ratio.
    """
    limits.check_rate_limits(
        SETTINGS.rate_limits,
        "processes_processid_costing",
        "post",
        auth_info,
        process_id,
    )
    request = execution_content.model_dump()
    table = cads_catalogue.database.Resource
    catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker(
        db_utils.ConnectionMode.read
    )
    with catalogue_sessionmaker() as catalogue_session:
        dataset = utils.lookup_resource_by_id(
            resource_id=process_id,
            table=table,
            session=catalogue_session,
            portals=auth_info.portals,
        )
    adaptor_properties = adaptors.get_adaptor_properties(dataset)
    costing_info = costing.compute_costing(
        request.get("inputs", {}), adaptor_properties, request_origin
    )
    cost = costing.compute_highest_cost_limit_ratio(costing_info)
    if costing_info.cost_bar_steps:
        cost.cost_bar_steps = costing_info.cost_bar_steps
    try:
        costing.check_request_validity(
            request=request,
            request_origin=request_origin,
            mandatory_inputs=mandatory_inputs,
            adaptor_properties=adaptor_properties,
        )
    except exceptions.InvalidRequest as exc:
        cost.request_is_valid = False
        cost.invalid_reason = exc.detail
    return cost


@exceptions.exception_logger
def get_api_request(
    process_id: str = fastapi.Path(..., description="Process identifier."),
    request: dict[str, Any] = fastapi.Body(...),
) -> dict[str, str]:
    """Get CADS API request equivalent to the provided processing request.

    Parameters
    ----------
    process_id : str, optional
        Process identifier, by default fastapi.Path(...)
    request : dict[str, Any], optional
        Request, by default fastapi.Body(...)

    Returns
    -------
    dict[str, str]
        CDS API request.
    """
    api_request_template = SETTINGS.api_request_template
    api_request = translators.format_api_request(
        api_request_template, process_id, request
    )
    return {"api_request": api_request}


@exceptions.exception_logger
def delete_jobs(
    request: models.DeleteJobs = fastapi.Body(...),
    auth_info: models.AuthInfo = fastapi.Depends(
        exceptions.exception_logger(auth.get_auth_info)
    ),
) -> models.JobList:
    """Delete jobs from the processing queue.

    Parameters
    ----------
    request : models.DeleteJobsRequest
        Request body containing job IDs to delete.

    Returns
    -------
    models.JobList
        List of jobs that were successfully deleted.
    """
    structlog.contextvars.bind_contextvars(user_uid=auth_info.user_uid)
    limits.check_rate_limits(
        SETTINGS.rate_limits,
        "jobs_delete",
        "post",
        auth_info,
    )
    job_ids = request.job_ids
    compute_sessionmaker = db_utils.get_compute_sessionmaker(
        mode=db_utils.ConnectionMode.write
    )
    jobs = []
    with compute_sessionmaker() as compute_session:
        for job_id in job_ids:
            try:
                job = utils.get_job_from_broker_db(
                    job_id=job_id, session=compute_session
                )
            except ogc_api_processes_fastapi.exceptions.NoSuchJob:
                continue
            try:
                auth.verify_permission(auth_info.user_uid, job)
            except exceptions.PermissionDenied:
                continue
            job = cads_broker.database.set_dismissed_request(
                request_uid=job_id, session=compute_session
            )
            jobs.append(utils.make_status_info(job))
    job_list = models.JobList(
        jobs=jobs,
    )
    return job_list


@exceptions.exception_logger
def get_job_receipt(
    job_id: str = fastapi.Path(..., description="Job identifier."),
    auth_info: models.AuthInfo = fastapi.Depends(
        exceptions.exception_logger(auth.get_auth_info)
    ),
) -> dict[str, Any]:
    """Get the receipt of a job.

    Parameters
    ----------
    job_id : str
        Job identifier.

    Returns
    -------
    dict[str, Any]
        Job receipt.
    """
    structlog.contextvars.bind_contextvars(user_uid=auth_info.user_uid)
    limits.check_rate_limits(
        SETTINGS.rate_limits,
        "jobs_jobid_receipt",
        "get",
        auth_info,
        job_id,
    )
    compute_sessionmaker = db_utils.get_compute_sessionmaker(
        mode=db_utils.ConnectionMode.read
    )
    with compute_sessionmaker() as compute_session:
        job = utils.get_job_from_broker_db(job_id=job_id, session=compute_session)
        auth.verify_permission(auth_info.user_uid, job)
        job_status = job.status
        if job_status in ("accepted", "running"):
            raise ogc_api_processes_fastapi.exceptions.ResultsNotReady(
                detail=f"status of {job_id} is '{job_status}'"
            )
        try:
            results_asset = utils.get_results_from_job(job, compute_session).get("asset", {})
            traceback = None
        except exceptions.JobResultsExpired as exc:
            results_asset = {}
            traceback = None
        except ogc_api_processes_fastapi.exceptions.JobResultsFailed as exc:
            results_asset = {}
            traceback = exc.traceback
    catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker(
        db_utils.ConnectionMode.read
    )
    # with catalogue_sessionmaker() as catalogue_session:
    #     dataset: cads_catalogue.database.Resource = utils.lookup_resource_by_id(
    #         resource_id=job.process_id,
    #         table=cads_catalogue.database.Resource,
    #         session=catalogue_session,
    #     )
    make_receipt_args = {
        "request": job.request_body["request"],
        "collection": cads_adaptors.models.CollectionMetadata( 
            **job.request_metadata.get("receipt", {})
        ),
        "job": cads_adaptors.models.JobMetadata(
            process_id=job.process_id,
            user_id=job.user_uid,
            job_id=job.request_uid,
            status=job.status,
            created=job.created_at,
            started=job.started_at,
            finished=job.finished_at,
            updated=job.updated_at,
            origin=job.origin,
            traceback=traceback,
            user_support_url=SETTINGS.user_support_url
        ),
        "results": cads_adaptors.models.ResultsMetadata(
            **results_asset.get("value", {})
        ),
    }
    # adaptor: cads_adaptors.AbstractAdaptor = adaptors.instantiate_adaptor(dataset)
    # receipt = adaptor.make_receipt(**make_receipt_args)
    receipt = make_receipt_args
    return receipt
