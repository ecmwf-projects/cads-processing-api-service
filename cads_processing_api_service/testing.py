import asyncio
import time

import cads_catalogue.database
import fastapi
import ogc_api_processes_fastapi.exceptions
import sqlalchemy as sa
import sqlalchemy.orm
import structlog

from . import db_utils

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


def get_catalogue_session():
    catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker()
    catalogue_session = catalogue_sessionmaker()
    try:
        yield catalogue_session
    finally:
        catalogue_session.close()


def lookup_resource_by_id(
    id: str,
    session: sqlalchemy.orm.Session,
) -> cads_catalogue.database.Resource:
    """Look for the resource identified by `id` into the Catalogue database.

    Parameters
    ----------
    id : str
        Resource identifier.
    record : type[cads_catalogue.database.Resource]
        Catalogue database table.
    session : sqlalchemy.orm.Session
        Catalogue database session.

    Returns
    -------
    cads_catalogue.database.Resource
        Found resource.

    Raises
    ------
    ogc_api_processes_fastapi.exceptions.NoSuchProcess
        Raised if no resource corresponding to the provided `id` is found.
    """
    record = cads_catalogue.database.Resource
    try:
        row: cads_catalogue.database.Resource = (
            session.execute(
                sa.select(record).filter(record.resource_uid == id)  # type: ignore
            )
            .unique()
            .scalar_one()
        )
    except sqlalchemy.orm.exc.NoResultFound:
        raise ogc_api_processes_fastapi.exceptions.NoSuchProcess()
    session.expunge(row)  # type:ignore
    return row


def do_something_not_related_to_database(sleep_time: int = 10) -> None:
    time.sleep(sleep_time)


def sleep_and_give_me_a_number_sync(sleep_time: int = 10) -> int:
    logger.info("sync operation STARTED", sleep_time=sleep_time)
    time.sleep(sleep_time)
    number = sleep_time + 2
    logger.info("sync operation FINISHED", sleep_time=sleep_time)
    return number


async def sleep_and_give_me_a_number_async(sleep_time: int = 10) -> int:
    logger.info("async operation STARTED", sleep_time=sleep_time)
    await asyncio.sleep(sleep_time)
    number = sleep_time + 2
    logger.info("async operation FINISHED", sleep_time=sleep_time)
    return number


def database_connect_with_dependency(
    catalogue_session: fastapi.Depends = fastapi.Depends(get_catalogue_session),
) -> fastapi.responses.JSONResponse:
    # this is just to show that db session is opened when first operation on db is performed
    do_something_not_related_to_database(5)
    resource = lookup_resource_by_id(
        id="reanalysis-era5-single-levels", session=catalogue_session
    )
    do_something_not_related_to_database(5)
    return fastapi.responses.JSONResponse(
        status_code=fastapi.status.HTTP_200_OK,
        content={"resource": resource.resource_uid},
    )


def database_connect_with_context_manager() -> fastapi.responses.JSONResponse:
    catalogue_sessionmaker = db_utils.get_catalogue_sessionmaker()
    with catalogue_sessionmaker() as catalogue_session:
        # this is just to show that db session is opened when first operation on db is performed
        do_something_not_related_to_database(5)
        resource = lookup_resource_by_id(
            id="reanalysis-era5-single-levels", session=catalogue_session
        )
    do_something_not_related_to_database(5)
    return fastapi.responses.JSONResponse(
        status_code=fastapi.status.HTTP_200_OK,
        content={"resource": resource.resource_uid},
    )


def sync_one_operation() -> fastapi.responses.JSONResponse:
    start_time = time.time()
    num_0 = 5
    sleep_and_give_me_a_number_sync(num_0)
    return fastapi.responses.JSONResponse(
        status_code=fastapi.status.HTTP_200_OK,
        content={"time_elapsed": time.time() - start_time, "total": num_0},
    )


async def async_one_operation() -> fastapi.responses.JSONResponse:
    start_time = time.time()
    num_0 = 5
    await sleep_and_give_me_a_number_async(num_0)
    return fastapi.responses.JSONResponse(
        status_code=fastapi.status.HTTP_200_OK,
        content={"time_elapsed": time.time() - start_time, "total": num_0},
    )


def sync_dependent_operations() -> fastapi.responses.JSONResponse:
    start_time = time.time()
    num_0 = 1
    num_1 = sleep_and_give_me_a_number_sync(num_0)
    num_2 = sleep_and_give_me_a_number_sync(num_1)
    sleep_and_give_me_a_number_sync(num_2)
    return fastapi.responses.JSONResponse(
        status_code=fastapi.status.HTTP_200_OK,
        content={
            "time_elapsed": time.time() - start_time,
            "total": num_0 + num_1 + num_2,
        },
    )


def sync_independent_operations() -> fastapi.responses.JSONResponse:
    start_time = time.time()
    num_0 = 1
    num_1 = num_0 + 2
    num_2 = num_1 + 2
    sleep_and_give_me_a_number_sync(num_0)
    sleep_and_give_me_a_number_sync(num_1)
    sleep_and_give_me_a_number_sync(num_2)
    return fastapi.responses.JSONResponse(
        status_code=fastapi.status.HTTP_200_OK,
        content={
            "time_elapsed": time.time() - start_time,
            "total": num_0 + num_1 + num_2,
        },
    )


# This makes no sense, it's actually synchronous
async def async_dependent_operations() -> fastapi.responses.JSONResponse:
    start_time = time.time()
    num_0 = 1
    num_1 = await sleep_and_give_me_a_number_async(num_0)
    num_2 = await sleep_and_give_me_a_number_async(num_1)
    await sleep_and_give_me_a_number_async(num_2)
    return fastapi.responses.JSONResponse(
        status_code=fastapi.status.HTTP_200_OK,
        content={
            "time_elapsed": time.time() - start_time,
            "total": num_0 + num_1 + num_2,
        },
    )


async def async_independent_operations() -> fastapi.responses.JSONResponse:
    start_time = time.time()
    num_0 = 1
    num_1 = num_0 + 2
    num_2 = num_1 + 2
    tasks = [
        sleep_and_give_me_a_number_async(num_0),
        sleep_and_give_me_a_number_async(num_1),
        sleep_and_give_me_a_number_async(num_2),
    ]
    await asyncio.gather(
        *tasks,
    )
    return fastapi.responses.JSONResponse(
        status_code=fastapi.status.HTTP_200_OK,
        content={
            "time_elapsed": time.time() - start_time,
            "total": num_0 + num_1 + num_2,
        },
    )


def add_testing_routes(app: fastapi.FastAPI) -> fastapi.FastAPI:
    app.add_api_route(
        "/testing/connect-with-dependency",
        database_connect_with_dependency,
        methods=["GET"],
    )
    app.add_api_route(
        "/testing/connect-with-context-manager",
        database_connect_with_context_manager,
        methods=["GET"],
    )
    app.add_api_route(
        "/testing/sync-one-operation",
        sync_one_operation,
        methods=["GET"],
    )
    app.add_api_route(
        "/testing/async-one-operation",
        async_one_operation,
        methods=["GET"],
    )
    app.add_api_route(
        "/testing/sync-dependent-operations",
        sync_dependent_operations,
        methods=["GET"],
    )
    app.add_api_route(
        "/testing/sync-independent-operations",
        sync_independent_operations,
        methods=["GET"],
    )
    app.add_api_route(
        "/testing/async-dependent-operations",
        async_dependent_operations,
        methods=["GET"],
    )
    app.add_api_route(
        "/testing/async-independent-operations",
        async_independent_operations,
        methods=["GET"],
    )

    return app
