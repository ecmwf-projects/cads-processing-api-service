"""Authentication and authorization functions."""

# Copyright 2022, European Union.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import urllib.parse
from typing import Any, Mapping

import cads_catalogue.database
import fastapi
import requests
import sqlalchemy.orm
import structlog

from . import config, crud, exceptions, tracing


def authenticate_user(
    user_auth_reqs: dict[str, str]
) -> dict[str, str | int | Mapping[str, str | int]]:
    settings = config.ensure_settings()
    verification_endpoint = user_auth_reqs["verification_endpoint"]
    request_url = urllib.parse.urljoin(
        settings.internal_proxy_url,
        f"{settings.profiles_base_url}{verification_endpoint}",
    )
    auth_header = {
        user_auth_reqs["auth_header_name"]: user_auth_reqs["auth_header_value"]
    }
    headers = tracing.add_request_id_header(auth_header)
    response = requests.post(request_url, headers=headers)
    if response.status_code == fastapi.status.HTTP_401_UNAUTHORIZED:
        raise exceptions.PermissionDenied(
            status_code=response.status_code, detail=response.json()["detail"]
        )
    response.raise_for_status()
    user: dict[str, str | int | Mapping[str, str | int]] = response.json()
    user["authentication_header"] = auth_header
    structlog.contextvars.bind_contextvars(user_id=user["id"])
    return user


def get_user_auth_requirements(
    pat: str
    | None = fastapi.Header(
        None, description="Personal Access Token", alias="PRIVATE-TOKEN"
    ),
    jwt: str
    | None = fastapi.Header(None, description="JSON Web Token", alias="Authorization"),
) -> dict[str, str]:
    if not pat and not jwt:
        raise exceptions.PermissionDenied(
            status_code=fastapi.status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
        )
    if pat:
        auth_requirements = {
            "auth_header_name": "PRIVATE-TOKEN",
            "auth_header_value": pat,
            "verification_endpoint": "/account/verification/pat",
        }
    elif jwt:
        auth_requirements = {
            "auth_header_name": "Authorization",
            "auth_header_value": jwt,
            "verification_endpoint": "/account/verification/oidc",
        }

    return auth_requirements


def get_contextual_accepted_licences(
    execution_content: dict[str, Any]
) -> set[tuple[str, int]]:
    licences = execution_content.get("acceptedLicences")
    if not licences:
        licences = []
    accepted_licences = {(licence["id"], licence["revision"]) for licence in licences}
    return accepted_licences


def get_stored_accepted_licences(auth_header: dict[str, str]) -> set[tuple[str, int]]:
    settings = config.ensure_settings()
    request_url = urllib.parse.urljoin(
        settings.internal_proxy_url,
        f"{settings.profiles_base_url}/account/licences",
    )
    headers = tracing.add_request_id_header(auth_header)
    response = requests.get(request_url, headers=headers)
    response.raise_for_status()
    licences = response.json()["licences"]
    accepted_licences = {(licence["id"], licence["revision"]) for licence in licences}
    return accepted_licences


def check_licences(
    required_licences: set[tuple[str, int]], accepted_licences: set[tuple[str, int]]
) -> set[tuple[str, int]]:
    missing_licences = required_licences - accepted_licences
    if not len(missing_licences) == 0:
        missing_licences_detail = [
            {"id": licence[0], "revision": licence[1]} for licence in missing_licences
        ]
        raise exceptions.PermissionDenied(
            title="required licences not accepted",
            detail=(
                "please accept the following licences to proceed: "
                f"{missing_licences_detail}"
            ),
        )
    return missing_licences


def validate_request(
    process_id: str,
    execution_content: dict[str, Any],
    auth_header: dict[str, str],
    session: sqlalchemy.orm.Session,
    process_table: type[cads_catalogue.database.Resource],
) -> cads_catalogue.database.Resource:
    """Validate retrieve process execution request.

    Check if requested dataset exists and if execution content is valid.
    In case the check is successful, returns the resource (dataset)
    associated to the process request.

    Parameters
    ----------
    process_id : str
        Process ID
    execution_content: dict[str, Any]
        Content of the process execution request
    auth_header: dict[str, str]
        Authorization header sent with the request
    session : sqlalchemy.orm.Session
        SQLAlchemy ORM session
    process_table: Type[cads_catalogue.database.Resource]
        Resources table

    Returns
    -------
    cads_catalogue.database.BaseModel
        Resource (dataset) associated to the process request.
    """
    resource = crud.lookup_resource_by_id(
        id=process_id, record=process_table, session=session
    )
    licences: list[cads_catalogue.database.Licence] = resource.licences  # type: ignore
    required_licences = {
        (licence.licence_uid, licence.revision) for licence in licences
    }
    contextual_accepted_licences = get_contextual_accepted_licences(execution_content)
    stored_accepted_licences = get_stored_accepted_licences(auth_header)
    accepted_licences = contextual_accepted_licences.union(stored_accepted_licences)
    check_licences(required_licences, accepted_licences)

    return resource


def verify_permission(
    user: Mapping[str, str | int | Mapping[str, str | int]], job: dict[str, Any]
) -> None:
    user_id = user.get("id", None)
    if job["request_metadata"]["user_id"] != user_id:
        raise exceptions.PermissionDenied(detail="Operation not permitted")
