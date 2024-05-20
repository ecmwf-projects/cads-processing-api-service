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

import threading
import urllib.parse
from typing import Any

import cachetools
import cads_broker
import fastapi
import requests

from . import config, costing, exceptions

VERIFICATION_ENDPOINT = {
    "PRIVATE-TOKEN": "/account/verification/pat",
    "Authorization": "/account/verification/oidc",
}

REQUEST_ORIGIN = {"PRIVATE-TOKEN": "api", "Authorization": "ui"}


def get_auth_header(
    pat: str | None = fastapi.Header(
        None, description="Personal Access Token", alias="PRIVATE-TOKEN"
    ),
    jwt: str | None = fastapi.Header(
        None, description="JSON Web Token", alias="Authorization"
    ),
) -> tuple[str, str]:
    """Get authentication header from the incoming HTTP request.

    Parameters
    ----------
    pat : str | None, optional
        Personal Access Token
    jwt : str | None, optional
        JSON Web Token

    Returns
    -------
    tuple[str, str]
        Authentication header.

    Raises
    ------
    exceptions.PermissionDenied
        Raised if none of the expected authentication headers is provided.
    """
    if not pat and not jwt:
        raise exceptions.PermissionDenied(
            status_code=fastapi.status.HTTP_401_UNAUTHORIZED,
            detail="authentication required",
        )
    if pat:
        auth_header = ("PRIVATE-TOKEN", pat)

    elif jwt:
        auth_header = ("Authorization", jwt)

    return auth_header


@cachetools.cached(
    cache=cachetools.TTLCache(
        maxsize=config.ensure_settings().cache_users_maxsize,
        ttl=config.ensure_settings().cache_users_ttl,
    ),
    lock=threading.Lock(),
)
def authenticate_user(
    auth_header: tuple[str, str], portal_header: str | None = None
) -> tuple[str | None, str | None]:
    """Verify user authentication.

    Verify if the provided authentication header corresponds to a registered user.
    If so, returns the registered user identifier.

    Parameters
    ----------
    auth_header : tuple[str, str]
        Authentication header.

    Returns
    -------
    tuple[str, str] | None
        User identifier and role.

    Raises
    ------
    exceptions.PermissionDenied
        Raised if the provided authentication header doesn't correspond to a
        registered/authorized user.
    """
    verification_endpoint = VERIFICATION_ENDPOINT[auth_header[0]]
    settings = config.ensure_settings()
    request_url = urllib.parse.urljoin(settings.profiles_api_url, verification_endpoint)
    response = requests.post(
        request_url,
        headers={
            auth_header[0]: auth_header[1],
            config.PORTAL_HEADER_NAME: portal_header,
        },
    )
    if response.status_code in (
        fastapi.status.HTTP_401_UNAUTHORIZED,
        fastapi.status.HTTP_403_FORBIDDEN,
    ):
        raise exceptions.PermissionDenied(
            status_code=response.status_code,
            title=response.json()["title"],
            detail="operation not allowed",
        )
    response.raise_for_status()
    user: dict[str, Any] = response.json()
    user_uid: str | None = user.get("sub", None)
    user_role: str | None = user.get("role", None)
    return user_uid, user_role


def verify_permission(user_uid: str, job: cads_broker.SystemRequest) -> None:
    """Verify if a user has permission to interact with a job.

    Parameters
    ----------
    user_uid : str
        User identifier.
    job : dict[str, Any]
        Job description.

    Raises
    ------
    exceptions.PermissionDenied
        Raised if the user has no permission to interact with the job.
    """
    if job.user_uid != user_uid:
        raise exceptions.PermissionDenied(
            detail="operation not allowed",
        )


@cachetools.cached(
    cache=cachetools.TTLCache(
        maxsize=config.ensure_settings().cache_users_maxsize,
        ttl=config.ensure_settings().cache_users_ttl,
    ),
    lock=threading.Lock(),
)
def get_accepted_licences(auth_header: tuple[str, str]) -> set[tuple[str, int]]:
    """Get licences accepted by a user stored in the Extended Profiles database.

    The user is identified by the provided authentication header.

    Parameters
    ----------
    auth_header : tuple[str, str]
        Authentication header

    Returns
    -------
    set[tuple[str, int]]
        Accepted licences.
    """
    settings = config.ensure_settings()
    request_url = urllib.parse.urljoin(settings.profiles_api_url, "account/licences")
    response = requests.get(request_url, headers={auth_header[0]: auth_header[1]})
    if response.status_code in (
        fastapi.status.HTTP_401_UNAUTHORIZED,
        fastapi.status.HTTP_403_FORBIDDEN,
    ):
        raise exceptions.PermissionDenied(
            status_code=response.status_code,
            title=response.json()["title"],
            detail="operation not allowed",
        )
    response.raise_for_status()
    licences = response.json()["licences"]
    accepted_licences = {(licence["id"], licence["revision"]) for licence in licences}
    return accepted_licences


def check_licences(
    required_licences: set[tuple[str, int]], accepted_licences: set[tuple[str, int]]
) -> set[tuple[str, int]]:
    """Check if accepted licences satisfy required ones.

    Parameters
    ----------
    required_licences : set[tuple[str, int]]
        Required licences.
    accepted_licences : set[tuple[str, int]]
        Accepted licences.

    Returns
    -------
    set[tuple[str, int]]
        Required licences which have not been accepted.

    Raises
    ------
    exceptions.PermissionDenied
        Raised if not all required licences have been accepted.
    """
    missing_licences = required_licences - accepted_licences
    if not len(missing_licences) == 0:
        missing_licences_detail = [
            {"id": licence[0], "revision": licence[1]} for licence in missing_licences
        ]
        raise exceptions.PermissionDenied(
            title="required licences not accepted",
            detail=(
                "required licences not accepted; "
                "please accept the following licences to proceed: "
                f"{missing_licences_detail}"
            ),
        )
    return missing_licences


def validate_licences(
    accepted_licences: set[tuple[str, str]],
    licences: list[tuple[str, int]],
) -> None:
    """Validate process execution request's payload in terms of required licences.

    Parameters
    ----------
    stored_accepted_licences : set[tuple[str, str]]
        Licences accepted by a user stored in the Extended Profiles database.
    licences : list[tuple[str, int]]
        Licences bound to the required process/dataset.
    """
    required_licences = set(licences)
    check_licences(required_licences, accepted_licences)  # type: ignore


def verify_if_disabled(disabled_reason: str | None, user_role: str | None) -> None:
    """Verify if a dataset's disabling reason grant access to the dataset for a specific user role.

    Parameters
    ----------
    disabled_reason : str | None
        Dataset's disabling reason.
    user_role : str | None
        User role.

    Raises
    ------
    exceptions.PermissionDenied
        Raised if the user role has no permission to interact with the dataset.
    """
    if disabled_reason and user_role != "manager":
        raise exceptions.PermissionDenied(
            detail=disabled_reason,
        )
    else:
        return


def verify_cost(request: dict[str, Any], adaptor_properties: dict[str, Any]) -> None:
    """Verify if the cost of a process execution request is within the allowed limits.

    Parameters
    ----------
    request : dict[str, Any]
        Process execution request.
    adaptor_properties : dict[str, Any]
        Adaptor properties.

    Raises
    ------
    exceptions.PermissionDenied
        Raised if the cost of the process execution request exceeds the allowed limits.
    """
    costing_info = costing.compute_costing(request, adaptor_properties)
    max_costs_exceeded = costing_info.max_costs_exceeded
    if max_costs_exceeded:
        raise exceptions.PermissionDenied(
            title="cost limits exceeded",
            detail=(
                "the cost of the submitted request exceeds the allowed limits; "
                f"the following limits have been exceeded: {max_costs_exceeded}"
            ),
        )
    else:
        return
