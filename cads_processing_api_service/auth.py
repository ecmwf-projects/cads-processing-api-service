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
from typing import Any

import cachetools
import cachetools.keys
import fastapi
import httpx

from . import cache, config, exceptions

VERIFICATION_ENDPOINTS = {
    "PRIVATE-TOKEN": "/account/verification/pat",
    "Authorization": "/account/verification/oidc",
}


def get_auth_header(
    pat: str
    | None = fastapi.Header(
        None, description="Personal Access Token", alias="PRIVATE-TOKEN"
    ),
    jwt: str
    | None = fastapi.Header(None, description="JSON Web Token", alias="Authorization"),
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


@cache.async_cached(
    cache=cachetools.TTLCache(
        maxsize=config.ensure_settings().cache_users_maxsize,
        ttl=config.ensure_settings().cache_users_ttl,
    ),
)
async def authenticate_user(auth_header: tuple[str, str]) -> str | None:
    """Verify user authentication.

    Verify if the provided authentication header corresponds to a registered user.
    If so, returns the registered user identifier.

    Parameters
    ----------
    auth_header : tuple[str, str]
        Authentication header.

    Returns
    -------
    str | None
        Registerd user identifier.

    Raises
    ------
    exceptions.PermissionDenied
        Raised if the provided authentication header doesn't correspond to a
        registered/authorized user.
    """
    verification_endpoint = VERIFICATION_ENDPOINTS[auth_header[0]]
    settings = config.ensure_settings()
    request_url = urllib.parse.urljoin(
        settings.internal_proxy_url,
        f"{settings.profiles_base_url}{verification_endpoint}",
    )
    async with httpx.AsyncClient() as client:
        response = await client.post(
            request_url, headers={auth_header[0]: auth_header[1]}
        )
    if response.status_code == fastapi.status.HTTP_401_UNAUTHORIZED:
        raise exceptions.PermissionDenied(
            status_code=response.status_code,
            title=response.json()["title"],
        )
    response.raise_for_status()
    user: dict[str, Any] = response.json()
    user_uid: str | None = user.get("sub", None)
    return user_uid


def verify_permission(user_uid: str, job: dict[str, Any]) -> None:
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
    if job["user_uid"] != user_uid:
        raise exceptions.PermissionDenied()


def get_contextual_accepted_licences(
    execution_content: dict[str, Any]
) -> set[tuple[str, int]]:
    """Get licences accepted in the context of a process execution request.

    Parameters
    ----------
    execution_content : dict[str, Any]
        Process execution request's payload.

    Returns
    -------
    set[tuple[str, int]]
        Accepted licences.
    """
    licences = execution_content.get("acceptedLicences")
    if not licences:
        licences = []
    accepted_licences = {(licence["id"], licence["revision"]) for licence in licences}
    return accepted_licences


@cache.async_cached(
    cache=cachetools.TTLCache(
        maxsize=config.ensure_settings().cache_users_maxsize,
        ttl=config.ensure_settings().cache_users_ttl,
    ),
)
async def get_stored_accepted_licences(
    auth_header: tuple[str, str]
) -> set[tuple[str, int]]:
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
    request_url = urllib.parse.urljoin(
        settings.internal_proxy_url,
        f"{settings.profiles_base_url}/account/licences",
    )
    async with httpx.AsyncClient() as client:
        response = await client.get(
            request_url, headers={auth_header[0]: auth_header[1]}
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
                "please accept the following licences to proceed: "
                f"{missing_licences_detail}"
            ),
        )
    return missing_licences


def validate_licences(
    execution_content: dict[str, Any],
    stored_accepted_licences: set[tuple[str, str]],
    licences: list[tuple[str, int]],
) -> None:
    """Validate process execution request's payload in terms of required licences.

    Parameters
    ----------
    execution_content : dict[str, Any]
        Process execution request's payload.
    stored_accepted_licences : set[tuple[str, str]]
        Licences accepted by a user stored in the Extended Profiles database.
    licences : list[tuple[str, int]]
        Licences bound to the required process/dataset.
    """
    required_licences = set(licences)
    contextual_accepted_licences = get_contextual_accepted_licences(execution_content)
    accepted_licences = contextual_accepted_licences.union(stored_accepted_licences)
    check_licences(required_licences, accepted_licences)  # type: ignore
