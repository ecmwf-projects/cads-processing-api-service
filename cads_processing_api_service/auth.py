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

import cachetools
import cads_catalogue.database
import fastapi
import requests

from . import config, exceptions

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

    if not pat and not jwt:
        raise exceptions.PermissionDenied(
            status_code=fastapi.status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
        )
    if pat:
        auth_header = ("PRIVATE-TOKEN", pat)

    elif jwt:
        auth_header = ("Authorization", jwt)

    return auth_header


@cachetools.cached(cache=cachetools.TTLCache(maxsize=1024, ttl=10), info=True)
def authenticate_user(
    auth_header: tuple[str, str]
) -> dict[str, str | int | Mapping[str, str | int]]:
    verification_endpoint = VERIFICATION_ENDPOINTS[auth_header[0]]
    settings = config.ensure_settings()
    request_url = urllib.parse.urljoin(
        settings.internal_proxy_url,
        f"{settings.profiles_base_url}{verification_endpoint}",
    )
    response = requests.post(request_url, headers={auth_header[0]: auth_header[1]})
    if response.status_code == fastapi.status.HTTP_401_UNAUTHORIZED:
        raise exceptions.PermissionDenied(
            status_code=response.status_code, detail=response.json()["detail"]
        )
    response.raise_for_status()
    user: dict[str, str | int | Mapping[str, str | int]] = response.json()
    return user


def verify_permission(
    user: Mapping[str, str | int | Mapping[str, str | int]], job: dict[str, Any]
) -> None:
    user_id = user.get("id", None)
    if job["request_metadata"]["user_id"] != user_id:
        raise exceptions.PermissionDenied(detail="Operation not permitted")


def get_contextual_accepted_licences(
    execution_content: dict[str, Any]
) -> set[tuple[str, int]]:
    licences = execution_content.get("acceptedLicences")
    if not licences:
        licences = []
    accepted_licences = {(licence["id"], licence["revision"]) for licence in licences}
    return accepted_licences


# @cachetools.cached(cache=cachetools.TTLCache(maxsize=128, ttl=30), info=True)
def get_stored_accepted_licences(auth_header: tuple[str, str]) -> set[tuple[str, int]]:
    settings = config.ensure_settings()
    request_url = urllib.parse.urljoin(
        settings.internal_proxy_url,
        f"{settings.profiles_base_url}/account/licences",
    )
    response = requests.get(request_url, headers={auth_header[0]: auth_header[1]})
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


def validate_licences(
    execution_content: dict[str, Any],
    auth_header: tuple[str, str],
    licences: list[cads_catalogue.database.Licence],
) -> None:
    required_licences = {
        (licence.licence_uid, licence.revision) for licence in licences
    }
    contextual_accepted_licences = get_contextual_accepted_licences(execution_content)
    stored_accepted_licences = get_stored_accepted_licences(auth_header)
    accepted_licences = contextual_accepted_licences.union(stored_accepted_licences)
    check_licences(required_licences, accepted_licences)
