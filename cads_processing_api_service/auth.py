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
import cads_broker
import fastapi
import requests

from . import config, costing, exceptions, models

SETTINGS = config.settings

VERIFICATION_ENDPOINT = {
    "PRIVATE-TOKEN": "/account/verification/pat",
    "Authorization": "/account/verification/oidc",
}

REQUEST_ORIGIN = {"PRIVATE-TOKEN": "api", "Authorization": "ui"}


def get_auth_header(pat: str | None = None, jwt: str | None = None) -> tuple[str, str]:
    """Infer authentication header based on authentication tokens.

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
        maxsize=SETTINGS.cache_users_maxsize,
        ttl=SETTINGS.cache_users_ttl,
    ),
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
    request_url = urllib.parse.urljoin(SETTINGS.profiles_api_url, verification_endpoint)
    response = requests.post(
        request_url,
        headers={
            auth_header[0]: auth_header[1],
            SETTINGS.portal_header_name: portal_header,
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


def get_auth_info(
    pat: str | None = fastapi.Header(
        None, description="Personal Access Token", alias="PRIVATE-TOKEN"
    ),
    jwt: str | None = fastapi.Header(
        None, description="JSON Web Token", alias="Authorization"
    ),
    portal_header: str | None = fastapi.Header(None, alias=config.PORTAL_HEADER_NAME),
) -> models.AuthInfo | None:
    """Get authentication information from the incoming HTTP request.

    Parameters
    ----------
    pat : str | None, optional
        Personal Access Token
    jwt : str | None, optional
        JSON Web Token
    portal_header : str | None, optional
        Portal header

    Returns
    -------
    dict[str, str] | None
        User identifier and role.

    Raises
    ------
    exceptions.PermissionDenied
        Raised if none of the expected authentication headers is provided.
    """
    auth_header = get_auth_header(pat, jwt)
    user_uid, user_role = authenticate_user(auth_header, portal_header)
    request_origin = REQUEST_ORIGIN[auth_header[0]]
    auth_info = models.AuthInfo(
        user_uid=user_uid,
        user_role=user_role,
        request_origin=request_origin,
        auth_header=auth_header,
        portal_header=portal_header,
    )
    return auth_info


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
    request_url = urllib.parse.urljoin(SETTINGS.profiles_api_url, "account/licences")
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


def verify_licences(
    accepted_licences: set[tuple[str, int]] | list[tuple[str, int]],
    required_licences: set[tuple[str, int]] | list[tuple[str, int]],
    api_request_url: str,
    process_id: str,
) -> set[tuple[str, int]]:
    """
    Verify if all the licences required for the process submission have been accepted.

    Parameters
    ----------
    accepted_licences : set[tuple[str, int]] | list[tuple[str, int]],
        Licences accepted by a user stored in the Extended Profiles database.
    required_licences : set[tuple[str, int]] | list[tuple[str, int]],
        Licences bound to the required process/dataset.
    api_request_url : str
        API request URL, required to generate the URL to the dataset licences page.
    process_id : str
        Process identifier, required to generate the URL to the dataset licences page.

    Returns
    -------
    set[tuple[str, int]]
        Required licences which have not been accepted.

    Raises
    ------
    exceptions.PermissionDenied
        Raised if not all required licences have been accepted.
    """
    if not isinstance(accepted_licences, set):
        accepted_licences = set(accepted_licences)
    if not isinstance(required_licences, set):
        required_licences = set(required_licences)
    missing_licences = required_licences - accepted_licences
    if not len(missing_licences) == 0:
        missing_licences_message_template = SETTINGS.missing_licences_message
        dataset_licences_url_template = SETTINGS.dataset_licences_url
        parsed_api_request_url = urllib.parse.urlparse(api_request_url)
        base_url = f"{parsed_api_request_url.scheme}://{parsed_api_request_url.netloc}"
        dataset_licences_url = dataset_licences_url_template.format(
            base_url=base_url, process_id=process_id
        )
        missing_licences_message = missing_licences_message_template.format(
            dataset_licences_url=dataset_licences_url
        )
        raise exceptions.PermissionDenied(
            title="required licences not accepted", detail=missing_licences_message
        )
    return missing_licences


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


def verify_cost(
    request: dict[str, Any], adaptor_properties: dict[str, Any], request_origin: str
) -> dict[str, float] | None:
    """Verify if the cost of a process execution request is within the allowed limits.

    Parameters
    ----------
    request : dict[str, Any]
        Process execution request.
    adaptor_properties : dict[str, Any]
        Adaptor properties.
    request_origin : str
        Origin of the request. Can be either "api" or "ui".

    Raises
    ------
    exceptions.PermissionDenied
        Raised if the cost of the process execution request exceeds the allowed limits.

    Returns
    -------
    dict[str, float] | None
        Request costs.
    """
    costing_info: models.CostingInfo = costing.compute_costing(
        request, adaptor_properties, request_origin
    )
    highest_cost: models.RequestCost = costing.compute_highest_cost_limit_ratio(
        costing_info
    )
    if highest_cost.cost > highest_cost.limit:
        raise exceptions.PermissionDenied(
            title="cost limits exceeded",
            detail="Your request is too large, please reduce your selection.",
        )
    else:
        return costing_info.costs
