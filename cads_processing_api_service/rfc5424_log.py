"""Syslog module for the CADS Processing API Service.

This module is used to log in rfc5424 format.
"""

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

import logging
import logging.handlers

from syslog_rfc5424_formatter import RFC5424Formatter

SD_ID = "cads_processing_api_service"


def get_logger(name: str) -> logging.Logger:
    fmt = RFC5424Formatter(
        sd_id=SD_ID,
    )

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    return logger


def log_job_submission(log: logging.Logger, job: dict) -> None:
    """Log a job submission."""
    # Check the job data
    if "kwargs" not in job:
        log.warning("Job submission without kwargs. Unable to log job submission info.")
        return
    job_kwargs = job["kwargs"]

    # Check the job kwargs
    if "config" not in job_kwargs:
        log.warning(
            "Job submission without process_id. Unable to log job submission info."
        )
        return

    # We need config and collection_id
    config = job_kwargs["config"]
    if "collection_id" not in config:
        log.warning(
            "Job submission without collection_id. Unable to log job submission info."
        )
        return

    # We need request
    if "request" not in job_kwargs:
        log.warning(
            "Job submission without request. Unable to log job submission info."
        )
        return

    # We have all the data we need, log it
    collection_id = config["collection_id"]
    request = job_kwargs["request"]

    log.info(
        "Submitted job",
        {
            "structured_data": {
                "collection_id": collection_id,
                "request": request,
            }
        },
    )
