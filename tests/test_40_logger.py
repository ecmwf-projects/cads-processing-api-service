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
import logging

from cads_processing_api_service.rfc5424_log import log_job_submission, get_logger


def test_logger_with_process() -> None:

    # this is the output expected from adaptors.make_system_job_kwargs
    mock_job_kwargs = {
        "setup_code": '',
        "entry_point": '',
        "kwargs": {
            "config": {
                "key": "00000:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                "url": "https://cds.climate.copernicus.eu/api/v2",
                "collection_id": "reanalysis-era5-single-levels"
            },
            "request": {
                "day": ["20"],
                "time": ["14:00"],
                "year": ["2022"],
                "month": ["10"],
                "format": "grib",
                "variable": ["mean_sea_level_pressure"],
                "product_type": ["reanalysis"]
            }
        }
    }

    # Log the job kwargs
    logger = get_logger(__name__)

    # Intercept log messages from the logging module in order to check the format
    # of the log message
    log_filter = LogFilter()
    logger.addFilter(log_filter)

    log_job_submission(logger, mock_job_kwargs)

    messages = log_filter.get_messages()
    assert len(messages) == 1

    sd = messages[0].sd

    assert sd.startswith("[")
    assert sd.endswith("]")
    assert "reanalysis-era5-single-levels" in sd
    assert "[request " in sd


class LogFilter(logging.Filter):

    def __init__(self):
        self.messages = []
        super().__init__()

    def get_messages(self):
        return self.messages

    def filter(self, record):
        self.messages.append(record)
        return True