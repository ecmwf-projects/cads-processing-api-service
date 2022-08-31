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

import cads_catalogue.database
import ogc_api_processes_fastapi.models

from cads_processing_api_service import adapters


def test_make_system_request_default() -> None:
    process_id = "test_process"
    inputs = {"input": "string_input"}
    execution_content = ogc_api_processes_fastapi.models.Execute(inputs=inputs)
    job_id = "test_job_id"
    resource = cads_catalogue.database.Resource()

    exp_setup_code = adapters.FALLBACK_SETUP_CODE
    exp_entry_point = adapters.FALLBACK_ENTRY_POINT
    exp_kwargs = {"request": inputs, "config": adapters.FALLBACK_CONFIG}
    exp_metadata = {"X-Process-ID": process_id, "X-Job-ID": job_id}

    request = adapters.make_system_request(
        process_id, execution_content, job_id, resource
    )

    assert request["inputs"]["setup_code"] == exp_setup_code
    assert request["inputs"]["entry_point"] == exp_entry_point
    assert request["inputs"]["kwargs"]["value"] == exp_kwargs
    assert request["metadata"] == exp_metadata
