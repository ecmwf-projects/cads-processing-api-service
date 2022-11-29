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

from cads_processing_api_service import adaptors


def test_make_system_job_kwargs_default() -> None:
    process_id = "test_process"
    inputs = {"input": "string_input"}
    execution_content = {"inputs": inputs}
    resource = cads_catalogue.database.Resource()

    exp_setup_code = adaptors.FALLBACK_SETUP_CODE
    config = adaptors.FALLBACK_CONFIG.copy()
    exp_entry_point = config.pop("entry_point")
    exp_kwargs = {
        "request": inputs,
        "config": config | {"collection_id": process_id} | {"mapping": {}},
    }

    job_kwargs = adaptors.make_system_job_kwargs(
        process_id, execution_content, resource
    )

    assert job_kwargs["setup_code"] == exp_setup_code
    assert job_kwargs["entry_point"] == exp_entry_point
    assert job_kwargs["kwargs"] == exp_kwargs
