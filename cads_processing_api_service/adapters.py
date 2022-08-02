"""User requests to system requests adapters."""

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

import ogc_api_processes_fastapi.models


def adapt_user_request(
    job_id: str,
    process_id: str,
    execution_content: ogc_api_processes_fastapi.models.Execute,
) -> ogc_api_processes_fastapi.models.Execute:

    return execution_content
