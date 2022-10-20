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

import cads_broker  # type: ignore
import sqlalchemy

from cads_processing_api_service import clients


def test_apply_jobs_filters() -> None:
    job_table = cads_broker.database.SystemRequest
    statement = sqlalchemy.select(job_table)

    filters = {"process_id": ["process"], "status": ["successful", "failed"]}
    statement = clients.apply_jobs_filters(statement, job_table, filters)
    compiled_statement = statement.compile()

    exp_params = {"process_id_1": ["process"], "status_1": ["successful", "failed"]}
    assert compiled_statement.params == exp_params
