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

import prometheus_client

from cads_processing_api_service import metrics


def test_handle_download_metrics() -> None:
    test_job = {"process_id": "test_process_id"}
    test_results = {"asset": {"value": {"file:size": 100}}}
    for _ in range(2):
        metrics.handle_download_metrics(test_job, test_results)

    exp_download_bytes_count = 2
    res_download_bytes_count = prometheus_client.REGISTRY.get_sample_value(
        "download_bytes_count", labels={"dataset_id": "test_process_id"}
    )
    assert res_download_bytes_count == exp_download_bytes_count

    exp_download_bytes_sum = 200
    res_download_bytes_sum = prometheus_client.REGISTRY.get_sample_value(
        "download_bytes_sum", labels={"dataset_id": "test_process_id"}
    )
    assert res_download_bytes_sum == exp_download_bytes_sum
