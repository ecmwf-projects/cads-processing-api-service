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

# mypy: ignore-errors
import os

import pytest


@pytest.fixture
def dev_env_proc_api_url() -> str:
    api_root_url = os.environ.get("CADS_API_ROOT_URL", "http://localhost:8080/api")
    proc_api_url = f"{api_root_url}/retrieve/v1/"
    return proc_api_url


@pytest.fixture
def dev_env_prof_api_url() -> str:
    api_root_url = os.environ.get("CADS_API_ROOT_URL", "http://localhost:8080/api")
    prof_api_url = f"{api_root_url}/profiles/v1/"
    return prof_api_url


@pytest.fixture(scope="session")
def prepare_env_for_download_nodes(tmp_path_factory) -> None:
    temp_dir = tmp_path_factory.mktemp("data")
    temp_file_path = temp_dir / "test-download-nodes.config"
    # Create and write to the file
    with open(temp_file_path, "w") as f:
        f.write("http://test_node")
    os.environ["DOWNLOAD_NODES_FILE"] = str(temp_file_path)
