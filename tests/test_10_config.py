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

import pathlib

from cads_processing_api_service import config


def test_load_download_nodes(tmp_path: pathlib.Path) -> None:
    download_nodes_file = tmp_path / "download-nodes.config"
    download_nodes_file.write_text(
        "http://download_node_1/\n\nhttp://download_node_2/\nhttp://download_node_3/"
    )
    downolad_nodes = config.load_download_nodes(download_nodes_file)
    exp_download_nodes = [
        "http://download_node_1/",
        "http://download_node_2/",
        "http://download_node_3/",
    ]
    assert downolad_nodes == exp_download_nodes
