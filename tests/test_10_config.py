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

import pytest
import yaml

from cads_processing_api_service import config


def test_load_download_nodes(tmp_path: pathlib.Path) -> None:
    download_nodes_file = tmp_path / "download-nodes.config"
    download_nodes_file.write_text(
        "http://download_node_1/\n\nhttp://download_node_2/\nhttp://download_node_3/"
    )
    download_nodes = config.load_download_nodes(download_nodes_file)
    exp_download_nodes = [
        "http://download_node_1/",
        "http://download_node_2/",
        "http://download_node_3/",
    ]
    assert download_nodes == exp_download_nodes

    not_existing_download_nodes_file = tmp_path / "not-existing-download-nodes.config"
    with pytest.raises(FileNotFoundError):
        _ = config.load_download_nodes(not_existing_download_nodes_file)

    empty_download_nodes_file = tmp_path / "empty-download-nodes.config"
    empty_download_nodes_file.write_text("")
    with pytest.raises(ValueError):
        _ = config.load_download_nodes(empty_download_nodes_file)


def test_validate_download_nodes_file(tmp_path: pathlib.Path) -> None:
    download_nodes_file = tmp_path / "download-nodes.config"
    download_nodes_file.write_text(
        "http://download_node_1/\n\nhttp://download_node_2/\nhttp://download_node_3/"
    )
    download_nodes_file_path = config.validate_download_nodes_file(
        str(download_nodes_file)
    )
    exp_download_nodes_file_path = download_nodes_file
    assert download_nodes_file_path == exp_download_nodes_file_path


def test_validate_rate_limits() -> None:
    rate_limits = ["1/second", "10/minute"]
    config.validate_rate_limits(rate_limits)

    rate_limits = ["invalid_limit"]
    with pytest.raises(ValueError):
        config.validate_rate_limits(rate_limits)


def test_load_rate_limits(tmp_path: pathlib.Path, caplog) -> None:
    rate_limits_file = None
    loaded_rate_limits = config.load_rate_limits(rate_limits_file)
    assert loaded_rate_limits == config.RateLimitsConfig()

    rate_limits_file = str(tmp_path / "rate-limits.yaml")
    rate_limits = {
        "auth": {
            "/jobs/{job_id}": {"get": {"api": ["1/second"], "ui": ["2/second"]}},
            "/processes/{process_id}/constraints": {
                "default": {"get": {"api": ["1/second"], "ui": ["2/second"]}},
                "process-id": {"post": {"api": ["1/second"], "ui": ["2/second"]}},
            },
        }
    }
    with open(rate_limits_file, "w") as file:
        yaml.dump(rate_limits, file)
    loaded_rate_limits = config.load_rate_limits(rate_limits_file).model_dump()
    expected_jobs_limits = {
        "get": {"api": ["1/second"], "ui": ["2/second"]},
        "post": {"api": [], "ui": []},
        "delete": {"api": [], "ui": []},
    }
    assert "auth" in loaded_rate_limits
    assert loaded_rate_limits["auth"]["jobs_jobsid"] == expected_jobs_limits
    expected_process_constraints_limits = {
        "default": {
            "get": {"api": ["1/second"], "ui": ["2/second"]},
            "post": {"api": [], "ui": []},
            "delete": {"api": [], "ui": []},
        },
        "process-id": {
            "get": {"api": [], "ui": []},
            "post": {"api": ["1/second"], "ui": ["2/second"]},
            "delete": {"api": [], "ui": []},
        },
    }
    assert (
        loaded_rate_limits["auth"]["processes_processid_constraints"]
        == expected_process_constraints_limits
    )

    rate_limits_file = str(tmp_path / "invalid-rate-limits.yaml")
    rate_limits = {
        "auth": {
            "/jobs/{job_id}": {"get": {"api": ["invalid_limit"]}},
        }
    }
    with open(rate_limits_file, "w") as file:
        yaml.dump(rate_limits, file)
    loaded_rate_limits = config.load_rate_limits(rate_limits_file)
    assert loaded_rate_limits == config.RateLimitsConfig()

    rate_limits_file = str(tmp_path / "not-found-rate-limits.yaml")
    loaded_rate_limits = config.load_rate_limits(rate_limits_file)
    assert loaded_rate_limits == config.RateLimitsConfig()
