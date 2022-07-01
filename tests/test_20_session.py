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

import cads_processing_api_service.config
import cads_processing_api_service.dbsession


def test_session_creation() -> None:
    settings = cads_processing_api_service.config.SqlalchemySettings()
    session = cads_processing_api_service.dbsession.Session.create_from_settings(
        settings
    )

    assert session.conn_string == settings.connection_string
