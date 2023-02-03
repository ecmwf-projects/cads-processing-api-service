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

from cads_processing_api_service import utils


def test_encode_decode_base64() -> None:
    exp_decoded = "2022-10-24T12:24:29.919877"
    encoded = utils.encode_base64(exp_decoded)
    decoded = utils.decode_base64(encoded)
    assert decoded == exp_decoded
