"""Caching module for the CADS Processing API Service."""

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

import cachetools


class TTLCache(cachetools.TTLCache):
    def expire(self, time=None):
        if time is None:
            time = self.timer()
        root = self.__root
        curr = root.next
        links = self.__links
        expired = []
        cache_delitem = cachetools.Cache.__delitem__
        cache_getitem = cachetools.Cache.__getitem__
        while curr is not root and not (time < curr.expires):
            try:
                expired_item = cache_getitem(self, curr.key)
                expired.append((curr.key, expired_item))
                cache_delitem(self, curr.key)
            except KeyError:
                pass
            try:
                del links[curr.key]
            except KeyError:
                pass
            next = curr.next
            curr.unlink()
            curr = next
        return expired
