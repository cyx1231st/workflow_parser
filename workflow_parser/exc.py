# Copyright (c) 2017 Yingxin Cheng
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

class WFException(Exception):
    def __init__(self, msg, e=None):
        assert isinstance(msg, str)
        if e:
            assert isinstance(e, Exception)

        self.e = e
        super(WFException, self).__init__(msg)

    def __str__(self):
        return self._to_str(0)

    def _to_str(self, indent):
        ret = "\n%s%s" % ("> "*indent, self.args[0])
        if self.e:
            if isinstance(self.e, WFException):
                ret += self.e._to_str(indent+1)
            else:
                ret += "\n%s%r" % ("> "*(indent+1), self.e)
        return ret
