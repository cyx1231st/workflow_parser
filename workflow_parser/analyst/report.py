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

from __future__ import print_function

from collections import OrderedDict
from numbers import Integral
from numbers import Real


class Report(object):
    def __init__(self, name):
        self.outfile = None
        self.contents = []
        self.key_len = 0
        self.name = name
        self.content_dict = OrderedDict()
        self.register("Name", name)

    def __getitem__(self, item):
        return self.content_dict[item]

    def __setitem__(self, item, val):
        return self.register(item, val)

    def __repr__(self):
        ret = "<REPORT(%s)>" % self.name
        return ret

    def __str__(self):
        ret = repr(self)
        for content in self.contents:
            if content is None:
                ret += "\n"
            elif content[1] is None:
                format_str = "{:<" + str(self.key_len + 2) + "}"
                ret += "\n" + format_str.format(content[0])
            elif isinstance(content[1], Integral):
                format_str = "{:<" + str(self.key_len + 2) + "}{:d}"
                ret += "\n" + format_str.format(content[0]+":", content[1])
            elif isinstance(content[1], Real):
                format_str = "{:<" + str(self.key_len + 2) + "}{:7.5f}"
                ret += "\n" + format_str.format(content[0]+":", content[1])
            else:
                format_str = "{:<" + str(self.key_len + 2) + "}{:s}"
                ret += "\n" + format_str.format(content[0]+":", content[1])
        return ret

    def register(self, key, value):
        if key in self.content_dict.keys():
            raise RuntimeError("Report %s already has key %s!"
                               % (self.name, key))
        self.content_dict[key] = value
        self.contents.append((key, value))
        self.key_len = max(self.key_len, len(key))

    def blank(self):
        self.contents.append(None)

    def set_outfile(self, outfile, print_header):
        self.outfile = outfile
        self.print_header = print_header

    def export(self):
        if self.outfile is None:
            print("%s" % self)
        else:
            print("%s" % self)
            outfile = open(self.outfile, "a+")
            if self.print_header:
                header_fields = [content[0] for content in self.contents
                                 if content is not None]
                outfile.write(",".join(header_fields) + '\n')
            row_fields = [str(content[1]) for content in self.contents
                          if content is not None]
            outfile.write(",".join(row_fields) + '\n')
            outfile.flush()
            outfile.close()
