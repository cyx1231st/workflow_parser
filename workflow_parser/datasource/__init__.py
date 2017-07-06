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

from functools import total_ordering
import numbers

from .. import reserved_vars as rv
from ..service_registry import Component


@total_ordering
class Line(object):
    def __init__(self, time,
                       seconds,
                       keyword,
                       request,
                       thread_obj,
                       vs,
                       entity,
                       prv_thread_line,
                       prv_target_line):
        assert isinstance(time, str)
        assert isinstance(seconds, numbers.Real)
        assert isinstance(keyword, str)
        assert isinstance(thread_obj, Thread)
        assert entity is not None
        assert entity._assigned is None

        assert isinstance(vs, dict)
        assert not rv.ALL_VARS & vs.viewkeys()

        self.time = time
        self._seconds = seconds
        self.keyword = keyword
        self.request = request

        self.line = entity.line
        self.thread_obj = thread_obj
        self._schema_vars = vs
        self._entity = entity
        self._assigned = None

        self.prv_thread_line = prv_thread_line
        self.nxt_thread_line = None
        self.prv_target_line = prv_target_line
        self.nxt_target_line = None

        if prv_thread_line:
            assert isinstance(prv_thread_line, Line)
            assert prv_thread_line <= self
            prv_thread_line.nxt_thread_line = self
        if prv_target_line:
            assert isinstance(prv_target_line, Line)
            assert prv_target_line <= self
            prv_target_line.nxt_target_line = self
        entity._assigned = self

    @property
    def seconds(self):
        return self._seconds + self.target_obj.offset

    @property
    def thread(self):
        return self.thread_obj.thread

    @property
    def target(self):
        return self.thread_obj.target

    @property
    def host(self):
        return self.thread_obj.host

    @property
    def component(self):
        return self.thread_obj.component

    @property
    def target_obj(self):
        return self.thread_obj.target_obj

    # total ordering
    __eq__ = lambda self, other: self.seconds == other.seconds
    __lt__ = lambda self, other: self.seconds < other.seconds

    @property
    def keys(self):
        return self._schema_vars.viewkeys() | rv.ALL_VARS

    def __contains__(self, item):
        return item in self.keys

    def __getitem__(self, key):
        assert isinstance(key, str)
        if key in rv.ALL_VARS:
            return getattr(self, key)
        else:
            return self._schema_vars.get(key)

    def __str_marks__(self):
        mark_str = ""
        if not self._assigned:
            mark_str += ", ~ASSIGN"
        return mark_str

    def __str__(self):
        ret = "<L>%.3f %s %s [%s %s %s] %s: <%s>%s%s" % (
              self.seconds,
              self.time,
              self.thread,
              self.target,
              self.host,
              self.component,
              self.request,
              self.keyword,
              ",".join(self._schema_vars.keys()),
              self.__str_marks__())
        return ret

    def __str_target__(self):
        ret = "<L>%.3f %s %s %s: <%s>%s%s" % (
              self.seconds,
              self.time,
              self.thread,
              self.request,
              self.keyword,
              ",".join(self._schema_vars.keys()),
              self.__str_marks__())
        return ret

    def __str_thread__(self):
        ret = "<L>%.3f %s %s: <%s>%s%s" % (
              self.seconds,
              self.time,
              self.request,
              self.keyword,
              ",".join(self._schema_vars.keys()),
              self.__str_marks__())
        return ret

    def __repr__(self):
        ret = str(self)
        ret += "\n  V:"
        for k, v in self._schema_vars.iteritems():
            ret += "%s=%s," % (k, v)
        ret += "\n  %s: %s" % (self.target_obj._entity.name, self.line)
        return ret


class Thread(object):
    _repr_lines_lim = 10

    def __init__(self, id_, target_obj, thread):
        assert isinstance(id_, int)
        assert isinstance(target_obj, Target)
        assert isinstance(thread, str)

        self.id_ = id_
        self.thread = thread
        self.target_obj = target_obj
        self.line_objs = []

        # after thread instances are built
        self.threadinss = []
        self.dangling_lineobjs = []

    @property
    def name(self):
        return "%s|td%d" % (self.target, self.id_)

    @property
    def target(self):
        return self.target_obj.target

    @property
    def component(self):
        return self.target_obj.component

    @property
    def host(self):
        return self.target_obj.host

    @property
    def lapse(self):
        assert self.threadinss
        return sum(ti.lapse for ti in self.threadinss)

    def __str__(self):
        return "<Thread#%s: %s, %d lines, %d dangling, %d threadinss>" %\
                (self.name,
                 self.thread,
                 len(self.line_objs),
                 len(self.dangling_lineobjs),
                 len(self.threadinss))

    def __repr__(self):
        ret = str(self)
        lim = self._repr_lines_lim
        ret += "\n^ ---- last ----"
        for line in reversed(self.line_objs):
            ret += "\n| %s" % line.__str_thread__()
            lim -= 1
            if lim <= 0:
                ret += "\n ......"
        ret += "\n  ---- first ---"
        return ret


class Target(object):
    _repr_lines_lim = 10

    def __init__(self, target, component, host, entity):
        assert isinstance(target, str)
        assert isinstance(component, Component)
        assert isinstance(host, str)

        self.target = target
        self.component = component
        self.host = host

        self._offset = 0
        self.line_objs = []
        self.thread_objs = {}
        self.threadobjs_list = []

        self._entity = entity

    @property
    def offset(self):
        return self._offset

    @offset.setter
    def offset(self, val):
        assert isinstance(val, numbers.Real)
        self._offset = val

    def __str__(self):
        return "<Target#%s: from %s, comp=%s, host=%s, off=%d, %d lines, %d threads>" % (
               self.target,
               self._entity.name,
               self.component,
               self.host,
               self._offset,
               len(self.line_objs),
               len(self.thread_objs))

    def __repr__(self):
        ret = str(self)
        lim = self._repr_lines_lim
        ret += "\n^ ---- last ----"
        for line in reversed(self.line_objs):
            ret += "\n| %s" % line.__str_target__()
            lim -= 1
            if lim <= 0:
                ret += "\n ......"
        ret += "\n  ---- first ---"
        return ret
