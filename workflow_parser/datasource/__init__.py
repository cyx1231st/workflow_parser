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

from abc import ABCMeta
from abc import abstractproperty
from functools import total_ordering
import numbers

from .. import reserved_vars as rv
from ..service_registry import Component


class LineStateBase(object):
    __metaclass__ = ABCMeta

    @abstractproperty
    def _ls_state(self):
        return "?"

    @abstractproperty
    def _ls_request(self):
        return "?"

    @abstractproperty
    def _ls_path(self):
        return "?"


@total_ordering
class Line(object):
    _str_lines_nxtlim = 10
    _str_lines_prvlim = 10

    def __init__(self, lino,
                       time,
                       seconds,
                       keyword,
                       request,
                       thread_obj,
                       vs,
                       line,
                       entity):
        assert isinstance(time, str)
        assert isinstance(seconds, numbers.Real)
        assert isinstance(keyword, str)
        assert isinstance(thread_obj, Thread)
        assert isinstance(line, str)
        assert entity is not None
        assert entity._assigned is None

        assert isinstance(vs, dict)
        assert not rv.ALL_VARS & vs.viewkeys()

        self.lino = lino
        self.time = time
        self._seconds = seconds
        self.keyword = keyword
        self.request = request

        self.line = line
        self.thread_obj = thread_obj
        self._schema_vars = vs
        self._built_from = entity
        self._line_state = None

        self.prv_thread_line = None
        self.nxt_thread_line = None
        self.prv_target_line = None
        self.nxt_target_line = None

        entity._assigned = self

    @property
    def name(self):
        return "%s~%s" % (self.target, self.lino)

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

    @property
    def keys(self):
        return self._schema_vars.viewkeys() | rv.ALL_VARS

    @property
    def keys_(self):
        return self._schema_vars.viewkeys()

    # total ordering
    __eq__ = lambda self, other: self.seconds == other.seconds
    __lt__ = lambda self, other: self.seconds < other.seconds

    def __contains__(self, item):
        return item in self.keys

    def __getitem__(self, key):
        assert isinstance(key, str)
        if key in rv.ALL_VARS:
            return getattr(self, key)
        else:
            return self._schema_vars.get(key)

    def __repr_marks__(self):
        mark_str = ""
        if self._schema_vars:
            kvs = [ str(item[0])+"="+str(item[1]) for item in self._schema_vars.iteritems()]
            mark_str += ", (%s)"% ",".join(kvs)
        if self._line_state is None:
            mark_str += ", ~PATH"
        else:
            mark_str += ", %s" % self._line_state._ls_path
        return mark_str

    def __repr__(self):
        ret = "<L#%s: %.3f %s [%s %s] %s %s `%s`%s>" % (
              self.name,
              self.seconds,
              self.time,
              self.host,
              self.component,
              self.thread,
              self.request,
              self.keyword,
              self.__repr_marks__())
        return ret

    def __repr_target__(self):
        ret = "#%s: %.3f %s %s %s `%s`%s" % (
              self.lino,
              self.seconds,
              self.time,
              self.thread,
              self.request,
              self.keyword,
              self.__repr_marks__())
        return ret

    def __repr_thread__(self):
        if not self._line_state:
            prefix = "."
            req = self.request
        else:
            prefix = self._line_state._ls_state
            req = self._line_state._ls_request

        ret = "%s #%s %.3f %s %s `%s`%s" % (
              prefix,
              self.lino,
              self.seconds,
              self.time,
              req,
              self.keyword,
              self.__repr_marks__())
        return ret

    def __str__(self):
        ret = repr(self.thread_obj)+":"
        line = self
        cnt_nxt = 0
        while cnt_nxt < self._str_lines_nxtlim:
            if line.nxt_thread_line:
                line = line.nxt_thread_line
                cnt_nxt += 1
            else:
                break

        ret += "\n------- end -----"
        if line is not None and line.nxt_thread_line is not None:
            ret += "\n "+self.thread_obj.last_lineobj.__repr_thread__()
            if line.nxt_thread_line is not self.thread_obj.last_lineobj:
                ret += "\n . ......"

        while cnt_nxt > 0:
            ret += "\n "+line.__repr_thread__()
            line = line.prv_thread_line
            cnt_nxt -= 1

        ret += "\n>"+line.__repr_thread__()

        cnt_prv = 0
        while cnt_prv < self._str_lines_prvlim:
            if line.prv_thread_line:
                line = line.prv_thread_line
                ret += "\n "+line.__repr_thread__()
                cnt_prv += 1
            else:
                break

        if line is not None and line.prv_thread_line is not None:
            if line.prv_thread_line is not self.thread_obj.start_lineobj:
                ret += "\n . ......"
            ret += "\n "+self.thread_obj.start_lineobj.__repr_thread__()
        ret += "\n------- start ---"
        return ret

    def set_linestate(self, ls):
        assert isinstance(ls, LineStateBase)
        assert self._line_state is None
        self._line_state = ls


class Thread(object):
    _str_lines_lim = 10

    def __init__(self, id_, target_obj, thread):
        assert isinstance(id_, int)
        assert isinstance(target_obj, Target)
        assert isinstance(thread, str)

        self.id_ = id_
        self.thread = thread
        self.target_obj = target_obj

        self.start_lineobj = None
        self.last_lineobj = None
        self.len_lineobjs = 0

        # after thread instances are built
        # TODO: change to interval
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

    def __repr__(self):
        return "<Thread#%s-%s: %d lines, %d threadinss, comp=%s, host=%s>" %\
                (self.name,
                 self.thread,
                 self.len_lineobjs,
                 len(self.threadinss),
                 self.component,
                 self.host)

    def __str__(self):
        ret = repr(self)
        lim = self._str_lines_lim
        ret += "\n------ end -----"

        line = self.last_lineobj
        while line is not None:
            ret += "\n%s" % line.__repr_thread__()
            lim -= 1
            if lim <= 0:
                break
            line = line.prv_thread_line

        if line is None:
            pass
        elif line is self.start_lineobj:
            ret += "\n%s" % line.__repr_thread__()
        else:
            ret += "\n. ......"
            ret += "\n%s" % self.start_lineobj.__repr_thread__()
        ret += "\n------ start ---"

        return ret

    def _append_line(self, **kwds):
        line_obj = Line(thread_obj=self,
                        **kwds)
        if self.start_lineobj is None:
            self.start_lineobj = line_obj
        else:
            assert line_obj.lino > self.last_lineobj.lino
            self.last_lineobj.nxt_thread_line = line_obj
            line_obj.prv_thread_line = self.last_lineobj
        self.last_lineobj = line_obj
        self.len_lineobjs += 1
        return line_obj

    def iter_lineobjs(self):
        line = self.start_lineobj
        while line is not None:
            yield line
            line = line.nxt_thread_line


class Target(object):
    _str_lines_lim = 10

    def __init__(self, target, component, host, entity):
        assert isinstance(target, str)
        assert isinstance(component, Component)
        assert isinstance(host, str)

        self.target = target
        self.component = component
        self.host = host

        self._offset = 0
        self.thread_objs = {}

        self.start_lineobj = None
        self.last_lineobj = None
        self.len_lineobjs = 0

        self._built_from = entity
        self._index_thread = 0

    @property
    def offset(self):
        return self._offset

    @offset.setter
    def offset(self, val):
        assert isinstance(val, numbers.Real)
        self._offset = val

    def __repr__(self):
        return "<%s#%s: comp=%s, host=%s, off=%d, %d lines, %d threads, built-from %s>" % (
               self.__class__.__name__,
               self.target,
               self.component,
               self.host,
               self._offset,
               self.len_lineobjs,
               len(self.thread_objs),
               self._built_from.name)

    def __str__(self):
        ret = repr(self)
        lim = self._str_lines_lim
        ret += "\n---- end -----"

        line = self.last_lineobj
        while line is not None:
            ret += "\n%s" % line.__repr_target__()
            lim -= 1
            if lim <= 0:
                break
            line = line.prv_target_line

        if line is None:
            pass
        elif line is self.start_lineobj:
            ret += "\n%s" % line.__repr_target__()
        else:
            ret += "\n ......"
            ret += "\n%s" % self.start_lineobj.__repr_target__()
        ret += "\n---- start ---"

        return ret

    def append_line(self, thread, **kwds):
        assert isinstance(thread, str)

        thread_obj = self.thread_objs.get(thread)
        if thread_obj is None:
            self._index_thread += 1
            thread_obj = Thread(self._index_thread, self, thread)
            self.thread_objs[thread] = thread_obj

        line_obj = thread_obj._append_line(**kwds)
        if self.start_lineobj is None:
            self.start_lineobj = line_obj
        else:
            self.last_lineobj.nxt_target_line = line_obj
            line_obj.prv_target_line = self.last_lineobj
        self.last_lineobj = line_obj
        self.len_lineobjs += 1
        return line_obj
