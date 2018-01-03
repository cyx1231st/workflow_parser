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
from .exc import LogError


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

    def __init__(self, source_obj,
                       lino,
                       line,
                       vs,
                       thread_obj,
                       time,
                       seconds,
                       keyword,
                       request=None):
        assert isinstance(source_obj, Source)
        assert isinstance(lino, int)
        assert isinstance(line, str)
        assert isinstance(vs, dict)
        assert not rv.ALL_VARS & vs.viewkeys()
        assert isinstance(thread_obj, Thread)
        assert isinstance(time, str)
        assert isinstance(seconds, numbers.Real)
        assert isinstance(keyword, str)
        if request is not None:
            assert isinstance(request, str)

        self.source_obj = source_obj
        self.lino = lino
        self.line = line.strip()
        self._schema_vars = vs
        self.thread_obj = thread_obj
        self._line_state = None

        self.time = time
        self._seconds = seconds
        self.keyword = keyword
        self.request = request

        self.prv_source_line = None
        self.nxt_source_line = None
        self.prv_thread_line = None
        self.nxt_thread_line = None

    @property
    def name(self):
        return "%s~%s" % (self.source_obj.name, self.lino)

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
            assert self.last_lineobj is None
            assert self.len_lineobjs == 0
        else:
            assert self.last_lineobj.lino < line_obj.lino
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
    def __init__(self):
        # NOTE: target may be not logical target name, but target_names is.
        self._target_alias = None
        self.target_names = set()
        self.component = None
        self.host = None

        self._offset = 0
        self.thread_objs = {}
        self.len_lineobjs = 0

        self._index_thread = 0

    @property
    def offset(self):
        return self._offset

    @offset.setter
    def offset(self, val):
        assert isinstance(val, numbers.Real)
        self._offset = val

    @property
    def target(self):
        return self._target_alias

    def __repr__(self):
        str_target = self.target
        return "<%s#%s: comp=%s, host=%s, off=%d, %d threads>" % (
               self.__class__.__name__,
               self.target,
               self.component,
               self.host,
               self._offset,
               len(self.thread_objs))

    def _append_line(self, thread, target_alias, target,
                     component=None, host=None,
                    **kwds):
        if not isinstance(thread, str):
            raise LogError("thread %s is not string!" % thread)

        if not isinstance(target_alias, str):
            raise LogError("target_alias %s is not string!" % target_alias)
        if self._target_alias is None:
            self._target_alias = target_alias
        elif self._target_alias != target_alias:
            raise LogError("cannot overwrite target_alias: %s -> %s!" %(
                self._target_alias, target_alias))

        if target is None:
            pass
        elif isinstance(target, str):
            self.target_names.add(target)
        else:
            raise LogError("target %s is not string!" % target)

        if host is not None:
            if not isinstance(host, str):
                raise LogError("host %s is not string!" % host)
            elif self.host is None:
                self.host = host
            elif self.host != host:
                raise LogError("cannot overwrite host: %s -> %s!" %(
                    self.host, host))
        if component is not None:
            if not isinstance(component, Component):
                raise LogError("componet %s is not Component!" % component)
            elif self.component is None:
                self.component = component
            elif self.component is not component:
                raise LogError("cannot overwrite component %s -> %s!" %(
                    self.component, component))

        thread_obj = self.thread_objs.get(thread)
        if thread_obj is None:
            self._index_thread += 1
            thread_obj = Thread(self._index_thread, self, thread)
            self.thread_objs[thread] = thread_obj

        line_obj = thread_obj._append_line(**kwds)
        self.len_lineobjs += 1
        return line_obj


# granularity: target, host
class Source(object):
    _str_lines_lim = 10

    def __init__(self, name, f_dir, vs):
        assert isinstance(name, str)
        assert isinstance(f_dir, str)
        assert isinstance(vs, dict)

        err_keys = {rv.REQUEST,
                    rv.SECONDS,
                    rv.KEYWORD,
                    rv.TIME} & vs.viewkeys()
        if err_keys:
            raise LogError("Cannot set keys %s in Source!" % err_keys)

        self.name = name
        self.where = f_dir

        self.start_lineobj = None
        self.last_lineobj = None
        self.len_lineobjs = 0

        self.targets_byalias = {}
        self.if_alias_required = None

        self.vars_ = vs

    def __repr__(self):
        marks = ""
        if self.vars_:
            marks += ", var("
            marks += ",".join("%s:%s" for k,v in self.vars_.iteritems())
            marks += ")"

        return "<%s#%s: %d lines, from %s, %s>" % (
               self.__class__.__name__,
               self.name,
               self.len_lineobjs,
               self.where,
               marks)

    def __str__(self):
        ret = repr(self)
        lim = self._str_lines_lim
        ret += "\n---- end ----"

        line = self.last_lineobj
        while line is not None:
            ret += "\n%r" % line
            lim -= 1
            if lim <= 0:
                break
            line = line.prv_source_line

        if line is None:
            pass
        elif line is self.start_lineobj:
            ret += "\n%r" % self.start_lineobj
        else:
            ret += "\n ......"
            ret += "\n%r" % self.start_lineobj
        ret += "\n---- start ---"

        return ret

    def append_line(self, lino, line, vs, targets_byname):
        assert isinstance(lino, int)
        assert isinstance(line, str)
        assert isinstance(vs, dict)
        assert isinstance(targets_byname, dict)

        #1. no conflcit of line vars and source vars
        # e.g. host/target defined in filename
        dup_keys = vs.viewkeys() & self.vars_.viewkeys()
        for k in dup_keys:
            if vs[k] != self.vars_[k]:
                raise LogError(
                        "Error in %s@%d %s: line var %s conflict with"
                        "source var, %s vs %s!" % (
                            self.name, lino, line,
                            k, vs[k], self.vars_[k]))
        vs.update(self.vars_)

        #2. split reserved vars
        resv = {}
        vars_ = {}
        for k,v in vs.iteritems():
            if k in rv.ALL_VARS:
                resv[k] = v
            elif k == rv.TARGET_ALIAS:
                resv[k] = v
            else:
                vars_[k] = v

        #3. check required line vars
        required = {rv.THREAD, rv.KEYWORD, rv.TIME, rv.SECONDS} - resv.viewkeys()
        if required:
            raise LogError(
                    "Error in %s@%d %s: cannot identify vars %s!" % (
                        self.name, lino, line, required))

        #4 process target_alias required
        target_alias = None
        target = None
        if rv.TARGET_ALIAS in resv:
            target_alias = resv.pop(rv.TARGET_ALIAS)
        if rv.TARGET in resv:
            target = resv.pop(rv.TARGET)

        if self.if_alias_required is None:
            if target_alias is None:
                self.if_alias_required = False
            else:
                self.if_alias_required = True
        if self.if_alias_required:
            if not isinstance(target_alias, str):
                raise LogError(
                    "Error in %s@%d %s: %s required, but got %s!" % (
                        self.name, lino, line,
                        rv.TARGET_ALIAS, target_alias))
        else:
            if target_alias:
                raise LogError(
                    "Error in %s@%d %s: %s not needed, but got %s!" % (
                        self.name, lino, line,
                        rv.TARGET_ALIAS, target_alias))
            if not isinstance(target, str):
                raise LogError(
                    "Error in %s@%d %s: %s required, but got %s!" % (
                        self.name, lino, line,
                        rv.TARGET, target))
            target_alias = target

        #5 get/create target_obj
        target_obj = self.targets_byalias.get(target_alias)
        if target_obj is None:
            target_obj = Target()
            self.targets_byalias[target_alias] = target_obj
        if target:
            target_obj_ = targets_byname.get(target)
            if target_obj_ is None:
                targets_byname[target] = target_obj
            elif target_obj is not target_obj_:
                raise LogError(
                        "Error in %s@%d %s: target %s conflict, "
                        "found both in %s and %s" % (
                            self.name, lino, line,
                            target, target_alias, target_obj_._target_alias))

        #6. create line_obj
        try:
            line_obj = target_obj._append_line(
                    source_obj=self,
                    lino=lino,
                    line=line,
                    vs=vars_,
                    target=target,
                    target_alias=target_alias,
                    **resv)
        except LogError as e:
            raise LogError("Error in %s@%d %s!" % (self.name, lino, line), e)

        #7. link line_obj
        if self.start_lineobj is None:
            self.start_lineobj = line_obj
            assert self.last_lineobj is None
            assert self.len_lineobjs == 0
        else:
            assert self.last_lineobj.lino < line_obj.lino
            self.last_lineobj.nxt_source_line = line_obj
            line_obj.prv_source_line = self.last_lineobj
        self.last_lineobj = line_obj
        self.len_lineobjs += 1

        return line_obj
