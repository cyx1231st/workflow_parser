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
from collections import defaultdict
from functools import total_ordering

from ... import reserved_vars as rv
from ...datasource import Line
from ...datasource import LineStateBase
from ...datasource import Thread
from ...graph.token import Step
from ..exc import StateError


@total_ordering
class IntervalBase(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self._from_pace = None
        self._to_pace = None

    ### from pace
    @property
    def from_pace(self):
        return self._from_pace

    @from_pace.setter
    def from_pace(self, val):
        assert isinstance(val, Pace)
        self._from_pace = val

    @property
    def from_time(self):
        if self._from_pace:
            return self._from_pace.time
        raise RuntimeError("Interval %s has no from_pace" % self.int_name)

    @property
    def from_seconds(self):
        if self._from_pace:
            return self._from_pace.seconds
        raise RuntimeError("Interval %s has no from_pace" % self.int_name)

    @property
    def from_edgename(self):
        if self._from_pace:
            return self._from_pace.edgename
        raise RuntimeError("Interval %s has no from_pace" % self.int_name)

    @property
    def from_keyword(self):
        if self._from_pace:
            return self._from_pace.keyword
        raise RuntimeError("Interval %s has no from_pace" % self.int_name)

    ### to pace
    @property
    def to_pace(self):
        return self._to_pace

    @to_pace.setter
    def to_pace(self, val):
        if val is None:
            assert self._to_pace is None
        else:
            assert isinstance(val, Pace)
            self._to_pace = val

    @property
    def to_time(self):
        if self._to_pace:
            return self._to_pace.time
        raise RuntimeError("Interval %s has no to_pace" % self.int_name)

    @property
    def to_seconds(self):
        if self._to_pace:
            return self._to_pace.seconds
        raise RuntimeError("Interval %s has no to_pace" % self.int_name)

    @property
    def to_edgename(self):
        if self._to_pace:
            return self._to_pace.edgename
        raise RuntimeError("Interval %s has no to_pace" % self.int_name)

    @property
    def to_keyword(self):
        if self._to_pace:
            return self._to_pace.keyword
        raise RuntimeError("Interval %s has no to_pace" % self.int_name)

    @property
    def is_violated(self):
        return self.from_seconds > self.to_seconds

    ### abstracts
    @abstractproperty
    def int_name(self):
        return None

    @abstractproperty
    def requestins(self):
        return None

    ### properties
    @property
    def is_interval(self):
        return self.from_pace and self.to_pace

    @property
    def path(self):
        ret = ""
        if self._from_pace:
            ret += "%s[" % self.from_edgename
        else:
            ret += "|["
        ret += str(self.int_name)
        if self._to_pace:
            ret += "]%s" % self.to_edgename
        else:
            ret += "]|"
        return ret

    @property
    def lapse(self):
        if self.is_interval:
            return self.to_seconds - self.from_seconds
        raise RuntimeError("Interval %s has no from/to pace" % self.int_name)

    @property
    def request(self):
        return self.requestins.request

    @property
    def request_type(self):
        return self.requestins.request_type

    ### prints
    def __repr_intlabels__(self):
        labels = ""
        if self._from_pace and self._to_pace:
            if self.from_seconds > self.to_seconds:
                labels += "#"
        elif not self.from_pace and not self.to_pace:
            labels += "X"
        return labels

    def __repr__(self):
        ret = "<%s#%s: %s " % (
                self.__class__.__name__,
                self.int_name,
                self.__repr_intlabels__())
        if self._from_pace:
            ret += "%.3f,%s`%s`->" % (self.from_seconds,
                                      self.from_edgename,
                                      self.from_keyword)
        ret += "."
        if self._to_pace:
            ret += "->%.3f,%s`%s`" % (self.to_seconds,
                                       self.to_edgename,
                                       self.to_keyword)
        ret += " >"
        return ret

    def __repr_from__(self):
        ret = "[%s %s" % (self.int_name, self.__repr_intlabels__())
        if self._from_pace:
            context = ",".join(str(k)+"="+str(v)
                    for k,v in self._from_pace.line_context.iteritems())
            if context:
                context = " " + context
            ret += "<-(%.3f,%s`%s`%s)]" % (
                    self.from_seconds,
                    self.from_edgename,
                    self.from_keyword,
                    context)
        else:
            ret += "]"
        return ret

    def __repr_to__(self):
        ret = "[%s %s" % (self.int_name, self.__repr_intlabels__())
        if self._to_pace:
            context = ",".join(str(k)+"="+str(v)
                    for k,v in self._to_pace.line_context.iteritems())
            if context:
                context = " " + context
            ret += "->(%.3f,%s`%s`%s)]" % (
                    self.to_seconds,
                    self.to_edgename,
                    self.to_keyword,
                    context)
        else:
            ret += "]"
        return ret

    __eq__ = lambda self, other:\
            self.from_pace == other.to_pace and\
            self.to_pace == other.to_pace
    __lt__ = lambda self, other:\
            (self.from_seconds, self.to_seconds) <\
            (other.from_seconds, other.to_seconds)


class RequestinsBase(IntervalBase):
    __metaclass__ = ABCMeta

    def __init__(self, request):
        self._request = None

        setattr(self, "request", request)

    @property
    def request(self):
        return self._request

    @request.setter
    def request(self, val):
        if val is not None:
            assert isinstance(val, str)
            assert self._request is None
            self._request = val

    @property
    def requestins(self):
        return self

    @property
    def int_name(self):
        return self.request


class ThreadinsBase(IntervalBase):
    __metaclass__ = ABCMeta

    def __init__(self, thread_obj):
        super(ThreadinsBase, self).__init__()

        self.thread_obj = thread_obj
        self.thread_vars = {}
        self.thread_vars_dup = defaultdict(set)
        self._request = None
        self._requestins = None

    @property
    def request(self):
        return self._request

    @request.setter
    def request(self, val):
        assert isinstance(val, str)
        if self._request:
            assert self._request == val
        else:
            self._request = val

    @property
    def requestins(self):
        return self._requestins

    @requestins.setter
    def requestins(self, val):
        assert isinstance(val, RequestinsBase)
        assert self._requestins is None
        self._requestins = val

    ### thread_obj
    @property
    def thread(self):
        return self.thread_obj.thread

    @property
    def target(self):
        return self.thread_obj.target

    @property
    def component(self):
        return self.thread_obj.component

    @property
    def host(self):
        return self.thread_obj.host

    @property
    def target_obj(self):
        return self.thread_obj.target_obj

    @property
    def thread_name(self):
        return self.thread_obj.name

    @property
    def int_name(self):
        return self.thread_name

    def _process_vars(self, line_obj):
        assert isinstance(line_obj, Line)
        for key in line_obj.keys:
            if key in ("keyword", "time", "seconds"):
                continue

            new_val = line_obj[key]
            if key in ("component", "target", "host", "thread"):
                val = getattr(self, key)
                if val != line_obj[key]:
                    raise StateError("(ThreadInstance) parse error: "
                            "variable %s mismatch: %s is not %s!"
                            % (key, val, new_val))
                else:
                    pass
            elif key == "request":
                if new_val is None:
                    pass
                elif self.request is None:
                    self.request = new_val
                elif self.request != new_val:
                    raise StateError("(ThreadInstance) parse error: "
                            "request mismatch: %s is not %s!"
                            % (val, new_val))
                else:
                    pass
            else:
                if key in self.thread_vars_dup:
                    self.thread_vars_dup[key].add(new_val)
                else:
                    val = self.thread_vars.get(key)
                    if val is None:
                        self.thread_vars[key] = new_val
                    elif val != new_val:
                        self.thread_vars_dup[key].add(val)
                        self.thread_vars_dup[key].add(new_val)
                        self.thread_vars.pop(key)
                    else:
                        pass


class ActivityBase(IntervalBase):
    __metaclass__ = ABCMeta

    _act_type = object()
    _act_lim_back = None
    _act_lim_forth = None

    def __init__(self, from_pace, to_pace, aname):
        assert isinstance(aname, str)
        super(ActivityBase, self).__init__()

        if from_pace:
            self.from_pace = from_pace
        if to_pace:
            self.to_pace = to_pace
        self.is_main = False
        self.activity_name = aname

    @property
    def int_name(self):
        return self.activity_name

    def __repr_intlabels__(self):
        marks = super(ActivityBase, self).__repr_intlabels__()
        if self.is_main:
            marks += "!"
        return marks


@total_ordering
class Pace(LineStateBase, object):
    """ Pace is relative to transition. """
    def __init__(self, line_obj, step, threadins):
        assert isinstance(line_obj, Line)
        assert isinstance(step, Step)
        assert isinstance(threadins, ThreadinsBase)
        assert line_obj.thread_obj is threadins.thread_obj

        self.line_obj = line_obj
        self.step = step
        self.threadins = threadins

        self.prv_activity_bytype = defaultdict(list)
        self.nxt_activity_bytype = defaultdict(list)

        self.is_main = False
        self.prv_main_activity = None
        self.nxt_main_activity = None

    ### step
    @property
    def path_step(self):
        return self.step.path

    @property
    def edgename(self):
        return self.step.edgename

    @property
    def joinable(self):
        return self.step.joinable

    ### threadins
    @property
    def requestins(self):
        return self.threadins.requestins

    @property
    def thread_obj(self):
        return self.threadins.thread_obj

    @property
    def target_obj(self):
        return self.threadins.target_obj

    ### LineState
    @property
    def line_keys(self):
        return self.line_obj.keys_

    @property
    def line_context(self):
        ret = {}
        for k in self.line_keys:
            ret[k] = self[k]
        return ret

    @property
    def refresh_vars(self):
        return self.step.refresh_vars

    # TODO: bug here
    @property
    def _ls_state(self):
        # if self.is_thread_start and self.is_thread_end:
        #     return "*"
        # elif self.is_thread_start:
        #     return "+"
        # elif self.is_thread_end:
        #     return "-"
        # else:
        if self.prv_main_activity or self.nxt_main_activity:
            return "!"
        else:
            return "|"

    @property
    def _ls_request(self):
        return self.request

    @property
    def _ls_path(self):
        return self.path_step

    # total ordering
    __eq__ = lambda self, other: self.seconds == other.seconds
    __lt__ = lambda self, other: self.seconds < other.seconds

    def __getattribute__(self, item):
        assert isinstance(item, str)

        if item in rv.ALL_VARS:
            ret = getattr(self.line_obj, item)
            if ret is None and item == rv.REQUEST:
                ret = getattr(self.threadins, "request")
            return ret
        else:
            return super(Pace, self).__getattribute__(item)

    def __getitem__(self, item):
        assert isinstance(item, str)

        if item in rv.ALL_VARS:
            return getattr(self, item)
        elif item in self.line_obj:
            return self.line_obj[item]
        elif item in self.threadins.thread_vars:
            return self.threadins.thread_vars[item]
        elif item in self.threadins.thread_vars_dup:
            raise StateError("(Pace) got multiple %s: %s" %
                    (item, self.threadins.thread_vars_dup[item]))
        else:
            raise StateError("(Pace) key %s not exist!" % item)

    def __repr_marks__(self):
        mark_str = ""
        for type_, acts in self.prv_activity_bytype.iteritems():
            mark_str += ", prv_"+type_+"("
            mark_str += ",".join(act.__repr_from__()
                    for act in acts)
            mark_str += ")"

        for type_, acts in self.nxt_activity_bytype.iteritems():
            mark_str += ", nxt_"+type_+"("
            mark_str += ",".join(act.__repr_to__()
                    for act in acts)
            mark_str += ")"

        return mark_str

    def __repr__(self):
        return "<P %.3f %s [%s %s] %s, `%s`, %s%s>" % (
                self.seconds,
                self.path_step,
                self.target,
                self.thread,
                self.request,
                self.keyword,
                self.line_context,
                self.__repr_marks__())

    def __repr_thread__(self):
        return "%.3f %s, `%s`, %s%s" % (
                self.seconds,
                self.path_step,
                self.keyword,
                self.line_context,
                self.__repr_marks__())

    def append_nxt(self, activity, template=None):
        assert isinstance(activity, ActivityBase)
        if not template:
            template = activity.__class__
        assert issubclass(template, ActivityBase)

        act_type = template._act_type
        lim = template._act_lim_forth
        assert not act_type is ActivityBase._act_type
        assert isinstance(act_type, str)
        assert isinstance(lim, bool)
        if lim:
            assert not self.nxt_activity_bytype[act_type]

        self.nxt_activity_bytype[act_type].append(activity)

    def append_prv(self, activity, template=None):
        assert isinstance(activity, ActivityBase)
        if not template:
            template = activity.__class__
        assert issubclass(template, ActivityBase)

        act_type = template._act_type
        lim = template._act_lim_back
        assert not act_type is ActivityBase._act_type
        assert isinstance(act_type, str)
        assert isinstance(lim, bool)
        if lim:
            assert not self.prv_activity_bytype[act_type]

        self.prv_activity_bytype[act_type].append(activity)

    def replace_nxt(self, act, newact):
        assert isinstance(act, ActivityBase)
        assert isinstance(newact, ActivityBase)
        act_type = act._act_type
        index = 0
        for act_ in self.nxt_activity_bytype[act_type]:
            if act_ is act:
                self.nxt_activity_bytype[act_type][index] = newact
                return
            index += 1
        raise RuntimeError("Cannot find nxt_act %r in pace" % act)

    def replace_prv(self, act, newact):
        assert isinstance(act, ActivityBase)
        assert isinstance(newact, ActivityBase)
        act_type = act._act_type
        index = 0
        for act_ in self.prv_activity_bytype[act_type]:
            if act_ is act:
                self.prv_activity_bytype[act_type][index] = newact
                return
            index += 1
        raise RuntimeError("Cannot find prv_act %r in pace" % act)

    def get_prv(self, act_type):
        assert not act_type is ActivityBase._act_type
        return self.prv_activity_bytype[act_type]

    def get_nxt(self, act_type):
        assert not act_type is ActivityBase._act_type
        return self.nxt_activity_bytype[act_type]
