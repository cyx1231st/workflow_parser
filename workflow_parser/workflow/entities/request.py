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

from collections import defaultdict
from itertools import chain

from .bases import ActivityBase
from .bases import IntervalBase
from .bases import Pace
from .bases import RequestinsBase
from .join import JoinActivityBase
from .join import InnerjoinActivity
from .join import RequestjoinActivity
from .join import CrossjoinActivity
from .threadins import ThreadActivity


class ExtendedInterval(IntervalBase):
    def __init__(self, from_pace, to_pace):
        assert isinstance(from_pace, Pace)
        assert isinstance(to_pace, Pace)
        assert from_pace.threadins is to_pace.threadins
        super(ExtendedInterval, self).__init__()
        self.from_pace = from_pace
        self.to_pace = to_pace

    @property
    def int_name(self):
        return str(self.component)

    @property
    def component(self):
        return self.from_pace.component

    @property
    def threadins(self):
        return self.from_pace.threadins

    @property
    def requestins(self):
        return self.from_pace.requestins

    def __repr_intlabels__(self):
        labels = super(ExtendedInterval, self).__repr_intlabels__()
        labels += "!EX"
        return labels


class RequestInstance(RequestinsBase):
    _index_dict = defaultdict(lambda: 0)

    def __init__(self, request, builder):
        super(RequestInstance, self).__init__(request)
        self.request_vars = defaultdict(set)
        self.threadinss = []
        self.thread_objs = set()
        self.target_objs = set()

        self.len_paces = 0
        self.len_main_paces = 0

        self._start_activity = None
        self._end_activity = None
        self.last_activity = None
        self.activities_bymark = defaultdict(list)

        self.innerjoin_activities = set()
        self.requestjoin_activities = set()
        self.crossjoinl_activities = set()
        self.crossjoinr_activities = set()
        self.emptyjoins_activities = set()
        self.emptyjoined_activities = set()

        self._builder = builder

    @property
    def start_activity(self):
        return self._start_activity

    @start_activity.setter
    def start_activity(self, act):
        assert isinstance(act, ThreadActivity)
        assert act.to_pace
        assert act.is_request_start
        self.from_pace = act.to_pace
        self._start_activity = act

    @property
    def end_activity(self):
        return self._end_activity

    @end_activity.setter
    def end_activity(self, act):
        assert isinstance(act, ThreadActivity)
        assert act.from_pace
        assert act.is_request_end
        self.to_pace = act.from_pace
        self._end_activity = act

    @property
    def request_type(self):
        ret = self.start_activity.request_type
        assert ret
        return ret

    @property
    def request_state(self):
        ret = self.end_activity.request_state
        assert ret
        return ret

    @property
    def components(self):
        return self.request_vars["component"]

    @property
    def hosts(self):
        return self.request_vars["host"]

    @property
    def last_seconds(self):
        return self.last_activity.from_seconds

    @property
    def last_time(self):
        return self.last_activity.from_time

    @property
    def cnt_nested(self):
        assert len(self.crossjoinl_activities) == len(self.crossjoinr_activities)
        return len(self.crossjoinl_activities)

    def __repr_marks__(self):
        marks = ""
        if self.requestjoin_activities:
            marks += ", %d reqs" % len(self.requestjoin_activities)
        if self.crossjoinr_activities or self.crossjoinl_activities:
            marks += ", (%d %d) cross" % (len(self.crossjoinl_activities),
                                          len(self.crossjoinr_activities))
        if self.emptyjoins_activities:
            marks += ", %d emptyjoins" % len(self.emptyjoins_activities)
        if self.emptyjoined_activities:
            marks += ", %d emptyjoined" % len(self.emptyjoined_activities)
        if self.activities_bymark:
            marks += ", %d marks" % len(self.activities_bymark)
        if not self._builder.is_built:
            marks =+ ", ~BUILD"
        return marks

    def __repr__(self):
        return "<RIns#%s: %s@%s, %d(main %d) paces, %d hosts, %d(%d) threads, "\
               "%d vars, %d inner, [%.3f-%.3f(%.3f)]%s>" % (
                self.request,
                self.request_type,
                self.request_state,
                self.len_paces,
                self.len_main_paces,
                len(self.hosts),
                len(self.thread_objs),
                len(self.threadinss),
                len(self.request_vars),
                len(self.innerjoin_activities),
                self.from_seconds,
                self.to_seconds,
                self.last_seconds,
                self.__repr_marks__())

    def __str__(self):
        ret = repr(self)
        ret += "\n VARS:"
        for k, v in self.request_vars.iteritems():
            ret += "\n  %s: %s" % (k, ",".join(str(v_) for v_ in v))
        ret += "\n THREADINSS:"
        for tis in self.threadinss:
            ret += "\n  %r" % tis
        if self.innerjoin_activities:
            ret += "\n INNER:"
            for j in self.innerjoin_activities:
                ret += "\n  %r" % j
        if self.requestjoin_activities:
            ret += "\n REQUEST:"
            for j in self.requestjoin_activities:
                ret += "\n  %r" % j
        if self.crossjoinl_activities:
            ret += "\n CROSSL:"
            for j in self.crossjoinl_activities:
                ret += "\n  %r" % j
        if self.crossjoinr_activities:
            ret += "\n CROSSR:"
            for j in self.crossjoinr_activities:
                ret += "\n  %r" % j
        if self.emptyjoins_activities:
            ret += "\n EMPTYJOINS:"
            for j in self.emptyjoins_activities:
                ret += "\n  %r" % j
        if self.emptyjoined_activities:
            ret += "\n EMPTYJOINED:"
            for j in self.emptyjoined_activities:
                ret += "\n  %r" % j
        ret += "\n\n### MAIN ###:"
        ret += "\n%r" %  self.start_activity.threadins
        activity = self.start_activity
        while activity:
            assert isinstance(activity, ActivityBase)
            assert activity.is_main
            if isinstance(activity, RequestjoinActivity):
                if not activity.is_nest:
                    ret += "\n%r" % activity.to_threadins
                mark = "~"
            elif isinstance(activity, InnerjoinActivity):
                ret += "\n%r" % activity.to_threadins
                mark = "-"
            elif isinstance(activity, CrossjoinActivity):
                if not activity.is_left:
                    ret += "\n%r" % activity.to_threadins
                mark = "~"
            else:
                mark = "|"
            ret += "\n%s %s" % (mark, activity.__repr_to__())
            if activity.to_pace:
                activity = activity.to_pace.nxt_main_activity
            else:
                break
        ret += "\n"
        return ret

    def automate_name(self):
        if self._request is None:
            assert self.request_type is not None
            self._index_dict[self.request_type] += 1
            self.request = "%s%d" % (self.request_type, self._index_dict[self.request_type])

    def iter_joins(self):
        for join in chain(self.innerjoin_activities,
                          self.requestjoin_activities):
            assert join.is_interval
            yield join

    def iter_mainints(self, extended=False):
        activity = self.start_activity.to_pace.nxt_main_activity

        if not extended:
            while activity.is_interval:
                yield activity
                activity = activity.to_pace.nxt_main_activity
        else:
            from_pace = activity.from_pace
            while activity.is_interval:
                if isinstance(activity, JoinActivityBase):
                    if from_pace is not activity.from_pace:
                        yield ExtendedInterval(from_pace, activity.from_pace)
                    yield activity
                    from_pace = activity.to_pace
                else:
                    assert isinstance(activity, ThreadActivity)
                activity = activity.to_pace.nxt_main_activity
            if from_pace is not activity.from_pace:
                yield ExtendedInterval(from_pace, activity.from_pace)
        assert activity is self.end_activity

    def iter_threadints(self):
        for tis in self.threadinss:
            for int_ in tis.iter_ints():
                yield int_
