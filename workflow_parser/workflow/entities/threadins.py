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

from ...datasource import Line
from ...datasource import Thread
from ...graph.token import State
from ...graph.token import Token
from .bases import Pace
from .bases import ActivityBase
from .bases import ThreadinsBase


class ThreadActivity(ActivityBase):
    _act_type = "THREAD"
    _act_lim_back = True
    _act_lim_forth = True

    def __init__(self, state, threadins, from_pace):
        assert isinstance(state, State)
        assert isinstance(threadins, ThreadInstance)
        super(ThreadActivity, self).__init__(from_pace,
                                             None,
                                             state.nodename)

        self.threadins = threadins
        self.state = state

        if from_pace:
            assert from_pace.threadins is threadins
            from_pace.append_nxt(self)

    ### threadins
    @property
    def requestins(self):
        return self.threadins.requestins

    @property
    def thread_obj(self):
        return self.threadins.thread_obj

    @property
    def target(self):
        return self.threadins.target

    @property
    def component(self):
        return self.threadins.component

    @property
    def host(self):
        return self.threadins.host

    @property
    def thread(self):
        return self.threadins.thread

    @property
    def target_obj(self):
        return self.threadins.target_obj

    ### state
    @property
    def nodename(self):
        return self.state.nodename

    @property
    def marks(self):
        return self.state.marks

    ### others
    @property
    def is_thread_start(self):
        if self.state.is_thread_start:
            assert self is self.threadins.start_activity
            return True
        else:
            return False

    @property
    def is_thread_end(self):
        if self.state.is_thread_end and\
                self is self.threadins.end_activity:
            return True
        else:
            return False

    @property
    def is_request_start(self):
        return self.state.is_request_start

    @property
    def request_type(self):
        if self.state.request_type:
            return self.state.request_type
        else:
            return super(ThreadActivity, self).request_type

    @property
    def is_request_end(self):
        return self.state.is_request_end

    @property
    def request_state(self):
        return self.state.request_state

    @property
    def prv_thread_activity(self):
        if self.from_pace:
            acts = self.from_pace.get_prv(self._act_type)
            assert len(acts) == 1
            act = acts[0]
            assert isinstance(act, ThreadActivity)
            return act
        else:
            return None

    @property
    def nxt_thread_activity(self):
        if self.to_pace:
            acts = self.to_pace.get_nxt(self._act_type)
            assert len(acts) == 1
            act = acts[0]
            assert isinstance(act, ThreadActivity)
            return act
        else:
            return None

    def __repr_intlabels__(self):
        marks = super(ThreadActivity, self).__repr_intlabels__()
        if self.is_thread_start:
            marks += "S"
        if self.is_request_start:
            marks += "@%s" % self.request_type
        if self.is_thread_end:
            marks += "E"
        if self.request_state:
            marks += "@%s" % self.request_state
        if self.marks:
            marks += "*"+"-".join(self.marks)
        return marks

    def set_nxt_pace(self, nxt_pace):
        assert isinstance(nxt_pace, Pace)
        assert nxt_pace.threadins is self.threadins

        assert self.to_pace is None
        self.to_pace = nxt_pace
        nxt_pace.append_prv(self)


class ThreadInstance(ThreadinsBase):
    def __init__(self, thread_obj, token):
        assert isinstance(token, Token)
        super(ThreadInstance, self).__init__(thread_obj)

        self.token = token
        self.start_activity = ThreadActivity(token.start_state, self, None)
        self._end_activity = None
        self.activities_bymark = defaultdict(list)
        self.len_activities = 1
        self.is_finish = False

        self.end_activity = self.start_activity
        assert self.start_activity.is_thread_start

    @property
    def end_activity(self):
        return self._end_activity

    @end_activity.setter
    def end_activity(self, val):
        assert isinstance(val, ThreadActivity)
        self.to_pace = val.from_pace
        self._end_activity = val

    @property
    def is_request_start(self):
        return self.start_activity.is_request_start

    @property
    def request_type(self):
        if self.requestins:
            return super(ThreadInstance, self).request_type
        else:
            return self.start_activity.request_type

    @property
    def is_request_end(self):
        return self.end_activity.is_request_end

    @property
    def request_state(self):
        return self.end_activity.request_state

    @property
    def threadgraph_name(self):
        return self.token.threadgraph_name

    @property
    def is_complete(self):
        return self.token.is_complete

    @classmethod
    def new(cls, mastergraph, line_obj, thread_obj):
        assert isinstance(line_obj, Line)
        assert isinstance(thread_obj, Thread)
        assert line_obj.thread_obj is thread_obj

        token = Token.new(mastergraph, line_obj.keyword,
                          thread_obj.component)
        if token:
            threadins = ThreadInstance(thread_obj, token)
            pace = threadins._apply_token(line_obj)
            threadins.from_pace = pace
            thread_obj.threadinss.append(threadins)
            return threadins, pace
        else:
            return None, None

    def __repr__(self):
        mark_str = self.__repr_intlabels__()
        if mark_str:
            mark_str = " "+mark_str
        if not self.is_finish:
            mark_str += ", PROCESS..."
        elif not self.is_complete:
            mark_str += ", INCOMPLETE"
        if self.is_request_start:
            mark_str += ", REQS@%s" % self.request_type
        if self.is_request_end:
            mark_str += ", REQE@%s" % self.request_state
        if self.activities_bymark:
            mark_str += ", MARKS(%s)" % \
                    ",".join(self.activities_bymark.iterkeys())
        if self.thread_vars:
            mark_str += ", VARS(%s)" % \
                    ",".join(self.thread_vars.iterkeys())
        if self.thread_vars_dup:
            mark_str += ", DUP(%s)" % \
                    ",".join(self.thread_vars_dup.iterkeys())

        return "<TdIns#%s-%s: %s %s %s, %d acts, graph %s%s>" % (
                self.thread_name,
                self.thread,
                self.host,
                self.component,
                self.request,
                self.len_activities,
                self.threadgraph_name,
                mark_str)

    def __str__(self):
        ret_str = repr(self)
        if self.thread_vars:
            ret_str += "\n VARS: "+\
                    ",".join("%s=%s" % (k,v) for k,v in self.thread_vars.iteritems())
        if self.thread_vars_dup:
            ret_str += "\n DUP: "+\
                    ",".join("%s(%s)" % (k," ".join(v))
                            for k,v in self.thread_vars_dup.iteritems())

        activity = self.start_activity
        while activity:
            ret_str += "\n| %s" % (activity.__repr_to__())
            activity = activity.nxt_thread_activity
        return ret_str

    def _apply_token(self, line_obj):
        assert not self.is_finish
        assert self.token.len_states == self.len_activities+1

        pace = Pace(line_obj, self.token.to_step, self)
        line_obj.set_linestate(pace)
        self.end_activity.set_nxt_pace(pace)
        activity = ThreadActivity(self.token.to_state,
                                  self, pace)
        self.end_activity = activity
        self.len_activities += 1
        self._process_vars(line_obj)
        for mark in activity.marks:
            self.activities_bymark[mark].append(activity)
        return pace

    def do_step(self, line_obj):
        assert isinstance(line_obj, Line)
        assert line_obj.thread_obj is self.thread_obj

        if self.token.do_step(line_obj.keyword):
            return self._apply_token(line_obj)
        else:
            return None

    def set_finish(self):
        self.is_finish = True

    def iter_ints(self):
        activity = self.start_activity.nxt_thread_activity
        while activity.is_interval:
            yield activity
            activity = activity.nxt_thread_activity
        assert activity is self.end_activity
