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
from itertools import chain

from ...graph.joinables import JoinBase
from ...graph.joinables import CrossJoin
from ...graph.joinables import InnerJoin
from ...graph.joinables import RequestJoin
from .bases import ActivityBase
from .bases import Pace


class JoinActivityBase(ActivityBase):
    __metaclass__ = ABCMeta

    def __init__(self, join_obj, from_pace, to_pace):
        assert isinstance(join_obj, JoinBase)
        assert isinstance(from_pace, Pace)
        assert isinstance(to_pace, Pace)
        super(JoinActivityBase, self).__init__(
                from_pace, to_pace, join_obj.name)

        self.join_obj = join_obj

        if not self.is_remote:
            assert self.from_targetobj is self.to_targetobj
        assert self.from_threadobj is not self.to_threadobj

    @property
    def jo_name(self):
        return self.join_obj.name

    @property
    def is_remote(self):
        return self.join_obj.is_remote

    @property
    def remote_type(self):
        if not self.is_remote:
            return "local"
        elif self.from_host != self.to_host:
            return "remote"
        else:
            return "local_remote"

    ### from_pace
    @property
    def from_threadins(self):
        return self.from_pace.threadins

    @property
    def from_thread(self):
        return self.from_pace.thread

    @property
    def from_host(self):
        return self.from_pace.host

    @property
    def from_component(self):
        return self.from_pace.component

    @property
    def from_target(self):
        return self.from_pace.target

    @property
    def from_threadobj(self):
        return self.from_pace.thread_obj

    @property
    def from_targetobj(self):
        return self.from_pace.target_obj

    ### to_pace
    @property
    def to_threadins(self):
        return self.to_pace.threadins

    @property
    def to_thread(self):
        return self.to_pace.thread

    @property
    def to_host(self):
        return self.to_pace.host

    @property
    def to_component(self):
        return self.to_pace.component

    @property
    def to_target(self):
        return self.to_pace.target

    @property
    def to_threadobj(self):
        return self.to_pace.thread_obj

    @property
    def to_targetobj(self):
        return self.to_pace.target_obj

    def __repr_intlabels__(self):
        marks = super(JoinActivityBase, self).__repr_intlabels__()
        if not self.is_remote:
            marks += "L(%s)" % self.from_host
        elif self.from_host == self.to_host:
            marks += "R(%s)" % self.from_host
        else:
            marks += "R(%s->%s)" % (self.from_host, self.to_host)
        return marks


class EmptyjoinActivity(ActivityBase):
    def __init__(self, pace, is_from, jos, j_cls):
        assert isinstance(is_from, bool)
        assert issubclass(j_cls, JoinActivityBase)
        if is_from:
            from_pace = pace
            to_pace = None
        else:
            from_pace = None
            to_pace = pace
        name = "/".join(jo.name for jo in jos)

        super(EmptyjoinActivity, self).__init__(from_pace, to_pace, name)

        self._pace = pace
        self.is_joins = is_from
        self.join_objs = jos
        self._join_cls = j_cls

        if is_from:
            self.from_pace.append_nxt(self, template=j_cls)
        else:
            self.to_pace.append_prv(self, template=j_cls)

    @property
    def threadins(self):
        return self._pace.threadins

    @property
    def requestins(self):
        return self._pace.requestins


class InnerjoinActivity(JoinActivityBase):
    _act_type = "INNER"
    _act_lim_back = True
    _act_lim_forth = False

    def __init__(self, join_obj, from_pace, to_pace):
        assert isinstance(join_obj, InnerJoin)

        super(InnerjoinActivity, self).__init__(join_obj, from_pace, to_pace)

        self.from_pace.append_nxt(self)
        self.to_pace.append_prv(self)

    @property
    def requestins(self):
        assert self.from_pace.requestins is self.to_pace.requestins
        return self.from_pace.requestins


class RequestjoinActivity(InnerjoinActivity):
    def __init__(self, join_obj, from_pace, to_pace):
        assert isinstance(join_obj, RequestJoin)
        self.is_nest = False
        super(RequestjoinActivity, self).__init__(join_obj, from_pace, to_pace)

        self.from_pace_unnested = from_pace
        self.to_pace_unnested = to_pace
        self._left_crossjoin = None
        self._right_crossjoin = None

    @property
    def left_crossjoin(self):
        return self._left_crossjoin

    @left_crossjoin.setter
    def left_crossjoin(self, val):
        assert isinstance(val, CrossjoinActivity)
        assert not self._left_crossjoin
        self._left_crossjoin = val
        if self._right_crossjoin:
            self._build_nest()

    @property
    def right_crossjoin(self):
        return self._right_crossjoin

    @right_crossjoin.setter
    def right_crossjoin(self, val):
        assert isinstance(val, CrossjoinActivity)
        assert not self._right_crossjoin
        self._right_crossjoin = val
        if self._left_crossjoin:
            self._build_nest()

    @property
    def is_remote(self):
        if self.is_nest:
            return False
        else:
            return super(RequestjoinActivity, self).is_remote

    @property
    def requestins(self):
        if self.is_nest:
            return self.left_crossjoin.from_pace.requestins
        else:
            return super(RequestjoinActivity, self).requestins

    @property
    def callee_requestins(self):
        if self.left_crossjoin:
            return self.left_crossjoin.callee_requestins
        elif self.right_crossjoin:
            return self.right_crossjoin.callee_requestins
        else:
            return None

    @property
    def cnt_nested(self):
        if self.callee_requestins:
            return self.callee_requestins.cnt_nested
        else:
            return 0

    def __repr_intlabels__(self):
        labels = super(RequestjoinActivity, self).__repr_intlabels__()
        if self.is_nest:
            labels += "NN(%d)" % self.cnt_nested
        else:
            labels += "N(%d)" % self.cnt_nested
        return labels

    def _build_nest(self):
        assert not self.is_nest
        from_pace_real = self.from_pace
        to_pace_real = self.to_pace
        from_pace_nest = self.left_crossjoin.to_pace
        to_pace_nest = self.right_crossjoin.from_pace
        from_pace = Pace(from_pace_nest.line_obj,
                         from_pace_nest.step,
                         from_pace_nest.threadins)
        to_pace = Pace(to_pace_nest.line_obj,
                       to_pace_nest.step,
                       to_pace_nest.threadins)
        assert from_pace.target_obj is to_pace.target_obj

        from_pace_real.replace_nxt(self, self.left_crossjoin)
        assert self.left_crossjoin.from_pace is from_pace_real
        self.left_crossjoin.to_pace = from_pace
        from_pace.append_prv(self.left_crossjoin,
                             template=self.__class__)
        from_pace.append_nxt(self)
        self.from_pace = from_pace
        self.to_pace = to_pace
        to_pace.append_prv(self)
        to_pace.append_nxt(self.right_crossjoin,
                           template=self.__class__)
        self.right_crossjoin.from_pace = to_pace
        assert self.right_crossjoin.to_pace is to_pace_real
        to_pace_real.replace_prv(self, self.right_crossjoin)

        self.is_nest = True


class CrossjoinActivity(JoinActivityBase):
    _act_type = "CROSS"
    _act_lim_back = False
    _act_lim_forth = False

    def __init__(self, join_obj, from_, to_):
        assert isinstance(join_obj, CrossJoin)
        if join_obj.is_left:
            caller = from_
            callee = to_
        else:
            caller = to_
            callee = from_
        assert isinstance(caller, RequestjoinActivity)
        assert isinstance(callee, Pace)

        if join_obj.is_left:
            super(CrossjoinActivity, self).__init__(
                    join_obj, caller.from_pace, callee)
            assert not caller.left_crossjoin
            caller.left_crossjoin = self
            self.to_pace.append_prv(self)
        else:
            super(CrossjoinActivity, self).__init__(
                    join_obj, callee, caller.to_pace)
            assert not caller.right_crossjoin
            caller.right_crossjoin = self
            self.from_pace.append_nxt(self)

        self.caller = caller
        self.callee = callee

    @property
    def requestins(self):
        return self.caller.requestins

    @property
    def callee_requestins(self):
        return self.callee.requestins

    @property
    def is_left(self):
        return self.join_obj.is_left

    @property
    def peer(self):
        if self.is_left:
            return self.caller.right_crossjoin
        else:
            return self.caller.left_crossjoin

    def __repr_intlabels__(self):
        labels = super(CrossjoinActivity, self).__repr_intlabels__()
        labels += "C"
        return labels
