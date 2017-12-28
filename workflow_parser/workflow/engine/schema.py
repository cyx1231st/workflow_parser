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
from collections import defaultdict
from itertools import chain

from ...graph.joinables import (
        InnerJoin,
        RequestJoin,
        JoinMasterMixin,
        InnerjoinMixin,
        CrossCalleeMixin)
from ...utils import Report
from ..entities.bases import ThreadinsBase
from ..entities.threadins import Pace
from ..entities.join import CrossjoinActivity
from ..entities.join import EmptyjoinActivity
from ..entities.join import InnerjoinActivity
from ..entities.join import RequestjoinActivity
from .schema_engine import JoiningProject


class JoinInfo(object):
    def __init__(self):
        self.innerjoins_bytis = defaultdict(list)
        self.innerjoined_bytis = defaultdict(list)
        self.requestjoins_bytis = defaultdict(list)
        self.requestjoined_bytis = defaultdict(list)
        self.crossjoinleft_bytis = defaultdict(list)
        self.crossjoinright_bytis = defaultdict(list)

        self.emptyjoins_bytis = defaultdict(list)
        self.emptyjoined_bytis = defaultdict(list)

        self.len_inners = 0
        self.len_requests = 0
        self.len_crossl = 0
        self.len_crossr = 0

    def add_join(self, join):
        if join.from_threadins is join.to_threadins:
            # case thread joins itself, it is not a join
            pass
        elif isinstance(join, InnerjoinActivity):
            if isinstance(join, RequestjoinActivity):
                self.requestjoins_bytis[join.from_threadins].append(join)
                self.requestjoined_bytis[join.to_threadins].append(join)
                self.len_requests += 1
            else:
                self.innerjoins_bytis[join.from_threadins].append(join)
                self.innerjoined_bytis[join.to_threadins].append(join)
                self.len_inners += 1
        elif isinstance(join, CrossjoinActivity):
            if join.is_left:
                self.crossjoinleft_bytis[join.callee.threadins].append(join)
                self.len_crossl += 1
            else:
                self.crossjoinright_bytis[join.callee.threadins].append(join)
                self.len_crossr += 1
        else:
            assert isinstance(join, EmptyjoinActivity)
            if join.is_joins:
                self.emptyjoins_bytis[join.threadins].append(join)
            else:
                self.emptyjoined_bytis[join.threadins].append(join)

    def iter_innerjoins(self, tis, is_request=None):
        assert isinstance(tis, ThreadinsBase)
        if is_request is None:
            for join in chain(self.innerjoins_bytis[tis],
                    self.requestjoins_bytis[tis]):
                yield join
        elif is_request:
            for join in self.requestjoins_bytis[tis]:
                yield join
        else:
            for join in self.innerjoins_bytis[tis]:
                yield join

    def iter_innerjoined(self, tis, is_request=None):
        assert isinstance(tis, ThreadinsBase)
        if is_request is None:
            for join in chain(self.innerjoined_bytis[tis],
                              self.requestjoined_bytis[tis]):
                yield join
        elif is_request:
            for join in self.requestjoined_bytis[tis]:
                yield join
        else:
            for join in self.innerjoined_bytis[tis]:
                yield join

    def iter_crossjoin(self, tis, is_left=None):
        assert isinstance(tis, ThreadinsBase)
        if is_left is None:
            for join in chain(self.crossjoinleft_bytis[tis],
                              self.crossjoinright_bytis[tis]):
                yield join
        elif is_left:
            for join in self.crossjoinleft_bytis[tis]:
                yield join
        else:
            for join in self.crossjoinright_bytis[tis]:
                yield join

    def iter_emptyjoin(self, tis, is_joins=None):
        assert isinstance(tis, ThreadinsBase)
        if is_joins is None:
            iteration = chain(self.emptyjoined_bytis[tis],
                              self.emptyjoins_bytis[tis])
        elif is_joins:
            iteration = self.emptyjoins_bytis[tis]
        else:
            iteration = self.emptyjoined_bytis[tis]
        for join in iteration:
            yield join


class SchemaEngine(object):
    def __init__(self, join_master):
        assert isinstance(join_master, JoinMasterMixin)

        self.innerj_proj = JoiningProject(
                "inner_joins", join_master.inner_joinobjs)
        self.crossj_proj = JoiningProject(
                "cross_joins", join_master.cross_joinobjs)

    def load_pace(self, pace):
        assert isinstance(pace, Pace)
        joinable = pace.joinable
        assert isinstance(joinable, InnerjoinMixin)

        if joinable._jm_inner_jsobjs:
            self.innerj_proj.load_fromitem(
                    strategy=joinable._jm_inner_jstype,
                    seconds=pace.seconds,
                    env=pace,
                    item=pace,
                    join_objs=joinable._jm_inner_jsobjs)
        if joinable._jm_inner_jedobjs:
            self.innerj_proj.load_toitem(
                    seconds=pace.seconds,
                    env=pace,
                    item=pace,
                    join_objs=joinable._jm_inner_jedobjs)

        assert isinstance(joinable, CrossCalleeMixin)
        if joinable._jm_callee_jsobjs:
            self.crossj_proj.load_fromitem(
                    strategy="ALL",
                    seconds=pace.seconds,
                    env=pace,
                    item=pace,
                    join_objs=joinable._jm_callee_jsobjs)
        if joinable._jm_callee_jedobjs:
            self.crossj_proj.load_toitem(
                    seconds=pace.seconds,
                    env=pace,
                    item=pace,
                    join_objs=joinable._jm_callee_jedobjs)

    def proceed(self, report):
        assert isinstance(report, Report)
        print("Join paces...")
        print("-------------")

        inner_from = 0
        inner_to = 0
        cross_from = 0
        cross_to = 0

        joininfo = JoinInfo()

        for jo, from_, to_ in self.innerj_proj.yield_results():
            if from_ is None:
                assert to_ is not None
                join = EmptyjoinActivity(to_, False, jo, InnerjoinActivity)
                inner_to += 1
            elif to_ is None:
                join = EmptyjoinActivity(from_, True, jo, InnerjoinActivity)
                inner_from += 1
            elif isinstance(jo, RequestJoin):
                join = RequestjoinActivity(jo, from_, to_)
                self.crossj_proj.load_fromitem(
                        strategy="ALL",
                        seconds=from_.seconds,
                        env=from_,
                        item=join,
                        join_objs=jo.joins_objs)
                self.crossj_proj.load_toitem(
                        seconds=to_.seconds,
                        env=to_,
                        item=join,
                        join_objs=jo.joined_objs)
            elif isinstance(jo, InnerJoin):
                if from_ is to_:
                    # case thread_instance joins itself
                    continue
                join = InnerjoinActivity(jo, from_, to_)
            else:
                raise StateError("SchemaEngine, invalid jo type: %s" %
                        jo.__class__)
            joininfo.add_join(join)

        for jo, from_, to_ in self.crossj_proj.yield_results():
            if from_ is None:
                assert to_ is not None
                join = EmptyjoinActivity(to_, False, jo, CrossjoinActivity)
            elif to_ is None:
                join = EmptyjoinActivity(from_, True, jo, CrossjoinActivity)
            else:
                join = CrossjoinActivity(jo, from_, to_)
            joininfo.add_join(join)

        #### report #####
        report.step("join_ps",
                    innerjoin=joininfo.len_inners,
                    innerjoined=joininfo.len_inners,
                    interfacejoin=joininfo.len_requests,
                    interfacejoined=joininfo.len_requests,
                    leftinterface=joininfo.len_crossl,
                    rightinterface=joininfo.len_crossr)

        if inner_from or inner_to or cross_from or cross_to:
            print("! WARN !")
            print("%d unjoins (inner)" % inner_from)
            print("%d unjoined(inner)" % inner_to)
            print("%d unjoins (cross)" % cross_from)
            print("%d unjoined(cross)" % cross_to)
            print()

        return joininfo
