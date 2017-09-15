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
from functools import total_ordering
import numpy as np
from itertools import chain
from itertools import izip

from ..workflow.entities.bases import IntervalBase
from ..workflow.entities.threadins import ThreadActivity
from ..workflow.entities.join import JoinActivityBase
from ..workflow.entities.request import ExtendedInterval


def projection_time(from_tos):
    from_tos.sort()

    total = 0
    start = None
    end = None
    for from_, to_ in from_tos:
        if start is None:
            start = from_
            end = to_
        elif from_ <= end:
            end = max(end, to_)
        else:
            total += (end-start)
            start = from_
            end = to_
    if start is not None:
        total += (end-start)
    return total


@total_ordering
class IntGroup(object):
    def __init__(self, int_name, from_edgename, to_edgename, weight, desc):
        self.int_name = int_name
        self.from_edgename = from_edgename
        self.to_edgename = to_edgename
        self.vis_weight = weight
        self.desc = desc
        self.intervals = []

        self._nxt_groups = set()
        self._prv_groups = set()

        self.links = None
        self.linked = None

    @property
    def path(self):
        return "%s[%s]%s" % (
                self.from_edgename,
                self.int_name,
                self.to_edgename)

    @property
    def len_ints(self):
        return len(self.intervals)

    @property
    def from_seconds(self):
        if not self.intervals:
            return 0
        return min(c.from_seconds for c in self.intervals)

    @property
    def to_seconds(self):
        if not self.intervals:
            return 0
        return max(c.to_seconds for c in self.intervals)

    @property
    def lapse(self):
        return self.to_seconds - self.from_seconds

    @property
    def cumulated_seconds(self):
        if not self.intervals:
            return 0
        return sum([c.lapse for c in self.intervals])

    @property
    def projection_seconds(self):
        if not self.intervals:
            return 0
        from_tos = [(c.from_seconds, c.to_seconds) for c in self.intervals]
        return projection_time(from_tos)

    @property
    def is_inlink(self):
        if self.links or self.linked:
            return True
        else:
            return False

    def __repr__(self):
        return "<IntGroup#%s: %d intervals, %d nxt_groups" % (
                self.path,
                len(self.intervals),
                len(self._nxt_groups))

    __eq__ = lambda self, other: len(self.intervals) == len(other.intervals)
    __lt__ = lambda self, other: len(self.intervals) > len(other.intervals)

    def append_interval(self, interval):
        assert isinstance(interval, IntervalBase)
        assert interval.path == self.path
        self.intervals.append(interval)

    def append_group(self, group):
        assert isinstance(group, IntGroup)
        assert group.from_edgename == self.to_edgename
        self._nxt_groups.add(group)
        group._prv_groups.add(self)

    def prepare_sort(self):
        assert not self.linked

        if self.links:
            prv_groups = chain(self._prv_groups, self.links._prv_groups)
            nxt_groups = chain(self._nxt_groups, self.links._nxt_groups)
        else:
            prv_groups = self._prv_groups
            nxt_groups = self._nxt_groups

        self._topo_in = set()
        for g in prv_groups:
            if g.linked:
                self._topo_in.add(g.linked)
            else:
                self._topo_in.add(g)

        self._topo_out = set()
        for g in nxt_groups:
            if g.linked:
                self._topo_out.add(g.linked)
            else:
                self._topo_out.add(g)

    def try_merge(self, group):
        assert isinstance(group, IntGroup)
        assert not self.is_inlink
        assert not group.is_inlink
        assert self.path == group.path
        self.links = group
        group.linked = self

    def abort_merge(self):
        self.links.linked = None
        self.links = None

    def apply_merge(self):
        assert self.links

        self.intervals.extend(self.links.intervals)
        for nxt_group in self.links._nxt_groups:
            nxt_group._prv_groups.remove(self.links)
            nxt_group._prv_groups.add(self)
        for prv_group in self.links._prv_groups:
            prv_group._nxt_groups.remove(self.links)
            prv_group._nxt_groups.add(self)
        self._nxt_groups.update(self.links._nxt_groups)
        self._prv_groups.update(self.links._prv_groups)

        self.links = None

    def iter_nxtgroups(self):
        for g in sorted(self._nxt_groups):
            yield g


class Workflow(object):
    def __init__(self, name):
        self.name = name
        self.start_group = None
        self.groups = set()
        self.len_intervals = 0
        self.reqs = []

        # for build
        self._group_byreq = {}

        # for reduce
        self.paths = {}

    @property
    def len_reqs(self):
        return len(self.reqs)

    def build(self, intervals):
        group_by_fromgroup_toedgename = {}
        newgroup_byreq = {}
        for interval in intervals:
            if interval is None:
                continue
            assert isinstance(interval, IntervalBase)
            assert interval.is_interval
            assert interval.request

            if not self._group_byreq:
                if not self.start_group:
                    self.start_group = IntGroup("START",
                                                None,
                                                interval.from_edgename,
                                                None,
                                                "None")
                from_group = self.start_group
            else:
                from_group = self._group_byreq[interval.request]
            group_key = (from_group, interval.to_edgename)
            group = group_by_fromgroup_toedgename.get(group_key)
            if not group:
                if isinstance(interval, ThreadActivity):
                    weight = interval.state.vis_weight
                elif isinstance(interval, JoinActivityBase):
                    weight = interval.join_obj.vis_weight
                elif isinstance(interval, ExtendedInterval):
                    weight = interval.component.vis_weight
                else:
                    raise RuntimeError("Illegal interval %r" % interval)
                desc = "%s -> %s" % (
                        interval.from_keyword,
                        interval.to_keyword)

                group = IntGroup(interval.int_name,
                                 interval.from_edgename,
                                 interval.to_edgename,
                                 weight,
                                 desc)
                from_group.append_group(group)
                group_by_fromgroup_toedgename[group_key] = group
                self.groups.add(group)
                self.paths[group.path] = group.vis_weight
            group.append_interval(interval)
            self.len_intervals += 1
            newgroup_byreq[interval.request] = group
        if not self.reqs:
            self.reqs = newgroup_byreq.keys()
        self._group_byreq = newgroup_byreq

    # NOTE: Depth first
    def sort_topologically(self):
        ret = []
        self.start_group.prepare_sort()
        for group in self.groups:
            group.prepare_sort()

        def _walk(group):
            for nxt_group in sorted(group._topo_out):
                nxt_group._topo_in.remove(group)
                if not nxt_group._topo_in:
                    ret.append(nxt_group)
                    _walk(nxt_group)

        _walk(self.start_group)
        if len(ret) == len(self.groups):
            return ret
        else:
            return None

    def reduce(self):
        print("Workflow: built %d groups" % len(self.groups))
        #1 sort states
        path_weights = sorted(self.paths.iteritems(),
                              key=lambda s:s[1],
                              reverse=True)
        for path, _ in path_weights:
            #2 sort groups of the same path
            groups = self.sort_topologically()
            assert groups

            groups = [group for group in groups if group.path == path]
            while groups:
                group = groups[0]
                groups = groups[1:]
                nxt_groups = []
                for to_merge in groups:
                    #3 try merge group pairs
                    group.try_merge(to_merge)
                    self.groups.remove(to_merge)
                    if self.sort_topologically() is not None:
                        group.apply_merge()
                    else:
                        group.abort_merge()
                        self.groups.add(to_merge)
                        nxt_groups.append(to_merge)
                groups = nxt_groups
        print("Workflow: reduced to %d groups" % len(self.groups))

    def ready(self):
        for group in self.groups:
            assert not group.is_inlink
            group.intervals.sort()

    def __repr__(self):
        return "<Workflow %s: %d groups, %d reqs, %d intervals>" % (
                self.name,
                len(self.groups),
                len(self.reqs),
                self.len_intervals)

    def __str__(self):
        ret = repr(self)

        lines = []
        attrs = []
        for group in self.sort_topologically():
            assert isinstance(group, IntGroup)
            lines.append(group.path)
            len_ints = group.len_ints
            proj = group.projection_seconds
            added = group.cumulated_seconds
            lapse = group.lapse
            avg = len_ints and added/len_ints
            ratio = len_ints and proj/len_ints
            attrs.append((len_ints, proj, added, lapse, avg, ratio, group.desc))

        len_line = max(len(l) for l in lines)
        ret += "\n"+" "*len_line + "|   cnt,"+\
                "project_s,cumulat_s,  lapse_s,   avg_MS, ratio_MS"
        format_str = "\n%-" + str(len_line) + "s|" + "%6s,"\
                + "%9.5f,"*5 + " %s"
        for line, (len_ints, proj, added, lapse, avg, ratio, desc)\
                in izip(lines, attrs):
            ret += format_str % (
                    line, len_ints, proj, added, lapse, avg*1000, ratio*1000, desc)
        return ret
