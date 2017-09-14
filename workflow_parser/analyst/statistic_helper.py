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

from functools import total_ordering
import numpy as np
from itertools import izip

from ..workflow.entities.bases import IntervalBase


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


class StepContent(object):
    def __init__(self, interval, step, from_content):
        assert isinstance(step, Step)
        assert interval.from_edgename == step.from_edgename
        assert interval.int_name == step.int_name
        assert interval.to_edgename == step.to_edgename

        self.interval = interval
        self.step = step

        self.prv_content = from_content
        self.nxt_content = None

        step.contents.append(self)
        if from_content:
            assert isinstance(from_content, StepContent)
            from_content.nxt_content = self

    @property
    def from_seconds(self):
        return self.interval.from_seconds

    @property
    def to_seconds(self):
        return self.interval.to_seconds

    @property
    def lapse(self):
        return self.to_seconds - self.from_seconds


class Step(object):
    def __init__(self, int_name, to_edgename, prv_step):
        self.int_name = int_name
        self.to_edgename = to_edgename
        self.contents = []

        self.nxt_steps = []
        self.prv_step = prv_step

        if prv_step:
            prv_step.nxt_steps.append(self)

    @property
    def from_edgename(self):
        return self.prv_step and self.prv_step.to_edgename

    @property
    def path(self):
        return "%s[%s]%s" % (
                self.from_edgename,
                self.int_name,
                self.to_edgename)

    @property
    def len_ints(self):
        return len(self.contents)

    @property
    def from_seconds(self):
        if not self.contents:
            return 0
        return min(c.from_seconds for c in self.contents)

    @property
    def to_seconds(self):
        if not self.contents:
            return 0
        return max(c.to_seconds for c in self.contents)

    @property
    def lapse(self):
        return self.to_seconds - self.from_seconds

    @property
    def cumulated_seconds(self):
        if not self.contents:
            return 0
        return sum([c.lapse for c in self.contents])

    @property
    def projection_seconds(self):
        if not self.contents:
            return 0
        from_tos = [(c.from_seconds, c.to_seconds) for c in self.contents]
        return projection_time(from_tos)

    def __repr__(self):
        return "<Step#%s: %d contents, %d nxt_steps" % (
                self.path,
                len(self.contents),
                len(self.nxt_steps))


class Workflow(object):
    def __init__(self, name):
        self.start_step = None
        self.len_steps = 0
        self.len_contents = 0
        self.name = name
        self.reqs = []

        self._content_by_req = {}

    @property
    def len_reqs(self):
        return len(self.reqs)

    def build(self, intervals):
        steps_by_fromstep_tostepname = {}
        newcontent_by_req = {}
        for interval in intervals:
            if interval is None:
                continue
            assert isinstance(interval, IntervalBase)
            if not self._content_by_req:
                if not self.start_step:
                    self.start_step = Step("START",
                                           interval.from_edgename,
                                           None)
                from_content = None
                from_step = self.start_step
            else:
                from_content = self._content_by_req[interval.request]
                from_step = from_content.step
            assert from_step.to_edgename == interval.from_edgename
            step_key = (from_step, interval.to_edgename)
            step = steps_by_fromstep_tostepname.get(step_key)
            if not step:
                step = Step(interval.int_name,
                            interval.to_edgename,
                            from_step)
                steps_by_fromstep_tostepname[step_key] = step
                self.len_steps += 1
            content = StepContent(interval, step, from_content)
            self.len_contents += 1
            newcontent_by_req[interval.request] = content
        if not self.reqs:
            self.reqs = newcontent_by_req.keys()
        self._content_by_req = newcontent_by_req

    def __repr__(self):
        return "<Workflow %s: %d steps, %d reqs, %d contents>" % (
                self.name,
                self.len_steps,
                len(self._content_by_req),
                self.len_contents)

    def __str__(self):
        ret = "Workflow %s" % self.name

        lines = []
        attrs = []
        def __str_step__(step, pad):
            if step is None:
                return
            assert isinstance(step, Step)
            step.contents.sort(key=lambda c: (c.from_seconds, c.to_seconds))
            lines.append("%s%s" % (pad, step.path))
            len_ints = step.len_ints
            proj = step.projection_seconds
            added = step.cumulated_seconds
            lapse = step.lapse
            avg = len_ints and added/len_ints
            ratio = len_ints and proj/len_ints
            attrs.append((len_ints, proj, added, lapse, avg, ratio))
            if len(step.nxt_steps) > 1:
                pad += "'"
                step.nxt_steps.sort(key=lambda s: len(s.contents),
                                    reverse=True)
            else:
                pad += " "
            for nxt_s in step.nxt_steps:
                __str_step__(nxt_s, pad)

        step = self.start_step
        if len(step.nxt_steps) > 1:
            pad = "'"
            step.nxt_steps.sort(key=lambda s: len(s.contents),
                                reverse=True)
        else:
            pad = " "
        for nxt_s in step.nxt_steps:
            __str_step__(nxt_s, pad)

        len_line = max(len(l) for l in lines)
        ret += "\n"+" "*len_line + "|   cnt,"+\
                "project_s,cumulat_s,  lapse_s,   avg_ms, ratio_ms"
        format_str = "\n%-" + str(len_line) + "s|" + "%6s,"\
                + "%9.5f,"*5
        for line, (len_ints, proj, added, lapse, avg, ratio)\
                in izip(lines, attrs):
            ret += format_str % (
                    line, len_ints, proj, added, lapse, avg*1000, ratio*1000)
        return ret
