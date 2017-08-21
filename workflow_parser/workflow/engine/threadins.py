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
from collections import OrderedDict

from ...datasource import Target
from ...datasource import Thread
from ...datasource import Line
from ...graph import Master
from ...graph.token import Token
from ...utils import Report
from ..entities.threadins import BlankInterval
from ..entities.threadins import Pace
from ..entities.threadins import ThreadInstance
from ..entities.threadins import ThreadInterval


class Error(object):
    def __init__(self, keyword):
        assert isinstance(keyword, str)
        self.keyword = keyword
        self.before_error = defaultdict(list)

    def append(self, line_obj):
        assert isinstance(line_obj, Line)
        assert line_obj.keyword == self.keyword

        before = line_obj.prv_thread_line
        if not before:
            self.before_error["START"].append(line_obj)
        else:
            pace = before._assigned
            # prev line_obj must be parsed if prv exists
            assert pace
            assert isinstance(pace, Pace)
            self.before_error[pace.step.where].append(line_obj)

    def report(self, blanks=0):
        s_blank = " "*blanks
        key_list = [(len(self.before_error[k]), k)
                for k in self.before_error.keys()]
        key_list.sort()
        for num, key in key_list:
            print("%s%s: %s" % (s_blank, key, num))


class Errors(object):
    def __init__(self):
        self.start_errors = {}
        self.incomplete_errors = {}
        self.workflow_incomplete = {}
        self.len_start_errors = 0
        self.len_incomplete_errors = 0
        self.len_incomplete_success = 0

        self.is_success = False

    def success(self):
        self.is_success = True

    @staticmethod
    def _append(errors_, line_obj):
        keyword = line_obj.keyword
        err = errors_.get(keyword)
        if not err:
            err = Error(keyword)
            errors_[keyword] = err
        err.append(line_obj)

    def append_incomplete_failed(self, line_obj):
        assert isinstance(line_obj, Line)
        if self.is_success:
            # line_obj must not be the first one
            assert line_obj.prv_thread_line
            assert line_obj.prv_thread_line._assigned

            self._append(self.incomplete_errors, line_obj)
            self.is_success = False
        self.len_incomplete_errors += 1

    def append_start_failed(self, line_obj):
        assert isinstance(line_obj, Line)
        if self.is_success:
            self._append(self.start_errors, line_obj)
            self.is_success = False
        self.len_start_errors += 1

    def append_incomplete_success(self, line_obj):
        assert isinstance(line_obj, Line)
        self._append(self.workflow_incomplete, line_obj)
        self.len_incomplete_success += 1

    def report(self):
        if self.start_errors:
            print("! ERROR !")
            print("Threadgraph start errors: %d" % self.len_start_errors)
            keys = sorted(self.start_errors.keys())
            for key in keys:
                print("  %s:" % key)
                self.start_errors[key].report(4)
            print()

        if self.incomplete_errors:
            print("! ERROR !")
            print("Threadgraph incomplete errors: %s" %
                    self.len_incomplete_errors)
            keys = sorted(self.incomplete_errors.keys())
            for key in keys:
                print("  %s:" % key)
                self.incomplete_errors[key].report(4)
            print()

        if self.workflow_incomplete:
            print("! WARN !")
            print("Thread workflow incomplete: %s" %
                    self.len_incomplete_success)
            keys = sorted(self.workflow_incomplete.keys())
            for key in keys:
                print("  %s:" % key)
                self.workflow_incomplete[key].report(4)
            print()

    @staticmethod
    def _debug(errors_, key_err, key_prv):
        found_err = []
        for key in errors_.keys():
            if key_err in key:
                found_err.append(errors_[key])

        ret = []
        for err in found_err:
            for key in err.before_error.keys():
                if key_prv in key:
                    ret.append(err.before_error[key][0])
        return ret


    def debug(self, key_err, key_prv):
        lines = self._debug(self.incomplete_errors, key_err, key_prv)
        if lines:
            print("Incomplete errors: %d" % len(lines))
            for line in lines:
                line.debug()


errors = Errors()


def _apply_token(token, threadins, line_obj, join_info):
    assert isinstance(token, Token)
    assert isinstance(threadins, ThreadInstance)
    assert isinstance(line_obj, Line)

    step = token.step
    pace = Pace(line_obj, step, threadins)
    if step.joins_objs:
        join_info["innerjoin"]["joins"].append((pace, step.joins_objs))
    if step.joined_objs:
        join_info["innerjoin"]["joined"].append((pace, step.joined_objs))
    if step.joined_interface:
        join_info["crossjoin"]["joined"].append((pace, step.joined_interface))
    if step.joins_interface:
        join_info["crossjoin"]["joins"].append((pace, step.joins_interface))

    for key in line_obj.keys:
        if key in ("keyword", "time", "seconds"):
            continue

        new_val = line_obj[key]
        if key in ("component", "target", "host", "thread"):
            val = getattr(threadins, key)
            if val != line_obj[key]:
                raise StateError("(ThreadInstance) parse error: "
                        "variable %s mismatch: %s is not %s!"
                        % (key, val, new_val))
            else:
                pass
        elif key == "request":
            if new_val is None:
                pass
            elif threadins.request is None:
                threadins.request = new_val
            elif threadins.request != new_val:
                raise StateError("(ThreadInstance) parse error: "
                        "request mismatch: %s is not %s!"
                        % (val, new_val))
            else:
                pass
        else:
            if key in threadins.thread_vars_dup:
                threadins.thread_vars_dup[key].add(new_val)
            else:
                val = threadins.thread_vars.get(key)
                if val is None:
                    threadins.thread_vars[key] = new_val
                elif val != new_val:
                    threadins.thread_vars_dup[key].add(val)
                    threadins.thread_vars_dup[key].add(new_val)
                    threadins.thread_vars.pop(key)
                else:
                    pass

    if threadins.intervals:
        prv_pace = threadins.intervals[-1].to_pace
        interval = ThreadInterval(prv_pace, pace)
        threadins.intervals.append(interval)
    elif threadins._start_pace:
        interval = ThreadInterval(threadins._start_pace, pace)
        threadins.intervals.append(interval)
    else:
        interval = None
        threadins._start_pace = pace
        thread_obj = threadins.thread_obj
        if thread_obj.threadinss:
            BlankInterval(thread_obj.threadinss[-1].to_pace, pace)

    if interval is not None:
        for mark in interval.marks:
            threadins.intervals_by_mark[mark].append(interval)


def _try_create_threadins(mastergraph, line_obj, thread_obj, join_info):
    assert isinstance(mastergraph, Master)
    assert isinstance(line_obj, Line)
    assert isinstance(thread_obj, Thread)

    token = Token.new(mastergraph, line_obj.keyword, thread_obj.component)
    if token:
        threadins = ThreadInstance(thread_obj, token.thread_graph)
        _apply_token(token, threadins, line_obj, join_info)
        thread_obj.threadinss.append(threadins)
    else:
        threadins = None
        # error: failed to create new graph
    return token, threadins


cnf_threadparse_proceed_at_failure = False


def build_thread_instances(target_objs, mastergraph, report):
    assert isinstance(mastergraph, Master)
    assert isinstance(report, Report)

    valid_lineobjs = 0
    thread_objs = []
    join_info = defaultdict(lambda: defaultdict(list))

    print("Build thread instances...")
    for target_obj in target_objs.itervalues():
        assert isinstance(target_obj, Target)
        for thread_obj in target_obj.thread_objs.itervalues():
            assert isinstance(thread_obj, Thread)
            token = None
            ongoing_threadins = None
            thread_valid_lineobjs = 0

            for line_obj in thread_obj.line_objs:
                assert isinstance(line_obj, Line)
                if token is not None:
                    assert ongoing_threadins is not None

                    if token.do_step(line_obj.keyword):
                        _apply_token(token, ongoing_threadins, line_obj,
                                     join_info)
                        # success: token proceed
                        errors.success()
                    else:
                        _token, _ongoing_threadins = _try_create_threadins(
                                mastergraph, line_obj, thread_obj, join_info)
                        if not token.is_complete:
                            if not _token:
                                # failed: token failed, threadins incomplete
                                errors.append_incomplete_failed(line_obj)
                                if not cnf_threadparse_proceed_at_failure:
                                    token = _token
                                    ongoing_threadins = _ongoing_threadins
                            else:
                                # ~success: token renewed, threadins incomplete
                                errors.append_incomplete_success(line_obj)
                                token = _token
                                ongoing_threadins = _ongoing_threadins
                        else:
                            if not _token:
                                # failed: token failed renew, threadins complete
                                errors.append_start_failed(line_obj)
                                if not cnf_threadparse_proceed_at_failure:
                                    token = _token
                                    ongoing_threadins = _ongoing_threadins
                            else:
                                # success: token renewed
                                errors.success()
                                token = _token
                                ongoing_threadins = _ongoing_threadins
                else:
                    token, ongoing_threadins = _try_create_threadins(
                            mastergraph, line_obj, thread_obj, join_info)
                    if not token:
                        # failed: token failed, no threadins
                        errors.append_start_failed(line_obj)
                    else:
                        # success: token newed
                        errors.success()

                if not token:
                    thread_obj.dangling_lineobjs.append(line_obj)
                    assert line_obj._assigned is None
                else:
                    thread_valid_lineobjs += 1
                    assert line_obj._assigned is not None

            assert len(thread_obj.dangling_lineobjs) + thread_valid_lineobjs\
                    == len(thread_obj.line_objs)

            thread_objs.append(thread_obj)
            valid_lineobjs += thread_valid_lineobjs
    print("-------------------------")

    #### collect ####
    ignored_lineobjs_by_component = defaultdict(lambda: [])
    components = set()
    hosts = set()
    targets = set()
    threadinss = []
    incomplete_threadinss_by_graph = defaultdict(list)
    complete_threadinss_by_graph = defaultdict(list)
    start_threadinss = []
    duplicated_vars = set()
    cnt_innerjoins_paces = len(join_info["innerjoin"]["joins"])
    cnt_innerjoined_paces = len(join_info["innerjoin"]["joined"])
    cnt_joinedinterface_paces = len(join_info["crossjoin"]["joined"])
    cnt_joinsinterface_paces = len(join_info["crossjoin"]["joins"])

    for thread_obj in thread_objs:
        if thread_obj.dangling_lineobjs:
            ignored_lineobjs_by_component[thread_obj.component]\
                    .extend(thread_obj.dangling_lineobjs)
        if thread_obj.threadinss:
            components.add(thread_obj.component)
            hosts.add(thread_obj.host)
            targets.add(thread_obj.target)
        for threadins in thread_obj.threadinss:
            if not threadins.is_complete:
                incomplete_threadinss_by_graph[threadins.threadgraph.name]\
                        .append(threadins)
            else:
                complete_threadinss_by_graph[threadins.threadgraph.name]\
                        .append(threadins)
            if threadins.is_request_start:
                start_threadinss.append(threadins)
            threadinss.append(threadins)
            duplicated_vars.update(threadins.thread_vars_dup.keys())

    #### summary ####
    print("%d valid line_objs" % valid_lineobjs)
    print("%d thread instances" % len(threadinss))
    if complete_threadinss_by_graph:
        for gname, tis in complete_threadinss_by_graph.iteritems():
            print("  %s: %d inss" % (gname, len(tis)))

    print("%d request start t_instances" % len(start_threadinss))
    print()

    #### report #####
    report.step("build_t",
                line=valid_lineobjs,
                component=len(components),
                host=len(hosts),
                target=len(targets),
                thread=len(thread_objs),
                request=len(start_threadinss),
                threadins=len(threadinss),
                innerjoin=cnt_innerjoins_paces,
                innerjoined=cnt_innerjoined_paces,
                leftinterface=cnt_joinedinterface_paces,
                rightinterface=cnt_joinsinterface_paces)

    #### errors #####
    errors.report()
    # import pdb; pdb.set_trace()

    if ignored_lineobjs_by_component:
        print("! WARN !")
        for comp, line_objs in ignored_lineobjs_by_component.iteritems():
            print("%s: %d ignored line_objs" % (comp, len(line_objs)))
        print()

    edges = mastergraph.get_unseenedges()
    if edges:
        print("! WARN !")
        print("Unseen graph edges: %s" %
                ",".join(edge.name for edge in edges))
        print()

    if duplicated_vars:
        print("! WARN !")
        print("Duplicated vars in t_instances: %s" %
                ",".join(duplicated_vars))
        print()

    if incomplete_threadinss_by_graph:
        print("! WARN !")
        print("Incompleted t_instances:")
        for gname, tis in incomplete_threadinss_by_graph.iteritems():
            print("  %s: %d t_instances" % (gname, len(tis)))
        print()

    return threadinss, join_info
