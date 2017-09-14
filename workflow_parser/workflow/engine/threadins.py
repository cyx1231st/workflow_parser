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

from ...datasource import Target
from ...datasource import Thread
from ...datasource import Line
from ...graph import Master
from ...utils import Report
from ..entities.threadins import Pace
from ..entities.threadins import ThreadInstance
from .schema import SchemaEngine


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
            pace = before._line_state
            # prev line_obj must be parsed if prv exists
            assert pace
            assert isinstance(pace, Pace)
            self.before_error[pace.edgename].append(line_obj)

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
            assert line_obj.prv_thread_line._line_state

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
cnf_threadparse_proceed_at_failure = False


def build_thread_instances(target_objs, mastergraph, schema_engine, report):
    assert isinstance(mastergraph, Master)
    assert isinstance(schema_engine, SchemaEngine)
    assert isinstance(report, Report)

    valid_lineobjs = 0
    thread_objs = []

    print("Build thread instances...")
    for target_obj in target_objs.itervalues():
        assert isinstance(target_obj, Target)
        for thread_obj in target_obj.thread_objs.itervalues():
            assert isinstance(thread_obj, Thread)
            threadins = None
            thread_valid_lineobjs = 0

            for line_obj in thread_obj.iter_lineobjs():
                assert isinstance(line_obj, Line)

                pace = None
                if threadins is not None:
                    pace = threadins.do_step(line_obj)
                    if pace is not None:
                        # success: threadins proceed
                        errors.success()
                    else:
                        nxt_threadins, pace = ThreadInstance.new(
                                mastergraph, line_obj, thread_obj)
                        if not threadins.is_complete:
                            if nxt_threadins is None:
                                # failed: renew failed, threadins incomplete
                                errors.append_incomplete_failed(line_obj)
                                if not cnf_threadparse_proceed_at_failure:
                                    threadins = None
                            else:
                                # ~success: threadins renewed, but incomplete
                                errors.append_incomplete_success(line_obj)
                                threadins.set_finish()
                                threadins = nxt_threadins
                        else:
                            if nxt_threadins is None:
                                # failed: renew failed, threadins complete
                                errors.append_start_failed(line_obj)
                                if not cnf_threadparse_proceed_at_failure:
                                    threadins = None
                            else:
                                # success: threadins renewed
                                errors.success()
                                threadins.set_finish()
                                threadins = nxt_threadins
                else:
                    threadins, pace = ThreadInstance.new(
                            mastergraph, line_obj, thread_obj)
                    if threadins is None:
                        # failed: new failed
                        errors.append_start_failed(line_obj)
                    else:
                        # success: new success
                        errors.success()

                if pace is None:
                    thread_obj.dangling_lineobjs.append(line_obj)
                    assert line_obj._line_state is None
                else:
                    thread_valid_lineobjs += 1
                    schema_engine.load_pace(pace)
                    assert line_obj._line_state is not None

            assert len(thread_obj.dangling_lineobjs) + thread_valid_lineobjs\
                    == thread_obj.len_lineobjs

            if threadins is not None:
                threadins.set_finish()
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
                incomplete_threadinss_by_graph[threadins.threadgraph_name]\
                        .append(threadins)
            else:
                complete_threadinss_by_graph[threadins.threadgraph_name]\
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
                innerjoin=len(schema_engine.innerj_proj.from_items),
                innerjoined=len(schema_engine.innerj_proj.to_items),
                leftinterface=len(schema_engine.crossj_proj.from_items),
                rightinterface=len(schema_engine.crossj_proj.to_items))

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

    return threadinss
