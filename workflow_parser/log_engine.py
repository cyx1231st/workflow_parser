from __future__ import print_function

from collections import defaultdict
import os
from os import path
import sys

from workflow_parser.log_parser import DriverPlugin
from workflow_parser.log_parser import LogError
from workflow_parser.service_registry import ServiceRegistry
from workflow_parser.state_machine import Target
from workflow_parser.utils import Report


class TargetsCollector(object):
    def __init__(self):
        # statistics
        self.total_files = 0
        self.total_files_hascontent = 0
        self.total_loglines = 0
        self.total_lines_loglines = 0
        self.total_lines = 0

        # indexes
        self.targets_by_component = defaultdict(set)
        self.targets_by_host = defaultdict(set)
        self.hosts_by_component = defaultdict(set)

        # entities
        self.targetobjs_by_target = {}

        self.targetobjs_by_errortype = defaultdict(list)
        self.targetobjs_by_warntype = defaultdict(list)

        # others
        self.requests = set()

    def __len__(self):
        return len(self.targetobjs_by_target)

    def iteritems(self, targets=None):
        if targets is None:
            # yield from
            for item in self.targetobjs_by_target.iteritems():
                yield item
        else:
            for target in targets:
                yield (target, self.targetobjs_by_target[target])

    def iterkeys(self):
        # yield from
        for key in self.targetobjs_by_target.iterkeys():
            yield key

    def itervalues(self, targets=None):
        if targets is None:
            # yield from
            for value in self.targetobjs_by_target.itervalues():
                yield value
        else:
            for target in targets:
                yield self.targetobjs_by_target[target]

    def collect(self, target_obj):
        assert isinstance(target_obj, Target)

        if target_obj.errors:
            for k in target_obj.errors.keys():
                self.targetobjs_by_errortype[k].append(target_obj)
            return

        if target_obj.warns:
            for k in target_obj.warns.keys():
                self.targetobjs_by_warntype[k].append(target_obj)
            return

        if target_obj.target in self.targetobjs_by_target:
            raise LogError("(TargetsCollector) target '%s' collition "
                           "from files: %s, %s" % (
                               target_obj.target,
                               target_obj.filename,
                               self.targetobjs_by_target[target_obj.target].filename))

        self.targetobjs_by_target[target_obj.target] = target_obj
        self.targets_by_component[target_obj.component].add(target_obj.target)
        self.targets_by_host[target_obj.host].add(target_obj.target)
        self.hosts_by_component[target_obj.component].add(target_obj.host)


class TargetsEngine(TargetsCollector):
    def __init__(self, sr, plugin, report):
        assert isinstance(sr, ServiceRegistry)
        assert isinstance(plugin, DriverPlugin)
        assert isinstance(report, Report)

        self.sr = sr
        self.plugin = plugin
        self.report = report

        # other collections
        self.targetobjs = []

        super(TargetsEngine, self).__init__()

    def loadfiles(self, log_folder):
        assert isinstance(log_folder, str)

        # current_path = path.dirname(os.path.realpath(__file__))
        print("Load targets...")
        current_path = os.getcwd()
        log_folder = path.join(current_path, log_folder)
        for f_name in os.listdir(log_folder):
            f_dir = path.join(log_folder, f_name)
            f_name, vs = self.plugin.do_filter_logfile(f_dir, f_name)
            if f_name is None:
                continue
            target_obj = Target(f_name, f_dir, self.sr, self.plugin, vs)
            self.targetobjs.append(target_obj)
        print("---------------")

        #### summary ####
        self.total_files = len(self.targetobjs)
        print("%d files" % self.total_files)
        self.report.step("load_t", target=self.total_files)
        print()
        #################

    def readfiles(self):
        print("Read targets...")
        for target_obj in self.targetobjs:
            target_obj.read()
            # ready line vars: time, seconds, keyword
            # ready target vars: component, host, target
            self.collect(target_obj)
        print("---------------")

        #### summary ####
        self.total_files_hascontent = len(self)
        print("%d targets, %d hosts" %
                (self.total_files_hascontent,
                 len(self.targets_by_host)))
        for comp in self.sr.sr_components:
            targets = self.targets_by_component.get(comp, [])
            if not targets:
                raise LogError("ERROR! miss component %s" % comp)
            else:
                hosts = self.hosts_by_component[comp]
                print("  %s: %d targets, %d hosts"
                      % (comp, len(targets), len(hosts)))

        self.total_lines = sum(to.total_lines for to in self.targetobjs)
        self.total_loglines = sum(len(to) for to in self.itervalues())
        self.total_lines_loglines = sum(to.total_lines for to in self.itervalues())
        print("%d loglines:" % self.total_loglines)
        print("  %.2f%%: %d lines (valid files)"
                % (float(self.total_loglines)/self.total_lines_loglines*100,
                   self.total_lines_loglines))
        print("  %.2f%%: %d lines (all files)"
                % (float(self.total_loglines)/self.total_lines*100,
                   self.total_lines))
        self.report.lines[0] = self.total_lines
        self.report.step("read_t", target=self.total_files_hascontent,
                                   component=len(self.targets_by_component),
                                   host=len(self.targets_by_host),
                                   line=self.total_loglines)
        print()
        #################

        if self.targetobjs_by_warntype:
            print("! WARN !")
            for e_type, targetobjs in self.targetobjs_by_warntype.iteritems():
                print("%d files: %s" % (len(targetobjs), e_type))
            print()

        if self.targetobjs_by_errortype:
            print("!! ERROR !!")
            for e_type, targetobjs in self.targetobjs_by_errortype.iteritems():
                print("%d files: %s" % (len(targetobjs), e_type))
            print()

    def preparethreads(self):
        print("Prepare threads...")
        for target_obj in self.itervalues():
            requests = target_obj.prepare()
            # NOTE: for debug
            # print("%s:  %s" % (target_obj.name, target_obj.thread_objs.keys()))
            # NOTE: requests collector?
            self.requests.update(requests)
        print("----------------")

        #### summary ####
        sum_threads = 0
        sum_loglines = 0
        for (comp, targets) in self.targets_by_component.iteritems():
            cnt_component_targets, cnt_component_threads = 0, 0
            cnt_component_loglines = 0
            min_target_threads, max_target_threads = sys.maxint, 0
            for target_obj in self.itervalues(targets):
                cnt_target_threads = len(target_obj.thread_objs)
                cnt_component_targets += 1
                cnt_component_threads += cnt_target_threads
                cnt_component_loglines += len(target_obj.loglines)
                min_target_threads = min(min_target_threads, cnt_target_threads)
                max_target_threads = max(max_target_threads, cnt_target_threads)
            sum_loglines += cnt_component_loglines
            sum_threads += cnt_component_threads
            print("%s: %.3f[%d, %d] threads, total %d loglines"
                  % (comp,
                     cnt_component_threads/float(cnt_component_targets),
                     min_target_threads,
                     max_target_threads,
                     cnt_component_loglines))
        print("%d loglines" % sum_loglines)
        print("%d threads" % sum_threads)
        print("%d requests" % len(self.requests))

        self.report.step("prepare", line=sum_loglines,
                                    thread=sum_threads,
                                    request=len(self.requests))
        print()
        #################

        self.plugin.do_report()
