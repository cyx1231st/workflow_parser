from collections import defaultdict
import os
from os import path
import sys

from workflow_parser.log_parser.log_parser import DriverPlugin
from workflow_parser.log_parser.log_parser import LogError
from workflow_parser.log_parser.log_parser import LogFile
from workflow_parser.log_parser.service_registry import ServiceRegistry


class TargetsCollector(object):
    def __init__(self):
        # statistics
        self.total_files = 0
        self.total_files_hascontent = 0
        self.total_loglines = 0
        self.total_lines_loglines = 0
        self.total_lines = 0

        # indexes
        self.targets_by_component = defaultdict(lambda: set())
        self.targets_by_host = defaultdict(lambda: set())
        self.hosts_by_component = defaultdict(lambda: set())

        # entities
        self.logfiles_by_target = {}

    def __len__(self):
        return len(self.logfiles_by_target)

    def iteritems(self, targets=None):
        if targets is None:
            # yield from
            for item in self.logfiles_by_target.iteritems():
                yield item
        else:
            for target in targets:
                yield (target, self.logfiles_by_target[target])

    def iterkeys(self):
        # yield from
        for key in self.logfiles_by_target.iterkeys():
            yield key

    def itervalues(self, targets=None):
        if targets is None:
            # yield from
            for value in self.logfiles_by_target.itervalues():
                yield value
        else:
            for target in targets:
                yield self.logfiles_by_target[target]

    def collect(self, logfile):
        assert isinstance(logfile, LogFile)

        if len(logfile) == 0:
            return
        if logfile.target in self.logfiles_by_target:
            raise LogError("(TargetsCollector) target '%s' collition "
                           "from files: %s, %s" % (
                               logfile.target,
                               logfile.filename,
                               self.logfiles_by_target[logfile.target].filename))

        self.logfiles_by_target[logfile.target] = logfile
        self.targets_by_component[logfile.component].add(logfile.target)
        self.targets_by_host[logfile.host].add(logfile.target)
        self.hosts_by_component[logfile.component].add(logfile.host)


class TargetsEngine(TargetsCollector):
    def __init__(self, sr, plugin):
        assert isinstance(sr, ServiceRegistry)
        assert isinstance(plugin, DriverPlugin)

        self.sr = sr
        self.plugin = plugin

        # other collections
        self.logfiles = []
        self.requests = set()

        super(TargetsEngine, self).__init__()

    def loadfiles(self, log_folder):
        assert isinstance(log_folder, str)

        # current_path = path.dirname(os.path.realpath(__file__))
        print("Load targets(logfiles)...")
        current_path = os.getcwd()
        log_folder = path.join(current_path, log_folder)
        for f_name in os.listdir(log_folder):
            f_dir = path.join(log_folder, f_name)
            f_name = self.plugin.do_filter_logfile(f_dir, f_name)
            if not f_name:
                continue
            logfile = LogFile(f_name, f_dir, self.sr, self.plugin)
            self.logfiles.append(logfile)
        self.total_files = len(self.logfiles)
        print("(TargetsEngine) detected %d targets" % self.total_files)
        print("ok\n")

    def readfiles(self):
        print("Read targets...")
        for logfile in self.logfiles:
            logfile.read()
            # ready line vars: time, seconds, keyword
            # ready target vars: component, host, target
            self.collect(logfile)

        self.total_files_hascontent = len(self)
        print("(TargetsEngine) %d targets has loglines, discarded %d targets" %
                (self.total_files_hascontent,
                 self.total_files - self.total_files_hascontent))


        self.total_lines = sum(logfile.total_lines for logfile in self.logfiles)
        self.total_loglines = sum(len(logfile) for logfile in self.itervalues())
        self.total_lines_loglines = sum(logfile.total_lines for logfile in self.itervalues())
        print("(TargetsEngine) built %d loglines from %d lines, total %d lines" %
                (self.total_loglines,
                 self.total_lines_loglines,
                 self.total_lines))

        print("(TargetEngine) components:")
        for comp in self.sr.sr_components:
            targets = self.targets_by_component.get(comp, [])
            if not targets:
                raise LogError("(TargetsEngine) targets miss component %s" % comp)
            else:
                hosts = self.hosts_by_component[comp]
                print("  %s has %d targets, and %d hosts"
                      % (comp, len(targets), len(hosts)))
        print("(TargetsEngine) detected %d hosts" % len(self.targets_by_host))
        print("ok\n")

    def buildthreads(self):
        print("Build threads...")
        for logfile in self.itervalues():
            requests = logfile.prepare()
            # NOTE: for debug
            # print("%s:  %s" % (logfile.name, logfile.loglines_by_thread.keys()))
            # NOTE: requests collector?
            self.requests.update(requests)
        self.plugin.do_report()
        print("(TargetsEngine) detected %d requests" % len(self.requests))
        sum_t = 0
        for (comp, targets) in self.targets_by_component.iteritems():
            cnt, sum_, min_, max_ = 0, 0, sys.maxint, 0
            for logfile in self.itervalues(targets):
                lent = len(logfile.loglines_by_thread)
                cnt += 1
                sum_ += lent
                min_ = min(min_, lent)
                max_ = max(max_, lent)
            sum_t += sum_
            print("  %s has %.3f[%d, %d] threads"
                  % (comp, sum_/float(cnt), min_, max_))
        print("(TargetsEngine) detected %d threads" % sum_t)
        print("ok\n")
