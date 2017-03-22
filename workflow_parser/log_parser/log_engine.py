from collections import defaultdict
import os
from os import path
import sys

from workflow_parser.log_parser.log_parser import DriverPlugin
from workflow_parser.log_parser.log_parser import LogError
from workflow_parser.log_parser.log_parser import LogFile
from workflow_parser.log_parser.service_registry import ServiceRegistry


class LogCollector(object):
    def __init__(self):
        self._targets = set()
        self.logfiles = []
        self.logfiles_by_target = {}
        self.logfiles_by_component = defaultdict(list)
        self.logfiles_by_host = defaultdict(list)

    def collect_logfile(self, logfile):
        assert isinstance(logfile, LogFile)

        if len(logfile) == 0:
            return
        target = logfile.target
        if target in self.logfiles_by_target:
            raise LogError("(LogCollector) target '%s' duplicate in "
                           "files: %s, %s" % (target, logfile.name,
                           self.logfiles_by_target[target].name))

        self.logfiles.append(logfile)
        self.logfiles_by_target[target] = logfile
        self.logfiles_by_component[logfile.component].append(logfile)
        self.logfiles_by_host[logfile.host].append(logfile)


def parse(log_folder, sr, plugin):
    assert isinstance(log_folder, str)
    assert isinstance(sr, ServiceRegistry)
    assert isinstance(plugin, DriverPlugin)

    collector = LogCollector()

    # step 1: load log files
    logfiles = []
    # current_path = path.dirname(os.path.realpath(__file__))
    current_path = os.getcwd()
    log_folder = path.join(current_path, log_folder)
    for f_name in os.listdir(log_folder):
        f_dir = path.join(log_folder, f_name)
        f_name = plugin.do_filter_logfile(f_dir, f_name)
        if not f_name:
            continue
        logfile = LogFile(f_name, f_dir, sr, plugin)
        logfiles.append(logfile)
    print("(LogEngine) detected %d files" % len(logfiles))
    print("Load logfiles ok..\n")

    # step 2: read log files
    for logfile in logfiles:
        logfile.read()
        collector.collect_logfile(logfile)
    print("(LogEngine) %d legal files" % len(collector.logfiles))
    len_lines = sum(len(logfile) for logfile in collector.logfiles)
    len_t_lines = sum(logfile.total_lines for logfile in collector.logfiles)
    print("(LogEngine) detected %d(%d) lines" % (len_lines, len_t_lines))
    for comp in sr.sr_components:
        t_len = len(collector.logfiles_by_component[comp])
        if t_len == 0:
            raise LogError("(LogCollector) missing component %s" % comp)
        else:
            hosts = set()
            for logfile in collector.logfiles_by_component[comp]:
                hosts.add(logfile.host)
            print("(LogEngine) Component#%s has %d files, with %d hosts"
                  % (comp, t_len, len(hosts)))
    print("(LogEngine) detected %d hosts" % len(collector.logfiles_by_host))
    print("Read logs ok..\n")

    # step 3: build threads
    requests = set()
    for logfile in collector.logfiles:
        ret = logfile.prepare()
        # NOTE: for debug
        # print("%s:  %s" % (logfile.name, logfile.loglines_by_thread.keys()))
        requests.update(ret)
    plugin.do_report()
    print("(LogEngine) detected %d requests" % len(requests))
    sum_t = 0
    for (comp, logfiles) in collector.logfiles_by_component.iteritems():
        cnt, sum_, min_, max_ = 0, 0, sys.maxint, 0
        for logfile in logfiles:
            lent = len(logfile.loglines_by_thread)
            cnt += 1
            sum_ += lent
            min_ = min(min_, lent)
            max_ = max(max_, lent)
        sum_t += sum_
        print("(LogEngine) %s has %.3f[%d, %d] threads"
              % (comp, sum_/float(cnt), min_, max_))
    print("(LogEngine) detected %d threads" % sum_t)
    print("Build threads ok..\n")
