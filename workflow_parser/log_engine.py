from __future__ import print_function

from collections import defaultdict
import os
from os import path
import sys

from workflow_parser.log_parser import LogError
from workflow_parser.log_parser import LogFile
from workflow_parser.service_registry import ServiceRegistry
from workflow_parser.state_runtime import Target
from workflow_parser.utils import Report


# the bridge between log engine and state engine
class TargetsCollector(object):
    def __init__(self):
        self.target_objs = None
        self.targetobjs_by_host = None
        self.targetobjs_by_target = None
        self.requests = None


class LogEngine(TargetsCollector):
    def __init__(self, sr, plugin, report):
        assert isinstance(sr, ServiceRegistry)
        assert isinstance(report, Report)

        self.sr = sr
        self.plugin = plugin
        self.report = report

        super(LogEngine, self).__init__()

    def loadfiles(self, log_folder):
        assert isinstance(log_folder, str)
        logfiles = []

        # current_path = path.dirname(os.path.realpath(__file__))
        print("Load targets...")
        current_path = os.getcwd()
        log_folder = path.join(current_path, log_folder)
        for f_name in os.listdir(log_folder):
            f_dir = path.join(log_folder, f_name)
            logfile = LogFile.create(f_name, f_dir, self.sr, self.plugin)
            if logfile is not None:
                logfiles.append(logfile)
        print("---------------")

        #### summary ####
        total_files = len(logfiles)
        print("%d files" % total_files)
        print()

        #### report #####
        self.report.step("load_t", target=total_files)
        #################

        return logfiles

    def readfiles(self, logfiles_in):
        logfiles_by_errortype = defaultdict(list)
        logfiles_by_warntype = defaultdict(list)

        logfiles = []
        logfiles_by_host = defaultdict(list)
        logfiles_by_component = defaultdict(list)
        hosts_by_component = defaultdict(list)

        print("Read targets...")
        for logfile in logfiles_in:
            assert isinstance(logfile, LogFile)

            logfile.read()
            # ready line vars: time, seconds, keyword
            # ready target vars: component, host, target

            if logfile.errors:
                for k in logfile.errors.iterkeys():
                    logfiles_by_errortype[k].append(logfile)
            elif logfile.warns:
                for k in logfile.warns.iterkeys():
                    logfiles_by_warntype[k].append(logfile)
            else:
                logfiles.append(logfile)
                logfiles_by_host[logfile.host].append(logfile)
                logfiles_by_component[logfile.component].append(logfile)
                hosts_by_component[logfile.component].append(logfile.host)
        print("---------------")

        #### summary ####
        total_files = len(logfiles)
        total_hosts = len(logfiles_by_host)
        total_components = len(hosts_by_component)
        print("%d targets, %d hosts" %
                (total_files,
                 total_hosts))
        for comp in self.sr.sr_components:
            files = logfiles_by_component.get(comp, [])
            if not files:
                raise LogError("ERROR! miss component %s" % comp)
            else:
                hosts = hosts_by_component[comp]
                print("  %s: %d targets, %d hosts"
                      % (comp, len(files), len(hosts)))

        total_lines = sum(file.total_lines for file in logfiles)
        total_loglines = sum(len(file) for file in logfiles)
        total_lines_in = sum(file.total_lines for file in logfiles_in)
        total_loglines_in = sum(len(file) for file in logfiles_in)

        threads = set()
        requests = set()
        for file in logfiles:
            threads.update(file.threads)
            requests.update(file.requests)

        print("%d loglines:" % total_loglines)
        print("  %.2f%%: %d lines (valid files)"
                % (float(total_loglines)/total_lines*100,
                   total_lines))
        print("  %.2f%%: %d lines (all files)"
                % (float(total_loglines)/total_lines_in*100,
                   total_lines_in))
        print()

        #### report #####
        self.report.lines[0] = total_lines_in
        self.report.step("read_t", line=total_loglines,
                                   component=total_components,
                                   host=total_hosts,
                                   target=total_files,
                                   thread=len(threads),
                                   request=len(requests))

        #### errors #####
        if logfiles_by_warntype:
            print("! WARN !")
            for e_type, _logfiles in logfiles_by_warntype.iteritems():
                print("%d files: %s" % (len(_logfiles), e_type))
            print()

        if logfiles_by_errortype:
            print("!! ERROR !!")
            for e_type, _logfiles in logfiles_by_errortype.iteritems():
                print("%d files: %s" % (len(_logfiles), e_type))
            print()
        #################

        return logfiles

    def preparethreads(self, logfiles_in):
        requests = set()

        target_objs = []
        targetobjs_by_component = defaultdict(list)
        requests_filtered = set()
        targetobjs_by_host = defaultdict(list)
        targetobjs_by_target = {}

        print("Prepare threads...")
        for logfile in logfiles_in:
            target_obj = Target(logfile)

            requests.update(logfile.requests)
            if target_obj.loglines:
                target_objs.append(target_obj)
                requests_filtered.update(target_obj.requests)
                targetobjs_by_component[target_obj.component].append(target_obj)
                targetobjs_by_host[target_obj.host].append(target_obj)
                targetobjs_by_target[target_obj.target] = target_obj
        print("----------------")

        #### summary1 ###
        total_requests = len(requests)
        total_threads = sum(len(file.threads) for file in logfiles_in)

        #### report1 ####
        self.report.step("prepare", thread=total_threads,
                                    request=total_requests)

        #### summary2 ###
        total_loglines_correct = sum(len(to.loglines) for to in target_objs)
        total_requests_correct = len(requests_filtered)
        total_threads_correct = sum(len(to.thread_objs) for to in target_objs)
        print("%d loglines, %d requests, %d threads" % (
            total_loglines_correct,
            total_requests_correct,
            total_threads_correct))

        sum_loglines_correct = sum(len(th.loglines)
                for to in target_objs
                for th in to.thread_objs.itervalues())
        assert sum_loglines_correct == total_loglines_correct

        for comp, target_objs_ in targetobjs_by_component.iteritems():
            hosts_ = set()
            component_threads = sum(len(to.thread_objs) for to in target_objs_)
            component_loglines = sum(len(to.loglines) for to in target_objs_)

            min_target_threads, max_target_threads = sys.maxint, 0
            min_target_loglines, max_target_loglines = sys.maxint, 0
            for target_obj in target_objs_:
                hosts_.add(target_obj.host)
                min_target_threads = min(min_target_threads, len(target_obj.thread_objs))
                max_target_threads = max(max_target_threads, len(target_obj.thread_objs))
                min_target_loglines = min(min_target_loglines, len(target_obj.loglines))
                max_target_loglines = max(max_target_loglines, len(target_obj.loglines))

            print("  %s: %d hosts, %d targets, %d threads, %d loglines"
                    % (comp, len(hosts_), len(target_objs_),
                       component_threads, component_loglines))
            print("    per-target: %.3f[%d, %d] threads, %.3f[%d, %d] loglines"
                    % (component_threads/float(len(target_objs_)),
                       min_target_threads,
                       max_target_threads,
                       component_loglines/float(len(target_objs_)),
                       min_target_loglines,
                       max_target_loglines))

        #### report2 ####
        self.report.step("prepare", line=total_loglines_correct,
                                    component=len(targetobjs_by_component),
                                    host=len(targetobjs_by_host),
                                    target=len(target_objs),
                                    thread=total_threads_correct,
                                    request=total_requests_correct)
        print()
        #################

        self.plugin.do_report()

        self.target_objs = target_objs
        self.targetobjs_by_host = targetobjs_by_host
        self.requests = requests_filtered
        self.targetobjs_by_target = targetobjs_by_target
