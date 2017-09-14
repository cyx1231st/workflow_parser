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
import os
from os import path
import sys

from ..service_registry import ServiceRegistry
from ..utils import Report
from . import Target
from . import Thread
from . import Line
from .exc import LogError
from .log_entities import LogFile
from .log_entities import LogLine


class LogEngine(object):
    def __init__(self, sr, plugin, report):
        assert isinstance(sr, ServiceRegistry)
        assert isinstance(report, Report)

        self.sr = sr
        self.plugin = plugin
        self.report = report

    # step1: load related log files
    def _loadfiles(self, log_folder):
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

        return logfiles

    # step2: read lines from log files
    def _readfiles(self, logfiles_in):
        logfiles_by_errortype = defaultdict(list)
        logfiles_by_warntype = defaultdict(list)

        logfiles = []
        logfiles_by_host = defaultdict(list)
        logfiles_by_component = defaultdict(list)
        hosts_by_component = defaultdict(list)

        print("Read targets...")
        for logfile in logfiles_in:
            assert isinstance(logfile, LogFile)

            logfile.read(self.plugin)
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

        return logfiles

    # step3: prepare Target, Thread and Line objects
    def _preparethreads(self, logfiles_in):
        hosts = set()
        target_objs = {}
        targetobjs_by_component = defaultdict(list)
        requests_detected = set()

        print("Prepare threads...")
        for logfile in logfiles_in:
            # build Target object
            target_obj = Target(logfile.target,
                                logfile.component,
                                logfile.host,
                                logfile)

            index_thread = 0
            for logline in logfile.yield_logs(self.plugin):
                assert isinstance(logline, LogLine)
                assert logline.logfile is logfile
                assert logline.target == target_obj.target

                if logline.request is not None:
                    requests_detected.add(logline.request)

                target_obj.append_line(
                        thread=logline.thread,
                        lino=logline.lino,
                        time=logline.time,
                        seconds=logline.seconds,
                        keyword=logline.keyword,
                        request=logline.request,
                        vs=logline._vars,
                        line=logline.line,
                        entity=logline)

            if target_obj.len_lineobjs:
                assert target_obj.target not in target_objs
                target_objs[target_obj.target] = target_obj
                targetobjs_by_component[target_obj.component].append(target_obj)
                hosts.add(target_obj.host)
        print("----------------")

        #### summary ####
        total_lines = sum(to.len_lineobjs for to in
                target_objs.itervalues())
        total_requests = len(requests_detected)
        total_threads = sum(len(to.thread_objs) for to in
                target_objs.itervalues())
        print("%d lines, %d requests, %d threads" % (
            total_lines,
            total_requests,
            total_threads))

        total_thread_lines = sum(th.len_lineobjs
                for to in target_objs.itervalues()
                for th in to.thread_objs.itervalues())
        assert total_thread_lines == total_lines

        for comp, target_objs_ in targetobjs_by_component.iteritems():
            hosts_ = set()
            component_threads = sum(len(to.thread_objs) for to in target_objs_)
            component_loglines = sum(to.len_lineobjs for to in target_objs_)

            min_target_threads, max_target_threads = sys.maxint, 0
            min_target_loglines, max_target_loglines = sys.maxint, 0
            for target_obj in target_objs_:
                hosts_.add(target_obj.host)
                min_target_threads = min(min_target_threads, len(target_obj.thread_objs))
                max_target_threads = max(max_target_threads, len(target_obj.thread_objs))
                min_target_loglines = min(min_target_loglines,
                        target_obj.len_lineobjs)
                max_target_loglines = max(max_target_loglines,
                        target_obj.len_lineobjs)

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
        print()

        #### report #####
        self.report.step("prepare", line=total_lines,
                                    component=len(targetobjs_by_component),
                                    host=len(hosts),
                                    target=len(target_objs),
                                    thread=total_threads,
                                    request=total_requests)

        #### errors #####
        self.plugin.do_report()

        return target_objs

    def proceed(self, logfolder):
        logfiles = self._loadfiles(logfolder)
        logfiles = self._readfiles(logfiles)
        targetobjs = self._preparethreads(logfiles)
        return targetobjs
