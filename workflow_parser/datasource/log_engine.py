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

from abc import ABCMeta
from abc import abstractmethod
from collections import defaultdict
import os
from os import path
import sys

from .. import reserved_vars as rv
from ..service_registry import Component
from ..service_registry import ServiceRegistry
from . import Line
from . import Source
from .exc import LogError


class DriverPlugin(object):
    __metaclass__ = ABCMeta

    def __init__(self, extensions=None):
        if extensions is None:
            self._extensions = ["log"]
        else:
            self._extensions = extensions

    def _purge_dict_empty_values(self, var_dict):
        for k in var_dict.keys():
            if var_dict[k] in {None, ""}:
                var_dict.pop(k)

    def do_filter_logfile(self, f_dir, f_name):
        assert isinstance(f_dir, str)
        assert isinstance(f_name, str)
        assert f_name in f_dir

        # skip non-file
        if not path.isfile(f_dir):
            return False, None

        # check file extension
        ext_match = False
        for ext in self._extensions:
            if f_name.endswith("." + ext):
                ext_match = True
        if not ext_match:
            return False, None

        try:
            var_dict = {}
            ret = self.filter_logfile(f_dir, f_name, var_dict)
            assert isinstance(ret, bool)
            if ret:
                # NOTE
                # print("(LogDriver) loaded: %s" % f_dir)
                assert all(isinstance(k, str) for k in var_dict.keys())
                self._purge_dict_empty_values(var_dict)
                return True, var_dict
            else:
                # skip
                return False, None
        except Exception as e:
            raise LogError(
                "(LogDriver) `filter_logfile` error when f_name=%s"
                % f_name, e)

    def do_filter_logline(self, line, lino, where):
        assert isinstance(line, str)
        assert isinstance(lino, int)
        assert isinstance(where, str)

        try:
            var_dict = {}
            ret = self.filter_logline(line, var_dict)
            assert all(isinstance(k, str) for k in var_dict.keys())
            self._purge_dict_empty_values(var_dict)
            assert isinstance(ret, bool)
            return ret, var_dict
        except Exception as e:
            raise LogError("(LogDriver) `filter_logline` error at %s@%d %s"
                % (where, lino, line), e)

    @abstractmethod
    def filter_logfile(self, f_dir, f_name, var_dict):
        pass

    @abstractmethod
    def filter_logline(self, line):
        pass


class FileDatasource(object):
    def __init__(self, name, f_dir, vs, sr, plugin):
        assert isinstance(sr, ServiceRegistry)
        assert isinstance(plugin, DriverPlugin)

        self.sr = sr
        self.plugin = plugin
        self.name = name
        self.f_dir = f_dir
        self.total_lines = 0

        self.source = Source(name, f_dir, vs)

        self.requests = set()

    @property
    def total_lineobjs(self):
        return self.source.len_lineobjs

    # def _buffer_lines(self, lines):
    #     buffer_lines = Heap(key=lambda a: a.seconds)

    #     prv_line = [None]
    #     def _flush_line(flush=None):
    #         while buffer_lines:
    #             if flush and buffer_lines.distance < flush:
    #                 break
    #             line = buffer_lines.pop()
    #             if prv_line[0] is not None:
    #                 prv_line[0].nxt_logline = line
    #                 line.prv_logline = prv_line[0]
    #                 assert prv_line[0] <= line
    #             yield line
    #             prv_line[0] = line

    #     for line in lines:
    #         assert isinstance(line, LogLine)
    #         buffer_lines.push(line)
    #         for line in _flush_line(1):
    #             yield line
    #     for line in _flush_line():
    #         yield line

    def yield_lineobjs(self, targets_byname):
        with open(self.f_dir, 'r') as reader:
            for line in reader:
                self.total_lines += 1
                lino = self.total_lines

                if_proceed, vs = self.plugin.do_filter_logline(
                        line, lino, self.name)
                if if_proceed:
                    # convert component
                    component = vs.get(rv.COMPONENT)
                    if component is not None:
                        c_obj = self.sr.f_to_component(component)
                        if not c_obj:
                            raise LogError(
                                    "Error in %s@%d %s: unrecognized component %s"
                                    % (self.name, lino, line, component))
                        else:
                            vs[rv.COMPONENT] = c_obj
                    # collect requests
                    request = vs.get(rv.REQUEST)
                    if request is not None:
                        self.requests.add(request)

                    lineobj = self.source.append_line(
                            lino, line, vs, targets_byname)
                    yield lineobj

    @classmethod
    def create_byfolder(cls, log_folder, sr, plugin):
        assert isinstance(log_folder, str)
        assert isinstance(plugin, DriverPlugin)

        datasources = []
        # current_path = path.dirname(os.path.realpath(__file__))
        current_path = os.getcwd()
        log_folder = path.join(current_path, log_folder)
        for f_name in os.listdir(log_folder):
            f_dir = path.join(log_folder, f_name)
            if_proceed, vs = plugin.do_filter_logfile(f_dir, f_name)
            if if_proceed:
                # convert component
                component = vs.get(rv.COMPONENT)
                if component is not None:
                    c_obj = self.sr.f_to_component(component)
                    if not c_obj:
                        raise LogError(
                                "Error in %s: unrecognized component %s"
                                % (f_name, component))
                    else:
                        vs[rv.COMPONENT] = c_obj
                ds = cls(f_name.rsplit(".", 1)[0], f_dir, vs, sr, plugin)
                datasources.append(ds)

        return log_folder, datasources

# step1: load related log files
def loadsources(log_folder, sr, plugin):
    print("Load data sources...")
    log_folder, datasources = FileDatasource.create_byfolder(
        log_folder, sr, plugin)
    print("---------------")

    #### summary ####
    print("%d datasources from %s" % (len(datasources), log_folder))
    print()

    return datasources

# step2: read sources
def readsources(datasources, sr, report):
    targets_byname = {}
    targets_byhost = defaultdict(list)
    targets_bycomponent = defaultdict(list)
    threads = set()

    print("Read data sources...")
    for datasource in datasources:
        for line_obj in datasource.yield_lineobjs(targets_byname):
            pass
    for targetobj in targets_byname.itervalues():
        if not isinstance(targetobj.target, str) or not targetobj.target:
            raise LogError("%s has invalid target: %s" % (
                    targetobj, target.target))
        if not isinstance(targetobj.host, str) or not targetobj.host:
            raise LogError("%s has invalid host: %s" % (
                    targetobj, target.host))
        if not isinstance(targetobj.component, Component):
            raise LogError("%s has invalid component: %s" % (
                    targetobj, target.component))
        targets_byhost[targetobj.host].append(targetobj)
        targets_bycomponent[targetobj.component].append(targetobj)
        threads.update(targetobj.thread_objs)
    print("---------------")

    #### summary ####
    total_targets = len(targets_byname)
    total_hosts = len(targets_byhost)
    total_components = len(targets_bycomponent)
    print("%d targets, %d hosts" %
            (total_targets,
             total_hosts))

    total_lines = sum(datasource.total_lines for datasource in datasources)
    total_lineobjs = sum(datasource.total_lineobjs
            for datasource in datasources)
    if not total_lines:
        print("0 valid lines")
    else:
        print("%.2f%% valid: %d lines -> %d lineobjs"
                % (float(total_lineobjs)/total_lines*100,
                   total_lines,
                   total_lineobjs))

    for comp in sr.sr_components:
        targets = targets_bycomponent.get(comp, [])
        if not targets:
            raise LogError("ERROR! miss component %s" % comp)
        else:
            component_threads = sum(len(target.thread_objs) for target in targets)
            component_lines = sum(target.len_lineobjs for target in targets)
            min_target_threads, max_target_threads = sys.maxint, 0
            min_target_lineobjs, max_target_lineobjs = sys.maxint, 0
            hosts_ = set()
            for target_obj in targets:
                hosts_.add(target_obj.host)
                min_target_threads = min(min_target_threads, len(target_obj.thread_objs))
                max_target_threads = max(max_target_threads, len(target_obj.thread_objs))
                min_target_lineobjs = min(min_target_lineobjs,
                        target_obj.len_lineobjs)
                max_target_lineobjs = max(max_target_lineobjs,
                        target_obj.len_lineobjs)

            print("  %s: %d hosts, %d targets, %d threads, %d lines"
                  % (comp, len(hosts_), len(targets),
                     component_threads,
                     component_lines))
            print("    per-target: %.3f[%d, %d] threads, %.3f[%d, %d] loglines"
                    % (component_threads/float(len(targets)),
                       min_target_threads,
                       max_target_threads,
                       component_lines/float(len(targets)),
                       min_target_lineobjs,
                       max_target_lineobjs))
    print()

    #### report #####
    requests = set()
    for ds in datasources:
        requests.update(ds.requests)
    report.step("read", line=total_lineobjs,
                        component=total_components,
                        host=total_hosts,
                        target=total_targets,
                        thread=len(threads),
                        request=len(requests))
    return targets_byname

def proceed(logfolder, sr, plugin, report):
    datasources = loadsources(logfolder, sr, plugin)
    targetobjs = readsources(datasources, sr, report)

    return targetobjs
