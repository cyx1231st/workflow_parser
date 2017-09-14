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
from functools import total_ordering
from os import path

from .. import reserved_vars as rv
from ..service_registry import ServiceRegistry
from ..utils import Heap
from .exc import LogError


class DriverPlugin(object):
    __metaclass__ = ABCMeta

    def __init__(self, extensions=None):
        if extensions is None:
            self._extensions = ["log"]
        else:
            self._extensions = extensions
        self._errors = set()
        self._occur = 0

    def do_report(self):
        if self._occur:
            print("!! WARN !!")
            print("error types: %d\n  %s" %
                    (len(self._errors), self._errors))
            print("occur: %d" % self._occur)
            print()
        self._errors = set()
        self._occur = 0

    def _purge_dict_empty_values(self, var_dict):
        for k in var_dict.keys():
            if var_dict[k] in {None, ""}:
                var_dict.pop(k)

    def do_filter_logfile(self, f_dir, f_name):
        assert isinstance(f_dir, str)
        assert isinstance(f_name, str)
        assert f_name in f_dir

        if not path.isfile(f_dir):
            return None, None

        ext_match = False
        for ext in self._extensions:
            if f_name.endswith("." + ext):
                ext_match = True
        if not ext_match:
            return None, None

        f_name = f_name.rsplit(".", 1)[0]
        assert f_name

        try:
            var_dict = {}
            ret = self.filter_logfile(f_dir, f_name, var_dict)
            assert isinstance(ret, bool)
            if ret:
                # NOTE
                # print("(LogDriver) loaded: %s" % f_dir)
                assert all(isinstance(k, str) for k in var_dict.keys())
                self._purge_dict_empty_values(var_dict)
                return f_name, var_dict
            else:
                return None, None
        except Exception as e:
            raise LogError(
                "(LogDriver) `filter_logfile` error when f_name=%s"
                % f_name, e)

    def do_filter_logline(self, line):
        assert isinstance(line, str)

        try:
            var_dict = {}
            ret = self.filter_logline(line, var_dict)
            assert all(isinstance(k, str) for k in var_dict.keys())
            self._purge_dict_empty_values(var_dict)
            assert isinstance(ret, bool)
            return ret, var_dict
        except Exception as e:
            raise LogError(
                "(LogDriver) `filter_logline` error when line=%s"
                % line, e)

    def do_preprocess_logline(self, logline):
        assert isinstance(logline, LogLine)

        try:
            ret = self.preprocess_logline(logline)
            assert ret is True or isinstance(ret, str)
            if ret is not True:
                self._errors.add(ret)
                self._occur += 1
                return False
            return True
        except Exception as e:
            raise LogError(
                "(LogDriver) `preprocess_logline` error when logline=\n%r"
                % logline, e)

    @abstractmethod
    def filter_logfile(self, f_dir, f_name, var_dict):
        pass

    @abstractmethod
    def filter_logline(self, line):
        pass

    @abstractmethod
    def preprocess_logline(self, logline):
        pass


@total_ordering
class LogLine(object):
    def __init__(self, lino, line, logfile, vs):
        assert isinstance(lino, int)
        assert isinstance(line, str)
        assert isinstance(logfile, LogFile)
        assert isinstance(vs, dict)

        # required immediately:
        self.time = None
        self.seconds = None
        self.keyword = None
        # required after 1st pass:
        self.thread = None
        # required after 2nd pass:
        self.request = None

        self.lino = lino
        self.logfile = logfile
        self.line = line.strip()
        self._vars = {}
        self._assigned = None

        self.prv_logline = None
        self.nxt_logline = None
        self.prv_thread_logline = None
        self.nxt_thread_logline = None

        # init
        for k, v in vs.iteritems():
            self[k] = v

        # required immediately
        if self.time is None:
            raise LogError("(LogLine) require 'time' when parse line: %s" % line)
        if self.seconds is None:
            raise LogError("(LogLine) require 'seconds' when parse line: %s" % line)
        if self.keyword is None:
            raise LogError("(LogLine) require 'keyword' when parse line: %s" % line)

    # total ordering
    __eq__ = lambda self, other: self.seconds == other.seconds
    __lt__ = lambda self, other: self.seconds < other.seconds

    def __getitem__(self, key):
        assert isinstance(key, str)

        if key in rv.ALL_VARS:
            return getattr(self, key)
        else:
            return self._vars[key]

    def __setitem__(self, key, value):
        assert isinstance(key, str)

        if key in rv.ALL_VARS:
            setattr(self, key, value)
        else:
            self._vars[key] = value

    def __getattribute__(self, item):
        assert isinstance(item, str)

        if item in rv.TARGET_VARS:
            return getattr(self.logfile, item)
        else:
            return super(LogLine, self).__getattribute__(item)

    def __setattr__(self, name, value):
        assert isinstance(name, str)

        if name in rv.ALL_VARS:
            # redirect access to self.ll_file
            if name in rv.TARGET_VARS:
                setattr(self.logfile, name, value)
                return

            # check types
            if value is not None:
                if name == rv.SECONDS:
                    if not isinstance(value, float):
                        raise LogError("(LogLine) the value of key '%s' is "
                                       "not float: %r" % (name, value))
                elif not isinstance(value, str):
                    raise LogError("(LogLine) the value key '%s' is "
                                   "not string: %r" % (name, value))

            # cannot overwrite
            old_v = self.__dict__.get(name)
            if old_v is not None and old_v != value:
                raise LogError("(LogLine) cannot overwrite attribute %s: "
                               "old_v=%s, new_v=%s" % (name, old_v, value))
        self.__dict__[name] = value

    def __contains__(self, item):
        assert isinstance(item, str)
        return item in self.get_keys(True)

    def get_keys(self, res=False):
        assert isinstance(res, bool)

        ret = set(self._vars.keys())
        if res:
            ret.update(rv.ALL_VARS)
        return ret

    def __str_marks__(self):
        mark_str = ""
        if not self._assigned:
            mark_str += ", ~ASSIGN"
        return mark_str

    def __str__(self):
        ret = "<LL>%.3f %s %s [%s %s %s] %s: <%s>%s" % (
              self.seconds,
              self.time,
              self.thread,
              self.component,
              self.host,
              self.target,
              self.request,
              self.keyword,
              self.__str_marks__())
        return ret

    def __str_target__(self):
        ret = "<LL>%.3f %s %s %s: <%s>%s" % (
              self.seconds,
              self.time,
              self.thread,
              self.request,
              self.keyword,
              self.__str_marks__())
        return ret

    def __str_thread__(self):
        ret = "<LL>%.3f %s %s: <%s>%s" % (
              self.seconds,
              self.time,
              self.request,
              self.keyword,
              self.__str_marks__())
        return ret

    def __repr__(self):
        ret = str(self)
        ret += "\n  V:"
        for k, v in self._vars.iteritems():
            ret += "%s=%s," % (k, v)
        ret += "\n  L[%s]: %s" % (self.logfile.filename, self.line)
        return ret


class LogFile(object):
    _repr_lines_lim = 10

    def __init__(self, f_name, f_dir, sr, vs):
        assert isinstance(f_name, str)
        assert isinstance(f_dir, str)
        assert isinstance(sr, ServiceRegistry)

        self.filename = f_name
        self.dir_ = f_dir
        self.sr = sr

        # required after line parsed
        self.component = None
        self.host = None
        self.target = None

        self.threads = set()
        self.requests = set()

        self.total_lines = 0
        self.loglines = []

        self.errors = {}
        self.warns = {}

        for k, v in vs.iteritems():
            if k not in rv.TARGET_VARS:
                raise LogError("(LogFile) key is not reserved: %r" % k)
            setattr(self, k, v)

    @property
    def name(self):
        return self.filename

    def __len__(self):
        return len(self.loglines)

    def __str__(self):
        target_str = ""
        if self.target:
            target_str += "#%s" % self.target

        return "<LogFile%s: fname=%s, comp=%s, host=%s, %d from %d"\
               "lines, %d threads, %d requests>" % (
               target_str,
               self.filename,
               self.component,
               self.host,
               len(self.loglines),
               self.total_lines,
               len(self.threads),
               len(self.requests))

    def __repr__(self):
        ret = str(self)
        lim = self._repr_lines_lim
        ret += "\n^ ---- last ----"
        for line in reversed(self.loglines):
            ret += "\n| %s" % line.__str_target__()
            lim -= 1
            if lim <= 0:
                ret += "\n ......"
        ret += "\n  ---- first ---"
        return ret

    def __setattr__(self, name, value):
        assert isinstance(name, str)

        if name in rv.ALL_VARS:
            # check leagal
            if name in rv.LINE_VARS:
                raise LogError("(LogFile) cannot set line var %s!" % name)

            # check types
            if value is not None:
                if name == rv.COMPONENT:
                    value = self.sr.f_to_component(value)
                    if not value:
                        raise LogError("(LogFile) unrecognized component: %r" %
                                value)
                elif not isinstance(value, str):
                    raise LogError("(LogFile) the log key '%s' is "
                                   "not str: %r" % (name, value))

            # cannot overwrite
            old_v = self.__dict__.get(name)
            if old_v is not None and old_v != value:
                raise LogError("(LogFile) cannot overwrite attribute %s: "
                               "old_v=%s, new_v=%s" % (name, old_v, value))
        self.__dict__[name] = value

    @classmethod
    def create(cls, f_name, f_dir, sr, plugin):
        f_name, vs = plugin.do_filter_logfile(f_dir, f_name)
        if f_name is None:
            return None
        else:
            return cls(f_name, f_dir, sr, vs)

    def _yield_lines(self):
        with open(self.dir_, 'r') as reader:
            for line in reader:
                assert isinstance(line, str)
                self.total_lines += 1
                yield self.total_lines, line

    def _build_loglines(self, lines, plugin):
        for lino,line in lines:
            assert isinstance(line, str)
            ret, vs = plugin.do_filter_logline(line)
            if not ret:
                continue
            try:
                lg = LogLine(lino, line, self, vs)
                if lg.thread is not None:
                    self.threads.add(lg.thread)
                if lg.request is not None:
                    self.requests.add(lg.request)
            except LogError as e:
                raise LogError("(LogFile) error when parse line '%s'"
                        % line.strip(), e)
            yield lg

    def _buffer_lines(self, lines):
        buffer_lines = Heap(key=lambda a: a.seconds)

        prv_line = [None]
        def _flush_line(flush=None):
            while buffer_lines:
                if flush and buffer_lines.distance < flush:
                    break
                line = buffer_lines.pop()
                if prv_line[0] is not None:
                    prv_line[0].nxt_logline = line
                    line.prv_logline = prv_line[0]
                    assert prv_line[0] <= line
                yield line
                prv_line[0] = line

        for line in lines:
            assert isinstance(line, LogLine)
            buffer_lines.push(line)
            for line in _flush_line(1):
                yield line
        for line in _flush_line():
            yield line

    def read(self, plugin):
        assert isinstance(plugin, DriverPlugin)

        lines = self._yield_lines()
        lines = self._build_loglines(lines, plugin)
        lines = self._buffer_lines(lines)
        self.loglines = list(lines)

        # required after line parsed
        if not self.loglines:
            self.errors["Empty loglines"] = self
            return
        if not self.component:
            self.errors["Require 'component' after read file"] = self
            return
        if not self.host:
            self.errors["Require 'host' after read file"] = self
            return
        if not self.target:
            self.errors["Require 'target' after read file"] = self
            return

    def yield_logs(self, plugin):
        assert isinstance(plugin, DriverPlugin)

        prv_line = None
        prv_line_by_thread = {}
        for line in self.loglines:
            assert isinstance(line, LogLine)

            ret = plugin.do_preprocess_logline(line)

            thread = line.thread
            if thread is not None:
                self.threads.add(thread)
                t_prv_line = prv_line_by_thread.get(thread)
                if t_prv_line is not None:
                    t_prv_line.nxt_thread_logline = line
                    line.prv_thread_logline = t_prv_line
                prv_line_by_thread[thread] = line

            if line.request is not None:
                self.requests.add(line.request)

            if prv_line is not None:
                prv_line.nxt_logline = line
                line.prv_logline = prv_line
            prv_line = line

            if ret:
                if thread is None:
                    raise LogError("Require 'thread' when parse line: %r" % self)
                yield line
            else:
                # dangling logline
                pass
