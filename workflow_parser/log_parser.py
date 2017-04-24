from __future__ import print_function

from abc import ABCMeta
from abc import abstractmethod
from collections import defaultdict
from collections import deque
import heapq
from functools import total_ordering
from os import path

from workflow_parser.service_registry import ServiceRegistry
from workflow_parser import reserved_vars as rv
from workflow_parser.exception import WFException
from workflow_parser.utils import Heap


class LogError(WFException):
    pass


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
            return None

        ext_match = False
        for ext in self._extensions:
            if f_name.endswith("." + ext):
                ext_match = True
        if not ext_match:
            return None

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
    def __init__(self, line, target_obj, vs):
        assert isinstance(line, str)
        assert isinstance(target_obj, Target)
        assert isinstance(vs, dict)

        # required immediately:
        self.time = None
        self.seconds = None
        self.keyword = None
        # required after 1st pass:
        self.thread = None
        # required after 2nd pass:
        self.request = None

        self.target_obj = target_obj
        self.line = line.strip()
        self._vars = {}

        self.correct = True
        self.ignored = False

        self.prv_logline = None
        self.nxt_logline = None

        self.prv_thread_logline = None
        self.nxt_thread_logline = None

        self.pace = None

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

    @property
    def seconds(self):
        seconds = self.__dict__.get(rv.SECONDS)
        if seconds is None:
            return None
        else:
            return seconds + self.target_obj.offset

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
            return getattr(self.target_obj, item)
        else:
            return super(LogLine, self).__getattribute__(item)

    def __setattr__(self, name, value):
        assert isinstance(name, str)

        if name in rv.ALL_VARS:
            # redirect access to self.ll_file
            if name in rv.TARGET_VARS:
                setattr(self.target_obj, name, value)
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
        if not self.correct:
            mark_str += ", ERROR"
        if self.ignored:
            mark_str += ", IGNORED"
        return mark_str

    def __str__(self):
        ret = "<LL>%.3f %s [%s %s %s] %s %s: <%s>%s" % (
              self.seconds,
              self.time,
              self.component,
              self.host,
              self.target,
              self.request,
              self.thread,
              self.keyword,
              self.__str_marks__())
        return ret

    def __str_target__(self):
        ret = "<LL>%.3f %s | %s %s: <%s>%s" % (
              self.seconds,
              self.time,
              self.request,
              self.thread,
              self.keyword,
              self.__str_marks__())
        return ret

    def __repr__(self):
        ret = str(self)
        ret += "\n  V:"
        for k, v in self._vars.iteritems():
            ret += "%s=%s," % (k, v)
        ret += "\n  L[%s]: %s" % (self.target_obj.filename, self.line)
        return ret


class Target(object):
    _repr_lines_lim = 10

    def __init__(self, f_name, f_dir, sr, plugin, vs):
        assert isinstance(f_name, str)
        assert isinstance(f_dir, str)
        assert isinstance(sr, ServiceRegistry)
        assert isinstance(plugin, DriverPlugin)

        self.filename = f_name
        self.dir_ = f_dir
        self.sr = sr
        self.plugin = plugin

        # required after line parsed
        self.component = None
        self.host = None
        self.target = None

        self.loglines = []
        self.loglines_by_thread = defaultdict(list)

        self.offset = 0
        self.total_lines = 0

        self.errors = {}
        self.warns = {}

        for k, v in vs.iteritems():
            if k not in rv.TARGET_VARS:
                raise LogError("(Target) key is not reserved: %r" % k)
            setattr(self, k, v)

    def __len__(self):
        return len(self.loglines)

    def __str__(self):
        target_str = ""
        if self.target:
            target_str += "#%s" % self.target

        return "<Target%s: fname=%s, comp=%s, host=%s, off=%d, %d from %d lines, %d threads>" % (
               target_str,
               self.filename,
               self.component,
               self.host,
               self.offset,
               len(self.loglines),
               self.total_lines,
               len(self.loglines_by_thread))

    def __repr__(self):
        ret = str(self)
        for line in self.loglines:
            ret += "\n| %s" % line.__str_target__()
        return ret

    def __setattr__(self, name, value):
        assert isinstance(name, str)

        if name in rv.ALL_VARS:
            # check leagal
            if name in rv.LINE_VARS:
                raise LogError("(Target) cannot set line var %s!" % name)

            # check types
            if value is not None:
                if name == rv.COMPONENT:
                    value = self.sr.f_to_component(value)
                    if not value:
                        raise LogError("(Target) unrecognized component: %r" %
                                value)
                elif not isinstance(value, str):
                    raise LogError("(Target) the log key '%s' is "
                                   "not str: %r" % (name, value))

            # cannot overwrite
            old_v = self.__dict__.get(name)
            if old_v is not None and old_v != value:
                raise LogError("(Target) cannot overwrite attribute %s: "
                               "old_v=%s, new_v=%s" % (name, old_v, value))
        self.__dict__[name] = value

    def _yield_lines(self):
        with open(self.dir_, 'r') as reader:
            for line in reader:
                assert isinstance(line, str)
                self.total_lines += 1
                yield line

    def _build_loglines(self, lines):
        for line in lines:
            assert isinstance(line, str)
            ret, vs = self.plugin.do_filter_logline(line)
            if not ret:
                continue
            try:
                lg = LogLine(line, self, vs)
            except LogError as e:
                raise LogError("(Target) error when parse line '%s'"
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
                    assert prv_line[0].seconds <= line.seconds
                yield line
                prv_line[0] = line

        for line in lines:
            assert isinstance(line, LogLine)
            buffer_lines.push(line)
            for line in _flush_line(1):
                yield line
        for line in _flush_line():
            yield line

    def read(self):
        lines = self._yield_lines()
        lines = self._build_loglines(lines)
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

    def _prepare_lines(self, lines):
        prv_line = None
        prv_line_by_thread = {}
        for line in lines:
            assert isinstance(line, LogLine)
            ret = self.plugin.do_preprocess_logline(line)
            if not ret:
                line.correct = False
                continue
            thread = line.thread
            if thread is None:
                raise LogError("(LogLine) require 'thread' when parse line: %r" % self)
            if prv_line is not None:
                prv_line.nxt_logline = line
                line.prv_logline = prv_line
            t_prv_line = prv_line_by_thread.get(thread)
            if t_prv_line is not None:
                t_prv_line.nxt_thread_logline = line
                line.prv_thread_logline = t_prv_line
            yield (line.thread, line)
            prv_line = line
            prv_line_by_thread[thread] = line

    def prepare(self):
        lines = self.loglines
        requests = set()
        self.loglines = []
        for thread, logline in self._prepare_lines(lines):
            self.loglines.append(logline)
            if logline.request is not None:
                requests.add(logline.request)
            self.loglines_by_thread[thread].append(logline)
        return requests
