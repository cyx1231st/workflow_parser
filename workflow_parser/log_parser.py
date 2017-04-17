from abc import ABCMeta
from abc import abstractmethod
from collections import defaultdict
from functools import total_ordering
from os import path

from workflow_parser.service_registry import ServiceRegistry
from workflow_parser import reserved_vars as rv
from workflow_parser.exception import WFException


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
            print("(LogDriver) WARN! error types: %d\n  %s" %
                    (len(self._errors), self._errors))
            print("(LogDriver) WARN! occur: %d" % self._occur)
        self._errors = set()
        self._occur = 0

    def _purge_dict(self, var_dict):
        for k in var_dict.keys():
            if not var_dict[k]:
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
                self._purge_dict(var_dict)
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
            self._purge_dict(var_dict)
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
    def __init__(self, line, logfile, plugin, vs):
        assert isinstance(line, str)
        assert isinstance(logfile, LogFile)
        assert isinstance(plugin, DriverPlugin)

        # required immediately:
        self.time = None
        self.seconds = None
        self.keyword = None
        # required after 1st pass:
        self.thread = None
        # required after 2nd pass:
        self.request = None

        self.logfile = logfile
        self.line = line.strip()
        self._vars = {}

        self.prv_logline = None
        self.nxt_logline = None

        self.prv_thread_logline = None
        self.nxt_thread_logline = None

        self.correct = True
        self.ignored = False

        self.pace = None

        for k, v in vs.iteritems():
            self[k] = v

        # required immediately
        if self.time is None:
            raise LogError("(LogLine) require 'time' when parse line: %s" % line)
        if self.seconds is None:
            raise LogError("(LogLine) require 'seconds' when parse line: %s" % line)
        if self.keyword is None:
            raise LogError("(LogLine) require 'keyword' when parse line: %s" % line)

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

        if item in rv.FILE_VARS:
            return getattr(self.logfile, item)
        else:
            return super(LogLine, self).__getattribute__(item)

    def __setattr__(self, name, value):
        assert isinstance(name, str)

        if name in rv.ALL_VARS:
            # redirect access to self.ll_file
            if name in rv.FILE_VARS:
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

    # total ordering
    __eq__ = lambda self, other: self.seconds == other.seconds
    __lt__ = lambda self, other: self.seconds < other.seconds

    @property
    def seconds(self):
        seconds = self.__dict__.get(rv.SECONDS)
        if seconds is None:
            return None
        else:
            return seconds + self.logfile.offset

    def get_keys(self, res=False):
        assert isinstance(res, bool)

        ret = set(self._vars.keys())
        if res:
            ret.update(rv.ALL_VARS)
        return ret

    def prepare(self, plugin):
        assert isinstance(plugin, DriverPlugin)

        ret = plugin.do_preprocess_logline(self)
        if not ret:
            self.correct = False
            if self.prv_logline:
                self.prv_logline.nxt_logline = self.nxt_logline
            if self.nxt_logline:
                self.nxt_logline.prv_logline = self.prv_logline
        else:
            # required after 1st pass
            if self.thread is None:
                raise LogError("(LogLine) require 'thread' when parse line: %r" %
                        self)

    def _str_marks(self):
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
              self._str_marks())
        return ret

    def __str1__(self):
        ret = "<LL>%.3f %s | %s %s: <%s>%s" % (
              self.seconds,
              self.time,
              self.request,
              self.thread,
              self.keyword,
              self._str_marks())
        return ret

    def __repr__(self):
        ret = str(self)
        ret += "\n  V:"
        for k, v in self._vars.iteritems():
            ret += "%s=%s," % (k, v)
        ret += "\n  L[%s]: %s" % (self.logfile.filename, self.line)
        return ret


# TODO: Rename to Target
class LogFile(object):
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

        for k, v in vs.iteritems():
            if k not in rv.FILE_VARS:
                raise LogError("(LogFile) key is not reserved: %r" % k)
            setattr(self, k, v)

    def __len__(self):
        return len(self.loglines)

    def __str__(self):
        target_str = ""
        if self.target:
            target_str += "#%s" % self.target

        return "<File%s: fname=%s, comp=%s, host=%s, off=%d, %d from %d lines, %d threads>" % (
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
            ret += "\n| %s" % line.__str1__()
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

    def read(self):
        with open(self.dir_, 'r') as reader:
            for line in reader:
                self.total_lines += 1
                ret, vs = self.plugin.do_filter_logline(line)
                if not ret:
                    continue
                try:
                    lg = LogLine(line, self, self.plugin, vs)
                except LogError as e:
                    raise LogError("(LogFile) error when parse line '%s'"
                            % line.strip(), e)
                self.loglines.append(lg)

        # required after line parsed
        if not self.loglines:
            self.errors["Empty lines after read file"] = self
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

        self.loglines.sort(key=lambda item: item.seconds)

        prv = None
        for f_obj in self.loglines:
            f_obj.prv_logline = prv
            if prv is not None:
                prv.nxt_logline = f_obj
            prv = f_obj

    def prepare(self):
        for line in self.loglines:
            line.prepare(self.plugin)
        self.loglines = [line for line in self.loglines if line.correct]

        requests = set()
        for line in self.loglines:
            self.loglines_by_thread[line.thread].append(line)
            if line.request:
                requests.add(line.request)
        for lines in self.loglines_by_thread.itervalues():
            prv = None
            for line in lines:
                line.prv_thread_logline = prv
                if prv:
                    prv.nxt_thread_logline = line
                prv = line
        return requests

    # def set_offset(self, lo, hi):
    #     # deprecated
    #     if lo is not None:
    #         if self.lo is None:
    #             self.lo = lo
    #         else:
    #             self.lo = max(self.lo, lo)
    #     if hi is not None:
    #         if self.hi is None:
    #             self.hi = hi
    #         else:
    #             self.hi = min(self.hi, hi)
    #     if self.lo is not None and self.hi is not None \
    #             and self.lo >= self.hi:
    #         return False
    #     else:
    #         return True

    # def correct(self, offset):
    #     for log in self.loglines:
    #         log.seconds -= offset

    # def correct_seconds(self):
    #     # deprecated
    #     if self.lo is None:
    #         return

    #     if self.hi is None:
    #         self.hi = self.lo + 0.02

    #     offset = (self.lo + self.hi) / 2
    #     for log in self.loglines:
    #         log.seconds += offset

    # def catg_logs(self, name_errors, mismatch_errors):
    #     for log in self.loglines:
    #         if log.correct and log.instance_name not in name_errors \
    #                 and log.instance_id not in mismatch_errors:
    #                 # and log.instance_name not in mismatch_errors \
    #             self.logs_by_ins[log.instance_name].append(log)
    #         else:
    #             self.errors.append(log)
