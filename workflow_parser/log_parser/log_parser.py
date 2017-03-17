from abc import ABCMeta
from abc import abstractmethod
import collections
import os
from os import path
import sys

from workflow_parser.log_parser.service_registry import ServiceRegistry
from workflow_parser.log_parser.exception import WFException


class Var(object):
    def __init__(self, name):
        assert isinstance(name, str)
        self._name = name

    def __str__(self):
        return self._name


class ReservedVars(object):
    def __init__(self, *r_vars):
        self._vars = {}
        for v in r_vars:
            assert isinstance(v, str)
            self._vars[v] = Var(v)

    def __getattr__(self, attr):
        ret = self._vars.get(attr)
        if ret:
            return ret
        else:
            raise AttributeError("'ReservedVars' doesn't have attr '%s'" % attr)

    def __getitem__(self, attr):
        if isinstance(attr, Var):
            if attr in self._vars.values():
                return attr
            else:
                raise WFException("Doesn't happen!")
        elif isinstance(attr, str):
            return self._vars.get(attr)
        else:
            return None

    def __iter__(self):
        for v in self._vars.itervalues():
            yield v


r_vars = ReservedVars(
# file level
    "component",
    "target",
    "host",
# line level
    "thread",
    "request",
    "keyword",
    "time",
    "seconds",
)


class LogError(WFException):
    pass


class DriverPlugin(object):
    __metaclass__ = ABCMeta

    def __init__(self, extensions=None):
        if extensions is None:
            self.extensions = ["log"]
        else:
            self.extensions = extensions

        self.errors = set()
        self.occur = 0

    def do_report(self):
        if not self.occur:
            print("(LogDriver) no error.")
        else:
            print("(LogDriver) errors: %d\n%s" % (len(self.errors), self.errors))
            print("(LogDriver) count: %d\n" % self.occur)
        self.errors = set()
        self.occur = 0

    def do_filter_logfile(self, f_dir, f_name):
        if not path.isfile(f_dir):
            return None

        ext_match = False
        for ext in self.extensions:
            if f_name.endswith("." + ext):
                ext_match = True
        if not ext_match:
            return None

        f_name = f_name.split(".")[0]

        try:
            if self.filter_logfile(f_dir, f_name):
                # NOTE
                # print("(LogDriver) loaded: %s" % f_dir)
                return f_name
            else:
                return None
        except Exception as e:
            raise LogError(
                "(LogDriver) `filter_logfile` error when f_name=%s"
                % f_name, e)

    def do_parse_logfilename(self, f_name):
        try:
            return self.parse_logfilename(f_name)
        except Exception as e:
            raise LogError(
                "(LogDriver) `parse_logfilename` error when f_name=%s"
                % f_name, e)

    def do_filter_logline(self, line):
        try:
            return self.filter_logline(line)
        except Exception as e:
            raise LogError(
                "(LogDriver) `filter_logline` error when line=%s"
                % line, e)

    def do_parse_logline(self, line):
        try:
            return self.parse_logline(line)
        except Exception as e:
            raise LogError(
                "(LogDriver) `parse_logline` error when line=%r"
                % line, e)

    def do_preprocess_logline(self, line_obj):
        try:
            ret = self.preprocess_logline(line_obj)
            if ret is not True:
                self.errors.add(ret)
                self.occur += 1
                return False
            return True
        except Exception as e:
            raise LogError(
                "(LogDriver) `preprocess_logline` error when line_obj=\n%r"
                % line_obj, e)

    @abstractmethod
    def filter_logfile(self, f_dir, f_name):
        pass

    @abstractmethod
    def parse_logfilename(self, f_name):
        pass

    @abstractmethod
    def filter_logline(self, line):
        pass

    @abstractmethod
    def parse_logline(self, line):
        pass

    @abstractmethod
    def preprocess_logline(self, line_obj):
        pass


class LogLine(object):
    _r_val_set = set([
        # required immediately
        r_vars.time,
        r_vars.seconds,
        r_vars.keyword,
        # required after 1st pass
        r_vars.thread,
        # required after 2nd pass
        r_vars.request,
    ])

    def __init__(self, line, log_file, plugin):

        for v in self._r_val_set:
            self.__dict__[str(v)] = None

        self.ll_file = log_file
        self.ll_line = line.strip()
        self.ll_vars = {}

        self.ll_prv = None
        self.ll_nxt = None

        self.ll_correct = True

        vs = plugin.do_parse_logline(line)
        for (k, v) in vs:
            self[k] = v

        # required immediately
        if not self.time:
            raise LogError("(LogLine) require 'time' when parse line: %s" % line)
        if not self.seconds:
            raise LogError("(LogLine) require 'seconds' when parse line: %s" % line)
        if not self.keyword:
            raise LogError("(LogLine) require 'keyword' when parse line: %s" % line)

    def __getitem__(self, key):
        r_key = r_vars[key]
        if r_key:
            return getattr(self, str(r_key))
        else:
            return self.ll_vars[key]

    def __setitem__(self, key, value):
        r_key = r_vars[key]
        if r_key:
            setattr(self, str(r_key), value)
        else:
            self.ll_vars[key] = value

    def __getattribute__(self, item):
        r_item = r_vars[item]
        if r_item and r_item not in self._r_val_set:
            return getattr(self.ll_file, str(r_item))
        else:
            return super(LogLine, self).__getattribute__(item)

    def __setattr__(self, name, value):
        r_name = r_vars[name]
        if r_name:
            name = str(r_name)
            # redirect access to self.ll_file
            if r_name not in self._r_val_set:
                setattr(self.ll_file, name, value)
                return

            # check types
            if r_name is r_vars.seconds:
                if not isinstance(value, float):
                    raise LogError("(LogLine) the value of key '%s' is "
                                   "not float: %r" % (name, value))
            elif not isinstance(value, str):
                raise LogError("(LogLine) the value key '%s' is "
                               "not string: %r" % (name, value))

            # cannot overwrite
            old_v = self.__dict__[name]
            if old_v is not None and old_v != value:
                raise LogError("(LogLine) cannot overwrite reserved key %s: "
                               "old_v=%s, new_v=%s" % (name, old_v, value))
        self.__dict__[name] = value

    def __contains__(self, item):
        item = str(item)
        return item in self.f_get_keys(True)

    @property
    def seconds(self):
        return self.__dict__[str(r_vars.seconds)] + self.ll_file.offset

    def f_get_keys(self, res=False):
        ret = set(self.ll_vars.keys())
        if res:
            for v in r_vars:
                ret.add(str(v))
        return ret

    def f_prepare(self, plugin):
        ret = plugin.do_preprocess_logline(self)
        if not ret:
            self.ll_correct = False
            if self.ll_prv:
                self.ll_prv.ll_nxt = self.ll_nxt
            if self.ll_nxt:
                self.ll_nxt.ll_prv = self.ll_prv
        else:
            # required after 1st pass
            if self.thread is None:
                raise LogError("(LogLine) require 'thread' when parse line: %r" %
                        self)

    def __str__(self):
        ret = "%.3f %s [%s %s %s] %s %s: <%s>" % (
              self.seconds,
              self.time,
              self.component,
              self.host,
              self.target,
              self.request,
              self.thread,
              self.keyword)
        for (k, v) in self.ll_vars.iteritems():
            ret += ", %s=%s" % (k, v)
        return ret

    def __str1__(self):
        ret = "%.3f %s | %s %s: <%s>" % (
              self.seconds,
              self.time,
              self.request,
              self.thread,
              self.keyword)
        for (k, v) in self.ll_vars.iteritems():
            ret += ", %s=%s" % (k, v)
        return ret

    def __repr__(self):
        ret = "[%s] %s" % (self.ll_file.name, self.ll_line)
        ret += "\n  -> %s" % self
        return ret

    # def assert_c(self, service, key_word):
    #     """ Deprecated """
    #     if self.service != service:
    #         return False
    #     if key_word not in self.action:
    #         return False
    #     return True


class LogFile(object):
    _r_val_set = set([
        # required after line parsed
        r_vars.component,
        r_vars.host,
        r_vars.target
    ])

    def __init__(self, f_name, f_dir, sr, plugin):
        assert isinstance(f_name, str)
        assert isinstance(f_dir, str)
        assert isinstance(sr, ServiceRegistry)
        assert isinstance(plugin, DriverPlugin)

        for v in self._r_val_set:
            self.__dict__[str(v)] = None

        self.name = f_name
        self.f_dir = f_dir
        self.sr = sr
        self.plugin = plugin

        self.loglines = []
        self.loglines_by_thread = collections.defaultdict(list)

        self.offset = 0
        self.total_lines = 0

        vs = plugin.do_parse_logfilename(f_name)
        for (k, v) in vs:
            r_k = r_vars[k]
            if not r_k:
                raise LogError("(LogFile) the key is not reserved: %r" % k)
            setattr(self, str(r_k), v)
        # self.logs_by_ins = collections.defaultdict(list)
        # self.lo = None
        # self.hi = None

    def __len__(self):
        return len(self.loglines)

    def __str__(self):
        ret = "<File#%s: comp=%s, host=%s, target=%s, off=%d, lines=%d>" % (
                self.name,
                self.component,
                self.host,
                self.target,
                self.offset,
                len(self.loglines))
        return ret

    def __repr__(self):
        ret = str(self)
        for line in self.loglines:
            ret += "\n| %s" % line.__str1__()
        return ret

    def __setattr__(self, name, value):
        r_name = r_vars[name]
        if r_name:
            # check leagal
            if r_name not in self._r_val_set:
                raise LogError("(LogFile) reserved key is not leagal:"
                               "%r" % r_name)
            # check types
            if r_name is r_vars.component:
                value = self.sr.f_to_component(value)
                if not value:
                    raise LogError("(LogFile) unrecognized component: %r" % value)
            elif not isinstance(value, str):
                raise LogError("(LogFile) the value key '%s' is "
                               "not string: %r" % (name, value))
            # cannot overwrite
            name = str(r_name)
            old_v = self.__dict__[name]
            if old_v is not None and old_v != value:
                raise LogError("(LogFile) cannot overwrite reserved key %s: "
                               "old_v=%s, new_v=%s" % (name, old_v, value))
        self.__dict__[name] = value

    def read(self):
        with open(self.f_dir, 'r') as reader:
            prv = None
            for line in reader:
                self.total_lines += 1
                if not self.plugin.do_filter_logline(line):
                    continue
                lg = LogLine(line, self, self.plugin)
                self.loglines.append(lg)

        # required after line parsed
        if not self.component:
            raise LogError("(LogFile) require 'component' when parse file: %s" % line)
        if not self.host:
            raise LogError("(LogFile) require 'host' when parse file: %s" % line)
        if not self.target:
            raise LogError("(LogFile) require 'target' when parse file: %s" % self.name)

        self.loglines.sort(key=lambda item: item.seconds)

        prv = None
        for f_obj in self.loglines:
            f_obj.ll_prv = prv
            if prv is not None:
                prv.ll_nxt = f_obj
            prv = f_obj

    def f_prepare(self):
        for line in self.loglines:
            line.f_prepare(self.plugin)

        self.loglines = [line for line in self.loglines if line.ll_correct]

        requests = set()
        for line in self.loglines:
            self.loglines_by_thread[line.thread].append(line)
            if line.request:
                requests.add(line.request)

        # NOTE: for debug
        # print("%s:  %s" % (self.name, self.loglines_by_thread.keys()))

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


class LogCollector(object):
    def __init__(self, sr, plugin):
        assert isinstance(sr, ServiceRegistry)
        assert isinstance(plugin, DriverPlugin)
        self.sr = sr
        self.plugin = plugin

        self.logfiles = []
        self.logfiles_by_target = {}
        self.logfiles_by_component = collections.defaultdict(list)
        self.logfiles_by_host = collections.defaultdict(list)

        self.relation = {}
        self.service_host_dict = collections.defaultdict(dict)

    def load_files(self, log_folder):
        assert isinstance(log_folder, str)

        # current_path = path.dirname(os.path.realpath(__file__))
        current_path = os.getcwd()
        log_folder = path.join(current_path, log_folder)

        for f_name in os.listdir(log_folder):
            f_dir = path.join(log_folder, f_name)
            f_name = self.plugin.do_filter_logfile(f_dir, f_name)
            if not f_name:
                continue
            f_obj = LogFile(f_name, f_dir, self.sr, self.plugin)
            self.logfiles.append(f_obj)

    def read_files(self):
        for f_obj in self.logfiles:
            f_obj.read()

        self.logfiles = [f_obj for f_obj in self.logfiles if len(f_obj) > 0]
        print("(LogCollector) detected %d files" % len(self.logfiles))

        len_lines = 0
        len_t_lines = 0
        for f_obj in self.logfiles:
            target = f_obj.target
            if target in self.logfiles_by_target:
                raise LogError("(LogCollector) target '%s' duplication in "
                               "files: %s, %s" % (f_obj.name,
                               self.logfiles_by_target[target].name))
            self.logfiles_by_target[target] = f_obj
            len_lines += len(f_obj)
            len_t_lines += f_obj.total_lines
        print("(LogCollector) detected %d(%d) lines" % (len_lines, len_t_lines))

        for f_obj in self.logfiles:
            component = f_obj.component
            self.logfiles_by_component[component].append(f_obj)
        for comp in self.sr.sr_components:
            t_len = len(self.logfiles_by_component[comp])
            if t_len == 0:
                raise LogError("(LogCollector) missing component %s" % comp)
            else:
                hosts = set()
                for f_obj in self.logfiles_by_component[comp]:
                    hosts.add(f_obj.host)
                print("(LogCollector) %s has %d files, with %d hosts"
                      % (comp, t_len, len(hosts)))

        for f_obj in self.logfiles:
            self.logfiles_by_host[f_obj.host].append(f_obj)
        print("(LogCollector) detected %d hosts" % len(self.logfiles_by_host))

    def build_threads(self):
        requests = set()
        for f_obj in self.logfiles:
            ret = f_obj.f_prepare()
            requests.update(ret)
        self.plugin.do_report()
        print("(LogCollector) detected %d requests" % len(requests))

        for (comp, f_objs) in self.logfiles_by_component.iteritems():
            cnt = 0
            sum_ = 0
            min_ = sys.maxint
            max_ = 0
            for f_obj in f_objs:
                lent = len(f_obj.loglines_by_thread)
                cnt += 1
                sum_ += lent
                min_ = min(min_, lent)
                max_ = max(max_, lent)
            print("(LogCollector) %s has %.3f[%d, %d] threads"
                  % (comp, sum_/float(cnt), min_, max_))
