from __future__ import print_function

from abc import ABCMeta
from abc import abstractmethod
from os import path

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
        #TODO remove
        from workflow_parser.state_machine import LogLine
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
