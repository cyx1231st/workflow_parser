from abc import ABCMeta
from abc import abstractmethod

from workflow_parser.log_parser.log_parser import DriverPlugin
from workflow_parser.log_parser.log_parser import r_vars
from workflow_parser.log_parser.parse import main1
from workflow_parser.log_parser.service_registry import ServiceRegistry
from workflow_parser.log_parser.state_graph import MasterGraph


r_vars = r_vars


class _DriverPlugin(DriverPlugin):
    def __init__(self, driver):
        self.driver = driver
        super(_DriverPlugin, self).__init__()

    def filter_logfile(self, f_dir, f_name):
        return self.driver.filter_logfile(f_dir, f_name)

    def parse_logfilename(self, f_name):
        return self.driver.parse_logfilename(f_name)

    def filter_logline(self, line):
        return self.driver.filter_logline(line)

    def parse_logline(self, line):
        return self.driver.parse_logline(line)

    def preprocess_logline(self, line_obj):
        return self.driver.preprocess_logline(line_obj)


class DriverBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, name=None):
        if name is None:
            self.name = self.__class__.__name__
        else:
            self.name = name
        self.services = ServiceRegistry()
        self.graph = MasterGraph(self.name)
        self.log_parser_plugin = _DriverPlugin(self)

        self.register_service(self.services)
        self.build_graph(self.services, self.graph)

    def cmdrun(self):
        main1(self)

    @abstractmethod
    def register_service(self, sr):
        pass

    @abstractmethod
    def build_graph(self, services, graph):
        pass

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

    # optional override
    def preprocess_logline(self, line_obj):
        pass

    @abstractmethod
    def build_statistics(self, s_engine, report):
        pass
