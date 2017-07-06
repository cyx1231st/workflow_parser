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

from abc import ABCMeta
from abc import abstractmethod

from . import reserved_vars as rv
from .datasource.log_entities import DriverPlugin
from .graph import Master
from .parse import main1
from .service_registry import ServiceRegistry


class DriverBase(DriverPlugin):
    __metaclass__ = ABCMeta

    def __init__(self, services, name=None):
        assert isinstance(services, ServiceRegistry)

        if name is None:
            self.name = self.__class__.__name__
        else:
            assert isinstance(name, str)
            self.name = name
        self.services = services
        self.graph = Master(self.name)

        self.build_graph(self.graph)
        super(DriverBase, self).__init__()

    def cmdrun(self):
        main1(self)

    @abstractmethod
    def build_graph(self, graph):
        pass

    # def filter_logfile(self, f_dir, f_name):
    # def parse_logfilename(self, f_name, var_dict):
    # def filter_logline(self, line):
    # def parse_logline(self, line, var_dict):
    # def preprocess_logline(self, logline):

    # @abstractmethod
    # def build_statistics(self, s_engine, report):
    #     pass
