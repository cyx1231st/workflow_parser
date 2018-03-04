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

import sys

from . import reserved_vars as rv
from .datasource.log_engine import DriverPlugin
from .graph import Master
from .service_registry import ServiceRegistry


class Driver(DriverPlugin):
    def __init__(self, services, graph, **kwgs):
        assert isinstance(services, ServiceRegistry)
        assert isinstance(graph, Master)

        self.name = graph.name
        self.services = services
        self.graph = graph

        # f_filter_logfile(self, f_dir, f_name):
        # f_filter_logline(self, line):
        # extentions = ["log"]
        super(Driver, self).__init__(**kwgs)


def init(name):
    return ServiceRegistry(), Master(name), rv


def register_driver(module_name,
        services, graph,
        f_filter_logfile,
        f_filter_logline,
        extensions=None):

    if not extensions:
        extensions = ["log"]

    driver = Driver(
            services=services,
            graph=graph,
            f_filter_logfile=f_filter_logfile,
            f_filter_logline=f_filter_logline,
            extensions=extensions)

    if module_name == "__main__":
        from .loader import execute
        execute(driver)
    else:
        module = sys.modules[module_name]
        setattr(module, graph.name, driver)
        module.__all__ = [graph.name]


__all__ = ["init", "register_driver"]
