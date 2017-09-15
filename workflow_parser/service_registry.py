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

class Component(object):
    def __init__(self, component, service):
        assert isinstance(component, str)

        self.name = component
        self.service = service
        self.color = None
        self.vis_weight = 0

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<Component: %s>" % self.name


class Service(object):
    def __init__(self, service, components):
        self._name = service
        self._components = {}
        for component in components:
            self._components[component] = Component(component, service)

    def __str__(self):
        return self._name

    def __repr__(self):
        return "<Service: %s>" % self._name

    def __getattr__(self, attr):
        return self._components[attr]

    def __iter__(self):
        for component in self._components.itervalues():
            yield component


class ServiceRegistry(object):
    def __init__(self):
        self._services = {}

    @property
    def sr_services(self):
        return set(self._services.values())

    @property
    def sr_components(self):
        ret = set()
        for service in self.sr_services:
            for component in service:
                ret.add(component)
        return ret

    def __getattr__(self, attr):
        return self._services[attr]

    def f_to_component(self, comp):
        if isinstance(comp, Component):
            assert comp in self.sr_components
            return comp
        elif isinstance(comp, str):
            for service in self._services.itervalues():
                ret = service._components.get(comp)
                if ret:
                    return ret
        return None

    def f_register(self, service, *components):
        assert isinstance(service, str)

        self._services[service] = Service(service, components)
