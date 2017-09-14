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

from .schema import SchemaEngine
from .threadins import build_thread_instances
from .request import build_requests
from .request import group_threads


__all__ = ["proceed"]


def proceed(target_objs, mastergraph, report):
    schema_engine = SchemaEngine(mastergraph)
    threadinss = build_thread_instances(target_objs,
                                        mastergraph,
                                        schema_engine,
                                        report)
    joininfo = schema_engine.proceed(report)
    threadgroup_by_request = group_threads(threadinss, joininfo, report)
    requestinss = build_requests(threadgroup_by_request, joininfo, report)
    return requestinss
