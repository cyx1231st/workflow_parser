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

from collections import defaultdict

from ..workflow.entities.request import RequestInstance
from ..workflow.entities.join import JoinActivityBase
from .relation_engine import CausalEngine


def adjust_clock(requestinss):
    remote_relations = set()
    targetobjs_by_host = defaultdict(set)
    for requestins in requestinss.itervalues():
        assert isinstance(requestins, RequestInstance)
        for join in requestins.iter_joins():
            if join.remote_type == "remote":
                remote_relations.add(join)
        for target_obj in requestins.target_objs:
            targetobjs_by_host[target_obj.host].add(target_obj)

    if not remote_relations:
        print("No remote relations detected, skip relation engine.\n")
        return

    print("Preparing constraints...")
    violated_joinints = set()
    causal_engine = CausalEngine()
    for relation in remote_relations:
        assert isinstance(relation, JoinActivityBase)
        causal_engine.register(relation.from_host,
                               relation.to_host,
                               relation.from_seconds,
                               relation.to_seconds)
        if relation.is_violated:
            violated_joinints.add(relation)

    print("------------------------")

    #### summary ####
    print("total %d host constraints" % len(causal_engine.hosts))
    print("total %d relation constraints" % len(causal_engine.relationcons))
    print("distance %f" % causal_engine.distance)
    print("%d violated joinints" %
            len(violated_joinints))
    print("%d violated relation constraints:" %
            len(causal_engine.violated_relationcons))
    for relationcon in causal_engine.violated_relationcons:
        print("  %s" % relationcon)
    print()
    #################

    print("Correcting...")
    if causal_engine.relax():
        print("-------------")

        #### summary ####
        print("%d relax attempts" % causal_engine.relax_counter)
        hosts = causal_engine.hosts.values()
        hosts.sort(key=lambda hostc: hostc.hostname)
        for hostc in hosts:
            if hostc.low != 0:
                print("adjust %r" % hostc)
                for target in targetobjs_by_host[hostc.hostname]:
                    target.offset = hostc.low
        for relation in remote_relations:
            assert not relation.is_violated
        #################
    else:
        print("No need to correct clocks")
    print()
