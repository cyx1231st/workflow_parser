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
from itertools import chain

from ...utils import Report
from ..entities.join import InnerjoinActivity
from ..entities.join import RequestjoinActivity
from ..entities.bases import ActivityBase
from .schema import JoinInfo
from ..entities.request import RequestInstance
from ..entities.threadins import ThreadInstance
from ..entities.threadins import ThreadActivity
from ..entities.join import InnerjoinActivity
from ..entities.join import CrossjoinActivity
from ..entities.join import EmptyjoinActivity
from ..exc import StateError


# step 3: group threads by request
def group_threads(threadinss, joininfo, report):
    assert isinstance(joininfo, JoinInfo)
    assert isinstance(report, Report)

    print("Group threads...")
    seen_threadinss = set()
    def group(threadins, threadins_group, requests):
        assert isinstance(threadins, ThreadInstance)

        if threadins in seen_threadinss:
            return
        seen_threadinss.add(threadins)
        threadins_group.add(threadins)
        if threadins.request:
            requests.add(threadins.request)

        for joins in joininfo.iter_innerjoins(threadins):
            if isinstance(joins, RequestjoinActivity):
                to_threadins = joins.to_pace_unnested.threadins
            else:
                to_threadins = joins.to_threadins
            assert to_threadins is not threadins
            group(to_threadins, threadins_group, requests)
        for joined in joininfo.iter_innerjoined(threadins):
            if isinstance(joined, RequestjoinActivity):
                from_threadins = joined.from_pace_unnested.threadins
            else:
                from_threadins = joined.from_threadins
            assert from_threadins is not threadins
            group(from_threadins, threadins_group, requests)

    threadgroup_by_request = defaultdict(set)
    threadgroups_with_multiple_requests = []
    threadgroups_without_request = []
    for threadins in threadinss:
        assert isinstance(threadins, ThreadInstance)
        if threadins not in seen_threadinss:
            threadins_group = set()
            requests = set()
            group(threadins, threadins_group, requests)
            len_req = len(requests)
            if len_req > 1:
                threadgroups_with_multiple_requests.append((threadins_group, requests))
            elif len_req == 1:
                threadgroup_by_request[requests.pop()].update(threadins_group)
            else:
                threadgroups_without_request.append(threadins_group)
    print("----------------")

    components = set()
    hosts = set()
    targets = set()
    threads = set()
    collected_threadinss = set()
    sum_ = defaultdict(lambda: 0)

    def collect_group(tgroup):
        for tiss in tgroup:
            for tis in tiss:
                assert isinstance(tis, ThreadInstance)
                components.add(tis.component)
                hosts.add(tis.host)
                targets.add(tis.target_obj)
                threads.add(tis.thread_obj)
                collected_threadinss.add(tis)
                sum_['lines'] += tis.len_activities-1
                for join in joininfo.iter_innerjoins(tis):
                    if isinstance(join, RequestjoinActivity):
                        sum_['rjoins'] += 1
                        if join.left_crossjoin:
                            sum_['cjoin'] += 1
                        if join.right_crossjoin:
                            sum_['cjoin'] += 1
                    else:
                        sum_['joins'] += 1
                for join in joininfo.iter_innerjoined(tis):
                    if isinstance(join, RequestjoinActivity):
                        sum_['rjoined'] += 1
                    else:
                        sum_['joined'] += 1

    collect_group(threadgroup_by_request.itervalues())
    collect_group(threadgroups_without_request)

    #### summary ####
    print("%d request groups" % (len(threadgroup_by_request)))
    print("%d thread instances" % len(collected_threadinss))
    print()

    #### report #####
    report.step("group_t",
                line=sum_['lines'],
                component=len(components),
                host=len(hosts),
                target=len(targets),
                thread=len(threads),
                request=len(threadgroup_by_request)+len(threadgroups_without_request),
                threadins=len(collected_threadinss),
                innerjoin=sum_['joins'],
                innerjoined=sum_['joined'],
                interfacejoin=sum_['rjoins'],
                interfacejoined=sum_['rjoined'],
                leftinterface=sum_['cjoin'],
                rightinterface=sum_['cjoin'])

    #### errors #####
    if len(collected_threadinss) != len(threadinss):
        print("! WARN !")
        print("%d thread instances, but previously built %d"
                % (len(collected_threadinss),
                   len(threadinss)))
        print()

    if threadgroups_without_request:
        print("! WARN !")
        print("%d groups of %d threadinstances cannot be identified "
              "with request" % (
                len(threadgroups_without_request),
                sum(len(tgroup) for tgroup in
                    threadgroups_without_request)))
        print()

    if threadgroups_with_multiple_requests:
        print("!! ERROR !!")
        print("%d thread groups have multiple requests" % (
                len(threadgroups_with_multiple_requests)))
        for i_group, reqs in threadgroups_with_multiple_requests:
            # TODO: can show whether there are multi request_types
            # from workflow_parser.draw_engine import DrawEngine
            # draw_engine.draw_debug_groups(reqs, i_group)
            # join_ints = set()
            # for ti in i_group:
            #     join_ints.update(ti.joinsints_by_type[InnerjoinInterval])
            #     join_ints.update(ti.joinsints_by_type[InterfaceInterval])
            #     join_ints.update(ti.joinedints_by_type[InnerjoinInterval])
            #     join_ints.update(ti.joinedints_by_type[InterfaceInterval])
            # join_ints_by_obj = defaultdict(list)
            # for join_int in join_ints:
            #     join_ints_by_obj[join_int.join_obj].append(join_int)
            # print("#### debug group ####")
            # for join_obj, join_ints in join_ints_by_obj.iteritems():
            #     print("%s:" % join_obj.name)
            #     schemas = join_obj.schemas
            #     for join_int in join_ints:
            #         j_str = ""
            #         for schema in schemas:
            #             j_str += "%s: %s, " % (schema, join_int.to_pace[schema[1]])
            #         print("  %s" % j_str)
            # print("#####################\n")
            print("  %d thread instances contains %d requests" %
                    (len(i_group), len(reqs)))
        print()
        raise StateError("(ParserEngine) thread group has multiple requests!")

    ret = []
    for req, tgroup in threadgroup_by_request.iteritems():
        ret.append((req, tgroup))
    for tgroup in threadgroups_without_request:
        ret.append((None, tgroup))
    print("OUT: %d group with req, %d group without req" % (
        len(threadgroup_by_request), len(threadgroups_without_request)))
    print()

    return ret


def _build_mainpath(reqins):
    assert isinstance(reqins, RequestInstance)
    activity = reqins.end_activity
    assert activity

    pace = None
    while True:
        assert isinstance(activity, ActivityBase)
        if isinstance(activity, EmptyjoinActivity):
            assert pace
            return ("Pace backward empty, empty join", pace.edgename)
        if activity.is_main:
            return ("Revisited", activity.activity_name)
        activity.is_main = True
        if pace:
            pace.prv_main_activity = activity

        pace = activity.from_pace
        reqins.len_main_paces += 1
        if pace is None:
            if activity is not reqins.start_activity:
                return ("Activity backward empty", activity.activity_name)
            else:
                break
        else:
            if pace.is_main:
                return ("Revisited", pace.edgename)
            pace.is_main = True
            pace.nxt_main_activity = activity

            prv_acts = pace.get_prv(InnerjoinActivity._act_type)
            if len(prv_acts) > 1:
                return ("Multiple backward joinacts", pace.edgename)
            elif len(prv_acts) == 1:
                activity = prv_acts[0]
            else:
                prv_acts = pace.get_prv(ThreadActivity._act_type)
                if len(prv_acts) > 1:
                    return ("Multiple backward threadacts", pace.edgename)
                elif len(prv_acts) == 1:
                    activity = prv_acts[0]
                else:
                    return ("Pace backward empty, no choices", pace.edgename)
    return None


class RequestBuilder(object):
    def __init__(self, request, joininfo, threadinss):
        assert isinstance(joininfo, JoinInfo)
        self.threadinss = threadinss
        self.requestins = RequestInstance(request, self)
        self.joininfo = joininfo

        self.is_built = False
        # error report
        self.errors = {}
        self.warns = {}
        # collects
        self.e_incomplete_threadinss = set()
        self.e_extra_s_threadinss = set()
        self.e_extra_e_threadinss = set()
        self.e_stray_threadinss = set()

    def __nonzero__(self):
        if self.errors or not self.is_built:
            return False
        else:
            return True

    def build(self):
        assert not self.is_built
        requestins = self.requestins
        joininfo = self.joininfo

        # init
        tis = set()
        for threadins in self.threadinss:
            assert isinstance(threadins, ThreadInstance)
            tis.add(threadins)
            if threadins.request:
                assert threadins.request == requestins.request
            threadins.requestins = requestins
            # start_activity
            if threadins.is_request_start:
                if requestins.start_activity is not None:
                    self.e_extra_s_threadinss.add(threadins)
                else:
                    requestins.start_activity = threadins.start_activity

        # error: multiple start threads
        if self.e_extra_s_threadinss:
            self.e_extra_s_threadinss.add(requestins.start_activity.threadins)
            error_str = "\n".join("%r" % ti for ti in self.e_extra_s_threadinss)
            self.errors["Has multiple start_activities"] = error_str
            return None

        # error: no start thread
        if requestins.start_activity is None:
            self.errors["Contains no start_activity"] = ""
            return None

        seen_tis = set()
        def _process(threadins):
            assert isinstance(threadins, ThreadInstance)
            if threadins in seen_tis:
                return
            seen_tis.add(threadins)

            # threadinss
            requestins.threadinss.append(threadins)

            # incomplete_threadinss
            if not threadins.is_complete:
                self.e_incomplete_threadinss.add(threadins)

            # end_activity
            if threadins.is_request_end:
                r_state = threadins.request_state
                assert r_state is not None
                if requestins.end_activity:
                    self.e_extra_e_threadinss.add(threadins)
                else:
                    requestins.end_activity = threadins.rend_activity

            # last_activity
            if threadins.end_activity.from_seconds is not None:
                if requestins.last_activity is None\
                        or requestins.last_activity.from_seconds < \
                        threadins.end_activity.from_seconds:
                    requestins.last_activity = threadins.end_activity

            # activities_bymark
            for mark, acts in threadins.activities_bymark.iteritems():
                requestins.activities_bymark[mark].extend(acts)

            # request vars
            requestins.request_vars["thread"].add(threadins.target+":"+threadins.thread)
            requestins.request_vars["host"].add(threadins.host)
            requestins.request_vars["component"].add(threadins.component)
            requestins.request_vars["target"].add(threadins.target)
            for key, val in threadins.thread_vars.iteritems():
                requestins.request_vars[key].add(val)
            for key, vals in threadins.thread_vars_dup.iteritems():
                requestins.request_vars[key].update(vals)

            # len_paces
            requestins.len_paces += threadins.len_activities-1

            # thread/target
            requestins.thread_objs.add(threadins.thread_obj)
            requestins.target_objs.add(threadins.target_obj)

            # join_activities
            for join in joininfo.iter_emptyjoin(threadins, is_joins=True):
                requestins.emptyjoins_activities.add(join)
            for join in joininfo.iter_emptyjoin(threadins, is_joins=False):
                requestins.emptyjoined_activities.add(join)
            for join in joininfo.iter_innerjoins(threadins, is_request=False):
                requestins.innerjoin_activities.add(join)
                _process(join.to_threadins)
            for join in joininfo.iter_innerjoins(threadins, is_request=True):
                requestins.requestjoin_activities.add(join)
                if join.is_nest:
                    requestins.innerjoin_activities.add(join.left_crossjoin)
                    requestins.innerjoin_activities.add(join.right_crossjoin)
                _process(join.to_pace_unnested.threadins)
            for join in joininfo.iter_crossjoin(threadins, is_left=True):
                requestins.crossjoinl_activities.add(join)
            for join in joininfo.iter_crossjoin(threadins, is_left=False):
                requestins.crossjoinr_activities.add(join)

        _process(requestins.start_activity.threadins)

        requestins.threadinss.sort(key=lambda ti: ti.start_activity.to_seconds)

        # error: incomplete threadinss
        if self.e_incomplete_threadinss:
            err_str = "\n".join("%r" % it for it in self.e_incomplete_threadinss)
            self.errors["Has incomplete threadinss"] = err_str

        # warn: stray threadinss
        self.e_stray_threadinss = tis - seen_tis
        if self.e_stray_threadinss:
            err_str = "\n".join("%r" % ti for ti in self.e_stray_threadinss)
            self.warns["Has stray threadinss"] = err_str

        # error: multiple end threads
        if self.e_extra_e_threadinss:
            self.e_extra_e_threadinss.add(requestins.end_activity.threadins)
            err_str = "\n".join("%r" % ti for ti in self.e_extra_e_threadinss)
            self.errors["Has multiple end activities"] = err_str

        # error: no end thread
        if not requestins.end_activity:
            self.errors["Contains no end activity"] = ""
        else:
            # parse main path!
            err = _build_mainpath(requestins)
            if err:
                self.errors["Main route parse error"] = err

        self.is_built = True
        if not self.errors:
            requestins.automate_name()
            return requestins


def build_requests(threadgroup_by_request, joininfo, report):
    requestinss = {}
    # error report
    error_builders = []
    builders_by_error = defaultdict(list)
    main_route_error = defaultdict(lambda: defaultdict(lambda: 0))
    # warn report
    warn_builders= []
    builders_by_warn = defaultdict(list)
    # others
    incomplete_threadinss = set()
    extra_start_threadinss = set()
    extra_end_threadinss = set()
    stray_threadinss = set()

    print("Build requests...")
    for request, threads in threadgroup_by_request:
        r_builder = RequestBuilder(request, joininfo, threads)
        requestins = r_builder.build()

        if requestins:
            if r_builder.warns:
                warn_builders.append(r_builder)
                for warn in r_builder.warns.keys():
                    builders_by_warn[warn].append(r_builder)
            assert requestins.request not in requestinss
            requestinss[requestins.request] = requestins
        else:
            assert r_builder.errors
            error_builders.append(r_builder)
            for error in r_builder.errors.keys():
                builders_by_error[error].append(r_builder)
                if error == "Main route parse error":
                    err = r_builder.errors[error]
                    main_route_error[err[0]][err[1]] += 1

        if r_builder.e_incomplete_threadinss:
            incomplete_threadinss.update(r_builder.e_incomplete_threadinss)
        if r_builder.e_extra_s_threadinss:
            extra_start_threadinss.update(r_builder.e_extra_s_threadinss)
        if r_builder.e_extra_e_threadinss:
            extra_end_threadinss.update(r_builder.e_extra_e_threadinss)
        if r_builder.e_stray_threadinss:
            stray_threadinss.update(r_builder.e_stray_threadinss)
    print("-----------------")

    #### collect ####
    cnt_lines = 0
    components = set()
    hosts = set()
    target_objs = set()
    thread_objs = set()
    threadinss = set()
    requests_vars = defaultdict(set)

    innerjoins = set()
    requestjoins = set()
    lcrossjoins = set()
    rcrossjoins = set()
    join_byremotetype = defaultdict(set)

    for requestins in requestinss.itervalues():
        assert isinstance(requestins, RequestInstance)
        cnt_lines += requestins.len_paces
        components.update(requestins.components)
        hosts.update(requestins.hosts)
        target_objs.update(requestins.target_objs)
        thread_objs.update(requestins.thread_objs)
        threadinss.update(requestins.threadinss)
        requests_vars["request"].add(requestins.request)
        for k, vs in requestins.request_vars.iteritems():
            requests_vars[k].update(vs)

        innerjoins.update(requestins.innerjoin_activities)
        requestjoins.update(requestins.requestjoin_activities)
        lcrossjoins.update(requestins.crossjoinl_activities)
        rcrossjoins.update(requestins.crossjoinr_activities)

    for join in chain(innerjoins, requestjoins, lcrossjoins, rcrossjoins):
        join_byremotetype[join.remote_type].add(join)

    #### summary ####
    print("%d valid request instances with %d thread instances"
            % (len(requestinss),
               len(threadinss)))
    print("%d relations:" % (len(innerjoins)+len(requestjoins)))
    for j_type, join in join_byremotetype.iteritems():
        print("  %d %s relations" % (len(join), j_type))

    print("%d vars:" % len(requests_vars))
    for k, vs in requests_vars.iteritems():
        print("  %s: %d" % (k, len(vs)))
    print()

    #### report #####
    report.step("build_r",
                line=cnt_lines,
                component=len(components),
                host=len(hosts),
                target=len(target_objs),
                thread=len(thread_objs),
                request=len(requestinss),
                threadins=len(threadinss),
                innerjoin=len(innerjoins),
                innerjoined=len(innerjoins),
                interfacejoin=len(requestjoins),
                interfacejoined=len(requestjoins),
                leftinterface=len(lcrossjoins)+len(rcrossjoins),
                rightinterface=len(rcrossjoins)+len(lcrossjoins))

    #### errors #####
    if len(requestinss) != len(threadgroup_by_request):
        print("! WARN !")
        print("%d request instances from %d request groups"
                % (len(requestinss),
                   len(threadgroup_by_request)))
        print()

    if error_builders:
        print("!! ERROR !!")
        print("%d error request instances" % len(error_builders))
        for err, builders in builders_by_error.iteritems():
            print("  %s: %d requests" % (err, len(builders)))
        print()

    if main_route_error:
        print("!! ERROR !!")
        print("Tracing errors:")
        for e_msg, where_count in main_route_error.iteritems():
            print("  %s:" % e_msg)
            for where, cnt in where_count.iteritems():
                print("    %s: %d" % (where, cnt))
        print()

    if warn_builders:
        print("! WARN !")
        print("%d warn request instances:" %
                len(warn_builders))
        for warn, builders in builders_by_warn.iteritems():
            print("  %s: %d requests" % (warn, len(builders)))
        print()

    if incomplete_threadinss:
        print("! WARN !")
        print("%d incomplete threadinss in requests" %
                len(incomplete_threadinss))
        print()

    if extra_start_threadinss:
        print("!! ERROR !!")
        print("%d extra start threadinss in requests" %
                len(extra_start_threadinss))
        print()

    if extra_end_threadinss:
        print("!! ERROR !!")
        print("%d extra end threadinss in requests" %
                len(extra_end_threadinss))
        print()

    if stray_threadinss:
        print("!! ERROR !!")
        print("%d stray threadinss in requests" %
                len(stray_threadinss))
        print()

    return requestinss
