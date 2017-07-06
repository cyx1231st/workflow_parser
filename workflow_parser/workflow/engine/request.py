from __future__ import print_function

from collections import defaultdict
from itertools import chain

from ...utils import Report
from ..entities.join import EmptyJoin
from ..entities.join import InnerjoinInterval
from ..entities.join import InterfaceInterval
from ..entities.join import InterfacejoinInterval
from ..entities.join import JoinIntervalBase
from ..entities.join import NestedrequestInterval
from ..entities.request import RequestInstance
from ..entities.request import RequestInterval
from ..entities.threadins import ThreadInstance
from ..entities.threadins import ThreadInterval
from ..exc import StateError


# step 3: group threads by request
def group_threads(threadinss, report):
    assert isinstance(report, Report)

    print("Group threads...")
    seen_threadinss = set()
    def group(threadins, t_set):
        assert isinstance(threadins, ThreadInstance)

        if threadins in seen_threadinss:
            return set()
        seen_threadinss.add(threadins)
        t_set.add(threadins)
        ret = set()
        if threadins.request:
            ret.add(threadins.request)

        assert {InnerjoinInterval, InterfaceInterval, EmptyJoin} ==\
               {InnerjoinInterval, InterfaceInterval, EmptyJoin} |\
               threadins.joinedints_by_type.viewkeys()
        assert {InnerjoinInterval, InterfaceInterval, EmptyJoin} ==\
               {InnerjoinInterval, InterfaceInterval, EmptyJoin} |\
               threadins.joinsints_by_type.viewkeys()
        assert {InterfacejoinInterval, EmptyJoin} == \
               {InterfacejoinInterval, EmptyJoin} |\
               threadins.joinedinterfaceints_by_type.viewkeys()
        assert {InterfacejoinInterval, EmptyJoin} == \
               {InterfacejoinInterval, EmptyJoin} |\
               threadins.joinsinterfaceints_by_type.viewkeys()

        for joins_int in chain(
                threadins.joinsints_by_type[InnerjoinInterval],
                threadins.joinsints_by_type[InterfaceInterval]):
            to_threadins = joins_int.to_threadins
            assert to_threadins is not threadins
            ret.update(group(to_threadins, t_set))
        for joined_int in chain(
                threadins.joinedints_by_type[InnerjoinInterval],
                threadins.joinedints_by_type[InterfaceInterval]):
            from_threadins = joined_int.from_threadins
            assert from_threadins is not threadins
            ret.update(group(from_threadins, t_set))

        return ret
    ##########################

    threadgroup_by_request = defaultdict(set)
    threadgroups_with_multiple_requests = []
    threadgroups_without_request = []
    for threadins in threadinss:
        assert isinstance(threadins, ThreadInstance)
        if threadins not in seen_threadinss:
            new_t_set = set()
            requests = group(threadins, new_t_set)
            len_req = len(requests)
            if len_req > 1:
                threadgroups_with_multiple_requests.append((new_t_set, requests))
            elif len_req == 1:
                threadgroup_by_request[requests.pop()].update(new_t_set)
            else:
                threadgroups_without_request.append(new_t_set)
    print("----------------")

    components = set()
    hosts = set()
    targets = set()
    threads = set()
    sum_dict = defaultdict(lambda: 0)

    def collect_group(tgroup):
        for tiss in tgroup:
            for tis in tiss:
                assert isinstance(tis, ThreadInstance)
                components.add(tis.component)
                hosts.add(tis.host)
                targets.add(tis.target_obj)
                threads.add(tis.thread_obj)
                sum_dict['sum_tis'] += 1
                sum_dict['sum_lines'] += len(tis.intervals)+1
                sum_dict['sum_joins'] +=\
                        len(tis.joinsints_by_type[InnerjoinInterval])
                sum_dict['sum_joined'] +=\
                        len(tis.joinedints_by_type[InnerjoinInterval])
                sum_dict['sum_ijoins'] +=\
                        len(tis.joinsints_by_type[InterfaceInterval])
                sum_dict['sum_ijoined'] +=\
                        len(tis.joinedints_by_type[InterfaceInterval])
                sum_dict['sum_ljoin'] +=\
                        len(tis.joinedinterfaceints_by_type[InterfacejoinInterval])
                sum_dict['sum_rjoin'] +=\
                        len(tis.joinsinterfaceints_by_type[InterfacejoinInterval])

    collect_group(threadgroup_by_request.itervalues())
    collect_group(threadgroups_without_request)

    #### summary ####
    print("%d request groups" % (len(threadgroup_by_request)))
    print("%d thread instances" % sum_dict['sum_tis'])
    print()

    #### report #####
    report.step("group_t",
                line=sum_dict['sum_lines'],
                component=len(components),
                host=len(hosts),
                target=len(targets),
                thread=len(threads),
                request=len(threadgroup_by_request)+len(threadgroups_without_request),
                threadins=sum_dict['sum_tis'],
                innerjoin=sum_dict['sum_joins'],
                innerjoined=sum_dict['sum_joined'],
                interfacejoin=sum_dict['sum_ijoins'],
                interfacejoined=sum_dict['sum_ijoined'],
                leftinterface=sum_dict['sum_ljoin'],
                rightinterface=sum_dict['sum_rjoin'])

    #### errors #####
    if sum_dict['sum_tis'] != len(threadinss):
        print("! WARN !")
        print("%d thread instances, but previously built %d"
                % (sum_dict['sum_tis'],
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
            # from workflow_parser.draw_engine import DrawEngine
            # draw_engine.draw_debug_groups(reqs, i_group)
            join_ints = set()
            for ti in i_group:
                join_ints.update(ti.joinsints_by_type[InnerjoinInterval])
                join_ints.update(ti.joinsints_by_type[InterfaceInterval])
                join_ints.update(ti.joinedints_by_type[InnerjoinInterval])
                join_ints.update(ti.joinedints_by_type[InterfaceInterval])
            join_ints_by_obj = defaultdict(list)
            for join_int in join_ints:
                join_ints_by_obj[join_int.join_obj].append(join_int)
            print("#### debug group ####")
            for join_obj, join_ints in join_ints_by_obj.iteritems():
                print("%s:" % join_obj.name)
                schemas = join_obj.schemas
                for join_int in join_ints:
                    j_str = ""
                    for schema in schemas:
                        j_str += "%s: %s, " % (schema, join_int.to_pace[schema[1]])
                    print("  %s" % j_str)
            print("#####################\n")
            print("  %d thread instances contains %d requests" %
                    (len(i_group), len(reqs)))
        print()
        raise StateError("(ParserEngine) thread group has multiple requests!")

    ret = []
    for req, tgroup in threadgroup_by_request.iteritems():
        ret.append((req, tgroup))
    for tgroup in threadgroups_without_request:
        ret.append((None, tgroup))
    return ret


class RequestBuilder(object):
    def __init__(self, mastergraph, request, threadinss):
        self.threadinss = threadinss
        self.requestins = RequestInstance(mastergraph, request, self)
        self.is_built = False
        # error report
        self.errors = {}
        self.warns = {}
        # collects
        self.e_incomplete_threadinss = set()
        self.e_extra_s_threadinss = set()
        self.e_extra_e_threadinss = set()
        self.e_stray_threadinss = set()
        self.e_unjoins_paces_by_edge = defaultdict(set)

    def __nonzero__(self):
        if self.errors or not self.is_built:
            return False
        else:
            return True

    def build(self):
        assert not self.is_built
        requestins = self.requestins

        # init
        tis = set()
        for threadins in self.threadinss:
            assert isinstance(threadins, ThreadInstance)
            tis.add(threadins)
            threadins.requestins = requestins
            threadins.request = requestins.request
            if threadins.is_request_start:
                if requestins.start_interval is not None:
                    self.e_extra_s_threadinss.add(threadins)
                else:
                    requestins.start_interval = threadins.start_interval

        # error: multiple start threads
        if self.e_extra_s_threadinss:
            self.e_extra_s_threadinss.add(requestins.start_interval.threadins)
            error_str = "\n".join("%r" % ti for ti in self.e_extra_s_threadinss)
            self.errors["Has multiple start intervals"] = error_str

        # error: no start thread
        if requestins.start_interval is None:
            self.errors["Contains no start interval"] = ""
            return None

        seen_tis = set()
        def _process(threadins):
            assert isinstance(threadins, ThreadInstance)
            if threadins in seen_tis:
                return
            seen_tis.add(threadins)

            # threadinss
            requestins.threadinss.append(threadins)
            requestins.thread_ints.update(threadins.intervals)

            # incomplete_threadinss
            if not threadins.is_complete:
                self.e_incomplete_threadinss.add(threadins)

            # end_interval
            r_state = threadins.request_state
            if r_state:
                assert threadins.is_request_end is True
                if requestins.end_interval:
                    self.e_extra_e_threadinss.add(threadins)
                else:
                    requestins.end_interval = threadins.end_interval

            # last_theadins
            if requestins.last_interval is None \
                    or requestins.last_interval.to_seconds < threadins.end_seconds:
                requestins.last_interval = threadins.end_interval

            # intervals_by_mark
            for mark, intervals in threadins.intervals_by_mark.iteritems():
                requestins.intervals_by_mark[mark].extend(intervals)

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
            requestins.len_paces += len(threadins.intervals)+1

            # thread/target
            requestins.thread_objs.add(threadins.thread_obj)
            requestins.target_objs.add(threadins.target_obj)

            # join_intervals
            for joins_int in threadins.joinsints_by_type[EmptyJoin]:
                requestins.joinints_by_type[EmptyJoin].add(joins_int)
                unjoins_pace = joins_int.from_pace
                self.e_unjoins_paces_by_edge[unjoins_pace.step._edge].add(unjoins_pace)
            for joins_int in threadins.joinsints_by_type[InnerjoinInterval]:
                requestins.joinints_by_type[InnerjoinInterval].add(joins_int)
                _process(joins_int.to_threadins)
            for joins_int in threadins.joinsints_by_type[InterfaceInterval]:
                assert isinstance(joins_int, InterfaceInterval)
                reqjoins = joins_int.joins_crossrequest_int
                if isinstance(reqjoins, EmptyJoin):
                    self.e_unjoins_paces_by_edge[joins_int.join_obj].add(joins_int)
                joins_int.build_nestedrequestinterval()
                requestins.thread_objs.add(
                        joins_int.joins_crossrequest_int.to_threadobj)
                requestins.thread_objs.add(
                        joins_int.joined_crossrequest_int.from_threadobj)
                requestins.joinints_by_type[InterfaceInterval].add(joins_int)
                assert joins_int.joins_crossrequest_int
                assert joins_int.nestedrequest_int
                assert joins_int.joined_crossrequest_int
                requestins.joinints_by_type[InterfacejoinInterval].add(
                        joins_int.joins_crossrequest_int)
                requestins.joinints_by_type[NestedrequestInterval].add(
                        joins_int.nestedrequest_int)
                requestins.joinints_by_type[InterfacejoinInterval].add(
                        joins_int.joined_crossrequest_int)
                _process(joins_int.to_threadins)

            for joins_int in threadins.joinsinterfaceints_by_type[InterfacejoinInterval]:
                requestins.joinsinterfaceints_by_type[InterfacejoinInterval].add(joins_int)
            for joins_int in threadins.joinsinterfaceints_by_type[EmptyJoin]:
                requestins.joinsinterfaceints_by_type[EmptyJoin].add(joins_int)
                unjoins_pace = joins_int.from_pace
                self.e_unjoins_paces_by_edge[unjoins_pace.step._edge].add(unjoins_pace)

            for joined_int in threadins.joinedinterfaceints_by_type[InterfacejoinInterval]:
                requestins.joinedinterfaceints_by_type[InterfacejoinInterval].add(joined_int)
            for joined_int in threadins.joinedinterfaceints_by_type[EmptyJoin]:
                requestins.joinedinterfaceints_by_type[EmptyJoin].add(joined_int)

        _process(requestins.start_interval.threadins)

        requestins.threadinss.sort(key=lambda ti: ti.start_seconds)

        # error: incomplete threads
        if self.e_incomplete_threadinss:
            err_str = "\n".join("%r" % ti for it in self.e_incomplete_threadinss)
            self.errors["Has incomplete threads"] = err_str

        # error: multiple end threads
        if self.e_extra_e_threadinss:
            self.e_extra_e_threadinss.add(requestins.end_interval.threadins)
            err_str = "\n".join("%r" % ti for ti in self.e_extra_e_threadinss)
            self.errors["Has multiple end intervals"] = err_str

        # error: no end thread
        if not requestins.end_interval:
            self.errors["Contains no end interval"] = ""
        else:
            int_extended = []
            try:
                last_int = None
                for int_ in requestins.iter_mainpath(reverse=True):
                    if isinstance(int_, ThreadInterval):
                        if last_int is None:
                            last_int = int_
                    else:
                        assert isinstance(int_, JoinIntervalBase)
                        if last_int is not None:
                            int_extended.append(RequestInterval(int_.to_pace,
                                                                last_int.to_pace))
                            last_int = None
                        else:
                            assert isinstance(int_, NestedrequestInterval) or\
                                   isinstance(int_, InterfacejoinInterval)
                        int_extended.append(int_)
                int_extended.append(
                        RequestInterval(requestins.start_interval.from_pace,
                                        last_int.to_pace))
            except StateError as e:
                self.errors["Main route parse error"] = (int_, e)
            else:
                for tint in reversed(int_extended):
                    if requestins.extended_ints:
                        assert requestins.extended_ints[-1].to_pace is tint.from_pace
                    else:
                        assert tint.from_pace is requestins.start_interval.from_pace
                    requestins.extended_ints.append(tint)
                assert requestins.extended_ints[-1].to_pace is requestins.end_interval.to_pace

        # error: stray thread instances
        self.e_stray_threadinss = tis - seen_tis
        if self.e_stray_threadinss:
            err_str = "\n".join("%r" % ti for ti in self.e_stray_threadinss)
            self.warns["Has stray threads"] = err_str

        # error: unjoins paces
        if self.e_unjoins_paces_by_edge:
            self.warns["Has unjoins paces"] = "%d joins edges" % len(self.e_unjoins_paces_by_edge)

        if not self.errors and requestins.request is None:
            RequestInstance._index_dict[requestins.request_type] += 1
            requestins.request = "%s%d" % (requestins.request_type,
                    RequestInstance._index_dict[requestins.request_type])
            for threadins in requestins.threadinss:
                assert threadins.request is None
                threadins.request = requestins.request

        self.is_built = True
        if not self.errors:
            return requestins


def build_requests(threadgroup_by_request, mastergraph, report):
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
    unjoinspaces_by_edge = defaultdict(set)

    print("Build requests...")
    for request, threads in threadgroup_by_request:
        r_builder = RequestBuilder(mastergraph, request, threads)
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
                    e = r_builder.errors[error][1]
                    main_route_error[e.message][e.where] += 1

        if r_builder.e_incomplete_threadinss:
            incomplete_threadinss.update(r_builder.e_incomplete_threadinss)
        if r_builder.e_extra_s_threadinss:
            extra_start_threadinss.update(r_builder.e_extra_s_threadinss)
        if r_builder.e_extra_e_threadinss:
            extra_end_threadinss.update(r_builder.e_extra_e_threadinss)
        if r_builder.e_stray_threadinss:
            stray_threadinss.update(r_builder.e_stray_threadinss)
        if r_builder.e_unjoins_paces_by_edge:
            for edge, paces in r_builder.e_unjoins_paces_by_edge.iteritems():
                unjoinspaces_by_edge[edge].update(paces)
    print("-----------------")

    #### collect ####
    cnt_lines = 0
    components = set()
    hosts = set()
    target_objs = set()
    thread_objs = set()
    threadinss = set()
    requests_vars = defaultdict(set)
    innerjoin_intervals = set()
    interface_intervals = set()
    linterfaces = set()
    rinterfaces = set()
    joinints_by_remotetype = defaultdict(set)

    thread_intervals = set()
    extended_intervals = set()
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

        innerjoin_intervals.update(requestins.joinints_by_type[InnerjoinInterval])
        interface_intervals.update(requestins.joinints_by_type[InterfaceInterval])
        linterfaces.update(requestins.joinedinterfaceints_by_type[InterfacejoinInterval])
        rinterfaces.update(requestins.joinsinterfaceints_by_type[InterfacejoinInterval])
        for j_ins in chain(requestins.joinints_by_type[InnerjoinInterval],
                           requestins.joinints_by_type[InterfaceInterval],
                           requestins.joinedinterfaceints_by_type[InterfacejoinInterval],
                           requestins.joinsinterfaceints_by_type[InterfacejoinInterval]):
            joinints_by_remotetype[j_ins.remote_type].add(j_ins)
        thread_intervals.update(requestins.thread_ints)
        extended_intervals.update(requestins.extended_ints)

    #### summary ####
    print("%d valid request instances with %d thread instances"
            % (len(requestinss),
               len(threadinss)))
    print("%d relations:" % len(innerjoin_intervals))
    for j_type, j_inss in joinints_by_remotetype.iteritems():
        print("  %d %s relations" % (len(j_inss), j_type))

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
                innerjoin=len(innerjoin_intervals),
                innerjoined=len(innerjoin_intervals),
                interfacejoin=len(interface_intervals),
                interfacejoined=len(interface_intervals),
                leftinterface=len(linterfaces),
                rightinterface=len(rinterfaces))

    #### errors #####
    if len(requestinss) != len(threadgroup_by_request):
        print("! WARN !")
        print("%d request instances from %d request groups"
                % (len(requestinss),
                   len(threadgroup_by_request)))
        print()

    if unjoinspaces_by_edge:
        print("! WARN !")
        for edge, paces in unjoinspaces_by_edge.iteritems():
            print("%s: %d unjoins paces" % (edge.name, len(paces)))
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
