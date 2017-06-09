from __future__ import print_function

from collections import defaultdict
from itertools import chain

from ...utils import Report
from ..entities.request import RequestInstance
from ..entities.threadins import ThreadInstance
from ..entities.join import EmptyJoin
from ..entities.join import InnerjoinInterval
from ..entities.join import InterfaceInterval
from ..entities.join import InterfacejoinInterval


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


def build_requests(threadgroup_by_request, mastergraph, report):
    requestinss = {}
    # error report
    error_requestinss = []
    requests_by_errortype = defaultdict(list)
    main_route_error = defaultdict(lambda: defaultdict(lambda: 0))
    # warn report
    warn_requestinss = []
    requests_by_warntype = defaultdict(list)
    # others
    incomplete_threadinss = set()
    extra_start_threadinss = set()
    extra_end_threadinss = set()
    stray_threadinss = set()
    unjoinspaces_by_edge = defaultdict(set)

    print("Build requests...")
    for request, threads in threadgroup_by_request:
        requestins = RequestInstance(mastergraph, request, threads)

        if requestins.warns:
            warn_requestinss.append(requestins)
            for warn in requestins.warns.keys():
                requests_by_warntype[warn].append(requestins)

        if requestins.errors:
            error_requestinss.append(requestins)
            for error in requestins.errors.keys():
                requests_by_errortype[error].append(requestins)
                if error == "Main route parse error":
                    e = requestins.errors[error][1]
                    main_route_error[e.message][e.where] += 1
        else:
            assert requestins.request not in requestinss
            requestinss[requestins.request] = requestins

        if requestins.e_incomplete_threadinss:
            incomplete_threadinss.update(requestins.e_incomplete_threadinss)
        if requestins.e_extra_s_threadinss:
            extra_start_threadinss.update(requestins.e_extra_s_threadinss)
        if requestins.e_extra_e_threadinss:
            extra_end_threadinss.update(requestins.e_extra_e_threadinss)
        if requestins.e_stray_threadinss:
            stray_threadinss.update(requestins.e_stray_threadinss)
        if requestins.e_unjoins_paces_by_edge:
            for edge, paces in requestins.e_unjoins_paces_by_edge.iteritems():
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
    join_intervals_by_type = defaultdict(set)

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
            join_intervals_by_type[j_ins.join_type].add(j_ins)
        thread_intervals.update(requestins.thread_ints)
        extended_intervals.update(requestins.extended_ints)

    #### summary ####
    print("%d valid request instances with %d thread instances"
            % (len(requestinss),
               len(threadinss)))
    print("%d relations:" % len(innerjoin_intervals))
    for j_type, j_inss in join_intervals_by_type.iteritems():
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

    if error_requestinss:
        print("!! ERROR !!")
        print("%d error request instances" % len(error_requestinss))
        for err, requestinss_ in requests_by_errortype.iteritems():
            print("  %s: %d requests" % (err, len(requestinss_)))
        print()

    if main_route_error:
        print("!! ERROR !!")
        print("Tracing errors:")
        for e_msg, where_count in main_route_error.iteritems():
            print("  %s:" % e_msg)
            for where, cnt in where_count.iteritems():
                print("    %s: %d" % (where, cnt))
        print()

    if warn_requestinss:
        print("! WARN !")
        print("%d warn request instances:" %
                len(warn_requestinss))
        for warn, requestinss_ in requests_by_warntype.iteritems():
            print("  %s: %d requests" % (warn, len(requestinss_)))
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
