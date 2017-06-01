from __future__ import print_function

from collections import defaultdict
from collections import OrderedDict

from workflow_parser.schema_engine import SchemaEngine
from workflow_parser.state_graph import MasterGraph
from workflow_parser.state_machine import empty_join
from workflow_parser.state_machine import StateError
from workflow_parser.state_machine import RequestInstance
from workflow_parser.state_machine import ThreadInstance
from workflow_parser.state_machine import InnerjoinIntervalBase
from workflow_parser.state_machine import InterfaceInterval
from workflow_parser.state_machine import InterfacejoinInterval
from workflow_parser.state_machine import InnerjoinInterval
from workflow_parser.utils import report_loglines
from workflow_parser.utils import Report


class StateEngine(object):
    def __init__(self, mastergraph, report):
        assert isinstance(mastergraph, MasterGraph)
        assert isinstance(report, Report)

        self.mastergraph = mastergraph
        self.report = report

    def build_thread_instances(self, targetobjs):
        valid_loglines = 0
        thread_objs = []

        print("Build thread instances...")
        for target_obj in targetobjs:
            for thread_obj in target_obj.thread_objs.itervalues():
                threadins = None
                cnt_valid_loglines = 0
                for logline in thread_obj.loglines:
                    if threadins is not None:
                        if threadins.step(logline):
                            # success!
                            pass
                        else:
                            threadins = None
                    if threadins is None:
                        threadins = ThreadInstance.create(self.mastergraph,
                                                          logline,
                                                          thread_obj)
                        if threadins is not None:
                            thread_obj.threadinss.append(threadins)
                        else:
                            # error
                            # print("(ParserEngine) parse error: cannot decide graph")
                            # report_loglines(loglines, c_index)
                            # print "-------- end -----------"
                            # raise StateError("(ParserEngine) parse error: cannot decide graph")
                            import pdb; pdb.set_trace()
                    if threadins is not None:
                        cnt_valid_loglines += 1
                    else:
                        thread_obj.ignored_loglines.append(logline)
                        logline.ignored = True

                assert len(thread_obj.ignored_loglines) + cnt_valid_loglines\
                        == len(thread_obj.loglines)

                thread_objs.append(thread_obj)
                valid_loglines += cnt_valid_loglines
        print("-------------------------")

        #### collect ####
        ignored_loglines_by_component = defaultdict(lambda: [])
        components = set()
        hosts = set()
        targets = set()
        threadinss = []
        incomplete_threadinss_by_graph = defaultdict(list)
        complete_threadinss_by_graph = defaultdict(list)
        start_threadinss = []
        duplicated_vars = set()
        cnt_innerjoins_paces = 0
        cnt_innerjoined_paces = 0
        cnt_leftinterface_paces = 0
        cnt_rightinterface_paces = 0

        for thread_obj in thread_objs:
            if thread_obj.ignored_loglines:
                ignored_loglines_by_component[thread_obj.component]\
                        .extend(thread_obj.ignored_loglines)
            if thread_obj.threadinss:
                components.add(thread_obj.component)
                hosts.add(thread_obj.host)
                targets.add(thread_obj.target)
            for threadins in thread_obj.threadinss:
                if not threadins.is_complete:
                    incomplete_threadinss_by_graph[threadins.threadgraph.name]\
                            .append(threadins)
                else:
                    complete_threadinss_by_graph[threadins.threadgraph.name]\
                            .append(threadins)
                if threadins.is_request_start:
                    start_threadinss.append(threadins)
                threadinss.append(threadins)
                duplicated_vars.update(threadins.thread_vars_dup.keys())
                cnt_innerjoins_paces += len(threadins.joins_paces)
                cnt_innerjoined_paces += len(threadins.joined_paces)
                cnt_leftinterface_paces += len(threadins.leftinterface_paces)
                cnt_rightinterface_paces += len(threadins.rightinterface_paces)

        #### summary ####
        print("%d valid loglines" % valid_loglines)
        print("%d thread instances" % len(threadinss))
        if complete_threadinss_by_graph:
            for gname, tis in complete_threadinss_by_graph.iteritems():
                print("  %s: %d inss" % (gname, len(tis)))

        print("%d request start t_instances" % len(start_threadinss))
        print()

        #### report #####
        self.report.step("build_t",
                         line=valid_loglines,
                         component=len(components),
                         host=len(hosts),
                         target=len(targets),
                         thread=len(thread_objs),
                         request=len(start_threadinss),
                         threadins=len(threadinss),
                         innerjoin=cnt_innerjoins_paces,
                         innerjoined=cnt_innerjoined_paces,
                         leftinterface=cnt_leftinterface_paces,
                         rightinterface=cnt_rightinterface_paces)

        #### errors #####
        if ignored_loglines_by_component:
            def _report_ignored(tup):
                # (logline, loglines, index, thread, component, target)
                print("  example:")
                report_loglines(tup[1], tup[2], blanks=4, printend=True)
            print("! WARN !")
            for comp, loglines in ignored_loglines_by_component.iteritems():
                print("%s: %d ignored loglines" % (comp, len(loglines)))
                _report_ignored(loglines[0])
            print()

        edges = self.mastergraph.edges - self.mastergraph.seen_edges
        if edges:
            print("! WARN !")
            print("Unseen graph edges: %s" %
                    ",".join(edge.name for edge in edges))
            print()

        if duplicated_vars:
            print("! WARN !")
            print("Duplicated vars in t_instances: %s" %
                    ",".join(duplicated_vars))
            print()

        if incomplete_threadinss_by_graph:
            print("! WARN !")
            print("Incompleted t_instances:")
            for gname, tis in incomplete_threadinss_by_graph.iteritems():
                print("  %s: %d t_instances" % (gname, len(tis)))
            print()

        return threadinss

    def join_paces(self, threadinss):
        join_attempt_cnt = 0

        print("Join paces...")
        target_objs = {}

        innerjoin_engine = SchemaEngine(InnerjoinIntervalBase)
        interfacejoin_engine = SchemaEngine(InterfacejoinInterval)

        for threadins in threadinss:
            assert isinstance(threadins, ThreadInstance)
            target_objs[threadins.target] = threadins.target_obj

            innerjoin_engine.register_fromitems(
                    threadins.joins_paces,
                    lambda p: p.edge.joins_objs)
            innerjoin_engine.register_toitems(
                    threadins.joined_paces,
                    lambda p: p.edge.joined_objs)

            interfacejoin_engine.register_fromitems(
                    threadins.rightinterface_paces,
                    lambda p: p.edge.right_interface)
            interfacejoin_engine.register_toitems(
                    threadins.leftinterface_paces,
                    lambda p: p.edge.left_interface)

        inner_relations = innerjoin_engine.proceed(target_objs)
        request_interfaces = [item for item in inner_relations
                              if isinstance(item, InterfaceInterval)]

        interfacejoin_engine.register_fromitems(
                request_interfaces,
                lambda i: i.join_obj.joins_interfaces)
        interfacejoin_engine.register_toitems(
                request_interfaces,
                lambda i: i.join_obj.joined_interfaces)

        interfacej_relations = interfacejoin_engine.proceed(target_objs)
        left_interfacejs = [item for item in interfacej_relations if
                item.is_left]
        print("-------------")

        # #### summary ####
        innerjoin_engine.report(self.mastergraph)
        interfacejoin_engine.report(self.mastergraph)
        print()

        #### report #####
        self.report.step("join_ps",
                         innerjoin=len(inner_relations)-len(request_interfaces),
                         innerjoined=len(inner_relations)-len(request_interfaces),
                         interfacejoin=len(request_interfaces),
                         interfacejoined=len(request_interfaces),
                         leftinterface=len(left_interfacejs),
                         rightinterface=len(interfacej_relations)-len(left_interfacejs))

        return inner_relations, request_interfaces, interfacej_relations


    # step 3: group threads by request
    def group_threads(self, threadinss):
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

            for joins_int in threadins.joins_ints:
                ret.update(group(joins_int.to_threadins, t_set))
            for joined_int in threadins.joined_ints:
                ret.update(group(joined_int.from_threadins, t_set))
            for ijoins_int in threadins.interfacejoins_ints:
                ret.update(group(ijoins_int.to_threadins, t_set))
            for ijoined_int in threadins.interfacejoined_ints:
                ret.update(group(ijoined_int.from_threadins, t_set))

            return ret
        ##########################

        threadgroup_by_request = defaultdict(set)
        threadgroups_with_multiple_requests = []
        threadgroups_without_request = []
        for threadins in threadinss:
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

        sum_lines = 0
        components = set()
        hosts = set()
        targets = set()
        threads = set()
        sum_tis = 0
        sum_joins = 0
        sum_joined = 0
        sum_ijoins = 0
        sum_ijoined = 0
        sum_ljoin = 0
        sum_rjoin = 0
        interface_ints = set()
        for tgroup in threadgroup_by_request.itervalues():
            sum_tis += len(tgroup)
            for tis in tgroup:
                assert isinstance(tis, ThreadInstance)
                sum_lines += len(tis.paces)
                components.add(tis.component)
                hosts.add(tis.host)
                targets.add(tis.target_obj)
                threads.add(tis.thread_obj)
                sum_joins += len(tis.joins_ints)
                sum_joined += len(tis.joined_ints)
                sum_ijoins += len(tis.interfacejoins_ints)
                sum_ijoined += len(tis.interfacejoined_ints)
                interface_ints.update(tis.interfacejoins_ints)
                sum_ljoin += len(tis.leftinterface_ints)
                sum_rjoin += len(tis.rightinterface_ints)

        for interface in interface_ints:
            if interface.joins_crossrequest_int is not None:
                sum_rjoin += 1
            if interface.joined_crossrequest_int is not None:
                sum_ljoin += 1

        #### summary ####
        print("%d request groups" % (len(threadgroup_by_request)))
        print("%d thread instances" % sum_tis)
        print()

        #### report #####
        self.report.step("group_t",
                         line=sum_lines,
                         component=len(components),
                         host=len(hosts),
                         target=len(targets),
                         thread=len(threads),
                         request=len(threadgroup_by_request),
                         threadins=sum_tis,
                         innerjoin=sum_joins,
                         innerjoined=sum_joined,
                         interfacejoin=sum_ijoins,
                         interfacejoined=sum_ijoined,
                         leftinterface=sum_ljoin,
                         rightinterface=sum_rjoin)

        #### errors #####
        if sum_tis != len(threadinss):
            print("! WARN !")
            print("%d thread instances, but previously built %d"
                    % (sum_tis,
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
                    join_ints.update(ti.joins_ints)
                    join_ints.update(ti.joined_ints)
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

        return threadgroup_by_request

    def build_requests(self, threadgroup_by_request):
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
        for request, threads in threadgroup_by_request.iteritems():
            requestins = RequestInstance(self.mastergraph, request, threads)

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

            innerjoin_intervals.update(requestins.join_ints)
            for j_ins in requestins.join_ints:
                join_intervals_by_type[j_ins.join_type].add(j_ins)
            thread_intervals.update(requestins.td_ints)
            extended_intervals.update(requestins.intervals_extended)

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
        self.report.step("build_r",
                         line=cnt_lines,
                         component=len(components),
                         host=len(hosts),
                         target=len(target_objs),
                         thread=len(thread_objs),
                         request=len(requestinss),
                         threadins=len(threadinss),
                         innerjoin=len(innerjoin_intervals),
                         innerjoined=len(innerjoin_intervals))

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
