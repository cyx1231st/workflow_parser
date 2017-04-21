from __future__ import print_function

from collections import defaultdict
from collections import deque
from collections import OrderedDict

from workflow_parser.log_engine import TargetsCollector
from workflow_parser.log_parser import LogLine
from workflow_parser.state_graph import MasterGraph
from workflow_parser.state_graph import Token
from workflow_parser.state_machine import empty_join
from workflow_parser.state_machine import JoinInterval
from workflow_parser.state_machine import NestedRequest
from workflow_parser.state_machine import StateError
from workflow_parser.state_machine import RequestInstance
from workflow_parser.state_machine import ThreadInstance
from workflow_parser.utils import report_loglines


class PacesCollector(object):
    def __init__(self):
        self.ignored_loglines_total_by_component = defaultdict(lambda: [[], 0])

        self.joins_paces = []
        self.joined_paces = []
        self.joined_paces_by_jo = defaultdict(list)
        self.joined_paces_by_jo_host = defaultdict(lambda: defaultdict(list))

        self.unjoinspaces_by_edge = defaultdict(list)
        self.unjoinedpaces_by_edge = defaultdict(list)
        self.relations = set()
        self.relation_by_jo = defaultdict(list)

        self.r_unjoinspaces_by_edge = defaultdict(set)
        self.join_intervals_by_type = defaultdict(set)
        self.join_intervals = set()
        self.thread_intervals = set()
        self.extended_intervals = set()

    def collect_ignored(self, logline, loglines, index, thread, component, target):
        assert isinstance(logline, LogLine)
        assert isinstance(index, int)
        self.ignored_loglines_total_by_component[component][0].append(
                (logline, loglines, index, thread, component, target))

    def collect_join(self, threadins):
        assert isinstance(threadins, ThreadInstance)

        self.joins_paces.extend(threadins.joins_paces)
        self.joined_paces.extend(threadins.joined_paces)
        for pace in threadins.joined_paces:
            for joined_obj in pace.joined_objs:
                self.joined_paces_by_jo[joined_obj].append(pace)
                self.joined_paces_by_jo_host[joined_obj][pace.host].append(pace)

    def collect_unjoin(self):
        for pace in self.joins_paces:
            if pace.joins_int is empty_join:
                self.unjoinspaces_by_edge[pace.edge].append(pace)
            elif isinstance(pace.joins_int, JoinInterval):
                self.relations.add(pace.joins_int)
                self.relation_by_jo[pace.joins_int.join_obj].append(pace.joins_int)
            else:
                assert False

        for pace in self.joined_paces:
            if pace.joined_int is empty_join:
                self.unjoinedpaces_by_edge[pace.edge].append(pace)
            elif isinstance(pace.joined_int, JoinInterval):
                assert pace.joined_int in self.relations
            else:
                assert False

    def collect_request(self, requestins):
        assert isinstance(requestins, RequestInstance)

        if requestins.e_unjoins_paces_by_edge:
            for edge, paces in requestins.e_unjoins_paces_by_edge.iteritems():
                self.r_unjoinspaces_by_edge[edge].update(paces)

        if not requestins.errors:
            self.join_intervals.update(requestins.join_ints)
            for j_ins in requestins.join_ints:
                self.join_intervals_by_type[j_ins.join_type].add(j_ins)
            self.thread_intervals.update(requestins.td_ints)
            self.extended_intervals.update(requestins.intervals_extended)


class ThreadInssCollector(object):
    def __init__(self, pcs):
        assert isinstance(pcs, PacesCollector)

        self.pcs = pcs

        self.incomplete_threadinss_by_graph = defaultdict(list)
        self.complete_threadinss_by_graph = defaultdict(list)
        self.start_threadinss = []
        self.duplicated_vars = set()

        self.threadinss = []

        self.threadgroup_by_request = defaultdict(set)
        self.sum_threads_in_group = 0
        self.threadgroups_with_multiple_requests = []
        self.threadgroups_without_request = []

        self.r_threadinss = set()
        self.r_incomplete_threadinss = set()
        self.r_extra_start_threadinss = set()
        self.r_extra_end_threadinss = set()
        self.r_stray_threadinss = set()

    def collect_thread(self, threadins):
        assert isinstance(threadins, ThreadInstance)

        if not threadins.is_complete:
            self.incomplete_threadinss_by_graph[threadins.threadgraph.name].append(threadins)
        else:
            self.complete_threadinss_by_graph[threadins.threadgraph.name].append(threadins)

        if threadins.is_request_start:
            self.start_threadinss.append(threadins)

        self.threadinss.append(threadins)

        self.duplicated_vars.update(threadins.thread_vars_dup.keys())

        self.pcs.collect_join(threadins)

    def collect_thread_group(self, requests, tgroup):
        assert isinstance(requests, set)
        assert isinstance(tgroup, set)

        len_req = len(requests)
        if len_req > 1:
            self.threadgroups_with_multiple_requests.append((tgroup, requests))
        elif len_req == 1:
            self.threadgroup_by_request[requests.pop()].update(tgroup)
        else:
            self.threadgroups_without_request.append(tgroup)

    def collect_request(self, requestins):
        assert isinstance(requestins, RequestInstance)

        self.r_threadinss.update(requestins.threadinss)
        if requestins.e_incomplete_threadinss:
            self.r_incomplete_threadinss.update(requestins.e_incomplete_threadinss)
        if requestins.e_extra_s_threadinss:
            self.r_extra_start_threadinss.update(requestins.e_extra_s_threadinss)
        if requestins.e_extra_e_threadinss:
            self.r_extra_end_threadinss.update(requestins.e_extra_e_threadinss)
        if requestins.e_stray_threadinss:
            self.r_stray_threadinss.update(requestins.e_stray_threadinss)


class RequestsCollector(object):
    def __init__(self, tis, pcs):
        assert isinstance(tis, ThreadInssCollector)
        assert isinstance(pcs, PacesCollector)

        self.tis= tis
        self.pcs= pcs

        self.requestinss = {}
        self.requests_vars = defaultdict(set)

        # error report
        self.error_requestinss = []
        self.requests_by_errortype = defaultdict(list)
        self.main_route_error = defaultdict(lambda: defaultdict(lambda: 0))

        # warn report
        self.warn_requestinss = []
        self.requests_by_warntype = defaultdict(list)

    def collect_request(self, requestins):
        assert isinstance(requestins, RequestInstance)
        if requestins.warns:
            self.warn_requestinss.append(requestins)
            for warn in requestins.warns.keys():
                self.requests_by_warntype[warn].append(requestins)

        if requestins.errors:
            self.error_requestinss.append(requestins)
            for error in requestins.errors.keys():
                self.requests_by_errortype[error].append(requestins)
                if error == "Main route parse error":
                    e = requestins.errors[error][1]
                    self.main_route_error[e.message][e.where] += 1
        else:
            assert requestins.request not in self.requestinss
            self.requestinss[requestins.request] = requestins
            self.requests_vars["request"].add(requestins.request)
            for k, vs in requestins.request_vars.iteritems():
                self.requests_vars[k].update(vs)

        self.tis.collect_request(requestins)
        self.pcs.collect_request(requestins)


# TODO: add count checks
class StateEngine(object):
    def __init__(self, mastergraph, tgs):
        assert isinstance(mastergraph, MasterGraph)
        assert isinstance(tgs, TargetsCollector)

        self.mastergraph = mastergraph
        self.tgs = tgs
        self.pcs = PacesCollector()
        self.tis = ThreadInssCollector(self.pcs)
        self.rqs = RequestsCollector(self.tis, self.pcs)

    def build_thread_instances(self):
        print("Build thread instances...")
        for f_obj in self.tgs.itervalues():
            for (thread, loglines) in f_obj.loglines_by_thread.iteritems():
                c_index = 0
                len_index = len(loglines)
                assert len_index > 0
                self.pcs.ignored_loglines_total_by_component[f_obj.component][1] += len_index
                threadins = None
                while c_index != len_index:
                    logline = loglines[c_index]
                    if not threadins:
                        token = Token.new(self.mastergraph, logline)
                        if not token:
                            # error
                            # print("(ParserEngine) parse error: cannot decide graph")
                            # report_loglines(loglines, c_index)
                            # print "-------- end -----------"
                            # raise StateError("(ParserEngine) parse error: cannot decide graph")
                            import pdb; pdb.set_trace()
                            self.pcs.collect_ignored(logline, loglines, c_index,
                                    thread, f_obj.component, f_obj.target)
                            logline.ignored = True
                        else:
                            threadins = ThreadInstance(thread, token, loglines, c_index)
                    else:
                        is_success = threadins.step(c_index)
                        if not is_success:
                            self.tis.collect_thread(threadins)
                            threadins = None
                            continue
                    c_index += 1
                if threadins:
                    self.tis.collect_thread(threadins)
        print("-------------------------")

        #### summary ####
        if_ignored = False
        for comp, ltup in self.pcs.ignored_loglines_total_by_component.iteritems():
            print("%s: %d loglines" % (comp, ltup[1]))
            if len(ltup[0]):
                if_ignored = True

        print("%d thread instances" % len(self.tis.threadinss))
        if self.tis.complete_threadinss_by_graph:
            for gname, tis in self.tis.complete_threadinss_by_graph.iteritems():
                print("  %s: %d inss" % (gname, len(tis)))

        print("%d request start t_instances" %
                len(self.tis.start_threadinss))
        print("%d paces to join" % len(self.pcs.joins_paces))
        print()
        #################

        if len(self.pcs.joins_paces) != len(self.pcs.joined_paces):
            print("! WARN !")
            print("%d join paces doesn't match %d to-be-joined paces"
                    % (len(self.pcs.joins_paces), len(self.pcs.joined_paces)))
            print()

        if len(self.tis.start_threadinss) != len(self.tgs.requests):
            print("! WARN !")
            print("%d requests doesn't match %d start t_instances!"
                    % (len(self.tis.start_threadinss),
                       len(self.tgs.requests)))
            print()

        if if_ignored:
            def _report_ignored(tup):
                # (logline, loglines, index, thread, component, target)
                print("  example:")
                report_loglines(tup[1], tup[2], blanks=4, printend=True)
            print("! WARN !")
            for comp, ltup in self.pcs.ignored_loglines_total_by_component.iteritems():
                if len(ltup[0]):
                    print("%s: %d ignored loglines" % (comp, len(ltup[0])))
                    _report_ignored(ltup[0][0])
            print()

        edges = self.mastergraph.edges - self.mastergraph.seen_edges
        if edges:
            print("! WARN !")
            print("Unseen graph edges: %s" %
                    ",".join(edge.name for edge in edges))
            print()

        if self.tis.duplicated_vars:
            print("! WARN !")
            print("Duplicated vars in t_instances: %s" %
                    ",".join(self.tis.duplicated_vars))
            print()

        if self.tis.incomplete_threadinss_by_graph:
            print("! WARN !")
            print("Incompleted t_instances:")
            for gname, tis in self.tis.incomplete_threadinss_by_graph.iteritems():
                print("  %s: %d t_instances" % (gname, len(tis)))
            print()

    def join_paces(self):
        print("Join paces...")
        self.pcs.joins_paces.sort()
        self.pcs.joined_paces.sort()
        for jo, p_list in self.pcs.joined_paces_by_jo.iteritems():
            p_list.sort()
            self.pcs.joined_paces_by_jo[jo] = OrderedDict.fromkeys(p_list)
        for jo, paces_by_host in self.pcs.joined_paces_by_jo_host.iteritems():
            for host, paces in paces_by_host.iteritems():
                paces.sort()
                self.pcs.joined_paces_by_jo_host[jo][host] = OrderedDict.fromkeys(paces)

        join_attempt_cnt = 0
        for joins_pace in self.pcs.joins_paces:
            join_objs = joins_pace.edge.joins_objs
            target_pace = None
            for join_obj in join_objs:
                schemas = join_obj.schemas

                from_schema = {}
                care_host = None
                for schema in schemas:
                    from_schema_key = schema[0]
                    to_schema_key = schema[1]
                    if to_schema_key == "host":
                        # TODO: add target speedup
                        care_host = joins_pace[from_schema_key]
                    from_schema[from_schema_key] = joins_pace[from_schema_key]
                if care_host is None:
                    target_paces = self.pcs.joined_paces_by_jo[join_obj]
                else:
                    target_paces = self.pcs.joined_paces_by_jo_host[join_obj][care_host]
                if target_paces:
                    assert isinstance(target_paces, OrderedDict)

                to_schemas = defaultdict(set)
                for pace in target_paces:
                    if pace.joined_int is not None:
                        target_paces.pop(pace)
                        continue

                    join_attempt_cnt += 1
                    match = True
                    for schema in schemas:
                        from_schema_key = schema[0]
                        from_schema_val = from_schema[from_schema_key]
                        to_schema_key = schema[1]
                        to_schema_val = pace[to_schema_key]

                        to_schemas[to_schema_key].add(to_schema_val)
                        if from_schema_key == "request":
                            assert to_schema_key == "request"
                            if from_schema_val and to_schema_val and from_schema_val != to_schema_val:
                                match = False
                                break
                        elif from_schema[from_schema_key] != to_schema_val:
                            assert from_schema_val is not None
                            assert to_schema_val is not None
                            match = False
                            break

                    if match:
                        target_pace = pace
                        target_paces.pop(pace)
                        break

                if target_pace:
                    break
            if target_pace:
                joins_pace.join_pace(target_pace, join_obj)
            else:
                # debug joins
                # print("from_schema: %s" % from_schema)
                # print("to_schemas:")
                # for k, v in to_schemas.iteritems():
                #     print("  %s: %s" % (k, v))
                # import pdb; pdb.set_trace()
                joins_pace.joins_int = empty_join

        for joined_pace in self.pcs.joined_paces:
            if joined_pace.joined_int is None:
                joined_pace.joined_int = empty_join
        print("-------------")

        #### summary ####
        self.pcs.collect_unjoin()
        print("%d join attempts" % join_attempt_cnt)
        if self.pcs.relations:
            print("%d relations:" % len(self.pcs.relations))
            for jo, relations in self.pcs.relation_by_jo.iteritems():
                print("  %s->%s: %d rels" % (jo.from_edge.name,
                                             jo.to_edge.name,
                                             len(relations)))
        print()
        #################

        jos = set(self.pcs.relation_by_jo.keys())
        g_jos = self.mastergraph.join_objs
        unseen_jos = g_jos - jos
        if unseen_jos:
            print("! WARN !")
            for jo in unseen_jos:
                print("%s unseen!" % jo)
            print()

        if len(self.pcs.relations) != len(self.pcs.joins_paces):
            print("! WARN !")
            for edge, notjoins_paces in self.pcs.unjoinspaces_by_edge.iteritems():
                print("%s: %d unjoins paces"
                        % (edge.name,
                           len(notjoins_paces)))
            print("--------")
            for edge, notjoined_paces in self.pcs.unjoinedpaces_by_edge.iteritems():
                print("%s: %d unjoined paces"
                        % (edge.name,
                           len(notjoined_paces)))
            print()
        else:
            assert not self.pcs.unjoinedpaces_by_edge
            assert not self.pcs.unjoinspaces_by_edge

    # step 3: group threads by request
    def group_threads(self):
        print("Group threads...")
        seen_threadinss = set()
        def group(threadins, t_set):
            assert isinstance(threadins, ThreadInstance)
            if threadins.is_shared:
                t_set.add(threadins)
                return set()

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
            return ret

        # TODO ugly implementation
        threadinss = []
        for threadins in self.tis.threadinss:
            if threadins.is_shared:
                generated = NestedRequest.generate(threadins)
                threadinss.extend(generated)
            else:
                threadinss.append(threadins)
        ##########################

        for threadins in threadinss:
            if not threadins.is_shared and threadins not in seen_threadinss:
                new_t_set = set()
                requests = group(threadins, new_t_set)
                self.tis.collect_thread_group(requests, new_t_set)
        print("----------------")

        #### summary ####
        print("%d request groups" % (len(self.tis.threadgroup_by_request)))
        sum_tis = sum(len(tgroup) for tgroup in self.tis.threadgroup_by_request.itervalues())
        self.tis.sum_threads_in_group = sum_tis
        print("%d thread instances" % sum_tis)
        print()
        #################

        if sum_tis != len(self.tis.threadinss):
            print("! WARN !")
            print("%d thread instances, but previously built %d"
                    % (sum_tis,
                       len(self.tis.threadinss)))
            print()

        req_set = set(self.tis.threadgroup_by_request.keys())
        unseen_reqs = self.tgs.requests - req_set
        if unseen_reqs:
            print("! WARN !")
            print("%s requests missing" % len(unseen_reqs))
            print()

        if self.tis.threadgroups_without_request:
            print("! WARN !")
            print("%d groups of %d threadinstances cannot be identified" % (
                    len(self.tis.threadgroups_without_request),
                    sum(len(tgroup) for tgroup in
                        self.tis.threadgroups_without_request)))
            print()

        if self.tis.threadgroups_with_multiple_requests:
            print("!! ERROR !!")
            print("%d thread groups have multiple requests" % (
                    len(self.tis.threadgroups_with_multiple_requests)))
            for i_group, reqs in self.tis.threadgroups_with_multiple_requests:
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

    def build_requests(self):
        print("Build requests...")
        for request, threads in self.tis.threadgroup_by_request.iteritems():
            request_obj = RequestInstance(self.mastergraph, request, threads)
            self.rqs.collect_request(request_obj)
        print("-----------------")

        #### summary ####
        print("%d request instances with %d thread instances"
                % (len(self.rqs.requestinss),
                   len(self.tis.r_threadinss)))
        print("%d relations:" % len(self.pcs.join_intervals))
        for j_type, j_inss in self.pcs.join_intervals_by_type.iteritems():
            print("  %d %s relations" % (len(j_inss), j_type))

        print("%d vars:" % len(self.rqs.requests_vars))
        for k, vs in self.rqs.requests_vars.iteritems():
            print("  %s: %d" % (k, len(vs)))
        print()
        #################

        if len(self.rqs.requestinss) != len(self.tis.threadgroup_by_request):
            print("! WARN !")
            print("%d request instances from %d request groups"
                    % (len(self.rqs.requestinss),
                       len(self.tis.threadgroup_by_request)))
            print()

        if len(self.tis.r_threadinss) != self.tis.sum_threads_in_group:
            print("! WARN !")
            print("%d t_instances in requests, but assume %d"
                    % (len(self.tis.r_threadinss),
                       self.tis.sum_threads_in_group))
            print()

        if len(self.pcs.join_intervals) != len(self.pcs.relations):
            print("! WARN !")
            print("%d relations in requests, but assume %d"
                    % (len(self.pcs.join_intervals),
                       len(self.pcs.relations)))
            print()

        if self.pcs.r_unjoinspaces_by_edge:
            print("! WARN !")
            for edge, paces in self.pcs.r_unjoinspaces_by_edge.iteritems():
                print("%s: %d unjoins paces" % (edge.name, len(paces)))
            print()

        if self.rqs.error_requestinss:
            print("!! ERROR !!")
            print("%d error request instances" % len(self.rqs.error_requestinss))
            for err, requestinss in self.rqs.requests_by_errortype.iteritems():
                print("  %s: %d requests" % (err, len(requestinss)))
            print()

        if self.rqs.main_route_error:
            print("!! ERROR !!")
            print("Tracing errors:")
            for e_msg, where_count in self.rqs.main_route_error.iteritems():
                print("  %s:" % e_msg)
                for where, cnt in where_count.iteritems():
                    print("    %s: %d" % (where, cnt))
            print()

        if self.rqs.warn_requestinss:
            print("! WARN !")
            print("%d warn request instances:" %
                    len(self.rqs.warn_requestinss))
            for warn, requestinss in self.rqs.requests_by_warntype.iteritems():
                print("  %s: %d requests" % (warn, len(requestinss)))
            print()

        if self.tis.r_incomplete_threadinss:
            print("! WARN !")
            print("%d incomplete threadinss in requests" %
                    len(self.tis.r_incomplete_threadinss))
            print()

        if self.tis.r_extra_start_threadinss:
            print("!! ERROR !!")
            print("%d extra start threadinss in requests" %
                    len(self.tis.r_extra_start_threadinss))
            print()

        if self.tis.r_extra_end_threadinss:
            print("!! ERROR !!")
            print("%d extra end threadinss in requests" %
                    len(self.tis.r_extra_end_threadinss))
            print()

        if self.tis.r_stray_threadinss:
            print("!! ERROR !!")
            print("%d stray threadinss in requests" %
                    len(self.tis.r_stray_threadinss))
            print()
