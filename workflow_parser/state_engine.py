from collections import defaultdict
from collections import deque
from collections import OrderedDict

from workflow_parser.log_engine import TargetsCollector
from workflow_parser.log_parser import LogLine
from workflow_parser.state_graph import MasterGraph
from workflow_parser.state_graph import seen_edges
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

        self.joins_edges = set()
        self.joinspaces_by_edge = defaultdict(list)
        self.unjoinspaces_by_edge = defaultdict(list)

        self.joined_edges = set()
        self.joinedpaces_by_edge = defaultdict(list)
        self.unjoinedpaces_by_edge = defaultdict(list)

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
            self.joins_edges.add(pace.edge)
            if pace.joins_int is empty_join:
                self.unjoinspaces_by_edge[pace.edge].append(pace)
            elif isinstance(pace.joins_int, JoinInterval):
                self.joinspaces_by_edge[pace.edge].append(pace)
            else:
                assert False

        for pace in self.joined_paces:
            self.joined_edges.add(pace.edge)
            if pace.joined_int is empty_join:
                self.unjoinedpaces_by_edge[pace.edge].append(pace)
            elif isinstance(pace.joined_int, JoinInterval):
                self.joinedpaces_by_edge[pace.edge].append(pace)
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
    def __init__(self, pcs_collector):
        assert isinstance(pcs_collector, PacesCollector)

        self.pcs_collector = pcs_collector

        self.incomplete_threadinss_by_graph = defaultdict(list)
        self.complete_threadinss_by_graph = defaultdict(list)
        self.start_threadinss = []
        self.duplicated_vars = set()

        self.threadinss = []

        self.threadgroup_by_request = defaultdict(set)
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

        self.pcs_collector.collect_join(threadins)

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
    def __init__(self, tis_collector, pcs_collector):
        assert isinstance(tis_collector, ThreadInssCollector)
        assert isinstance(pcs_collector, PacesCollector)

        self.tis_collector = tis_collector
        self.pcs_collector = pcs_collector

        self.requestinss = {}
        self.requests_vars = defaultdict(set)

        # error report
        self.error_requestinss = []
        self.requests_by_errortype = defaultdict(list)

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
        else:
            assert requestins.request not in self.requestinss
            self.requestinss[requestins.request] = requestins
            self.requests_vars["request"].add(requestins.request)
            for k, vs in requestins.request_vars.iteritems():
                self.requests_vars[k].update(vs)

        self.tis_collector.collect_request(requestins)
        self.pcs_collector.collect_request(requestins)


# TODO: add count checks
def state_parse(tgs_collector, master_graph):
    assert isinstance(master_graph, MasterGraph)
    assert isinstance(tgs_collector, TargetsCollector)

    pcs_collector = PacesCollector()
    tis_collector = ThreadInssCollector(pcs_collector)
    rqs_collector = RequestsCollector(tis_collector, pcs_collector)

    # step 1: build thread instances
    print("Build thread instances...")
    for f_obj in tgs_collector.itervalues():
        for (thread, loglines) in f_obj.loglines_by_thread.iteritems():
            c_index = 0
            len_index = len(loglines)
            assert len_index > 0
            pcs_collector.ignored_loglines_total_by_component[f_obj.component][1] += len_index
            while c_index != len_index:
                logline = loglines[c_index]
                graph = master_graph.decide_threadgraph(logline)

                if not graph:
                    # error
                    # print("(ParserEngine) parse error: cannot decide graph")
                    # report_loglines(loglines, c_index)
                    # print "-------- end -----------"
                    # raise StateError("(ParserEngine) parse error: cannot decide graph")
                    pcs_collector.collect_ignored(logline, loglines, c_index,
                            thread, f_obj.component, f_obj.target)
                    logline.ignored = True
                    c_index += 1
                    continue

                threadins = ThreadInstance(thread, graph, loglines, c_index)
                tis_collector.collect_thread(threadins)
                assert c_index < threadins.f_index
                c_index = threadins.f_index

    print("(ParserEngine) ignored summary:")

    def _report_ignored(tup):
        # (logline, loglines, index, thread, component, target)
        print("    example:")
        report_loglines(tup[1], tup[2], blanks=4, printend=True)

    for comp, ltup in pcs_collector.ignored_loglines_total_by_component.iteritems():
        print("  %s: total %d loglines, %d ignored" % (comp, ltup[1], len(ltup[0])))
        if len(ltup[0]):
            _report_ignored(ltup[0][0])
    print("(ParserEngine) parsed %d thread instances" % len(tis_collector.threadinss))
    if tis_collector.complete_threadinss_by_graph:
        print("(ParserEngine) complete:")
        for gname, tis in tis_collector.complete_threadinss_by_graph.iteritems():
            print("  %s: %d instances" % (gname, len(tis)))
    if tis_collector.incomplete_threadinss_by_graph:
        print("(ParserEngine) WARN! incompleted:")
        for gname, tis in tis_collector.incomplete_threadinss_by_graph.iteritems():
            print("  %s: %d instances" % (gname, len(tis)))

    edges = master_graph.edges - seen_edges
    if edges:
        print("(ParserEngine) WARN! unused edges: %s" %
                " ".join(edge.name for edge in edges))

    if tis_collector.duplicated_vars:
        print("(ParserEngine) WARN! duplicated vars: %s" %
                " ".join(tis_collector.duplicated_vars))
    print("(ParserEngine) request_start_threads: %d" %
            len(tis_collector.start_threadinss))
    print("(ParserEngine) joins_paces: %d" % len(pcs_collector.joins_paces))
    print("(ParserEngine) joined_paces: %d" % len(pcs_collector.joined_paces))
    print("ok\n")

    # step 2: join paces by schema
    print("Join paces...")
    pcs_collector.joins_paces.sort()
    pcs_collector.joined_paces.sort()
    for jo, p_list in pcs_collector.joined_paces_by_jo.iteritems():
        p_list.sort()
        pcs_collector.joined_paces_by_jo[jo] = OrderedDict.fromkeys(p_list)
    for jo, paces_by_host in pcs_collector.joined_paces_by_jo_host.iteritems():
        for host, paces in paces_by_host.iteritems():
            paces.sort()
            pcs_collector.joined_paces_by_jo_host[jo][host] = OrderedDict.fromkeys(paces)

    join_attempt_cnt = 0
    for joins_pace in pcs_collector.joins_paces:
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
                target_paces = pcs_collector.joined_paces_by_jo[join_obj]
            else:
                target_paces = pcs_collector.joined_paces_by_jo_host[join_obj][care_host]
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

    for joined_pace in pcs_collector.joined_paces:
        if joined_pace.joined_int is None:
            joined_pace.joined_int = empty_join

    pcs_collector.collect_unjoin()
    print("(ParserEngine) %d join attempts" % join_attempt_cnt)
    if pcs_collector.joins_edges:
        print("(ParserEngine) joins edges summary:")
        for edge in pcs_collector.joins_edges:
            joins_paces = pcs_collector.joinspaces_by_edge[edge]
            notjoins_paces = pcs_collector.unjoinspaces_by_edge[edge]
            warn_str = ""
            if notjoins_paces:
                warn_str += ", WARN! %d unjoins paces" % len(notjoins_paces)
            print("  %s: %d%s" % (edge.name, len(joins_paces), warn_str))
    if pcs_collector.joined_edges:
        print("(ParserEngine) joined edges summary:")
        for edge in pcs_collector.joined_edges:
            joined_paces = pcs_collector.joinedpaces_by_edge[edge]
            notjoined_paces = pcs_collector.unjoinedpaces_by_edge[edge]
            warn_str = ""
            if notjoined_paces:
                warn_str += ", WARN! %d unjoined paces" % len(notjoined_paces)
            print("  %s: %d%s" % (edge.name, len(joined_paces), warn_str))
    print("ok\n")

    # step 3: group threads by request
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

    threadinss = []
    for threadins in tis_collector.threadinss:
        if threadins.is_shared:
            generated = NestedRequest.generate(threadins)
            threadinss.extend(generated)
        else:
            threadinss.append(threadins)

    for threadins in threadinss:
        if not threadins.is_shared and threadins not in seen_threadinss:
            new_t_set = set()
            requests = group(threadins, new_t_set)
            tis_collector.collect_thread_group(requests, new_t_set)

    print("(ParserEngine) detected %d groups of %d threadinstances" % (
            len(tis_collector.threadgroup_by_request),
            sum(len(tgroup) for tgroup in
                tis_collector.threadgroup_by_request.itervalues())))
    if tis_collector.threadgroups_with_multiple_requests:
        print("(ParserEngine) ERROR! %d groups have multiple requests" % (
                len(tis_collector.threadgroups_with_multiple_requests)))
        for i_group, reqs in tis_collector.threadgroups_with_multiple_requests:
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
        raise StateError("(ParserEngine) thread group has multiple requests!")
    if tis_collector.threadgroups_without_request:
        print("(ParserEngine) WARN! cannot identify request: %d groups of %d threadinstances" % (
                len(tis_collector.threadgroups_without_request),
                sum(len(tgroup) for tgroup in
                    tis_collector.threadgroups_without_request)))
    print("ok\n")

    # step 4: build requests
    print("Build requests...")
    for request, threads in tis_collector.threadgroup_by_request.iteritems():
        request_obj = RequestInstance(master_graph, request, threads)
        rqs_collector.collect_request(request_obj)

    if rqs_collector.error_requestinss:
        print("(ParserEngine) ERROR! %d error request instances" %
                len(rqs_collector.error_requestinss))
        for err, requestinss in rqs_collector.requests_by_errortype.iteritems():
            print("  %s: %d requests" % (err, len(requestinss)))

    if rqs_collector.warn_requestinss:
        print("(ParserEngine) WARN! %d warn request instances" %
                len(rqs_collector.warn_requestinss))
        for warn, requestinss in rqs_collector.requests_by_warntype.iteritems():
            print("  %s: %d requests" % (warn, len(requestinss)))

    if tis_collector.r_incomplete_threadinss:
        print("(ParserEngine) WARN! %d incomplete threadinss in requests" %
                len(tis_collector.r_incomplete_threadinss))
    if tis_collector.r_extra_start_threadinss:
        print("(ParserEngine) ERROR! %d extra start threadinss in requests" %
                len(tis_collector.r_extra_start_threadinss))
    if tis_collector.r_extra_end_threadinss:
        print("(ParserEngine) ERROR! %d extra end threadinss in requests" %
                len(tis_collector.r_extra_end_threadinss))
    if tis_collector.r_stray_threadinss:
        print("(ParserEngine) ERROR! %d stray threadinss in requests" %
                len(tis_collector.r_stray_threadinss))

    print("(ParserEngine) relation summary:")
    if pcs_collector.r_unjoinspaces_by_edge:
        print("  WARN! unjoins paces in requests")
        for edge, paces in pcs_collector.r_unjoinspaces_by_edge.iteritems():
            print("    %s: %d paces" % (edge.name, len(paces)))
    for j_type, j_inss in pcs_collector.join_intervals_by_type.iteritems():
        print("  %d %s relations" % (len(j_inss), j_type))

    print("(ParserEngine) vars summary: %d" % len(rqs_collector.requests_vars))
    for k, vs in rqs_collector.requests_vars.iteritems():
        print("  %s: %d" % (k, len(vs)))
    print("(ParserEngine) built %d request instances with %d thread instances"
            % (len(rqs_collector.requestinss),
               len(tis_collector.r_threadinss)))
    print("ok\n")

    return pcs_collector, tis_collector, rqs_collector
