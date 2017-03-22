from collections import defaultdict
from collections import deque

from workflow_parser.log_parser.log_engine import LogCollector
from workflow_parser.log_parser.state_graph import MasterGraph
from workflow_parser.log_parser.state_graph import seen_edges
from workflow_parser.log_parser.state_machine import empty_join
from workflow_parser.log_parser.state_machine import PException
from workflow_parser.log_parser.state_machine import RequestInstance
from workflow_parser.log_parser.state_machine import ThreadInstance


class InstanceCollector(object):
    def __init__(self):
        self.threadinstances = set()

        self.start_threads = []

        self.joins_paces = []
        self.joined_paces = []
        self.joined_paces_by_join = defaultdict(list)

        self.requestinstances = {}
        self.incompletedrequests = {}
        self.cnt_unjoined_by_edgename = defaultdict(lambda:0)

        self.c_vars = defaultdict(set)

    def collect_thread(self, thread):
        assert isinstance(thread, ThreadInstance)

        self.threadinstances.add(thread)
        if thread.is_start:
            self.start_threads.append(thread)

        self.joins_paces.extend(thread.joins_paces)
        self.joined_paces.extend(thread.joined_paces)
        for pace in thread.joined_paces:
            for joined_obj in pace.joined_objs:
                self.joined_paces_by_join[joined_obj].append(pace)
        if thread.joined:
            for pace in thread.joined:
                for join in pace.edge.joined:
                    self.tobejoined_paces_by_join[join].append(pace)
            self.tobejoined_paces.extend(thread.joined)

    def collect_request(self, request):
        assert isinstance(thread, ThreadInstance)

        if request.incomplete_threads:
            self.incompletedrequests[reqeust.request] = request
        elif request.state is None:
            self.incompletedrequests[request.request] = request
        else:
            self.requestinstances[request.request] = request
            for edgename, cnt in request.cnt_unjoined_by_edgename.iteritems():
                self.cnt_unjoined_by_edgename[edgename] += cnt
            self.c_vars["request"].add(request.request)
            for k, vs in request.request_vars.iteritems():
                self.c_vars[k].update(vs)


def parse(log_collector, master_graph):
    assert isinstance(master_graph, MasterGraph)
    assert isinstance(log_collector, LogCollector)

    collector = InstanceCollector()
    # step 1: build thread instances
    not_completed = []
    for f_obj in self.log_collector.logfiles:
        for (thread, loglines) in f_obj.loglines_by_thread.iteritems():
            c_index = 0
            len_index = len(loglines)
            assert len_index > 0
            while c_index != len_index:
                logline = loglines[c_index]
                graph = master_graph.decide_subgraph(logline)

                if not graph:
                    # error
                    print("(ParserEngine) parse error: cannot decide graph")
                    print "-------- LogLines ------"
                    from_t = c_index - 3
                    to_t = c_index + 7
                    if from_t < 0:
                        print "   <start>"
                        from_t = 0
                    else:
                        print "   ..."
                    for i in range(from_t, c_index):
                        print "   %s" % loglines[i]
                    print " > %s" % loglines[c_index]
                    for i in range(c_index+1, min(to_t, len(loglines))):
                        print "   %s" % loglines[i]
                    if to_t >= len(loglines):
                        print "   <end>"
                    else:
                        print "   ..."
                    print "-------- end -----------"
                    raise PException("(ParserEngine) parse error: cannot decide graph")

                thread_obj = ThreadInstance(thread, graph, loglines, c_index)
                if not thread_obj.is_complete:
                    not_completed.append(thread_obj)
                    # NOTE: collect incompleted threads
                    collector.collect_thread(thread_obj)
                else:
                    collector.collect_thread(thread_obj)
                assert c_index < thread_obj.f_index
                c_index = thread_obj.f_index
    print("(ParserEngine) parsed %d thread objects" % len(collector.threadinstances))
    if len(not_completed):
        print("(ParserEngine) WARN! incompleted threads: %d" %
                len(not_completed))

    edges = master_graph.edges - seen_edges
    if edges:
        print_str = "(ParserEngine) WARN! unused edges:"
        for edge in edges:
            print_str += " %s," % edge.name
        print(print_str)

    m_vars = set()
    for thread in collector.threadinstances:
        m_keys = thread.thread_vars_1.keys()
        m_vars.update(m_keys)
    if m_vars:
        print_str = "(ParserEngine) WARN! duplicated vars:"
        for m_var in m_vars:
            print_str += " %s," % m_var
        print(print_str)
    print("(ParserEngine) start_threads: %d" % len(collector.start_threads))
    print("(ParserEngine) tojoin_paces: %d" % len(collector.tojoin_paces))
    print("(ParserEngine) tobejoined_paces: %d" % len(collector.tobejoined_paces))
    print("Parse threads ok..\n")

    # step 2: join threads by schema
    collector.tojoin_paces.sort(key=lambda p: p["seconds"])
    collector.tobejoined_paces.sort(key=lambda p: p["seconds"])

    for join, p_list in collector.tobejoined_paces_by_join.iteritems():
        p_list.sort(key=lambda p: p["seconds"])
        collector.tobejoined_paces_by_join[join] = deque(p_list)

    not_joined = []
    join_cnt = defaultdict(lambda: [0, 0])
    for tojoin_pace in collector.tojoin_paces:
        join_objs = tojoin_pace.edge.joins
        assert len(join_objs) == 1
        join_obj = list(join_objs)[0]
        schemas = join_obj.schemas[:]
        target_paces = collector.tobejoined_paces_by_join[join_obj]
        assert isinstance(target_paces, deque)

        while target_paces and target_paces[0].joined_prv is not None:
            target_paces.popleft()

        target_pace = None
        from_schema = {}
        to_schema = defaultdict(set)
        from_schema["request"] = tojoin_pace["request"]
        # FIXME is shared?
        if not join_obj.is_remote:
            schemas.append(("host", "host"))
        if schemas:
            for schema in schemas:
                from_schema[schema[0]] = tojoin_pace[schema[0]]
        for pace in target_paces:
            if pace.joined_prv is not None:
                continue

            if schemas:
                match = True
                for schema in schemas:
                    to_schema[schema[1]].add(pace[schema[1]])
                    if tojoin_pace[schema[0]] != pace[schema[1]]:
                        match = False
                        break
                if not match:
                    continue

            from_req = tojoin_pace["request"]
            to_req = pace["request"]
            to_schema["request"].add(to_req)
            if not from_req or not to_req or from_req == to_req:
                target_pace = pace
                break

        if target_pace:
            tojoin_pace.join(target_pace, join_obj)
            join_cnt[join_obj.name][0] += 1
        else:
            # debug joins
            # print("from_schema: %s" % from_schema)
            # print("to_schema:")
            # for k, v in to_schema.iteritems():
            #     print("  %s: %s" % (k, v))
            # import pdb; pdb.set_trace()
            tojoin_pace.joined_nxt = empty_join
            not_joined.append(tojoin_pace)
            # FIXME: show join name instead of edge name
            join_cnt[join_obj.name][1] += 1

    not_beenjoined = []
    for tobejoin in collector.tobejoined_paces:
        if tobejoin.joined_prv is None:
            not_beenjoined.append(tobejoin)
            tobejoin.joined_prv = empty_join

    print("(ParserEngine) join summary:")
    for k, (cnt1, cnt2) in join_cnt.iteritems():
        unjoined_str = ""
        if cnt2:
            unjoined_str += ", WARN! %d unjoined" % cnt2
        print("  %s: %d joined%s" % (k, cnt1, unjoined_str))

    if not_joined or not_beenjoined:
        print("(ParserEngine) WARN! not join: %d paces" % len(not_joined))
        print("(ParserEngine) WARN! not been joined: %d paces"
                % len(not_beenjoined))
        join_cnt = defaultdict(lambda:0)
        for n_j in not_beenjoined:
            for j in n_j.edge.joined:
                join_cnt[j.name] += 1
        for name, cnt in join_cnt.iteritems():
            print("  Join#%s has %d unjoined!" % (name, cnt))
    print("Join threads ok..\n")

    # step 3: group requests
    seen_thread = set()
    def group(thread, t_set):
        if thread.is_shared:
            t_set.add(thread)
            return set()

        if thread in seen_thread:
            return set()
        seen_thread.add(thread)
        t_set.add(thread)
        ret = set()
        if thread.request:
            ret.add(thread.request)

        for pace in thread.joins:
            if pace.joined_nxt and pace.joined_nxt is not empty_join:
                ret.update(group(pace.joined_nxt.to_thread_obj, t_set))
        for pace in thread.joined:
            if pace.joined_prv and pace.joined_prv is not empty_join:
                ret.update(group(pace.joined_prv[0].from_thread_obj, t_set))
        return ret

    threads_by_request = defaultdict(set)
    unidentified_group = []
    cnt_unidentified_threads = 0
    for thread in collector.threadinstances:
        if not thread.is_shared and thread not in seen_thread:
            new_t_set = set()
            requests = group(thread, new_t_set)
            len_req = len(requests)
            if len_req > 1:
                raise PException("(ParserEngine) detected %d requests in a group" % len_req)
            elif len_req == 1:
                threads_by_request[requests.pop()].update(new_t_set)
            else:
                unidentified_group.append(new_t_set)
                cnt_unidentified_threads += len_req
    print("(ParserEngine) detected %d request groups" % len(threads_by_request))
    if unidentified_group:
        print("(ParserEngine) WARN! unidentified groups: %d" % len(unidentified_group))
        print("(ParserEngine) WARN! total threads: %d" % cnt_unidentified_threads)
    print("Group requests ok..\n")

    # step 4: build requests
    errors = defaultdict(list)
    for request, threads in threads_by_request.iteritems():
        try:
            request_obj = RequestInstance(master_graph, request, threads)
        except PException as e:
            errors[e.message].append(reqeust_obj)
        collector.collect_request(request_obj)

    if errors:
        for err, r_l in errors.iteritems():
            print("(ParserEngine) WARN! %d errors: %s" % (len(r_l), err))
    if collector.incompletedrequests:
        print("(ParserEngine) WARN! incomplete: %s" %
                collector.incompletedrequests.keys())
    if collector.cnt_unjoined_by_edgename:
        # FIXME: show join name instead of edge name
        for name, cnt in collector.cnt_unjoined_by_edgename.iteritems():
            print("(ParserEngine) WARN! %d unjoined from edge %s" %
                    (cnt, name))
    print("(ParserEngine) built %d correct requests!" %
            len(collector.requestinstances))
    print("(ParserEngine) vars summary: %d" % len(collector.c_vars))
    for k, vs in collector.c_vars.iteritems():
        print("  %s: %d" % (k, len(vs)))
    print("Build requests ok..\n")

    return collector
