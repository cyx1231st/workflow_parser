import bisect
from collections import defaultdict
from collections import deque

from workflow_parser.log_parser.log_parser import LogCollector
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
        self.tojoin_paces = []
        self.tobejoined_paces = []
        self.tobejoined_paces_by_join = defaultdict(list)

    def collect_thread(self, thread):
        assert isinstance(thread, ThreadInstance)
        self.threadinstances.add(thread)
        if thread.is_start:
            self.start_threads.append(thread)

        if thread.joins:
            self.tojoin_paces.extend(thread.joins)
        if thread.joined:
            for pace in thread.joined:
                for join in pace.edge.joined:
                    self.tobejoined_paces_by_join[join].append(pace)
            self.tobejoined_paces.extend(thread.joined)
    #     self.ident = ident
    #     self.instances_by_service_host = \
    #         defaultdict(lambda: defaultdict(list))
    #     self.instances_by_graph_host = \
    #         defaultdict(lambda: defaultdict(list))
    #     self.instances_by_graph = defaultdict(list)
    #     self.instance_set = set()

    # def iter_by_sh(self, service, host):
    #     for ins in self.instances_by_service_host[service][host]:
    #         if ins._available:
    #             yield ins

    # def iter_by_graph(self, graph, host):
    #     if host is None:
    #         for ins in self.instances_by_graph[graph]:
    #             if ins._available:
    #                 yield ins
    #     else:
    #         for ins in self.instances_by_graph_host[graph][host]:
    #             if ins._available:
    #                 yield ins

    # def add(self, ins):
    #     assert ins.ident == self.ident
    #     assert isinstance(ins, LeafInstance)
    #     ins._available = True
    #     self.instances_by_service_host[ins.service][ins.host].append(ins)
    #     self.instances_by_graph_host[ins.graph][ins.host].append(ins)
    #     self.instances_by_graph[ins.graph].append(ins)
    #     self.instance_set.add(ins)

    # def sort(self):
    #     for instances in self.instances_by_graph.itervalues():
    #         instances.sort(key=lambda ins: ins.sort_key)

    # def remove(self, ins):
    #     assert ins in self.instance_set
    #     ins._available = False
    #     self.instance_set.remove(ins)

    # def __bool__(self):
    #     return bool(self.instance_set)
    # __nonzero__ = __bool__


class ParserEngine(object):
    def __init__(self, graph, log_collector):
        assert isinstance(graph, MasterGraph)
        assert isinstance(log_collector, LogCollector)
        self.graph = graph
        self.log_collector = log_collector

    def parse(self):
        collector = InstanceCollector()
        # step 1: build thread instances
        for f_obj in self.log_collector.logfiles:
            for (thread, loglines) in f_obj.loglines_by_thread.iteritems():
                c_index = 0
                len_index = len(loglines)
                assert len_index > 0
                while c_index != len_index:
                    logline = loglines[c_index]
                    graph = self.graph.decide_subgraph(logline)

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
                    collector.collect_thread(thread_obj)
                    assert c_index < thread_obj.f_index
                    c_index = thread_obj.f_index
        print("(ParserEngine) parsed %d threads" % len(collector.threadinstances))

        edges = self.graph.edges - seen_edges
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
        for tojoin in collector.tojoin_paces:
            joins = tojoin.edge.joins
            assert len(joins) == 1
            join = list(joins)[0]
            schemas = join.schemas
            target_paces = collector.tobejoined_paces_by_join[join]
            assert isinstance(target_paces, deque)

            while target_paces and target_paces[0].joined_prv is not None:
                target_paces.popleft()

            target_pace = None
            for pace in target_paces:
                if pace.joined_prv is not None:
                    continue

                match = True
                if schemas:
                    for schema in schemas:
                        if tojoin[schema[0]] != pace[schema[1]]:
                            match = False
                            break

                from_req = tojoin["request"]
                to_req = pace["request"]
                if from_req == to_req:
                    target_pace = pace
                    break

            if target_pace:
                tojoin.joined_nxt = target_pace
                target_pace.joined_prv = tojoin
            else:
                tojoin.joined_nxt = empty_join
                not_joined.append(tojoin)

        not_beenjoined = []
        for tobejoin in collector.tobejoined_paces:
            if tobejoin.joined_prv is None:
                not_beenjoined.append(tobejoin)
                tobejoin.joined_prv = empty_join

        if not_joined or not_beenjoined:
            # print("\n(ParserEngine) WARN! not join: %d paces" % len(not_joined))
            # for n_j in not_joined[:3]:
            #     print("%r" % n_j)
            # print("......")
            join_cnt = defaultdict(lambda:0)
            for n_j in not_joined:
                for j in n_j.edge.joins:
                    join_cnt[j.name] += 1
            for name, cnt in join_cnt.iteritems():
                print("  Join#%s has %d unjoin!" % (name, cnt))
            print("(ParserEngine) WARN! not been joined: %d paces"
                    % len(not_beenjoined))
            join_cnt = defaultdict(lambda:0)
            for n_j in not_beenjoined:
                for j in n_j.edge.joins:
                    join_cnt[j.name] += 1
            for name, cnt in join_cnt.iteritems():
                print("  Join#%s has %d unjoined!" % (name, cnt))
        print("Join threads ok..\n")

        # step 3: build requests
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
                    ret.update(group(pace.joined_nxt.thread_obj, t_set))
            for pace in thread.joined:
                if pace.joined_prv and pace.joined_prv is not empty_join:
                    ret.update(group(pace.joined_prv.thread_obj, t_set))
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
        print("(ParserEngine) detected %d requests" % len(threads_by_request))
        if unidentified_group:
            print("(ParserEngine) WARN! unidentified groups: %d" % len(unidentified_group))
            print("(ParserEngine) WARN! total threads: %d" % cnt_unidentified_threads)
        print("Build requests ok..\n")




        # instances = {}
        # for ident, helper in helpers_by_ident.iteritems():
        #     instance = NestedInstance(self.graph, ident)

        #     try:
        #         while True:
        #             graphs = instance.assume_graphs()
        #             if not graphs:
        #                 break

        #             try:
        #                 for graph in graphs:
        #                     host = instance.assume_host
        #                     for ins in helper.iter_by_graph(graph, host):
        #                         if instance.confirm(ins):
        #                             helper.remove(ins)
        #                             raise BreakIt()
        #             except BreakIt:
        #                 pass
        #             else:
        #                 instance.fail_message = "%r cannot find next LeafInstance!" \
        #                                         % instance
        #                 break
        #     except ParseError as e:
        #         instance.fail_message = e.message

        #     if instance.fail_message:
        #         pass
        #     elif helper:
        #         instance.fail_message = "%r has unexpected instances!" \
        #                                 % instance
        #     elif instance.is_end is False:
        #         instance.fail_message = "%r is not ended!" % instance

        #     if instance.is_failed:
        #         print "PARSE FAIL >>>>>>>>>>>>>>>>>>>"
        #         print "Fail message"
        #         print "------------"
        #         print instance.fail_message
        #         print ""
        #         print "Parsed instance"
        #         print "---------------"
        #         print instance
        #         if helper:
        #             print("Unexpected instances")
        #             print "--------------------"
        #             for ins in helper.instance_set:
        #                 print("\n%s" % ins)

        #     instances[ident] = instance

        return collector
