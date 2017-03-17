from collections import defaultdict

from workflow_parser.log_parser.log_parser import LogCollector
from workflow_parser.log_parser.state_graph import MasterGraph
from workflow_parser.log_parser.state_graph import seen_edges
from workflow_parser.log_parser.state_machine import ThreadInstance
from workflow_parser.log_parser.state_machine import PException


class InstanceCollector(object):
    def __init__(self):
        self.threadinstances = set()

    def collect_thread(self, thread):
        assert isinstance(thread, ThreadInstance)
        self.threadinstances.add(thread)
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

        edges = self.graph.edges - seen_edges
        if edges:
            print_str = "(ParserEngine) warn unused edges:"
            for edge in edges:
                print_str += " %s," % edge.name
            print(print_str)

        m_vars = set()
        for thread in collector.threadinstances:
            m_keys = thread.thread_vars_1.keys()
            m_vars.update(m_keys)
        if m_vars:
            print_str = "(ParserEngine) warn duplicated vars:"
            for m_var in m_vars:
                print_str += " %s," % m_var
            print(print_str)

        # step 2: build request instances

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
