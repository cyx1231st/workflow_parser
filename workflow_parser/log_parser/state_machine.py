import abc
from collections import defaultdict

from state_graph import MasterGraph
from state_graph import Node

from workflow_parser.log_parser.exception import WFException
from workflow_parser.log_parser.state_graph import LeafGraph
from workflow_parser.log_parser.state_graph import Node
from workflow_parser.log_parser.state_graph import Edge
from workflow_parser.log_parser.log_parser import LogLine


empty_join = object()


class PException(WFException):
    pass


class Pace(object):
    """ Pace is relative to transition. """
    def __init__(self, log, from_node, edge, thread_obj):
        assert isinstance(log, LogLine)
        assert isinstance(from_node, Node)
        assert isinstance(edge, Edge)
        assert isinstance(thread_obj, ThreadInstance)

        self.log = log
        self.from_node = from_node
        self.to_node = edge.node
        self.edge = edge
        self.thread_obj = thread_obj

        self.prv = None
        self.nxt = None

        self.joined_prv = None
        self.joined_nxt = None

    def __getitem__(self, item):
        if item in self.log.f_get_keys(True):
            return self.log[item]
        elif item in self.thread_obj.thread_vars:
            return self.thread_obj.thread_vars[item]
        elif item in self.thread_obj.thread_vars_1:
            raise PException("(Pace) got multiple %s: %s" %
                    (item, self.thread_obj.thread_vars_1[item]))
        else:
            raise PException("(Pace) %s not exist!" % item)

    def __str__(self):
        return "<Pace %.3f [%s %s %s], %s %s>" % (
                self.log.seconds,
                self.from_node.name,
                self.edge.name,
                self.to_node.name,
                self.log.thread,
                self.log.request)

    def __repr__(self):
        ret_str = str(self)
        ret_str += "\n  %s" % self.log.ll_line
        return ret_str

"""
class RemotePace(PaceBase):
    def __init__(self, sub_instance, from_pace=None):
        super(NestedPace, self).__init__(
            sub_instance, sub_instance.graph, sub_instance.from_node,
            sub_instance.to_node, from_pace)

    @property
    def sub_instance(self):
        return self.content

    @property
    def assume_host(self):
        return self.content.assume_host

    @property
    def from_seconds(self):
        return self.sub_instance.from_seconds

    @property
    def to_seconds(self):
        return self.sub_instance.to_seconds

    def connect(self, p):
        super(NestedPace, self).connect(p)
        self.sub_instance.connect(p.sub_instance)

    def confirm_pace(self, ins):
        super(NestedPace, self).confirm_pace(ins)
        host = self.assume_host
        if host is not None and ins.host != host:
            return None

        if self.to_node.accept_edge(ins.from_edge):
            p = NestedPace(ins, self)
            return p
        else:
            return None

    # NestedPace
    def __repr__(self):
        ret_str = "<NestPace ins:%r>" % self.sub_instance
        return ret_str

    def __str__(self):
        ret_str = "%r:" % self
        ret_str += "\n%s" % self.sub_instance
        return ret_str
"""


class InstanceBase(object):
    def __init__(self, graph, ident):
        self.graph = graph
        self.ident = ident

        self.from_pace = None
        self.to_pace = None

        self.fail_message = ""

    @property
    def from_node(self):
        return self.from_pace.from_node

    @property
    def to_node(self):
        if self.to_pace is None:
            return None
        else:
            return self.to_pace.to_node

    @property
    def start_leaf_pace(self):
        return None

    @property
    def from_seconds(self):
        return self.from_pace.from_seconds

    @property
    def to_seconds(self):
        return self.to_pace.to_seconds

    @property
    def is_end(self):
        if self.to_pace and self.to_pace.to_node in self.graph.end_nodes:
            return True
        else:
            return False

    @property
    def is_failed(self):
        return bool(self.fail_message) or not self.is_end

    @property
    def state(self):
        if not self.to_node:
            return "UNKNOWN"
        else:
            state = self.to_node.state
            if state is Node.UNKNOWN_STATE:
                return "-"
            else:
                return state

    @property
    def assume_host(self):
        if not self.is_end or self.to_pace is None:
            return None
        else:
            return self.to_pace.assume_host

    @property
    def name(self):
        return self.graph.name

    def iterall(self):
        p = self.start_leaf_pace
        while p:
            yield p
            if p.to_node in self.graph.end_nodes:
                break
            p = p.nxt

    def __iter__(self):
        p = self.from_pace
        while p:
            yield p.content
            if p is self.to_pace:
                break
            p = p.nxt

    def __str__(self):
        ret_str = "%r:" % self

        ret_str += "\nPaces:"
        p = self.from_pace
        while p:
            ret_str += "\n    %r" % p
            if p is self.to_pace:
                break
            p = p.nxt
        ret_str += "\n"
        return ret_str


class ThreadInstance(object):
    def __init__(self, thread, graph, loglines, s_index):
        assert isinstance(thread, str)
        assert isinstance(graph, LeafGraph)
        assert isinstance(loglines, list)
        assert isinstance(s_index, int)
        s_log = loglines[s_index]

        self.thread = thread
        self.graph = graph
        self.loglines = loglines

        self.is_shared = graph.is_shared
        if self.is_shared:
            self.requests = set()
            self.request_objs = set()
        else:
            self.request = None
            self.request_obj = None

        self.component = s_log.component
        self.host = s_log.host
        self.target = s_log.target

        self.s_index = s_index
        self.f_index = None

        self.paces = []

        self.joins = []
        self.joined = []

        # init
        node = graph.decide_node(s_log)
        index = s_index
        while index != len(loglines):
            log = loglines[index]
            edge = node.decide_edge(log)
            if not edge:
                break
            pace = Pace(log, node, edge, self)
            if edge.joins:
                self.joins.append(pace)
            if edge.joined:
                self.joined.append(pace)
            self.paces.append(pace)
            # if log.request is not None:
            #     if self.request is not None:
            #         assert self.request == log.request
            #     else:
            #         self.request = log.request
            node = edge.node
            index += 1

        # check
        if not self.paces or not self.paces[-1].to_node.is_end:
            print "(ThreadInstance) parse error: partial parse"
            print "-------- Thread --------"
            print "%r" % self
            print "-------- LogLines ------"
            from_ = s_index
            to_ = index - 1
            from_t = s_index - 3
            to_t = index + 7
            if from_t < 0:
                print "  <start>"
                from_t = 0
            else:
                print "  ..."
            for i in range(from_t, from_):
                print "  %s" % loglines[i]
            for i in range(from_, to_-1):
                print "| %s" % loglines[i]
            print "|>%s" % loglines[to_]
            for i in range(to_+1, min(to_t, len(loglines))):
                print "  %s" % loglines[i]
            if to_t >= len(loglines):
                print "  <end>"
            else:
                print "  ..."
            print "-------- end -----------"
            raise PException("(ThreadInstance) parse error")

        # set others
        self.f_index = index
        prv = None
        for pace in self.paces:
            pace.prv = prv
            if prv:
                prv.nxt = pace
            prv = pace

        # check and set thread vars
        v_dict = defaultdict(set)
        for pace in self.paces:
            log = pace.log
            keys = log.f_get_keys(res=True)
            for key in keys:
                v_dict[key].add(log[key])

        for name in ("keyword", "time", "seconds"):
            v_dict.pop(name)
        for name in ("component", "target", "host", "thread"):
            values = v_dict.pop(name)
            if not len(values) == 1 or values.pop() != getattr(self, name):
                print("(ThreadInstance) parse error: variable mismatch")
                print("-------- Thread --------")
                print("%r" % self)
                print("-------- Desc ----------")
                print("key %s not match: [%r] %r" % (
                    name, getattr(self, name), values))
                print("-------- end -----------")
                raise PException("(ThreadInstance) parse error: variable mismatch")

        requests = v_dict.pop("request")
        if None in requests:
            requests.discard(None)
        if self.is_shared:
            self.requests.update(requests)
        else:
            if len(requests) == 0:
                pass
            elif len(requests) == 1:
                self.request = requests.pop()
            else:
                raise PException("(ThreadInstance) thread is not shared!")

        # generate vars
        self.thread_vars = {}
        self.thread_vars_1 = {}
        for k, v in v_dict.iteritems():
            if len(v) == 0:
                pass
            elif len(v) == 1:
                self.thread_vars[k] = v.pop()
            else:
                self.thread_vars_1[k] = v

    @property
    def is_start(self):
       return self.paces[0].from_node.is_g_start

    def __str__(self):
        return "<ThIns#%s(%s, %s) [%s %s %s]: graph %s, %d paces>" % (
                self.thread,
                self.s_index,
                self.f_index,
                self.component,
                self.host,
                self.target,
                self.graph.name,
                len(self.paces))

    def __repr__(self):
        ret_str = str(self)
        ret_str += "\n%r" % self.graph
        ret_str += "\nPaces:"
        for pace in self.paces:
            ret_str += "\n| %s" % pace
        return ret_str

    # @property
    # def from_edge(self):
    #     return self.from_pace.edge

    # @property
    # def sort_key(self):
    #     return self.from_pace.log.seconds

    # @property
    # def service(self):
    #     return self.graph.service

    # @property
    # def start_leaf_pace(self):
    #     return self.from_pace

    # def connect(self, ins):
    #     self.to_pace.connect(ins.from_pace)

    # def confirm(self, log):
    #     assert self.host == log.host

    #     if self.ident != log.ident:
    #         return False

    #     if not self.is_end:
    #         p = None
    #         if self.to_pace is None:
    #             node, edge = self.graph.decide_node_edge(log)
    #             if node and edge:
    #                 p = LeafPace(log, node, edge, None)
    #                 self.from_pace = p
    #         else:
    #             p = self.to_pace.confirm_pace(log)

    #         if p:
    #             self.to_pace = p
    #             return True

    #     # extra edges handling
    #     edge = self.graph.decide_edge_ignored(log)
    #     if edge is not None:
    #         if edge not in self.extra_logs:
    #             assert self.host == log.host
    #             self.extra_logs[edge] = log
    #             return True
    #         else:
    #             return False
    #     else:
    #         return False



class RequestInstance(InstanceBase):
    def __init__(self, graph, start_thread_obj):
        assert isinstance(graph, MasterGraph)
        assert isinstance(start_thread_obj, ThreadInstance)
        self.start_thread = start_thread_obj
        self.graph = graph
        self.instances = set()
        self.thread = None


    @property
    def start_leaf_pace(self):
        return self.from_pace.content.from_pace

    def confirm(self, ins):
        if self.ident != ins.ident:
            return False

        if not self.is_end:
            p = None
            if self.to_pace is None:
                # TODO self.graph.
                for node in self.graph.start_nodes:
                    if node.accept_edge(ins.from_edge):
                        p = NestedPace(ins)
                        self.from_pace = p
                        break
            else:
                p = self.to_pace.confirm_pace(ins)

            if p:
                self.to_pace = p
                if not ins.is_end:
                    raise ParseError("Instance %r is not complete!" % ins)
                else:
                    return True

        return False

    def assume_graphs(self):
        """ Acceptable graphs """
        graphs = set()
        if self.is_end:
            return graphs

        if self.to_pace is None:
            for node in self.graph.start_nodes:
                for edge in node.edges:
                    graphs.add(edge.graph)
        else:
            node = self.to_pace.to_node
            for edge in node.edges:
                graphs.add(edge.graph)
        return graphs

    def __repr__(self):
        ret_str = "<NestedIns ident:%s graph:%s end:%s state:%s>" \
                  % (self.ident, self.graph.name, self.is_end, self.state)
        return ret_str

    def __str__(self):
        ret_str = ">>------------\n"
        ret_str += super(NestedInstance, self).__str__()

        p = self.from_pace
        while p:
            ret_str += "\n%s" % p.sub_instance
            if p is self.to_pace:
                break
            p = p.nxt
        return ret_str
