import abc
from collections import defaultdict
from functools import total_ordering

from workflow_parser.log_parser import reserved_vars as rv
from workflow_parser.log_parser.exception import WFException
from workflow_parser.log_parser.state_graph import ThreadGraph
from workflow_parser.log_parser.state_graph import MasterGraph
from workflow_parser.log_parser.state_graph import Node
from workflow_parser.log_parser.state_graph import Edge
from workflow_parser.log_parser.log_parser import LogLine
from workflow_parser.log_parser.state_graph import Join
from workflow_parser.log_parser.utils import report_loglines


empty_join = object()


class StateError(WFException):
    pass


# TODO: support rvs
@total_ordering
class Pace(object):
    """ Pace is relative to transition. """
    def __init__(self, logline, from_node, edge, threadins):
        assert isinstance(logline, LogLine)
        assert isinstance(from_node, Node)
        assert isinstance(edge, Edge)
        assert isinstance(threadins, ThreadInstance)

        self.logline = logline
        self.from_node = from_node
        self.to_node = edge.node
        self.edge = edge
        self.threadins = threadins

        self.prv_pace = None
        self.nxt_pace = None

        self.joins_relation = None
        self.joined_relation = None

    # total ordering
    __eq__ = lambda self, other: self.logline.seconds == other.logline.seconds
    __lt__ = lambda self, other: self.logline.seconds < other.logline.seconds

    @property
    def request_state(self):
        return self.to_node.request_state

    @property
    def marks(self):
        return self.to_node.marks

    @property
    def joins_pace(self):
        if self.joins_relation and self.joins_relation is not empty_join:
            return self.joins_relation.to_pace
        else:
            return None

    @property
    def joined_pace(self):
        if self.joined_relation and self.joined_relation is not empty_join:
            return self.joined_relation.from_pace
        else:
            return None

    @property
    def joins_objs(self):
        return self.edge.joins_objs

    @property
    def joined_objs(self):
        return self.edge.joined_objs

    @property
    def is_request_start(self):
        return self.from_node.is_request_start

    @property
    def is_request_end(self):
        return self.to_node.is_request_end

    @property
    def is_thread_start(self):
        return self.from_node.is_thread_start

    @property
    def is_thread_end(self):
        return self.to_node.is_thread_end

    def __getitem__(self, item):
        assert isinstance(item, str)

        if item in rv.ALL_VARS:
            ret = getattr(self.logline, item)
            if item == rv.REQUEST:
                if ret is None:
                    ret = getattr(self.logline, rv.REQUEST)
                return ret
            else:
                assert ret is not None
                return ret
        elif item in self.logline.get_keys():
            return self.logline[item]
        elif item in self.threadins.thread_vars:
            return self.threadins.thread_vars[item]
        elif item in self.threadins.thread_vars_dup:
            raise StateError("(Pace) got multiple %s: %s" %
                    (item, self.threadins.thread_vars_dup[item]))
        else:
            raise StateError("(Pace) key %s not exist!" % item)

    def _mark_str(self):
        mark_str = ""
        if self.joins_objs:
            mark_str += ", join(%d):" % len(self.joins_objs)
            if self.joins_relation:
                if self.joins_relation is empty_join:
                    mark_str += "EMPTY"
                else:
                    mark_str += self.joins_relation.name
            else:
                mark_str += "?"
        if self.joined_objs:
            mark_str += ", joined(%d):" % len(self.joined_objs)
            if self.joined_relation:
                if self.joined_relation is empty_join:
                    mark_str += "EMPTY"
                else:
                    mark_str += self.joined_relation.name
            else:
                mark_str += "?"
        if self.is_thread_start:
            mark_str += ",s"
        if self.is_request_start:
            mark_str += ",S"
        if self.is_thread_end:
            mark_str += ",e"
        if self.is_request_end:
            mark_str += ",E"
        return mark_str

    def __str__(self):
        mark_str = self._mark_str()
        return "<Pace %.3f {%s>%s>%s}, `%s`, %d lvars, %s-%s %s%s>" % (
                self.logline.seconds,
                self.from_node.name,
                self.edge.name,
                self.to_node.name,
                self.logline.keyword,
                len(self.logline.get_keys()),
                self.logline.target,
                self.logline.thread,
                self.logline.request,
                mark_str)

    def __str__thread__(self):
        mark_str = self._mark_str()

        req_str = ""
        if self.threadins.is_shared:
            req_str = ", req#%s" % self.logline.request

        return "%.3f {%s>%s>%s}, `%s`, %d lvars%s%s" % (
                self.logline.seconds,
                self.from_node.name,
                self.edge.name,
                self.to_node.name,
                self.logline.keyword,
                len(self.logline.get_keys()),
                req_str,
                mark_str)


    def __repr__(self):
        ret_str = str(self)
        ret_str += "\n  >>%s" % self.logline.line
        return ret_str

    def join_pace(self, next_pace, join_obj):
        assert isinstance(next_pace, Pace)
        assert isinstance(join_obj, Join)
        assert join_obj in self.edge.joins_objs
        assert self.joins_relation is None
        assert next_pace.joined_relation is None

        relation = JoinRelation(join_obj, self, next_pace)
        self.joins_relation = relation
        next_pace.joined_relation = relation
        return relation


class JoinRelation(object):
    def __init__(self, join_obj, from_pace, to_pace):
        assert isinstance(join_obj, Join)
        assert isinstance(from_pace, Pace)
        assert isinstance(to_pace, Pace)

        self.name = join_obj.name
        self.join_obj = join_obj
        self.from_pace = from_pace
        self.to_pace = to_pace

    @property
    def from_threadins(self):
        return self.from_pace.threadins

    @property
    def to_threadins(self):
        return self.to_pace.threadins

    @property
    def is_shared(self):
        return self.join_obj.is_shared

    @property
    def is_remote(self):
        return self.join_obj.is_remote

    @property
    def from_host(self):
        return self.from_pace.logline.host

    @property
    def to_host(self):
        return self.to_pace.logline.host


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


# class InstanceBase(object):
#     def __init__(self, graph, ident):
#         self.graph = graph
#         self.ident = ident

#         self.from_pace = None
#         self.to_pace = None

#         self.fail_message = ""

#     @property
#     def from_node(self):
#         return self.from_pace.from_node

#     @property
#     def to_node(self):
#         if self.to_pace is None:
#             return None
#         else:
#             return self.to_pace.to_node

#     @property
#     def start_leaf_pace(self):
#         return None

#     @property
#     def from_seconds(self):
#         return self.from_pace.from_seconds

#     @property
#     def to_seconds(self):
#         return self.to_pace.to_seconds

#     @property
#     def is_end(self):
#         if self.to_pace and self.to_pace.to_node in self.graph.end_nodes:
#             return True
#         else:
#             return False

#     @property
#     def is_failed(self):
#         return bool(self.fail_message) or not self.is_end

#     @property
#     def state(self):
#         if not self.to_node:
#             return "UNKNOWN"
#         else:
#             state = self.to_node.state
#             if state is Node.UNKNOWN_STATE:
#                 return "-"
#             else:
#                 return state

#     @property
#     def assume_host(self):
#         if not self.is_end or self.to_pace is None:
#             return None
#         else:
#             return self.to_pace.assume_host

#     @property
#     def name(self):
#         return self.graph.name

#     def iterall(self):
#         p = self.start_leaf_pace
#         while p:
#             yield p
#             if p.to_node in self.graph.end_nodes:
#                 break
#             p = p.nxt

#     def __iter__(self):
#         p = self.from_pace
#         while p:
#             yield p.content
#             if p is self.to_pace:
#                 break
#             p = p.nxt

#     def __str__(self):
#         ret_str = "%r:" % self

#         ret_str += "\nPaces:"
#         p = self.from_pace
#         while p:
#             ret_str += "\n    %r" % p
#             if p is self.to_pace:
#                 break
#             p = p.nxt
#         ret_str += "\n"
#         return ret_str


class ThreadInstance(object):
    def __init__(self, thread, threadgraph, loglines, s_index):
        assert isinstance(thread, str)
        assert isinstance(threadgraph, ThreadGraph)
        assert isinstance(loglines, list)
        assert isinstance(s_index, int)

        self.thread = thread
        self.threadgraph = threadgraph
        self.thread_vars = {}
        self.thread_vars_dup = {}

        self.paces = []
        self.paces_by_mark = defaultdict(list)

        self.s_index = s_index
        self.f_index = None
        self.loglines = loglines

        self.component = None
        self.host = None
        self.target = None

        self.joined_paces = []
        self.joins_paces = []
        self.joined_relations = set()
        self.joins_relations = set()

        self.is_shared = threadgraph.is_shared
        if self.is_shared:
            self.requests = set()
            self.requestinss = set()
        else:
            self.request = None
            self.requestins = None

        # init
        s_log = loglines[s_index]
        node = threadgraph.decide_node(s_log)
        index = s_index
        while index != len(loglines):
            log = loglines[index]
            edge = node.decide_edge(log)
            if not edge:
                break
            pace = Pace(log, node, edge, self)
            marks = edge.marks
            for mark in marks:
                self.paces_by_mark[mark].append(pace)
            if edge.joins_objs:
                self.joins_paces.append(pace)
            if edge.joined_objs:
                self.joined_paces.append(pace)
            self.paces.append(pace)
            node = edge.node
            index += 1

        self.f_index = index
        prv = None
        for pace in self.paces:
            pace.prv_pace = prv
            if prv:
                prv.nxt_pace = pace
            prv = pace

        # check
        if not self.paces or not self.is_complete:
            self.report("Thread is not complete")
            raise StateError("(ThreadInstance) parse error")

        # check and set thread vars
        v_dict = defaultdict(set)
        for pace in self.paces:
            logline = pace.logline
            keys = logline.get_keys(True)
            for key in keys:
                v_dict[key].add(logline[key])

        for name in ("keyword", "time", "seconds"):
            v_dict.pop(name)

        for name in ("component", "target", "host"):
            values = v_dict.pop(name)
            if len(values) == 1:
                setattr(self, name, values.pop())
            else:
                self.report("Thread key %s contains: %s" % (name, values))
                raise StateError("(ThreadInstance) parse error: variable mismatch")

        threads = v_dict.pop("thread")
        if len(threads) == 1:
            thread = threads.pop()
            if thread != self.thread:
                self.report("Thread %s collides: %s" % (self.thread, thread))
                raise StateError("(ThreadInstance) thread mismatch")
        else:
            self.report("Thread %s collides: %s" % (self.thread, threads))
            raise StateError("(ThreadInstance) thread mismatch")

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
                self.report("Thread has multiple requests %s" % requests)
                raise StateError("(ThreadInstance) thread is not shared!")

        # generate vars
        for k, v in v_dict.iteritems():
            if len(v) == 0:
                pass
            elif len(v) == 1:
                self.thread_vars[k] = v.pop()
            else:
                self.thread_vars_dup[k] = v
    @property
    def request_state(self):
        return self.end_pace.request_state

    @property
    def is_request_start(self):
       return self.paces[0].from_node.is_request_start

    @property
    def is_request_end(self):
        return self.paces[-1].to_node.is_request_end

    @property
    def is_complete(self):
        return self.paces[-1].to_node.is_thread_end

    @property
    def start_pace(self):
        return self.paces[0]

    @property
    def end_pace(self):
        return self.paces[-1]

    @property
    def len_paces(self):
        return len(self.paces)

    def __str__(self):
        mark_str = ""
        if self.is_shared:
            mark_str += ", SHARED"
        if not self.is_complete:
            mark_str += ", INCOMPLETE"
        if self.is_request_start:
            mark_str += ", RSTART"
        if self.is_request_end:
            mark_str += ", REND"
        if self.paces_by_mark:
            mark_str += ", %dMARKS" % len(self.paces_by_mark)

        if self.is_shared:
            req_str = self.requests
        else:
            req_str = self.request

        return "<ThIns#%s-%s: log#%d[%d, %d) comp#%s, host#%s, graph#%s, "\
               "req#%s, %d-%d vars, %d(%d) joined_p, %d(%d) joins_p%s>" % (
                self.target,
                self.thread,
                self.len_paces,
                self.s_index,
                self.f_index,
                self.component,
                self.host,
                self.threadgraph.name,
                req_str,
                len(self.thread_vars),
                len(self.thread_vars_dup),
                len(self.joined_paces),
                len(self.joined_relations),
                len(self.joins_paces),
                len(self.joins_relations),
                mark_str)

    def __repr__(self):
        ret_str = str(self)
        ret_str += "\n  %s" % self.threadgraph
        ret_str += "\n  Paces:"
        for pace in self.paces:
            ret_str += "\n  | %s" % pace.__str__thread__()
        return ret_str

    def report(self, reason):
        print "(ThreadInstance) report: %s" % reason
        print "-------- Thread --------"
        print "%r" % self
        report_loglines(self.loglines, self.s_index, self.f_index)
        print "-------- end -----------"

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
class NestedRequest(object):
    def __init__(self, threadins, requestins):
        assert isinstance(threadins, ThreadInstance)
        assert threadins.is_shared == True
        assert isinstance(requestins, RequestInstance)

        self.threadins = threadins

        self.paces = []
        self.paces_by_mark = {}

        self.joined_paces = []
        self.joins_paces = []
        self.joined_relations = set()
        self.joins_relations = set()

        self.is_shared = False
        self.request = requestins.request
        self.requestins = requestins

        self.request_obj = request_obj
        self.request = request_obj.request

        dummy_pace1 = None
        for pace in threadins.joined_paces:
            if pace.joined_pace.requestins is requestins:
                dummy_pace1 = Pace(pace.logline, pace.from_node, pace.edge, self)
                relation = pace.joined_relation
                relation.to_pace = dummy_pace1
                dummy_pace1.joined_relation = relation

                self.joined_paces.append(dummy_pace1)
                self.joined_relations.add(dummy_relation)
                self.paces.append(dummy_pace1)
                break
        if not dummy_pace1:
            raise StateError("(NestedRequest) dummy_pace1 failed")

        dummy_pace2 = None
        for pace in threadins.joins_paces:
            if pace.joins_pace.requestins is requestins:
                dummy_pace2 = Pace(pace.logline, pace.from_node, pace.edge, self)
                relation = pace.joins_relation
                relation.from_pace = dummy_pace2
                dummy_pace2.joins_relation = relation

                self.joins_paces.append(dummy_pace2)
                self.joins_relations.add(dummy_relation)
                self.paces.append(dummy_pace2)

                dummy_pace1.nxt_pace = dummy_pace2
                dummy_pace2.prv_pace = dummy_pace1
                break
        if not dummy_pace2:
            raise StateError("(NestedRequest) dummy_pace2 failed")

    def __getattribute__(self, item):
        if item in self.__dict__:
            return self.__dict__[item]
        else:
            return getattr(self.threadins, name)


class RequestInstance(object):
    def __init__(self, mastergraph, request, threadinss):
        assert isinstance(mastergraph, MasterGraph)
        assert isinstance(request, str)
        assert isinstance(threadinss, set)

        self.request = request
        self.mastergraph = mastergraph
        self.request_vars = defaultdict(set)

        self.threadinss = []
        self.nestedreqs = set()
        self.start_threadins = None
        self.end_threadins = None

        self.paces_by_mark = defaultdict(list)
        self.len_paces = 0

        self.remote_relations = set()
        self.remote_l_relations = set()
        self.local_relations = set()

        # error report
        self.errors = {}
        self.warns = {}

        # collect others
        self.e_incomplete_threadinss = set()
        self.e_extra_s_threadinss = set()
        self.e_extra_e_threadinss = set()
        self.e_stray_threadinss = set()
        self.e_unjoins_paces_by_edge = defaultdict(set)

        # init
        shared_tis = set()
        tis = set()
        for threadins in threadinss:
            assert isinstance(threadins, ThreadInstance)
            if threadins.is_shared:
                shared_tis.add(threadins)
                threadins.requestinss.add(self)
                threadins.requests.add(request)
            else:
                tis.add(threadins)
                threadins.request_obj = self
                threadins.request = request
                if threadins.is_request_start:
                    if self.start_threadins is not None:
                        self.e_extra_s_threadinss.add(threadins)
                    else:
                        self.start_threadins = threadins

        # error: multiple start threads
        if self.e_extra_s_threadinss:
            self.e_extra_s_threadinss.add(self.start_threadins)
            error_str = "\n".join("%r" % ti for ti in self.e_extra_s_threadinss)
            self.errors["Has multiple start threads"] = error_str

        # error: no start thread
        if self.start_threadins is None:
            self.errors["Contains no start thread"] = ""

        for shared_ti in shared_tis:
            tis.add(NestedRequest(shared_ti, self))

        seen_tis = set()
        def _process(threadins):
            if threadins in seen_tis:
                return
            seen_tis.add(threadins)

            # threadinss
            self.threadinss.append(threadins)

            # nestedreqs
            if isinstance(threadins, NestedRequest):
                self.nestedreqs.add(threadins)

            # incomplete_threadinss
            if not threadins.is_complete:
                self.e_incomplete_threadinss.add(threadins)

            # end_threadins
            r_state = threadins.request_state
            if r_state:
                assert threadins.is_request_end is True
                if self.end_threadins:
                    self.e_extra_e_threadinss.add(threadins)
                else:
                    self.end_threadins = threadins

            # paces_by_mark
            for mark, paces in threadins.paces_by_mark.iteritems():
                self.paces_by_mark[mark].extend(paces)

            # request vars
            self.request_vars["thread"].add(threadins.target+"-"+threadins.thread)
            self.request_vars["host"].add(threadins.host)
            self.request_vars["component"].add(threadins.component)
            self.request_vars["target"].add(threadins.target)
            for key, val in threadins.thread_vars.iteritems():
                self.request_vars[key].add(val)
            for key, vals in threadins.thread_vars_dup.iteritems():
                self.request_vars[key].update(vals)
            self.len_paces += threadins.len_paces

            for joins_pace in threadins.joins_paces:
                relation = joins_pace.joins_relation
                if relation is empty_join:
                    # cnt_unjoined_paces_by_edgename
                    self.e_unjoins_paces_by_edge[joins_pace.edge].add(joins_pace)
                else:
                    assert joins_pace.joins_pace.joined_pace is joins_pace
                    if relation.is_remote:
                        if relation.from_host == relation.to_host:
                            # remote_relations
                            self.remote_relations.add(relation)
                        else:
                            # remote_l_relations
                            self.remote_l_relations.add(relation)
                    else:
                        assert relation.from_host == relation.to_host
                        # local_relations
                        self.local_relations.add(relation)
                    _process(joins_pace.joins_pace.threadins)

        _process(self.start_threadins)

        self.threadinss.sort(key=lambda ti: ti.start_pace.logline.seconds)

        # error: incomplete threads
        if self.e_incomplete_threadinss:
            err_str = "\n".join("%r" % ti for it in self.e_incomplete_threadinss)
            self.errors["Has incomplete threads"] = err_str

        # error: multiple end threads
        if self.e_extra_e_threadinss:
            self.e_extra_e_threadinss.add(self.end_threadins)
            err_str = "\n".join("%r" % ti for ti in self.e_extra_e_threadinss)
            self.errors["Has multiple end threads"] = err_str

        # error: no end thread
        if not self.end_threadins:
            self.errors["Contains no end thread"] = ""

        # error: stray thread instances
        self.e_stray_threadinss = tis - seen_tis
        if self.e_stray_threadinss:
            err_str = "\n".join("%r" % ti for ti in self.e_stray_threadinss)
            self.warns["Has stray threads"] = err_str

        # error: unjoins paces
        if self.e_unjoins_paces_by_edge:
            self.warns["Has unjoins paces"] = "%d joins edges" % len(self.e_unjoins_paces_by_edge)

    @property
    def request_state(self):
        return self.end_threadins.request_state
    # def confirm(self, ins):
    #     if self.ident != ins.ident:
    #         return False

    #     if not self.is_end:
    #         p = None
    #         if self.to_pace is None:
    #             # TODO self.graph.
    #             for node in self.graph.start_nodes:
    #                 if node.accept_edge(ins.from_edge):
    #                     p = NestedPace(ins)
    #                     self.from_pace = p
    #                     break
    #         else:
    #             p = self.to_pace.confirm_pace(ins)

    #         if p:
    #             self.to_pace = p
    #             if not ins.is_end:
    #                 raise ParseError("Instance %r is not complete!" % ins)
    #             else:
    #                 return True

    #     return False

    # def assume_graphs(self):
    #     """ Acceptable graphs """
    #     graphs = set()
    #     if self.is_end:
    #         return graphs

    #     if self.to_pace is None:
    #         for node in self.graph.start_nodes:
    #             for edge in node.edges:
    #                 graphs.add(edge.graph)
    #     else:
    #         node = self.to_pace.to_node
    #         for edge in node.edges:
    #             graphs.add(edge.graph)
    #     return graphs

    # def __repr__(self):
    #     ret_str = "<NestedIns ident:%s graph:%s end:%s state:%s>" \
    #               % (self.ident, self.graph.name, self.is_end, self.state)
    #     return ret_str

    # def __str__(self):
    #     ret_str = ">>------------\n"
    #     ret_str += super(NestedInstance, self).__str__()

    #     p = self.from_pace
    #     while p:
    #         ret_str += "\n%s" % p.sub_instance
    #         if p is self.to_pace:
    #             break
    #         p = p.nxt
    #     return ret_str
