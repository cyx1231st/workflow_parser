from __future__ import print_function

from collections import defaultdict
from orderedset import OrderedSet

from workflow_parser.service_registry import Component
from workflow_parser.log_parser import LogLine

# TODO: implement RequestGraph for nested-request support


class Node(object):
    def __init__(self, id_, thread_graph):
        assert isinstance(thread_graph, ThreadGraph)
        assert isinstance(id_, str)\
               or isinstance(id_, int)

        self.id_ = id_
        self.thread_graph = thread_graph
        self.master_graph = thread_graph.master_graph

        self.edges = OrderedSet()
        self.is_lock = False
        self.marks = OrderedSet()
        self.request_state = None

    @property
    def name(self):
        assert self.id_ is not None
        return "n%s" % self.id_

    @property
    def component(self):
        return self.thread_graph.component

    @property
    def is_thread_start(self):
        assert self not in self.thread_graph.start_nodes
        return False

    @property
    def is_thread_end(self):
        return self in self.thread_graph.end_nodes

    @property
    def is_request_start(self):
        assert self not in self.master_graph.start_nodes
        return False

    @property
    def is_request_end(self):
        if self.request_state is not None:
            assert self in self.master_graph.end_nodes
            return True
        else:
            return False

    def __str__state__(self):
        state_str = ""
        if self.request_state:
            state_str += ", @%s" % self.request_state
        if self.marks:
            for mark in self.marks:
                state_str += ", *%s" % mark
        return state_str

    def __str__(self):
        return "<%s#%s: %d branches, %s, in %s%s>" \
               % (self.__class__.__name__,
                  self.name,
                  len(self.edges),
                  self.component,
                  self.thread_graph.name,
                  self.__str__state__())

    def __str__thread__(self):
        return "<%s#%s%s>"\
               % (self.__class__.__name__,
                  self.name,
                  self.__str__state__())

    def __repr__(self):
        ret_str = str(self)
        for edge in self.edges:
            ret_str += "\n  %s" % edge
        return ret_str

#-------
    def build(self, to_node_or_id, keyword_or_edge):
        edge, to_node = self.thread_graph._build(
                self, to_node_or_id, keyword_or_edge)
        self.edges.add(edge)

        ret_node = None
        ret_edge = None
        if not isinstance(to_node_or_id, Node):
            ret_node = to_node
        if not isinstance(keyword_or_edge, Edge):
            ret_edge = edge
        return ret_edge, ret_node

    def set_lock(self):
        assert not self.is_lock
        self.is_lock = True

    def set_state(self, state_str, is_mark=False):
        assert isinstance(state_str, str)
        assert isinstance(is_mark, bool)

        if not is_mark:
            assert self.is_thread_end is True
            assert self.request_state is None
            self.master_graph.request_states.add(state_str)
            self.request_state = state_str
            self.master_graph.end_nodes.add(self)
        else:
            self.master_graph.marks.add(state_str)
            self.marks.add(state_str)

########
    def decide_edge(self, logline):
        assert isinstance(logline, LogLine)

        for edge in self.edges:
            if edge.decide(logline):
                self.master_graph.seen_edges.add(edge)
                return edge
        return None


class StartNode(Node):
    @property
    def is_thread_start(self):
        assert self in self.thread_graph.start_nodes
        return True

    @property
    def is_thread_end(self):
        assert self not in self.thread_graph.end_nodes
        return False

    @property
    def is_request_start(self):
        return self in self.master_graph.start_nodes

    @property
    def is_request_end(self):
        assert self not in self.master_graph.end_nodes
        return False


class Join(object):
    def __init__(self, name, from_edge, to_edge, schemas, is_remote, is_shared):
        assert isinstance(name, str)
        assert isinstance(from_edge, Edge)
        assert isinstance(to_edge, Edge)
        assert isinstance(is_remote, bool)
        assert isinstance(is_shared, bool)
        if schemas is None:
            schemas = []
        assert isinstance(schemas, list)

        self.name = name
        self.from_edge = from_edge
        self.to_edge = to_edge
        self.is_shared = is_shared
        self.is_remote = is_remote
        self.master_graph = from_edge.master_graph

        self.schemas = OrderedSet()
        for schema in schemas:
            if isinstance(schema, str):
                self.schemas.add((schema, schema))
            else:
                assert isinstance(schema, tuple)
                assert isinstance(schema[0], str)
                assert isinstance(schema[1], str)
                self.schemas.add(schema)
        if not is_remote:
            self.schemas.add(("target", "target"))
            self.schemas.add(("host", "host"))
        else:
            assert ("host", "host") not in self.schemas
            assert ("target", "target") not in self.schemas
        if not is_shared:
            self.schemas.add(("request", "request"))

    def __repr__(self):
        marks_str = ""
        if self.is_shared:
            marks_str += ", shared"

        if self.is_remote:
            name = "RemoteJoin"
        else:
            name = "LocalJoin"

        return "<%s#%s: Edge#%s->%s [%s|-->%s], %d schemas%s>" % (
                name,
                self.name,
                self.from_edge.name,
                self.to_edge.name,
                self.from_edge.keyword,
                self.to_edge.keyword,
                len(self.schemas),
                marks_str)


class Edge(object):
    def __init__(self, name, node, keyword):
        assert isinstance(name, str)
        assert isinstance(node, Node)
        assert not isinstance(node, StartNode)
        assert isinstance(keyword, str)

        self.name = name
        self.thread_graph = node.thread_graph
        self.master_graph = node.master_graph
        self.node = node
        self.keyword = keyword
        self.joins_objs = OrderedSet()
        self.joined_objs = OrderedSet()

    @property
    def component(self):
        return self.thread_graph.component

    @property
    def request_state(self):
        return self.node.request_state

    @property
    def marks(self):
        return self.node.marks

    def __str__join__(self):
        join_str=""
        if self.joins_objs:
            join_str += ", joins(%s)" % ",".join(jo.name for jo in self.joins_objs)
        if self.joined_objs:
            join_str += ", joined(%s)" % ",".join(jo.name for jo in self.joined_objs)
        return join_str

    def __str__(self):
        return "<Edge#%s->%s: `%s`, %s, in %s%s>" \
               % (self.name,
                  self.node.name,
                  self.keyword,
                  self.component,
                  self.thread_graph.name,
                  self.__str__join__())

    def __str__thread__(self):
        return "<Edge#%s->%s: `%s`%s>"\
               % (self.name,
                  self.node.name,
                  self.keyword,
                  self.__str__join__())

    def __repr__(self):
        ret_str = str(self)
        ret_str += "\n  %s" % self.node
        ret_str += "\n  %s" % self.thread_graph
        if self.joins_objs:
            ret_str += "\n  joins:"
            for join_obj in self.joins_objs:
                ret_str += "\n    %s" % join_obj
        if self.joined_objs:
            ret_str += "\n  joined:"
            for join_obj in self.joined_objs:
                ret_str += "\n    %s" % join_obj
        return ret_str

#-------
    def _join(self, edge, schemas, is_remote, is_shared):
        assert isinstance(edge, Edge)
        assert isinstance(is_remote, bool)
        assert isinstance(is_shared, bool)
        # assert edge not in self.joins
        # assert self not in edge.joined

        join_obj = self.master_graph._create_joinobj(self, edge, schemas,
                                                     is_remote, is_shared)
        self.joins_objs.add(join_obj)
        edge.joined_objs.add(join_obj)
        if is_shared:
            edge.thread_graph.is_shared = True
        return join_obj

    def join_local(self, edge, schemas=None, is_shared=False):
        return self._join(edge, schemas, False, is_shared)

    def join_remote(self, edge, schemas=None, is_shared=False):
        return self._join(edge, schemas, True, is_shared)

########
    def decide(self, logline):
        assert isinstance(logline, LogLine)
        return self.keyword in logline.keyword


class ThreadGraph(object):
    def __init__(self, name, component, master_graph):
        assert isinstance(name, str)
        assert isinstance(component, Component)
        assert isinstance(master_graph, MasterGraph)

        self.name = name
        self.component = component
        self.master_graph = master_graph
        self.is_shared = False

        self.nodes = OrderedSet()
        self.start_nodes = OrderedSet()
        self.end_nodes = OrderedSet()
        self.edges = OrderedSet()

    def __str__(self):
        mark_str = ""
        if self.is_shared:
            mark_str += ", shared"
        return ("<ThreadGraph#%s: %d(s%d, e%d) nodes, %d edges, %s%s>"
                    % (self.name,
                       len(self.nodes),
                       len(self.start_nodes),
                       len(self.end_nodes),
                       len(self.edges),
                       self.component,
                       mark_str))

    def __repr__(self):
        ret_str = [""]
        ret_str[0] += "%s:" % self

        included = set()
        def parse_node(node):
            assert isinstance(node, Node)
            if node in included:
                return
            included.add(node)
            try:
                assert node.thread_graph is self
            except Exception:
                import pdb; pdb.set_trace()
            assert node.component is self.component

            # print node
            blank = [" ", " "]
            if node in self.start_nodes:
                blank[0] = "+"
            if node in self.end_nodes:
                blank[1] = "-"
            ret_str[0] += "\n%s%s" %("".join(blank), node.__str__thread__())

            # print edge
            for edge in node.edges:
                assert edge.thread_graph is self
                ret_str[0] += "\n    %s" % edge.__str__thread__()
            # recursive
            for edge in node.edges:
                parse_node(edge.node)

        for start in self.start_nodes:
            parse_node(start)
        return ret_str[0]

    def _build(self, from_node, to_node_or_id, keyword_or_edge):
        assert isinstance(from_node, Node)
        assert from_node.thread_graph is self
        assert from_node in self.nodes

        # build to_node and edge
        if isinstance(keyword_or_edge, str):
            keyword = keyword_or_edge
            if isinstance(to_node_or_id, int):
                to_node_id = to_node_or_id
                to_node = self.master_graph._create_node(self, to_node_id)
                self.nodes.add(to_node)
            else:
                assert isinstance(to_node_or_id, Node)
                to_node = to_node_or_id
                self._merge(to_node.thread_graph)
            edge = self.master_graph._create_edge(to_node, keyword)
            self.edges.add(edge)
        else:
            assert isinstance(keyword_or_edge, Edge)
            edge = keyword_or_edge
            self._merge(edge.thread_graph)
            assert isinstance(to_node_or_id, Node)
            to_node = to_node_or_id
            assert to_node is edge.node

        assert to_node.thread_graph is self
        assert to_node in self.nodes
        assert edge.thread_graph is self
        assert edge in self.edges

        from_to_ = (from_node.id_, to_node.id_)
        assert from_to_ not in self.master_graph.edges_by_from_to
        self.master_graph.edges_by_from_to[from_to_] = edge

        if isinstance(to_node_or_id, int):
            self.end_nodes.add(to_node)
            self.end_nodes.discard(self)

        return edge, to_node

    def _merge(self, thread_graph):
        assert isinstance(thread_graph, ThreadGraph)
        assert thread_graph.component is self.component
        assert thread_graph.master_graph is self.master_graph

        if thread_graph is self:
            return

        # merge thread_graph
        for node in thread_graph.nodes:
            self.nodes.add(node)
            node.thread_graph = self
        for edge in thread_graph.edges:
            self.edges.add(edge)
            edge.thread_graph = self
        for node in thread_graph.start_nodes:
            self.start_nodes.add(node)
        for node in thread_graph.end_nodes:
            self.end_nodes.add(node)
        self.master_graph._remove_thread(thread_graph)

########
    def decide_node(self, logline):
        assert isinstance(logline, LogLine)
        for node in self.start_nodes:
            edge = node.decide_edge(logline)
            if edge is not None:
                return node
        return None


class MasterGraph(object):
    def __init__(self, name):
        assert isinstance(name, str)

        self.name = name
        self.join_objs = OrderedSet()

        self.edges = OrderedSet()
        self.edges_by_from_to = {}
        self.seen_edges = OrderedSet()

        self.nodes_by_id = {}
        self.start_nodes = OrderedSet()
        self.end_nodes = OrderedSet()

        self.thread_graphs = OrderedSet()

        self.request_states = OrderedSet()
        self.marks = OrderedSet()

        # after check
        self.threadgraphs_by_component = defaultdict(OrderedSet)

        self._thread_index = 0
        self._edge_index = 0
        self._start_node_index = 0
        self._joinobj_index = 0

    @property
    def components(self):
        components = OrderedSet()
        for th_obj in self.thread_graphs:
            components.add(th_obj.component)
        return components

    @property
    def thread_names(self):
        names = OrderedSet()
        for th_obj in self.thread_graphs:
            names.add(th_obj.name)
        return names

    def __str__(self):
        return ("<Master#%s: %d(s%d) nodes, %d(%d) edges, %d threads, "
                "%d components, %d req_states, %d marks, %d joins>" %
                (self.name,
                 len(self.nodes_by_id),
                 len(self.start_nodes),
                 len(self.edges),
                 len(self.edges_by_from_to),
                 len(self.thread_graphs),
                 len(self.components),
                 len(self.request_states),
                 len(self.marks),
                 len(self.join_objs)))

    def __repr__(self):
        ret_str = str(self)
        ret_str += "\n  Threads:"
        for th_obj in self.thread_graphs:
            ret_str += "\n    %s" % th_obj
        ret_str += "\n  Start nodes:"
        for node in self.start_nodes:
            ret_str += "\n    %s" % node
        ret_str += "\n  End nodes:"
        for node in self.end_nodes:
            ret_str += "\n    %s" % node
        ret_str += "\n  Components:%s"\
                   % (",".join(str(c) for c in self.components))
        ret_str += "\n  Request states:%s"\
                   % (",".join(s for s in self.request_states))
        ret_str += "\n  Marks:%s"\
                   % (",".join(m for m in self.marks))
        ret_str += "\n  Joins:"
        for join_obj in self.join_objs:
            ret_str += "\n    %s" % join_obj
        return ret_str

    def _create_thread(self, component):
        self._thread_index += 1
        name = "t%d" % self._thread_index
        thread = ThreadGraph(name, component, self)
        self.thread_graphs.add(thread)
        return thread

    def _remove_thread(self, thread_graph):
        assert isinstance(thread_graph, ThreadGraph)
        assert thread_graph.master_graph is self

        self.thread_graphs.remove(thread_graph)
        self._thread_index = 0
        for thread_graph in self.thread_graphs:
            self._thread_index += 1
            thread_graph.name = "t%d" % self._thread_index

    def _create_node(self, thread_graph, node_id=None):
        assert isinstance(thread_graph, ThreadGraph)
        assert thread_graph.master_graph is self

        if node_id is not None:
            assert isinstance(node_id, int)
            node = Node(node_id, thread_graph)
        else:
            self._start_node_index += 1
            node_id = "s%d" % self._start_node_index
            node = StartNode(node_id, thread_graph)

        assert node_id not in self.nodes_by_id
        self.nodes_by_id[node.id_] = node
        return node

    def _create_edge(self, to_node, keyword):
        assert isinstance(to_node, Node)
        assert to_node.master_graph is self
        assert to_node.id_ in self.nodes_by_id

        self._edge_index += 1
        name = "e%d" % self._edge_index
        edge = Edge(name, to_node, keyword)
        self.edges.add(edge)
        return edge

    def _create_joinobj(self, from_edge, to_edge, schemas, is_remote, is_shared):
        assert from_edge in self.edges
        assert to_edge in self.edges

        self._joinobj_index += 1
        name = "j%d" % self._joinobj_index
        join_obj = Join(name, from_edge, to_edge,
                        schemas, is_remote, is_shared)
        self.join_objs.add(join_obj)
        return join_obj

#-------
    def build_thread(self, component, to_node_or_id, keyword_or_edge,
                     is_request_start=False):
        thread = self._create_thread(component)
        from_node = self._create_node(thread)
        thread.start_nodes.add(from_node)
        thread.nodes.add(from_node)
        if is_request_start:
            self.start_nodes.add(from_node)
        return from_node.build(to_node_or_id, keyword_or_edge)

########
    def check(self):
        for thread_graph in self.thread_graphs:
            assert thread_graph.start_nodes
            assert thread_graph.end_nodes
            self.threadgraphs_by_component[thread_graph.component].add(thread_graph)

        print("\n>>--------------")
        print("%r\n" % self)
        print("--<ThreadGraphs>--")
        for th_g in self.thread_graphs:
            print("%r\n" % th_g)
        print("--------------<<\n")

    def decide_threadgraph(self, logline):
        assert len(self.threadgraphs_by_component) > 0
        assert isinstance(logline, LogLine)
        threadgraphs = self.threadgraphs_by_component.get(logline.component, OrderedSet())
        for thread_graph in threadgraphs:
            node = thread_graph.decide_node(logline)
            if node:
                return thread_graph
        return None
