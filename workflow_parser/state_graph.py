from __future__ import print_function

from collections import defaultdict
from orderedset import OrderedSet

from workflow_parser.service_registry import Component
from workflow_parser.log_parser import LogLine

# TODO: implement RequestGraph for nested-request support

# Node(id_, name, component, edges,
#      request_state, marks,
#      is_thread_end, is_thread_start,
#      is_request_start, is_request_end,
#      thread_graph,
#      master_graph)
#   >append_edge(edge)
#   >>decide_edge(logline)

# Join(name, from_edge, to_edge, schemas,
#      is_shared, is_remote)

# Edge(name, component, node, keyword,
#      request_state, marks,
#      joins_objs, joined_objs,
#      thread_graph,
#      master_graph)
#   >>join_edge(edge)
#   >>>decide(logline)

# ThreadGraph(name, component, is_shared,
#             nodes, start_nodes, end_nodes,
#             edges,
#             master_graph)
#   >add_edge(from_node, edge)
#   >remove()
#   >>>decide_node(self, logline)

# MasterGraph(name, join_objs,
#             edges, edges_by_from_to,
#             nodes_by_id, start_nodes,
#             thread_graphs,
#             threadgraphs_by_component,
#             request_states, marks,
#             components, thread_names)
#   >add_thread(thread_graph)
#   >remove_thread(thread_graph)
#   >generate_edge_name()
#   >add_joinobj(join_obj)
#   >>build_edge(from_node_id, to_node_id,
#                component, keyword,
#                is_requesstart=False)
#   >>set_state(node_id, state_str,
#               is_mark=False)
#   >>check()
#   >>>decide_threadgraph(logline)


class Node(object):
    def __init__(self, id_, thread_graph):
        assert isinstance(thread_graph, ThreadGraph)

        self.id_ = id_
        self.thread_graph = thread_graph
        self.master_graph = thread_graph.master_graph

        self.edges = OrderedSet()
        self.request_state = None
        self.is_lock = False
        self.marks = OrderedSet()

        thread_graph.add_node(self)
        assert self.id_ is not None

    @property
    def name(self):
        return "n%s" % self.id_

    @property
    def component(self):
        return self.thread_graph.component

    @property
    def is_thread_start(self):
        if isinstance(self, StartNode):
            assert self in self.thread_graph.start_nodes
            return True
        else:
            assert self not in self.thread_graph.start_nodes
            return False

    @property
    def is_thread_end(self):
        return self in self.thread_graph.end_nodes

    @property
    def is_request_start(self):
        return self in self.master_graph.start_nodes

    @property
    def is_request_end(self):
        return self.request_state is not None

    def __str__(self):
        state_str = ""
        if self.request_state:
            state_str += ", @%s" % self.request_state
        if self.marks:
            for mark in self.marks:
                state_str += ", *%s" % mark
        return "<Node#%s: %d edges, Component#%s, Thread#%s%s>" \
               % (self.name, len(self.edges), self.component, self.thread_graph.name, state_str)

    def __repr__(self):
        ret_str = str(self)
        for edge in self.edges:
            ret_str += "\n  %s" % edge
        return ret_str

    def build(self, to_node_or_id, keyword_or_edge):
        if isinstance(keyword_or_edge, str):
            keyword = keyword_or_edge
            if isinstance(to_node_or_id, int):
                to_node_id = to_node_or_id
                to_node = Node(to_node_id, self.thread_graph)
            else:
                assert isinstance(to_node_or_id, Node)
                to_node = to_node_or_id
                self.thread_graph.merge(to_node.thread_graph)
            edge = Edge(to_node, keyword)
            assert edge.name is not None
        else:
            assert isinstance(keyword_or_edge, Edge)
            edge = keyword_or_edge
            self.thread_graph.merge(edge.thread_graph)
            assert isinstance(to_node_or_id, Node)
            to_node = to_node_or_id
            assert to_node is edge.node

        from_to_ = (self.id_, to_node.id_)
        assert self.thread_graph is to_node.thread_graph
        assert from_to_ not in self.master_graph.edges_by_from_to
        self.master_graph.edges_by_from_to[from_to_] = edge

        self.edges.add(edge)

        if isinstance(to_node_or_id, int):
            self.thread_graph.end_nodes.add(to_node)
            self.thread_graph.end_nodes.discard(self)

        ret_node = None
        ret_edge = None
        if not isinstance(to_node_or_id, Node):
            ret_node = to_node
        if not isinstance(keyword_or_edge, Edge):
            ret_edge = edge
        return ret_edge, ret_node

    # TODO: improve schema join based on lock
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
    def __init__(self, thread_graph):
        super(StartNode, self).__init__(None,
                                        thread_graph)

    @classmethod
    def create(cls, component, master_graph):
        thread = ThreadGraph(component, master_graph)
        node = StartNode(thread)
        thread.start_nodes.add(node)
        return node


class Join(object):
    def __init__(self, from_edge, to_edge, schemas, is_remote, is_shared):
        assert isinstance(from_edge, Edge)
        assert isinstance(to_edge, Edge)
        assert isinstance(schemas, list)
        assert isinstance(is_remote, bool)
        assert isinstance(is_shared, bool)
        assert from_edge.master_graph is to_edge.master_graph

        self.name = None
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

        self.master_graph.add_joinobj(self)
        assert self.name is not None

    def __repr__(self):
        marks_str = ""
        if self.is_shared:
            marks_str += "shared, "
        if self.is_remote:
            marks_str += "remote, "

        schema_str = "["
        for schema in self.schemas:
            schema_str += str(schema)
        schema_str += "]"

        return "<Join#%s: Edge#%s -> Edge#%s, %s%s>" % (
                self.name,
                self.from_edge.name,
                self.to_edge.name,
                marks_str,
                schema_str)


class Edge(object):
    def __init__(self, node, keyword):
        assert isinstance(node, Node)
        assert isinstance(keyword, str)

        self.name = None
        self.thread_graph = node.thread_graph
        self.master_graph = node.master_graph
        self.node = node
        self.keyword = keyword
        self.joins_objs = OrderedSet()
        self.joined_objs = OrderedSet()

        self.thread_graph.add_edge(self)

    @property
    def component(self):
        return self.thread_graph.component

    @property
    def request_state(self):
        return self.node.request_state

    @property
    def marks(self):
        return self.node.marks

    def __str__(self):
        join_str=""
        if self.joins_objs:
            join_str += ", joins(%s)" % ",".join(jo.name for jo in self.joins_objs)
        if self.joined_objs:
            join_str += ", joined(%s)" % ",".join(jo.name for jo in self.joined_objs)
        return "<Edge#%s -> Node#%s, `%s`, Component#%s, Thread#%s%s>" \
               % (self.name, self.node.name, self.keyword,
                  self.component, self.thread_graph.name, join_str)

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

        if schemas is None:
            schemas = []

        join_obj = Join(self, edge, schemas, is_remote, is_shared)
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
    def __init__(self, component, master_graph):
        assert isinstance(component, Component)
        assert isinstance(master_graph, MasterGraph)

        self.name = None
        self.component = component
        self.is_shared = False
        self.master_graph = master_graph

        self.nodes = OrderedSet()
        self.start_nodes = OrderedSet()
        self.end_nodes = OrderedSet()
        self.edges = OrderedSet()

        master_graph.add_thread(self)
        assert self.name is not None
        assert self.master_graph is not None

    def __str__(self):
        mark_str = ""
        if self.is_shared:
            mark_str += ", shared"
        return ("<ThreadGraph#%s: %d(%d, %d) nodes, %d edges, Component#%s%s>"
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

        included = OrderedSet()
        def parse_node(node):
            assert isinstance(node, Node)
            if node in included:
                return
            included.add(node)
            assert node.thread_graph is self
            assert node.component is self.component

            # print node
            blank = [" ", " "]
            if node in self.start_nodes:
                blank[0] = "+"
            if node in self.end_nodes:
                blank[1] = "-"
            state_str = ""
            if node.request_state:
                state_str += ", @%s" % node.request_state
            for mark in node.marks:
                state_str += ", *%s" % mark
            ret_str[0] += "\n%s<Node#%s%s>" %("".join(blank), node.name, state_str)

            # print edge
            for edge in node.edges:
                assert edge.thread_graph is self
                assert edge.component == self.component
                join_str=""
                if edge.joins_objs:
                    join_str += ", joins(%s)" % ",".join(jo.name for jo in edge.joins_objs)
                if edge.joined_objs:
                    join_str += ", joined(%s)" % ",".join(jo.name for jo in edge.joined_objs)
                ret_str[0] += "\n    <Edge#%s -> Node#%s, `%s`%s>" % \
                                (edge.name, edge.node.name,
                                 edge.keyword, join_str)
            # recursive
            for edge in node.edges:
                parse_node(edge.node)

        for start in self.start_nodes:
            parse_node(start)
        return ret_str[0]

    def add_node(self, node):
        assert isinstance(node, Node)
        assert node.thread_graph is self
        assert node not in self.nodes

        self.nodes.add(node)
        self.master_graph.add_node(node)

    def add_edge(self, edge):
        assert isinstance(edge, Edge)
        assert edge not in self.edges
        assert edge.thread_graph is self

        self.edges.add(edge)
        self.master_graph.add_edge(edge)

    def merge(self, thread_graph):
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
        thread_graph.remove()

    def remove(self):
        self.master_graph.remove_thread(self)

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

        # after check
        self.threadgraphs_by_component = defaultdict(OrderedSet)

        self.request_states = OrderedSet()
        self.marks = OrderedSet()

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
        return ("<Master#%s: %d(%d) nodes, %d(%d) edges, %d threads, "
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
        ret_str += "\n  Components:    "
        for component in self.components:
            ret_str += " %s," % component
        ret_str += "\n  Request states:"
        for state in self.request_states:
            ret_str += " %s," % state
        ret_str += "\n  Marks:         "
        for mark in self.marks:
            ret_str += " %s," % mark
        ret_str += "\n  Joins:"
        for join_obj in self.join_objs:
            ret_str += "\n    %s" % join_obj
        return ret_str

    def add_thread(self, thread_graph):
        assert isinstance(thread_graph, ThreadGraph)
        assert thread_graph.master_graph is self
        assert thread_graph.name is None

        self._thread_index += 1
        thread_graph.name = "t%d" % self._thread_index
        self.thread_graphs.add(thread_graph)

    def remove_thread(self, thread_graph):
        assert isinstance(thread_graph, ThreadGraph)
        self.thread_graphs.remove(thread_graph)
        self._thread_index = 0
        for thread_graph in self.thread_graphs:
            self._thread_index += 1
            thread_graph.name = "t%d" % self._thread_index

    def add_node(self, node):
        assert isinstance(node, Node)
        assert node.master_graph is self

        if isinstance(node, StartNode):
            assert node.id_ is None
            self._start_node_index += 1
            node.id_ = "s%d" % self._start_node_index
        else:
            assert isinstance(node.id_, int)
        assert node.id_ not in self.nodes_by_id
        self.nodes_by_id[node.id_] = node

    def add_edge(self, edge):
        assert isinstance(edge, Edge)
        assert edge.master_graph is self
        assert edge not in self.edges
        assert edge.name is None

        self._edge_index += 1
        edge.name = "e%d" % self._edge_index
        self.edges.add(edge)

    def add_joinobj(self, join_obj):
        assert isinstance(join_obj, Join)
        assert join_obj.master_graph is self
        assert join_obj not in self.join_objs
        assert join_obj.name is None

        self._joinobj_index += 1
        join_obj.name = "j%d" % self._joinobj_index
        self.join_objs.add(join_obj)

#-------
    def build_thread(self, component, to_node_or_id, keyword_or_edge,
                     is_request_start=False):
        from_node = StartNode.create(component, self)
        if is_request_start:
            self.start_nodes.add(from_node)
        return from_node.build(to_node_or_id, keyword_or_edge)

    def check(self):
        for thread_graph in self.thread_graphs:
            assert thread_graph.start_nodes
            assert thread_graph.end_nodes
            self.threadgraphs_by_component[thread_graph.component].add(thread_graph)

        print("\n>>-------------->>")
        print("%r\n" % self)
        print("ThreadGraphs:\n")
        for th_g in self.thread_graphs:
            print("%r\n" % th_g)
        print("<<--------------<<\n")

########
    def decide_threadgraph(self, logline):
        assert len(self.threadgraphs_by_component) > 0
        assert isinstance(logline, LogLine)
        threadgraphs = self.threadgraphs_by_component.get(logline.component, OrderedSet())
        for thread_graph in threadgraphs:
            node = thread_graph.decide_node(logline)
            if node:
                return thread_graph
        return None
