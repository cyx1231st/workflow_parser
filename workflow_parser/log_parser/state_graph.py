from __future__ import print_function

from collections import defaultdict
from orderedset import OrderedSet

from workflow_parser.log_parser.service_registry import Component
from workflow_parser.log_parser.log_parser import LogLine

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
#   >add_join_obj(join_obj)
#   >>build_edge(from_node_id, to_node_id,
#                component, keyword,
#                is_requesstart=False)
#   >>set_state(node_id, state_str,
#               is_mark=False)
#   >>check()
#   >>>decide_threadgraph(logline)

# FIXME: move
seen_edges  = set()


class Node(object):
    def __init__(self, id_, component, master_graph):
        assert isinstance(id_, int)
        assert isinstance(component, Component)
        assert isinstance(master_graph, MasterGraph)

        self.id_ = id_
        self.name = "n-%d" % self.id_
        self.component = component
        self.edges = OrderedSet()

        self.request_state = None
        self.marks = set()

        self.thread_graph = None
        self.master_graph = master_graph
        #---

        self.thread_graph = ThreadGraph(self)

    @property
    def is_thread_start(self):
        return self in self.thread_graph.start_nodes

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

    def append_edge(self, edge):
        assert isinstance(edge, Edge)
        assert edge.component == self.component

        self.edges.add(edge)

########
    def decide_edge(self, logline):
        assert isinstance(logline, LogLine)

        for edge in self.edges:
            if edge.decide(logline):
                seen_edges.add(edge)
                return edge
        return None


join_id = 0
class Join(object):
    def __init__(self, from_edge, to_edge, schemas, is_remote, is_shared):
        assert isinstance(from_edge, Edge)
        assert isinstance(to_edge, Edge)
        assert isinstance(is_remote, bool)
        assert isinstance(is_shared, bool)

        global join_id
        join_id += 1

        self.name = "j-%d" % join_id
        self.from_edge = from_edge
        self.to_edge = to_edge
        self.schemas = schemas
        self.is_shared = is_shared
        self.is_remote = is_remote

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

    @property
    def schemas(self):
        schemas = self.__dict__.get("schemas", set())
        if not self.is_remote:
            schemas.add(("host", "host"))
        else:
            assert ("host", "host") not in schemas
        if not self.is_shared:
            schemas.add(("request", "request"))
        return schemas

    @schemas.setter
    def schemas(self, value):
        if value is None:
            value = set()
        assert isinstance(value, set)
        self.__dict__["schemas"] = value


class Edge(object):
    def __init__(self, node, keyword):
        assert isinstance(node, Node)
        assert isinstance(keyword, str)

        master_graph = node.master_graph

        self.name = master_graph.generate_edge_name()
        self.component = node.component
        self.node = node
        self.keyword = keyword

        self.joins_objs = set()
        self.joined_objs = set()

        self.thread_graph = node.thread_graph
        self.master_graph = master_graph

    @property
    def request_state(self):
        return self.node.request_state

    @property
    def marks(self):
        return self.node.marks

    def __str__(self):
        join_str=""
        if self.joins_objs:
            join_str += ", %d joins" % len(self.joins_objs)
        if self.joined_objs:
            join_str += ", %d joined" % len(self.joined_objs)
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
    def join_edge(self, edge, schemas=None, is_remote=True, is_shared=False):
        assert isinstance(edge, Edge)
        assert isinstance(is_remote, bool)
        assert isinstance(is_shared, bool)
        # assert edge not in self.joins
        # assert self not in edge.joined

        join_obj = Join(self, edge, schemas, is_remote, is_shared)
        self.joins_objs.add(join_obj)
        edge.joined_objs.add(join_obj)
        self.master_graph.add_join_obj(join_obj)
        if is_shared:
            edge.thread_graph.is_shared = True

########
    def decide(self, logline):
        assert isinstance(logline, LogLine)
        return self.keyword in logline.keyword


class ThreadGraph(object):
    def __init__(self, node):
        assert isinstance(node, Node)

        self.name = None
        self.component = node.component
        self.is_shared = False

        self.nodes = set((node,))
        self.start_nodes = set((node,))
        self.end_nodes = set((node,))

        self.edges = set()

        self.master_graph = node.master_graph

        self.master_graph.add_thread(self)

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

        included = set()
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
                    join_str += ", %d joins" % len(edge.joins_objs)
                if edge.joined_objs:
                    join_str += ", %d joined" % len(edge.joined_objs)
                ret_str[0] += "\n    <Edge#%s -> Node#%s, `%s`%s>" % \
                                (edge.name, edge.node.name,
                                 edge.keyword, join_str)
            # recursive
            for edge in node.edges:
                parse_node(edge.node)

        for start in self.start_nodes:
            parse_node(start)
        return ret_str[0]

    def add_edge(self, from_node, edge):
        assert edge not in self.edges
        assert edge.component is self.component
        assert edge.master_graph is self.master_graph

        self.edges.add(edge)

        thread_graph = edge.thread_graph
        if thread_graph is not self:
            # merge thread_graph
            for node in thread_graph.nodes:
                self.nodes.add(node)
                node.thread_graph = self
            edge.thread_graph = self
            for edge in thread_graph.edges:
                self.edges.add(edge)
                edge.thread_graph = self
            for node in thread_graph.start_nodes:
                self.start_nodes.add(node)
            for node in thread_graph.end_nodes:
                self.end_nodes.add(node)
            thread_graph.remove()

        to_node = edge.node
        if from_node is not to_node:
            self.start_nodes.discard(to_node)
            self.end_nodes.discard(from_node)

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
        self.join_objs = set()

        self.edges = set()
        self.edges_by_from_to = {}

        self.nodes_by_id = {}
        self.start_nodes = set()

        self.thread_graphs = set()

        # after check
        self.threadgraphs_by_component = defaultdict(set)

        self.request_states = set()
        self.marks = set()

        self._thread_index = 0
        self._edge_index = 0

    @property
    def components(self):
        components = set()
        for th_obj in self.thread_graphs:
            components.add(th_obj.component)
        return components

    @property
    def thread_names(self):
        names = set()
        for th_obj in self.thread_graphs:
            names.add(th_obj.name)
        return names

    def __str__(self):
        return ("<Master#%s: %d(%d) nodes, %d edges, %d threads, "
                "%d components, %d req_states, %d marks, %d joins>" %
                (self.name,
                 len(self.nodes_by_id),
                 len(self.start_nodes),
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

    def _generate_thread_name(self):
        self._thread_index += 1
        return "t-%d" % self._thread_index

    def add_thread(self, thread_graph):
        assert isinstance(thread_graph, ThreadGraph)
        thread_graph.name = self._generate_thread_name()
        self.thread_graphs.add(thread_graph)

    def remove_thread(self, thread_graph):
        assert isinstance(thread_graph, ThreadGraph)
        self.thread_graphs.remove(thread_graph)
        self._thread_index = 0
        for thread_graph in self.thread_graphs:
            thread_graph.name = self._generate_thread_name()

    def generate_edge_name(self):
        self._edge_index += 1
        return "e-%d" % self._edge_index

    def add_join_obj(self, join_obj):
        assert isinstance(join_obj, Join)
        assert join_obj not in self.join_objs

        self.join_objs.add(join_obj)

    def _track_node(self, node_id, component):
        """ Get node from tracked nodes. """
        node = self.nodes_by_id.get(node_id)
        if node is None:
            node = Node(node_id, component, self)
            self.nodes_by_id[node_id] = node

        assert component is node.component
        return node

#-------
    def build_edge(self, from_node_id, to_node_id,
                   component, keyword, is_request_start=False):
        assert isinstance(from_node_id, int)
        assert isinstance(to_node_id, int)
        assert isinstance(component, Component)
        assert isinstance(keyword, str)
        assert (from_node_id, to_node_id) not in self.edges_by_from_to
        assert isinstance(is_request_start, bool)

        from_node = self._track_node(from_node_id, component)
        to_node = self._track_node(to_node_id, component)
        edge = Edge(to_node, keyword)
        from_node.append_edge(edge)
        from_node.thread_graph.add_edge(from_node, edge)
        self.edges_by_from_to[(from_node_id, to_node_id)] = edge
        self.edges.add(edge)

        if is_request_start:
            self.start_nodes.add(from_node)
        return edge

    def set_state(self, node_id, state_str, is_mark=False):
        assert isinstance(node_id, int)
        assert isinstance(state_str, str)
        assert isinstance(is_mark, bool)

        node = self.nodes_by_id.get(node_id)
        assert node is not None
        if not is_mark:
            assert node.is_thread_end is True
            assert node.request_state is None
            self.request_states.add(state_str)
            node.request_state = state_str
        else:
            self.marks.add(state_str)
            node.marks.add(state_str)

    def check(self):
        for thread_graph in self.thread_graphs:
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
        threadgraphs = self.threadgraphs_by_component.get(logline.component)
        for thread_graph in threadgraphs:
            node = thread_graph.decide_node(logline)
            if node:
                return thread_graph
        return None
