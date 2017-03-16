from __future__ import print_function
from orderedset import OrderedSet

from workflow_parser.log_parser.service_registry import Component

# Node(id_, name, component, state, is_end, is_start, graph, master_graph)
#   append_edge(edge)
# Edge(name, component, node, keyword, joins, joined, graph, master_graph)
#   join(edge)
# LeafGraph(name, component, nodes, start_nodes, end_nodes, edges, master_graph)
#   add_edge(from_node, edge)
#   remove()
# MasterGraph(name, nodes_by_id, edges_by_from_to, graphs, joins,
#             components, graph_names, states)
#   add_graph(graph)
#   remove_graph(graph)
#   set_state(node_id, state_str)
#   generate_edge_name()
#   add_join(from_edge, to_edge)
#
#   build_edge(from_id, to_id, component, keyword)
#   build_from_driver(driver)


class Node(object):
    def __init__(self, id_, component, master_graph):
        self.id_ = id_
        self.state = None
        self.component = component
        self.edges = OrderedSet()
        self.master_graph = master_graph

        self.graph = LeafGraph(self)

    @property
    def name(self):
        return "n-%d" % self.id_

    @property
    def is_end(self):
        return self in self.graph.end_nodes

    @property
    def is_start(self):
        return self in self.graph.start_nodes

    def __str__(self):
        state_str = ""
        if self.state:
            state_str += ", at %s" % self.state
        return "<Node#%s: %d edges, in %s|%s%s>" \
               % (self.name, len(self.edges), self.component, self.graph.name, state_str)

    def __repr__(self):
        ret_str = str(self)
        for edge in self.edges:
            ret_str += "\n  %s" % edge
        return ret_str

    def append_edge(self, edge):
        assert edge.component == self.component
        self.edges.add(edge)


    # def decide_edge(self, log):
    #     for edge in self.edges:
    #         if edge.accept(log):
    #             return edge
    #     return None

    # def accept_edge(self, edge):
    #     return edge in self.edges


class Edge(object):
    def __init__(self, node, keyword):
        self.component = node.component
        self.node = node
        self.keyword = keyword
        self.joins = set()
        self.joined = set()

        self.graph = node.graph
        self.master_graph = node.master_graph
        self.name = self.master_graph.generate_edge_name()

    def __str__(self):
        join_str=""
        if self.joins:
            join_str += ", %d joins" % len(self.joins)
        if self.joined:
            join_str += ", %d joined" % len(self.joined)
        return "<Edge#%s: Node#%s, `%s`, in %s|%s%s>" \
               % (self.name, self.name, self.keyword,
                  self.component, self.graph.name, join_str)

    def __repr__(self):
        ret_str = str(self)
        ret_str += "\n  %s" % self.node
        ret_str += "\n  %s" % self.graph
        if self.joins:
            ret_str += "\n  joins:"
            for edge in self.joins:
                ret_str += "\n    %s" % edge
        if self.joined:
            ret_str += "\n  joined:"
            for edge in self.joined:
                ret_str += "\n    %s" % edge
        return ret_str

    def join(self, edge):
        assert isinstance(edge, Edge)
        assert edge not in self.joins
        assert self not in edge.joined
        self.joins.add(edge)
        edge.joined.add(self)
        self.master_graph.add_join(self, edge)

    # def accept(self, log):
    #     return self.service == log.service and self.keyword in log.action


class LeafGraph(object):
    def __init__(self, node):
        self.master_graph = node.master_graph
        self.name = None
        self.component = node.component
        self.nodes = set((node,))
        self.start_nodes = set((node,))
        self.end_nodes = set((node,))
        self.edges = set()

        self.master_graph.add_graph(self)

    def __str__(self):
        return ("<LeafGraph#%s: %d(%d, %d) nodes, %d edges, in %s>"
                    % (self.name,
                       len(self.nodes),
                       len(self.start_nodes),
                       len(self.end_nodes),
                       len(self.edges),
                       self.component))

    def __repr__(self):
        included = set()
        ret_str = [""]
        ret_str[0] += "%s:" % self

        def parse_node(node):
            if node in included:
                return
            included.add(node)
            assert node.graph is self
            assert node.component == self.component

            blank = [" ", " "]
            if node in self.start_nodes:
                blank[0] = "+"
            if node in self.end_nodes:
                blank[1] = "-"
            state_str = ""
            if node.state:
                state_str += ", `%s`" % node.state
            ret_str[0] += "\n%s<Node#%s%s>" %("".join(blank), node.name, state_str)
            for edge in node.edges:
                assert edge.graph is self
                assert edge.component == self.component
                join_str=""
                if edge.joins:
                    join_str += ", %d joins" % len(edge.joins)
                if edge.joined:
                    join_str += ", %d joined" % len(edge.joined)
                ret_str[0] += "\n    <Edge#%s: Node#%s, `%s`%s>" % \
                                (edge.name, edge.node.name,
                                 edge.keyword, join_str)

            for edge in node.edges:
                parse_node(edge.node)

        for start in self.start_nodes:
            parse_node(start)
        return ret_str[0]

    def add_edge(self, from_node, edge):
        assert edge not in self.edges
        assert edge.component == self.component
        assert edge.master_graph is self.master_graph

        self.edges.add(edge)
        graph = edge.graph
        to_node = edge.node

        if graph is not self:
            for node in graph.nodes:
                self.nodes.add(node)
                node.graph = self
            edge.graph = self
            for edge in graph.edges:
                self.edges.add(edge)
                edge.graph = self
            for node in graph.start_nodes:
                self.start_nodes.add(node)
            for node in graph.end_nodes:
                self.end_nodes.add(node)
            graph.remove()

        if from_node is not to_node:
            self.start_nodes.discard(to_node)
            self.end_nodes.discard(from_node)

    def remove(self):
        self.master_graph.remove_graph(self)

    # def decide_node_edge(self, log):
    #     for node in self.start_nodes:
    #         edge = node.decide_edge(log)
    #         if edge is not None:
    #             return node, edge
    #     return None, None

    # def accept(self, log):
    #     node, edge = self.decide_node_edge(log)
    #     if node is not None and edge is not None:
    #         return True
    #     else:
    #         return False


class MasterGraph(object):
    def __init__(self, name):
        self.name = name
        self.edges_by_from_to = {}
        self.nodes_by_id = {}
        self.graphs = set()
        self.joins = []

        self._graph_index = 0
        self._edge_index = 0

    @property
    def components(self):
        components = set()
        for sub in self.graphs:
            components.add(sub.component)
        return components

    @property
    def graph_names(self):
        names = set()
        for sub in self.graphs:
            names.add(sub.name)
        return names

    @property
    def states(self):
        states = set()
        for graph in self.graphs:
            for node in graph.end_nodes:
                if node.state is not None:
                    states.add(node.state)
        return states

    def __str__(self):
        return ("<Master#%s: %d nodes, %d edges, %d graphs, "
                "%d components, %d states, %d joins>" %
                (self.name, len(self.nodes_by_id),
                 len(self.edges_by_from_to), len(self.graphs),
                 len(self.components), len(self.states), len(self.joins)))

    def __repr__(self):
        ret_str = str(self)
        ret_str += "\n  Subgraphs:"
        for graph in self.graphs:
            ret_str += "\n    %s" % graph
        ret_str += "\n  Components:"
        for component in self.components:
            ret_str += " %s," % component
        ret_str += "\n  States:    "
        for state in self.states:
            ret_str += " %s," % state
        ret_str += "\n  Joins:"
        for (from_edge, to_edge) in self.joins:
            ret_str += "\n    %s -> %s" % (from_edge, to_edge)
        return ret_str

    def _generate_graph_name(self):
        self._graph_index += 1
        return "g-%d" % self._graph_index

    def add_graph(self, graph):
        graph.name = self._generate_graph_name()
        self.graphs.add(graph)

    def remove_graph(self, graph):
        self.graphs.remove(graph)
        self._graph_index = 0
        for graph in self.graphs:
            graph.name = self._generate_graph_name()

    def generate_edge_name(self):
        self._edge_index += 1
        return "e-%d" % self._edge_index

    def add_join(self, from_edge, to_edge):
        self.joins.append((from_edge, to_edge))

    def _track_node(self, node_id, component):
        """ Get node from tracked nodes. """
        node = self.nodes_by_id.get(node_id)
        if node is None:
            node = Node(node_id, component, self)
            self.nodes_by_id[node_id] = node

        assert component == node.component
        return node

    def build_edge(self, from_id, to_id, component, keyword):
        assert isinstance(from_id, int)
        assert isinstance(from_id, int)
        assert isinstance(component, Component)
        assert isinstance(keyword, str)
        assert (from_id, to_id) not in self.edges_by_from_to

        from_node = self._track_node(from_id, component)
        to_node = self._track_node(to_id, component)
        edge = Edge(to_node, keyword)
        from_node.append_edge(edge)
        from_node.graph.add_edge(from_node, edge)
        self.edges_by_from_to[(from_id, to_id)] = edge
        return edge

    def set_state(self, node_id, state_str):
        assert isinstance(node_id, int)
        assert isinstance(state_str, str)

        node = self.nodes_by_id.get(node_id)
        assert node
        assert node.is_end is True
        node.state = state_str

    def check(self):
        print("\n>>-------------->>")
        print("%r\n" % self)
        print("Graphs:\n")
        for sub in self.graphs:
            print("%r\n" % sub)
        print("<<--------------<<\n")

    # def get_edge(self, from_, to):
    #     return self.edges_by_from_to.get((from_, to))
    # def decide_subgraph(self, log):
    #     for sub in self.graphs:
    #         if sub.accept(log):
    #             return sub
    #     return None
