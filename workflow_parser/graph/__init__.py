# Copyright (c) 2017 Yingxin Cheng
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from collections import defaultdict
from collections import OrderedDict
from orderedset import OrderedSet

from ..service_registry import Component


class NodeBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, name, id_, graph):
        assert isinstance(name, str)
        if id_ is not None:
            assert isinstance(id_, int)
        assert isinstance(graph, Graph)

        self.name = "n%s" % name
        self.id_ = id_
        self.graph = graph
        self.master = graph.master
        self.edges = OrderedSet()
        self.marks = OrderedSet()

    @property
    def namespace(self):
        return self.graph.namespace

    @abstractproperty
    def is_start(self):
        pass

    @abstractproperty
    def is_end(self):
        pass

    def __str_marks__(self):
        state_str = ""
        if self.marks:
            for mark in self.marks:
                state_str += ", *%s" % mark
        return state_str

    def __str__(self):
        return "<%s#%s: %d branches, in %s%s>" \
               % (self.__class__.__name__,
                  self.name,
                  len(self.edges),
                  self.graph.name,
                  self.__str_marks__())

    def __str_graph__(self):
        return "<%s#%s%s>"\
               % (self.__class__.__name__,
                  self.name,
                  self.__str_marks__())

    def __repr__(self):
        ret_str = str(self)
        for edge in self.edges:
            ret_str += "\n  %s" % edge
        return ret_str

#-------
    def build(self, tonode_or_id, payload_or_edge):
        ret_tonode = None
        ret_edge = None

        # create tonode and edge
        if isinstance(payload_or_edge, EdgeBase):
            edge = payload_or_edge
            if isinstance(tonode_or_id, int):
                if edge.node.id_ != tonode_or_id:
                    raise RuntimeError(
                            "build() failed: existing edge %s to_node_id %s"
                            "doesn't match the defined node id %s!"
                            % (edge.name, edge.node.id_, tonode_or_id))
                to_node = edge.node
            else:
                assert isinstance(tonode_or_id, NodeBase)
                to_node = tonode_or_id
                if to_node is not edge.node:
                    raise RuntimeError(
                            "build() failed: existing edge %s to_node %s"
                            "doesn't match the defined node %s!"
                            % (edge.name, edge.node.id_, to_node.id_))
        else:
            payload = payload_or_edge
            if isinstance(tonode_or_id, int):
                edge, to_node = self.graph.create_edge_withnid(
                        self, tonode_or_id, payload)
                ret_tonode = to_node
            else:
                to_node = tonode_or_id
                edge = to_node.graph.create_edge_withnode(
                        to_node, payload)
            ret_edge = edge

        othergraph = edge.graph
        if self.graph is not othergraph:
            self.graph.merge(othergraph)

        assert to_node.graph is self.graph
        assert edge.graph is self.graph

        self.namespace.register_rel(self, edge)
        if self.decide_edge(edge.keyword) is not None:
            raise RuntimeError(
                    "build() failed: cannot build edge with "
                    "conflicting keyword `%s`" % edge.keyword)
        self.edges.add(edge)

        return ret_edge, ret_tonode

########
    def decide_edge(self, keyword):
        assert isinstance(keyword, str)

        for edge in self.edges:
            if edge.keyword in keyword:
                return edge
        return None


class EdgeBase(object):
    def __init__(self, name, node):
        assert isinstance(name, str)
        assert isinstance(node, NodeBase)
        assert not node.is_start
        if isinstance(node.graph, FunctionGraph):
            name = node.graph.func_name.capitalize() + name

        self.name = "e" + name
        self.graph = node.graph
        self.master = node.master
        self.node = node

    @abstractproperty
    def keyword(self):
        pass

    def __str_marks__(self):
        return ""

    def __str__(self):
        return "<%s#%s->%s: `%s`, in %s%s>" \
               % (self.__class__.__name__,
                  self.name,
                  self.node.name,
                  self.keyword,
                  self.graph.name,
                  self.__str_marks__())

    def __str_graph__(self):
        return "<%s#%s->%s: `%s`%s>"\
               % (self.__class__.__name__,
                  self.name,
                  self.node.name,
                  self.keyword,
                  self.__str_marks__())

    def __repr__(self):
        ret_str = str(self)
        ret_str += "\n  %s" % self.node
        ret_str += "\n  %s" % self.graph

        return ret_str

#-------
    def _join(self, edge, schemas, interface, is_remote):
        if interface is None:
            join_obj = self.master._create_innerjoin(
                    self, edge, schemas, is_remote)
        else:
            assert isinstance(interface, str)
            join_obj = self.master._create_interface(
                    self, edge, schemas, is_remote, interface)
        return join_obj

    def join_local(self, edge, schemas=None, interface=None):
        return self._join(edge, schemas, interface, False)

    def join_remote(self, edge, schemas=None, interface=None):
        return self._join(edge, schemas, interface, True)


class KwEdge(EdgeBase):
    def __init__(self, keyword, node, index):
        assert isinstance(keyword, str)
        assert isinstance(index, int)
        super(KwEdge, self).__init__(str(index), node)

        self._keyword = keyword
        # inner request
        self.joins_objs = OrderedSet()
        self.joined_objs = OrderedSet()
        # cross request
        self.joined_interface = None
        self.joins_interface = None

    @property
    def keyword(self):
        return self._keyword

    def __str_marks__(self):
        join_str=super(KwEdge, self).__str_marks__()
        for jo in self.joins_objs:
            assert isinstance(jo, InnerJoin)
            if isinstance(jo, RequestInterface):
                join_str += ", %s~~>%s" % (jo.name, jo.to_edge.name)
            else:
                join_str += ", %s->%s" % (jo.name, jo.to_edge.name)
        for jo in self.joined_objs:
            assert isinstance(jo, InnerJoin)
            if isinstance(jo, RequestInterface):
                join_str += ", %s<~~%s" % (jo.name, jo.from_edge.name)
            else:
                join_str += ", %s<-%s" % (jo.name, jo.from_edge.name)

        if self.joins_interface:
            assert isinstance(self.joins_interface, InterfaceJoin)
            join_str += ", %s=>%s" % (self.joins_interface.name,
                                      self.joins_interface.upper_edge.name)
        if self.joined_interface:
            assert isinstance(self.joined_interface, InterfaceJoin)
            join_str += ", %s<=%s" % (self.joined_interface.name,
                                      self.joined_interface.upper_edge.name)
        return join_str

    def __repr__(self):
        ret_str = super(FnEdge, self).__repr__()
        if self.joins_objs:
            ret_str += "\n  joins:"
            for join_obj in self.joins_objs:
                ret_str += "\n    %s" % join_obj
        if self.joined_objs:
            ret_str += "\n  joined:"
            for join_obj in self.joined_objs:
                ret_str += "\n    %s" % join_obj
        if self.joined_interface:
            ret_str += "\n  joined interface: %s" % self.joined_interface
        if self.joins_interface:
            ret_str += "\n  joins interface: %s" % self.joins_interface
        return ret_str


class Graph(object):
    __metaclass__ = ABCMeta

    def __init__(self, name, master):
        assert isinstance(name, str)

        self.name = name
        self.master = master

        self.nodes = OrderedSet()
        self.edges = OrderedSet()

    @abstractproperty
    def namespace(self):
        return None

    def __str_marks__(self):
        return ""

    def __str__(self):
        return ("<%s#%s: %d nodes, %d edges%s>"
                    % (self.__class__.__name__,
                       self.name,
                       len(self.nodes),
                       len(self.edges),
                       self.__str_marks__()))

    def __str_parsenode__(self, node, included, ret_str):
        assert isinstance(node, NodeBase)
        assert isinstance(included, set)
        assert isinstance(ret_str, str)
        if node in included:
            return ret_str
        assert node.graph is self
        included.add(node)
        # print node
        blank = [" ", " "]
        if node.is_start:
            blank[0] = "+"
        if node.is_end:
            blank[1] = "-"
        ret_str += "\n%s%s" %("".join(blank), node.__str_graph__())
        # print edge
        for edge in node.edges:
            assert edge.graph is self
            ret_str += "\n    %s" % edge.__str_graph__()
        # recursive
        for edge in node.edges:
            ret_str = self.__str_parsenode__(edge.node, included, ret_str)
        return ret_str

    def create_edge_withnode(self, to_node, payload):
        assert isinstance(to_node, NodeBase)
        assert to_node.graph is self
        edge = self.namespace.create_edge(to_node, payload)
        self.edges.add(edge)

        return edge

    @abstractmethod
    def create_edge_withnid(self, cls, from_node, tonode_id, payload):
        to_node = self.namespace.create_node(cls, self, tonode_id)
        self.nodes.add(to_node)
        edge = self.create_edge_withnode(to_node, payload)

        return edge, to_node

    def merge(self, graph):
        assert isinstance(graph, Graph)
        raise RuntimeError(
                "merge() failed: cannot merge graph %s with %s"
                % (self.graph.name, othergraph.name))


class GraphNamespace(object):
    def __init__(self, namespace, **kwds):
        super(GraphNamespace, self).__init__(**kwds)
        assert isinstance(namespace, str)

        self.namespace = namespace
        self.nodes_by_name = {}
        self.nodes_by_id = {}
        self.edges_by_name = OrderedDict()
        self.edges_by_fromto = defaultdict(list)

        self._kedge_index = 0
        self._fedgeindex_byfunc = defaultdict(lambda: 0)

    def iter_edges(self):
        return self.edges_by_name.itervalues()

    def iter_nodes(self):
        return self.nodes_by_name.itervalues()

    def create_node_unindexed(self, cls, graph, name):
        assert issubclass(cls, NodeBase)
        assert isinstance(graph, Graph)

        node = cls(name, None, graph)
        if node.name in self.nodes_by_name:
            raise RuntimeError(
                    "Failed to create start node %s: "
                    "name confliction." % node.name)
        self.nodes_by_name[node.name] = node
        return node

    def create_node(self, cls, graph, node_id):
        assert issubclass(cls, NodeBase)
        assert isinstance(graph, Graph)
        node = cls(str(node_id), node_id, graph)

        if node.id_ in self.nodes_by_id:
            raise RuntimeError(
                    "Failed to create node id %s: "
                    "node id already exists." % node_id)
        self.nodes_by_id[node.id_] = node

        if node.name in self.nodes_by_name:
            raise RuntimeError(
                    "Failed to create node %s: "
                    "name confliction %s" % node.name)
        self.nodes_by_name[node.name] = node
        return node

    def create_edge(self, to_node, payload):
        assert isinstance(to_node, NodeBase)
        assert to_node.namespace is self
        assert to_node.name in self.nodes_by_name
        if to_node.id_ is not None:
            assert to_node.id_ in self.nodes_by_id

        if isinstance(payload, str):
            self._kedge_index += 1
            edge = KwEdge(payload, to_node, self._kedge_index)
        else:
            assert isinstance(payload, FunctionGraph)
            self._fedgeindex_byfunc[payload.func_name] += 1
            edge = FnEdge(payload, to_node,
                    self._fedgeindex_byfunc[payload.func_name])
        assert edge.name not in self.edges_by_name
        self.edges_by_name[edge.name] = edge
        return edge

    def register_rel(self, from_node, edge):
        assert isinstance(from_node, NodeBase)
        assert isinstance(edge, EdgeBase)
        assert from_node.name in self.nodes_by_name
        assert edge.name in self.edges_by_name

        key = (from_node.name, edge.node.name)
        for _edge in self.edges_by_fromto[key]:
            if _edge.keyword == edge.keyword:
                raise RuntimeError(
                        "Failed to build edge: already has keyword %s "
                        "in relationship from node %s to %s with edge %s" %
                        (edge.keyword, key[0], key[1], _edge.name))
        self.edges_by_fromto[key].append(edge)

    def __str_marks__(self):
        return "%d(%d) nodes, %d(%d) edges" % (
                len(self.nodes_by_name),
                len(self.nodes_by_id),
                len(self.edges_by_name),
                sum(len(l) for l in
                    self.edges_by_fromto.itervalues()))


class JoinBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, name, from_entity, to_entity, schemas, is_remote):
        assert isinstance(name, str)
        assert from_entity.master is to_entity.master
        assert isinstance(is_remote, bool)
        if schemas is None:
            schemas = []
        assert isinstance(schemas, list)

        self.name = "jr"+name if is_remote else "jl"+name
        self.from_entity = from_entity
        self.to_entity = to_entity
        self.is_remote = is_remote
        self.master = from_entity.master

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
        assert ("thread", "thread") not in self.schemas

    def __str_marks__(self):
        if self.is_remote:
            return ", remote"
        else:
            return ", local"

    def __repr__(self):
        return "<%s#%s: %s->%s, %d schemas%s>" % (
                self.__class__.__name__,
                self.name,
                self.from_entity.name,
                self.to_entity.name,
                len(self.schemas),
                self.__str_marks__())


class InnerJoin(JoinBase):
    def __init__(self, name, from_edge, to_edge, schemas, is_remote):
        super(InnerJoin, self).__init__(name, from_edge, to_edge,
                                          schemas, is_remote)
        assert isinstance(from_edge, KwEdge)
        assert isinstance(to_edge, KwEdge)
        assert self not in from_edge.joins_objs
        assert self not in to_edge.joined_objs

        self.schemas.add(("request", "request"))
        # NOTE: current multiple joins_objs are or-split
        from_edge.joins_objs.add(self)
        to_edge.joined_objs.add(self)

    @property
    def from_edge(self):
        return self.from_entity

    @property
    def to_edge(self):
        return self.to_entity

    def __str_marks__(self):
        mark = super(InnerJoin, self).__str_marks__()
        mark += ", [%s|-->%s]" % (self.from_edge.keyword,
                                  self.to_edge.keyword)
        return mark


class RequestInterface(InnerJoin):
    def __init__(self, name, from_edge, to_edge, schemas, is_remote):
        super(RequestInterface, self).__init__(name.capitalize(), from_edge, to_edge,
                                               schemas, is_remote)
        self.join_pairs = []
        self.req_name = name

    @property
    def joins_interfaces(self):
        ret = []
        for l, r in self.join_pairs:
            ret.append(l)
        return ret

    @property
    def joined_interfaces(self):
        ret = []
        for l, r in self.join_pairs:
            ret.append(r)
        return ret

    def __str_marks__(self):
        mark = super(InnerJoin, self).__str_marks__()
        mark += ", [%s|~~>%s]" % (self.from_edge.keyword,
                                  self.to_edge.keyword)
        for joins, joined in self.join_pairs:
            mark += ", [=>%s %s | %s %s=>]" % (
                    joins.edge.name,
                    joins.edge.keyword,
                    joined.edge.name,
                    joined.edge.keyword)

        if not self.join_pairs:
            mark += ", EMPTY INTERFACE"
        return mark

    def call_req(self, joins_edge, joins_schema, joins_isremote,
                       joined_edge, joined_schema, joined_isremote):
        joins = InterfaceJoin(True, self, joins_edge, joins_schema,
                joins_isremote)
        joined = InterfaceJoin(False, self, joined_edge, joined_schema,
                joined_isremote)
        joins.pair = joined
        joined.pair = joins
        self.join_pairs.append((joins, joined))


class InterfaceJoin(JoinBase):
    def __init__(self, is_left, interface, edge, schemas, is_remote):
        assert isinstance(is_left, bool)
        assert isinstance(interface, RequestInterface)
        assert isinstance(edge, KwEdge)
        assert ("request", "request") not in schemas

        self.is_left = is_left
        self.interface = interface
        self.edge = edge
        self.pair = None
        if is_left:
            super(InterfaceJoin, self).__init__(
                    "z"+interface.req_name.capitalize(),
                    interface, edge,
                    schemas, is_remote)
            assert edge.joined_interface is None
            edge.joined_interface = self
        else:
            super(InterfaceJoin, self).__init__(
                    "y"+interface.req_name.capitalize(),
                    edge, interface,
                    schemas, is_remote)
            assert edge.joins_interface is None
            edge.joins_interface = self

    @property
    def upper_edge(self):
        if self.is_left:
            return self.interface.from_edge
        else:
            return self.interface.to_edge

    def __str_marks__(self):
        mark = super(InterfaceJoin, self).__str_marks__()
        if self.is_left:
            mark += ", left"
        else:
            mark += ", right"
        return mark


class FnEdge(EdgeBase):
    def __init__(self, func_graph, node, index):
        assert isinstance(func_graph, FunctionGraph)
        assert isinstance(index, int)

        if index == 1:
            name = "\""+func_graph.func_name.capitalize()+"\""
        else:
            name = "\""+func_graph.func_name.capitalize()+str(index)+"\""
        super(FnEdge, self).__init__(name, node)
        self.func_graph = func_graph

    @property
    def keyword(self):
        return self.func_graph.keyword

    @property
    def func_name(self):
        return self.func_graph.func_name

    def __repr__(self):
        ret_s = super(FnEdge, self).__repr__(self)
        ret_s += "\n  %s" % self.func_graph
        return ret_s


class FnNode(NodeBase):
    def __init__(self, name, id_, graph):
        assert isinstance(graph, FunctionGraph)
        super(FnNode,self).__init__(
                graph.func_name.capitalize()+name,
                id_, graph)

    @property
    def is_start(self):
        return self is self.graph.start_node

    @property
    def is_end(self):
        return self is self.graph.end_node

    def set_state(self, state_str, is_mark=False):
        if not is_mark:
            raise RuntimeError("FnNode %s cannot be assigned "
                    "request_state %s." % (self.name, state_str))
        else:
            super(FnNode, self).set_state(state_str)


    def build_endf(self, payload_or_edge):
        assert self.graph.end_node
        self.graph._tail_nodes.discard(self)
        return self.build(self.graph.end_node, payload_or_edge)


class FunctionGraph(Graph):
    def __init__(self, func_name, master):
        assert isinstance(func_name, str)
        super(FunctionGraph, self).__init__("g"+func_name.capitalize(), master)

        self._namespace = GraphNamespace(func_name)
        self.func_name = func_name
        self.start_node = self.namespace.create_node_unindexed(
                FnNode, self, "S")
        self.end_node = self.namespace.create_node_unindexed(
                FnNode, self, "E")
        self._tail_nodes = OrderedSet()

        assert self.start_node.is_start
        assert self.end_node.is_end
        self.nodes.update([self.start_node, self.end_node])

    @property
    def namespace(self):
        return self._namespace

    @property
    def keyword(self):
        return self.start_edge.keyword

    @property
    def start_edge(self):
        assert len(self.start_node.edges) == 1
        return iter(self.start_node.edges).next()

    def __str__(self):
        str_tail = ""
        if self._tail_nodes:
            str_tail += ", TAILNODES: %s" % (",".join(node.name
                for node in self._tail_nodes))
        return ("<%s#%s: %s, `%s`%s>" % (
            self.__class__.__name__,
            self.name,
            self.namespace.__str_marks__(),
            self.keyword,
            str_tail))

    def __repr__(self):
        ret_str = "%s:" % self

        included = set()
        ret_str = self.__str_parsenode__(self.start_node, included, ret_str)
        assert included == set(self.nodes)

        return ret_str

    def create_edge_withnid(self, from_node, tonode_id, payload):
        edge, to_node = super(FunctionGraph, self).create_edge_withnid(
                FnNode, from_node, tonode_id, payload)
        self._tail_nodes.add(to_node)
        self._tail_nodes.discard(from_node)
        return edge, to_node


class TdNode(NodeBase):
    def __init__(self, name, id_, graph):
        assert isinstance(graph, ThreadGraph)
        super(TdNode, self).__init__(name, id_, graph)

        self.request_state = None

    @property
    def is_start(self):
        return self in self.graph.start_nodes

    @property
    def is_end(self):
        return self in self.graph.end_nodes

    @property
    def is_request_end(self):
        if self.request_state:
            assert self.is_end
            assert self in self.master.end_nodes
            return True
        else:
            return False

    def __str_marks__(self):
        ret_s = super(TdNode, self).__str_marks__()
        if self.request_state:
            ret_s += ", @%s" % self.request_state
        return ret_s

    def set_state(self, state_str, is_mark=False):
        if not is_mark:
            if not self.is_end:
                raise RuntimeError("TdNode %s must be an end node."
                        % self.name)
            if self.request_state is not None:
                raise RuntimeError("TdNode %s already has request state"
                        " %s, cannot be assigned %s."
                        % (self.name, self.request_state, state_str))
            self.request_state = state_str
            self.master.end_nodes.add(self)
        else:
            super(TdNode, self).set_state(state_str)


class ReqNode(TdNode):
    def __init__(self, request_type, id_, graph):
        assert isinstance(request_type, str)
        assert id_ is None
        super(ReqNode, self).__init__(
                "r"+request_type.capitalize(),
                id_, graph)

        self.request_type = request_type

    @property
    def is_start(self):
        assert super(ReqNode, self).is_start is True
        return True

    @property
    def is_end(self):
        assert super(ReqNode, self).is_end is False
        return False

    def __str_marks__(self):
        state = super(ReqNode, self).__str_marks__()
        state += ", %s" % self.request_type
        return state


class ThreadGraph(Graph):
    startnode_index = 0

    def __init__(self, t_name, component, master, request_type):
        super(ThreadGraph, self).__init__(t_name, master)
        assert isinstance(component, Component)

        self.component = component
        self.start_nodes = OrderedSet()
        self.end_nodes = OrderedSet()

        # internal init
        if request_type is not None:
            n_cls = ReqNode
            name = request_type
        else:
            n_cls = TdNode
            self.__class__.startnode_index += 1
            name = "S%d" % self.startnode_index
        s_node = self.namespace.create_node_unindexed(
                n_cls, self, name)
        self.start_nodes.add(s_node)
        self.nodes.add(s_node)
        if request_type is not None:
            self.master._register_request(s_node)
        assert s_node.is_start

    @property
    def namespace(self):
        return self.master.namespace

    def __str_marks__(self):
        ret = super(ThreadGraph, self).__str_marks__()
        ret += ", %s" % self.component
        ret += ", s%d/e%d nodes" % (len(self.start_nodes), len(self.end_nodes))
        return ret

    def __repr__(self):
        ret_str = "%s:" % self

        included = set()
        for sn in self.start_nodes:
            ret_str = self.__str_parsenode__(sn, included, ret_str)
        assert included == set(self.nodes)

        return ret_str

    def create_edge_withnid(self, from_node, tonode_id, payload):
        edge, to_node = super(ThreadGraph, self).create_edge_withnid(
                TdNode, from_node, tonode_id, payload)
        self.end_nodes.add(to_node)
        self.end_nodes.discard(from_node)

        return edge, to_node

    def merge(self, graph):
        if not isinstance(graph, ThreadGraph):
            return super(ThreadGraph, self).merge(graph)

        assert graph.component is self.component
        assert graph.master is self.master

        if graph is self:
            return

        # merge graph
        for node in graph.nodes:
            self.nodes.add(node)
            node.graph = self
        for edge in graph.edges:
            self.edges.add(edge)
            edge.graph = self
        for node in graph.start_nodes:
            self.start_nodes.add(node)
        for node in graph.end_nodes:
            self.end_nodes.add(node)
        self.master._remove_thread(graph)



class Master(object):
    def __init__(self, name):
        assert isinstance(name, str)

        self.namespace = GraphNamespace(name)

        self.name = name
        self.join_objs = OrderedSet()
        self.interfaces = OrderedDict()

        self.reqnode_byrtype = OrderedDict()
        self.end_nodes = OrderedSet()

        self.thread_graphs = OrderedSet()
        self.funcgraphs_byname = OrderedDict()

        self.marks = OrderedSet()

        # after check
        self.threadgraphs_by_component = defaultdict(OrderedSet)

        self._thread_index = 0
        self._innerjoin_index = 0
        self._interfacejoin_index = 0

        self.seen_edges = OrderedSet()

    @property
    def request_types(self):
        return list(self.reqnode_byrtype.iterkeys())

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

    @property
    def request_states(self):
        ret = OrderedSet()
        for node in self.end_nodes:
            ret.add(node.request_state)
        return ret

    def __str__(self):
        return ("<Master#%s: %d functions, %s, %d threads, "
                "%d components, s%d/e%d reqs, %d marks, "
                "%d joins, %d interfaces>" %
                (self.name,
                 len(self.funcgraphs_byname),
                 self.namespace.__str_marks__(),
                 len(self.thread_graphs),
                 len(self.components),
                 len(self.reqnode_byrtype),
                 len(self.end_nodes),
                 len(self.marks),
                 len(self.join_objs),
                 len(self.interfaces)))

    def __repr__(self):
        ret_str = str(self)
        ret_str += "\n  Functions:"
        for fn_obj in self.funcgraphs_byname.itervalues():
            ret_str += "\n    %s" % fn_obj
        ret_str += "\n  Threads:"
        for th_obj in self.thread_graphs:
            ret_str += "\n    %s" % th_obj
        ret_str += "\n  Request nodes:"
        for node in self.reqnode_byrtype.itervalues():
            ret_str += "\n    %s" % node
        ret_str += "\n  End nodes:"
        for node in self.end_nodes:
            ret_str += "\n    %s" % node
        ret_str += "\n  Components: %s"\
                   % (",".join(str(c) for c in self.components))
        ret_str += "\n  Request states: %s"\
                   % (",".join(s for s in self.request_states))
        if self.marks:
            ret_str += "\n  Marks:%s"\
                       % (",".join(m for m in self.marks))
        ret_str += "\n  Joins:"
        for join_obj in self.join_objs:
            ret_str += "\n    %s" % join_obj
        ret_str += "\n  Interfaces:"
        for interface in self.interfaces.itervalues():
            ret_str += "\n    %s" % interface
        return ret_str

    def _remove_thread(self, graph):
        assert isinstance(graph, ThreadGraph)
        assert graph.master is self

        self.thread_graphs.remove(graph)
        self._thread_index = 0
        for thread_graph in self.thread_graphs:
            self._thread_index += 1
            thread_graph.name = "t%d" % self._thread_index

    def _register_request(self, rnode):
        assert isinstance(rnode, ReqNode)
        assert rnode.is_start
        rtype = rnode.request_type

        if rtype in self.reqnode_byrtype:
            raise RuntimeError(
                    "Failed to register request node: "
                    "Already defined request type %s" % rtype)
        self.reqnode_byrtype[rtype] = rnode

    def _create_innerjoin(self, from_edge, to_edge,
                          schemas, is_remote):
        self._innerjoin_index += 1
        join_obj = InnerJoin(str(self._innerjoin_index),
                             from_edge, to_edge,
                             schemas, is_remote)
        assert isinstance(join_obj, JoinBase)
        self.join_objs.add(join_obj)
        return join_obj

    def _create_interface(self, from_edge, to_edge,
                          schemas, is_remote, interface):
        assert isinstance(interface, str)
        assert interface not in self.interfaces
        join_obj = RequestInterface(interface, from_edge, to_edge,
                                    schemas, is_remote)
        self.interfaces[interface] = join_obj
        return join_obj

#-------
    def build_thread(self, component, tonode_or_id, payload_or_edge,
                     request_type=None):
        if request_type is not None and not isinstance(request_type, str):
            raise RuntimeError("build_thread() requires request_type as str!")
        if not isinstance(component, Component):
            raise RuntimeError("build_thread() requires component as Component!")

        self._thread_index += 1
        t_name = "g%d" % self._thread_index
        thread = ThreadGraph(t_name, component, self, request_type)
        self.thread_graphs.add(thread)
        assert len(thread.start_nodes) == 1
        from_node = iter(thread.start_nodes).next()
        return from_node.build(tonode_or_id, payload_or_edge)

    def build_func(self, tonode_or_id, payload_or_edge, func_name):
        if not isinstance(func_name, str):
            raise RuntimeError("build_func() requires func_name as str!")
        if func_name in self.funcgraphs_byname:
            raise RuntimeError("build_func() error: already has function %s!"
                    % func_name)

        function = FunctionGraph(func_name, self)
        self.funcgraphs_byname[func_name] = function
        from_node = function.start_node
        edge, to_node = from_node.build(tonode_or_id, payload_or_edge)
        return edge, to_node, function

########
    def check(self):
        for thread_graph in self.thread_graphs:
            assert thread_graph.start_nodes
            assert thread_graph.end_nodes
            self.threadgraphs_by_component[thread_graph.component].add(thread_graph)

        print("\n>>--------------")
        print("%r\n" % self)
        print("--<FunctionGraphs>--")
        for fn_g in self.funcgraphs_byname.itervalues():
            print("%r\n" % fn_g)
        print("--<ThreadGraphs>--")
        for th_g in self.thread_graphs:
            print("%r\n" % th_g)
        print("--------------<<\n")

    def get_unseenedges(self):
        edges = OrderedSet()
        edges.update(self.namespace.iter_edges())
        for func in self.funcgraphs_byname.itervalues():
            edges.update(func.namespace.iter_edges())

        return edges - self.seen_edges
