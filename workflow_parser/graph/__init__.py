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
from .joinables import CrossCalleeMixin
from .joinables import InnerjoinMixin
from .joinables import JoinMasterMixin


def _defensive_check(cls, obj, *attrs):
    for attr in attrs:
        assert not hasattr(super(cls, obj), attr)


def _node_build(from_node, tonode_or_id, payload_or_edge):
    assert isinstance(from_node, NodeBase)
    graph = from_node.graph
    namespace = graph.namespace

    othergraph = None
    ret_tonode = None
    ret_edge = None

    def _validate_othergraph():
        if namespace is not othergraph.namespace:
            raise RuntimeError(
                    "NodeBuild failed, namespace confliction: %s =/=> %s!" % (
                        namespace.namespace, othergraph.namespace.namespace))

    # create tonode, edge and othergraph
    if isinstance(payload_or_edge, EdgeBase):
        edge = payload_or_edge
        othergraph = edge.graph
        _validate_othergraph()
        if isinstance(tonode_or_id, NodeBase):
            to_node = tonode_or_id
            if to_node is not edge.node:
                raise RuntimeError(
                        "NodeBuild failed, node doesn't match: edge %s's node %s"
                        "!= input node %s!" % (
                            edge.name, edge.node.index, to_node.index))
        elif isinstance(tonode_or_id, int):
            to_node = edge.node
            if to_node.index != tonode_or_id:
                raise RuntimeError(
                        "NodeBuild failed: node doesn't match: edge %s's node_id %s"
                        "!= input node_id %s!" % (
                            edge.name, edge.node.index, tonode_or_id))
        else:
            raise RuntimeError(
                    "NodeBuild failed, wrong to_id type: %s!" %
                    tonode_or_id.__class__)
    else:
        payload = payload_or_edge
        if isinstance(tonode_or_id, NodeBase):
            to_node = tonode_or_id
            othergraph = to_node.graph
            _validate_othergraph()
            edge = othergraph.create_edge_withnode(to_node, payload)
        elif isinstance(tonode_or_id, int):
            edge, to_node = graph.create_edge_withnid(tonode_or_id, payload)
            ret_tonode = to_node
        else:
            raise RuntimeError(
                    "NodeBuild failed, wrong to_id type: %s!" %
                    tonode_or_id.__class__)
        ret_edge = edge

    if othergraph and graph is not othergraph:
        if not isinstance(graph, ThreadGraph):
            raise RuntimeError(
                    "NodeBuild failed, cannot merge: %s %s!" % (
                        graph.__class__.__name__, graph.name))
        if not isinstance(othergraph, ThreadGraph):
            raise RuntimeError(
                    "NodeBuild failed, cannot be merged: %s %s!" % (
                        othergraph.__class__.__name__, othergraph.name))
        graph.merge(othergraph)

    assert to_node.graph is graph
    assert to_node in graph.nodes

    return ret_edge, ret_tonode, edge


class NodeBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, name, index, graph):
        assert isinstance(name, str)
        assert isinstance(index, int) or isinstance(index, str)
        assert isinstance(graph, GraphBase)

        self.name = "n"+name
        self.index = index
        self.graph = graph
        self.edges = OrderedSet()
        self.marks = OrderedSet()
        self.vis_weight = 0

    @property
    def is_start(self):
        return self in self.graph.start_nodes

    @property
    def is_end(self):
        return self in self.graph.end_nodes

    def __repr_marks__(self):
        ret = ""
        if self.marks:
            ret += ", *" + "-".join(self.marks)
        return ret

    def __repr__(self):
        return "<%s#%s: %d branches, in %s%s>" \
               % (self.__class__.__name__,
                  self.name,
                  len(self.edges),
                  self.graph.name,
                  self.__repr_marks__())

    def __str__(self):
        ret_str = repr(self)
        for edge in self.edges:
            ret_str += "\n  "+repr(edge)
        ret_str += "\n  "+repr(self.graph)
        return ret_str

    def __repr_graph__(self):
        return "<%s#%s%s>"\
               % (self.__class__.__name__,
                  self.name,
                  self.__repr_marks__())

    def set_state(self, state_str, is_mark=False):
        assert not self.is_start
        if not is_mark:
            raise RuntimeError("set_state() failed, "
                    "cannot assign reqstate:%s %s <- %s!" % (
                        self.__class__.__name__, self.name, state_str))
        else:
            self.marks.add(state_str)

    def build(self, tonode_or_id, payload_or_edge):
        ret_edge, ret_tonode, edge = _node_build(
                self, tonode_or_id, payload_or_edge)
        assert edge.graph is self.graph

        conflict = self.decide_edge(edge.keyword)
        if conflict is not None:
            raise RuntimeError(
                    "build() failed, keyword conflict:"
                    "%s-> %s`%s` vs %s`%s`" % (
                        self.name,
                        conflict.name, conflict.keyword,
                        edge.name, edge.keyword))
        self.edges.add(edge)
        self.graph.register_edge(self, edge)
        return ret_edge, ret_tonode

    def decide_edge(self, keyword):
        assert isinstance(keyword, str)

        for edge in self.edges:
            if edge.keyword in keyword:
                return edge
        return None


class EdgeBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, name, node, keyword):
        assert isinstance(name, str)
        assert isinstance(node, NodeBase)
        assert isinstance(keyword, str)
        assert not node.is_start

        self.name = "e"+name
        self.graph = node.graph
        self.node = node
        self.keyword = keyword

    def __repr_marks__(self):
        return ""

    def __repr__(self):
        return "<%s#%s->%s: `%s`, in %s%s>" \
               % (self.__class__.__name__,
                  self.name,
                  self.node.name,
                  self.keyword,
                  self.graph.name,
                  self.__repr_marks__())

    def __repr_graph__(self):
        return "<%s#%s->%s: `%s`%s>"\
               % (self.__class__.__name__,
                  self.name,
                  self.node.name,
                  self.keyword,
                  self.__repr_marks__())

    def __str__(self):
        ret_str = repr(self)
        ret_str += "\n  "+repr(self.node)
        ret_str += "\n  "+repr(self.graph)
        return ret_str


class KwEdge(InnerjoinMixin, CrossCalleeMixin, EdgeBase):
    def __init__(self, keyword, node, name):
        assert isinstance(node, NodeBase)
        super(KwEdge, self).__init__(
                jm_name="e"+name,
                jm_master=node.graph.master,
                jm_keyword=keyword,
                name=name,
                node=node,
                keyword=keyword)


class ClEdge(EdgeBase):
    def __init__(self, func_graph, node, name):
        assert isinstance(func_graph, FunctionGraph)

        super(ClEdge, self).__init__(
                name=name,
                node=node,
                keyword=func_graph.keyword)

        self.func_graph = func_graph

    @property
    def func_name(self):
        return self.func_graph.func_name

    @property
    def func_startedge(self):
        return self.func_graph.start_edge

    def __str__(self):
        ret_s = super(ClEdge, self).__str__()
        ret_s += "\n  Calls:"+repr(self.func_graph)
        return ret_s


class GraphBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, name, master, namespace):
        assert isinstance(name, str)
        assert isinstance(master, Master)
        assert isinstance(namespace, GraphNamespaceMixin)

        self.name = "g"+name
        self.master = master
        self.namespace = namespace

        self.start_nodes = OrderedSet()
        self.end_nodes = OrderedSet()
        self.nodes = OrderedSet()
        self.edges = OrderedSet()

    def __repr_marks__(self):
        return ""

    def __repr__(self):
        return ("<%s#%s: %d(%d,%d) nodes, %d edges, in %s%s>"
                    % (self.__class__.__name__,
                       self.name,
                       len(self.nodes),
                       len(self.start_nodes),
                       len(self.end_nodes),
                       len(self.edges),
                       self.namespace.ns_name,
                       self.__repr_marks__()))

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
        ret_str += "\n%s%s" %("".join(blank), node.__repr_graph__())
        # print edge
        for edge in node.edges:
            assert edge.graph is self
            ret_str += "\n    %s" % edge.__repr_graph__()
        # recursive
        for edge in node.edges:
            ret_str = self.__str_parsenode__(edge.node, included, ret_str)
        return ret_str

    def __str__(self):
        ret_str = "%r:" % self

        included = set()
        for sn in self.start_nodes:
            ret_str = self.__str_parsenode__(sn, included, ret_str)
        if included-self.nodes:
            ret_str += "\nERROR EXTRA NODES: " +\
                    ",".join(n.name for n in included-self.nodes)
        if self.nodes-included:
            ret_str += "\nERROR UNSEEN NODES: " +\
                    ",".join(n.name for n in self.nodes-included)

        return ret_str

    @abstractmethod
    def _create_node_override(self, id_):
        pass

    @abstractmethod
    def _create_edge_override(self, to_node, payload):
        pass

    def create_edge_withnode(self, to_node, payload):
        assert isinstance(to_node, NodeBase)
        assert to_node.graph is self
        edge = self._create_edge_override(to_node, payload)
        self.edges.add(edge)
        return edge

    def create_edge_withnid(self, tonode_id, payload):
        to_node = self._create_node_override(tonode_id)
        self.nodes.add(to_node)

        edge = self.create_edge_withnode(to_node, payload)
        self.end_nodes.add(to_node)
        return edge, to_node

    def register_edge(self, from_node, edge):
        assert from_node in self.nodes
        assert edge in self.edges
        self.namespace._ns_register_edge(from_node, edge)
        self.end_nodes.discard(from_node)


class GraphNamespaceMixin(object):
    def __init__(self, ns_namespace, **kwds):
        assert isinstance(ns_namespace, str)

        super(GraphNamespaceMixin, self).__init__(**kwds)
        _defensive_check(GraphNamespaceMixin, self,
                "ns_name", "ns_node_byname", "ns_node_byid",
                "ns_edge_byname", "ns_edges_byfromto",
                "_ns_edge_index", "_ns_fnedge_index_byfn",
                "_ns_iter_edges", "_ns_iter_nodes")

        self.ns_name = "ns_"+ns_namespace
        self.ns_node_byname = {}
        self.ns_node_byid = {} # all nodes
        self.ns_edge_byname = OrderedDict() # all edges
        self.ns_edges_byfromto = defaultdict(list)

        self._ns_edge_index = 0
        self._ns_fnedge_index_byfn = defaultdict(lambda: 0)

    def _ns_iter_edges(self):
        for edge in self.ns_edge_byname.itervalues():
            yield edge

    def _ns_iter_nodes(self):
        for node in self.ns_node_byid.itervalues():
            yield node

    def _ns_create_node(self, cls, graph, index, is_id=False, prefix=""):
        assert issubclass(cls, NodeBase)
        assert isinstance(graph, GraphBase)
        assert isinstance(is_id, bool)

        if is_id:
            if not isinstance(index, int):
                raise RuntimeError(
                        "ns_create_node failed, node id isn't int:"
                        "%s!" % index)
            if index in self.ns_node_byid:
                raise RuntimeError(
                        "ns_create_node failed, node id confliction:"
                        "%s!" % index)
            node = cls(prefix+str(index), index, graph)
            self.ns_node_byid[index] = node
        else:
            assert isinstance(index, str)
            node = cls(prefix+index, index, graph)

        if node.name in self.ns_node_byname:
            raise RuntimeError(
                    "ns_create_node failed, node name confliction: "
                    "%s!" % node.name)
        self.ns_node_byname[node.name] = node
        return node

    def _ns_create_edge(self, to_node, payload, prefix=""):
        assert isinstance(to_node, NodeBase)
        assert to_node.graph.namespace is self
        assert to_node.name in self.ns_node_byname

        if isinstance(payload, str):
            self._ns_edge_index += 1
            edge = KwEdge(payload, to_node,
                    prefix+str(self._ns_edge_index))
        else:
            assert isinstance(payload, FunctionGraph)
            self._ns_fnedge_index_byfn[payload.func_name] += 1
            index = self._ns_fnedge_index_byfn[payload.func_name]
            name = prefix+"_"+payload.func_name
            if index != 1:
                name += str(index)
            edge = ClEdge(payload, to_node, name)

        assert edge.name not in self.ns_edge_byname
        if edge.name in self.ns_edge_byname:
            raise RuntimeError(
                    "ns_create_edge failed, edge name confliction: "
                    "%s!" % edge.name)
        self.ns_edge_byname[edge.name] = edge
        return edge

    def _ns_register_edge(self, from_node, edge):
        assert isinstance(from_node, NodeBase)
        assert isinstance(edge, EdgeBase)
        assert from_node.name in self.ns_node_byname
        assert edge.name in self.ns_edge_byname

        key = (from_node.name, edge.node.name)
        self.ns_edges_byfromto[key].append(edge)

    def __repr_marks__(self):
        marks = super(GraphNamespaceMixin, self).__repr_marks__()
        marks += ", %s[%d(%d) nodes, %d(%d) edges]" % (
                self.ns_name,
                len(self.ns_node_byid),
                len(self.ns_node_byname),
                len(self.ns_edge_byname),
                sum(len(l) for l in
                    self.ns_edges_byfromto.itervalues()))
        return marks


class FnNode(NodeBase):
    def __init__(self, name, index, graph):
        assert isinstance(graph, FunctionGraph)
        super(FnNode,self).__init__(name, index, graph)

    def build_endf(self, payload_or_edge):
        if isinstance(payload_or_edge, EdgeBase):
            edge = payload_or_edge
            if edge.node is not self.graph.end_node:
                raise RuntimeError(
                        "Edge %s to_node is not end_node"
                        % edge)
        else:
            assert self.graph.end_node
            self.graph.end_nodes.discard(self)
        return self.build(self.graph.end_node, payload_or_edge)


class FunctionGraph(GraphNamespaceMixin, GraphBase):
    def __init__(self, func_name, master):
        assert isinstance(func_name, str)

        super(FunctionGraph, self).__init__(
                ns_namespace=func_name,
                name="_"+func_name,
                master=master,
                namespace=self)

        self.func_name = func_name
        self.start_node = self.namespace._ns_create_node(
                FnNode, self, "_s", prefix="_"+self.func_name)
        self.end_node = self.namespace._ns_create_node(
                FnNode, self, "_e", prefix="_"+self.func_name)

        # init
        self.start_nodes.add(self.start_node)
        self.end_nodes.add(self.end_node)
        self.nodes.update([self.start_node, self.end_node])
        assert self.start_node.is_start
        assert self.end_node.is_end

    @property
    def keyword(self):
        return self.start_edge.keyword

    @property
    def start_edge(self):
        assert len(self.start_node.edges) == 1
        return iter(self.start_node.edges).next()

    def __repr_marks__(self):
        mark = super(FunctionGraph, self).__repr_marks__()
        mark += ", `%s`" % self.keyword
        if self.end_node not in self.end_nodes or len(self.end_nodes) != 1:
            mark += ", ERROR ENDNODES: %s" % (",".join(node.name
                for node in self.end_nodes))
        return mark

    def _create_node_override(self, id_):
        return self.namespace._ns_create_node(
                FnNode, self, id_, is_id=True, prefix="_"+self.func_name)

    def _create_edge_override(self, to_node, payload):
        return self.namespace._ns_create_edge(to_node, payload,
                prefix="_"+self.func_name)


# TODO: need to specify end node in some cases.
class TdNode(NodeBase):
    def __init__(self, name, index, graph):
        assert isinstance(graph, ThreadGraph)
        super(TdNode, self).__init__(name, index, graph)
        _defensive_check(TdNode, self, "request_state")

        self.request_state = None

    @property
    def is_request_start(self):
        return False

    @property
    def is_request_end(self):
        if self.request_state:
            assert self.is_end
            assert self in self.graph.master.req_endnodes
            return True
        else:
            return False

    def __repr_marks__(self):
        mark = super(TdNode, self).__repr_marks__()
        if self.request_state:
            mark += ", R@%s" % self.request_state
        return mark

    def set_state(self, state_str, is_mark=False):
        if not is_mark:
            assert not self.is_start
            if not self.is_end:
                raise RuntimeError("set_state() failed, "
                        "isn't endnode for reqstate: %s %s!" % (
                        self.__class__.__name__, self.name))
            if self.request_state is not None:
                raise RuntimeError("set_state() failed, "
                        "already has reqstate %s: %s %s <- %s!" % (
                        self.request_state, self.__class__.__name__,
                        self.name, state_str))
            if not state_str:
                raise RuntimeError("set_state() failed, "
                        "empty reqstate %s!" % state_str)
            self.request_state = state_str
            self.graph.master._register_requestend(self)
        else:
            super(TdNode, self).set_state(state_str, True)

    def set_end(self):
        self.graph.end_nodes.add(self)


class ReqNode(TdNode):
    def __init__(self, name, request_type, graph):
        super(ReqNode, self).__init__("q_"+name, request_type, graph)
        _defensive_check(ReqNode, self, "request_type")

        self.request_type = request_type

    @property
    def is_request_start(self):
        assert self.request_type in self.graph.master.req_startnode_bytype
        return True


class ThreadGraph(GraphBase):
    startnode_index = 0

    def __init__(self, name, component, master, request_type):
        assert isinstance(component, Component)

        super(ThreadGraph, self).__init__(
                name=name,
                master=master,
                namespace=master)

        self.component = component

        # internal init
        if request_type is not None:
            n_cls = ReqNode
            name = request_type
        else:
            n_cls = TdNode
            self.__class__.startnode_index += 1
            name = "s%d" % self.startnode_index
        s_node = self.namespace._ns_create_node(
                n_cls, self, name)
        self.start_nodes.add(s_node)
        self.end_nodes.add(s_node)
        self.nodes.add(s_node)
        assert s_node.is_start

        if request_type is not None:
            self.master._register_requeststart(s_node)
            assert s_node.is_request_start

    def __repr_marks__(self):
        ret = super(ThreadGraph, self).__repr_marks__()
        ret += ", %s" % self.component
        return ret

    def _create_node_override(self, id_):
        return self.namespace._ns_create_node(
                cls=TdNode, graph=self, index=id_, is_id=True)

    def _create_edge_override(self, to_node, payload):
        return self.namespace._ns_create_edge(to_node, payload)

    def merge(self, graph):
        assert isinstance(graph, ThreadGraph)
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


class MasterBase(object):
    def __init__(self):
        pass

    def __repr_marks__(self):
        return ""

    def __str__(self):
        return ""


class Master(JoinMasterMixin, GraphNamespaceMixin, MasterBase):
    def __init__(self, name):
        assert isinstance(name, str)

        super(Master, self).__init__(ns_namespace=name)

        self.name = name

        self.req_startnode_bytype = OrderedDict()
        self.req_endnodes = OrderedSet()

        self._threadgraph_index = 0
        self.thread_graphs = OrderedSet()
        self.threadgraphs_bycomponent = defaultdict(OrderedSet)

        self.funcgraph_byname = OrderedDict()

        self.marks = OrderedSet()
        self.seen_edges = OrderedSet()

    @property
    def request_types(self):
        return list(self.req_startnode_bytype.iterkeys())

    @property
    def request_states(self):
        ret = OrderedSet()
        for node in self.req_endnodes:
            state = node.request_state
            assert isinstance(state, str)
            ret.add(state)
        return ret

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

    def __repr_marks__(self):
        return super(Master, self).__repr_marks__()

    def __repr__(self):
        return ("<Master#%s: %d functions, %d threads, "
                "%d components, s%d/e%d reqs, %d marks%s>" %
                (self.name,
                 len(self.funcgraph_byname),
                 len(self.thread_graphs),
                 len(self.components),
                 len(self.req_startnode_bytype),
                 len(self.req_endnodes),
                 len(self.marks),
                 self.__repr_marks__()))

    def __str__(self):
        ret_str = "\n>>--------------"
        ret_str += "\n %r" % self
        ret_str += "\n  Functions:"
        for fn_obj in self.funcgraph_byname.itervalues():
            ret_str += "\n    %r" % fn_obj
        ret_str += "\n  Threads:"
        for th_obj in self.thread_graphs:
            ret_str += "\n    %r" % th_obj
        ret_str += "\n  Request start nodes:"
        for node in self.req_startnode_bytype.itervalues():
            ret_str += "\n    %r" % node
        ret_str += "\n  Request end nodes:"
        for node in self.req_endnodes:
            ret_str += "\n    %r" % node
        ret_str += "\n  Components: %s"\
                   % (",".join(str(c) for c in self.components))
        ret_str += "\n  Request states: %s"\
                   % (",".join(s for s in self.request_states))
        if self.marks:
            ret_str += "\n  Marks:%s"\
                       % (",".join(m for m in self.marks))
        ret_str += super(Master, self).__str__()
        ret_str += "\n"

        if self.funcgraph_byname:
            ret_str += "\n--<FunctionGraphs>--\n"
            ret_str += "\n\n".join(str(fn_g)
                    for fn_g in self.funcgraph_byname.itervalues())
            ret_str += "\n"

        ret_str += "\n--<ThreadGraphs>--\n"
        ret_str += "\n\n".join(str(th_g)
                for th_g in self.thread_graphs)
        ret_str += "\n"

        ret_str += "--------------<<\n"
        return ret_str


    def _remove_thread(self, graph):
        assert isinstance(graph, ThreadGraph)
        assert graph.master is self

        self.thread_graphs.remove(graph)
        self.threadgraphs_bycomponent(graph.component).discard(graph)
        self._threadgraph_index = 0
        for thread_graph in self.thread_graphs:
            self._threadgraph_index += 1
            thread_graph.name = "gt%d" % self._threadgraph_index

    def _register_requeststart(self, rnode):
        assert isinstance(rnode, ReqNode)
        assert rnode.is_start
        rtype = rnode.request_type

        if rtype in self.req_startnode_bytype:
            raise RuntimeError(
                    "Failed to register reqnode, request_type confliction: "
                    "type %s!" % rtype)
        self.req_startnode_bytype[rtype] = rnode

    def _register_requestend(self, node):
        assert isinstance(node, TdNode)
        assert node.is_end
        assert node.request_state
        self.req_endnodes.add(node)

#-------
    def build_thread(self, component, tonode_or_id, payload_or_edge,
                     request_type=None):
        if request_type is not None and not isinstance(request_type, str):
            raise RuntimeError("build_thread() requires request_type as str!")
        if not isinstance(component, Component):
            raise RuntimeError("build_thread() requires component as Component!")

        self._threadgraph_index += 1
        name = "t%d" % self._threadgraph_index
        thread = ThreadGraph(name, component, self, request_type)
        self.thread_graphs.add(thread)
        self.threadgraphs_bycomponent[component].add(thread)

        assert len(thread.start_nodes) == 1
        from_node = iter(thread.start_nodes).next()
        return from_node.build(tonode_or_id, payload_or_edge)

    def build_func(self, tonode_or_id, payload_or_edge, func_name):
        if not isinstance(func_name, str):
            raise RuntimeError("build_func() requires func_name as str!")
        if func_name in self.funcgraph_byname:
            raise RuntimeError("build_func() error: already has function %s!"
                    % func_name)

        function = FunctionGraph(func_name, self)
        self.funcgraph_byname[func_name] = function
        from_node = function.start_node
        edge, to_node = from_node.build(tonode_or_id, payload_or_edge)
        return edge, to_node, function

    def iter_namespaces(self):
        yield self
        for fn in self.funcgraph_byname.itervalues():
            yield fn

    def iter_edges(self):
        for ns in self.iter_namespaces():
            for edge in ns._ns_iter_edges():
                yield edge

    def iter_nodes(self):
        for ns in self.iter_namespaces():
            for node in ns._ns_iter_nodes():
                yield node

########
    def get_unseenedges(self):
        edges = OrderedSet()
        edges.update(self.iter_edges())

        return edges - self.seen_edges
