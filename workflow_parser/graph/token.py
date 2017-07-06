from orderedset import OrderedSet

from ..service_registry import Component
from . import (FnEdge,
               FnNode,
               KwEdge,
               Master,
               NodeBase,
               ReqNode,
               TdNode,
               ThreadGraph)


class GraphRuntime(object):
    def __init__(self, thread_graph):
        assert isinstance(thread_graph, ThreadGraph)
        self.thread_graph = thread_graph
        self.states = []


class State(object):
    def __init__(self, node, callstack, from_step=None, runtime=None):
        assert isinstance(node, NodeBase)
        assert isinstance(callstack, list)
        if from_step is not None:
            assert isinstance(from_step, Step)
            from_step.to_state = self
            runtime = from_step.runtime
        assert isinstance(runtime, GraphRuntime)
        runtime.states.append(self)

        self._node = node
        self.from_step = from_step
        self.to_step = None
        self.callstack = callstack
        self.runtime = runtime

    @property
    def name(self):
        return self._node.name

    @property
    def path(self):
        if self.from_step is None:
            from_ = "|["
        else:
            from_ = "%s[" % self.from_step.name
        if self.to_step is None:
            to_ = "]|"
        else:
            to_ = "]%s" % self.to_step.name
        return from_ + self.name + to_

    @property
    def marks(self):
        return self._node.marks

    @property
    def request_state(self):
        if isinstance(self._node, TdNode):
            return self._node.request_state
        return None

    @property
    def request_type(self):
        if self.is_request_start:
            ret = self._node.request_type
            assert ret
            return ret
        else:
            return None

    @property
    def is_thread_start(self):
        if isinstance(self._node, TdNode):
            return self._node.is_start
        return False

    @property
    def is_thread_end(self):
        if isinstance(self._node, TdNode):
            return self._node.is_end
        return False

    @property
    def is_request_start(self):
        if isinstance(self._node, ReqNode):
            assert self._node.is_start
            return True
        return False

    @property
    def is_request_end(self):
        if isinstance(self._node, TdNode):
            return self._node.is_request_end
        return False

    def __repr__(self):
        marks = ""
        if self.is_request_start:
            marks += ", S"
        elif self.is_thread_start:
            marks += ", s"
        if self.is_request_end:
            marks += ", E@%s" % self.request_state
        else:
            marks += ", e"
        if self.marks:
            marks += ", m(%s)" % ",".join(self.marks)
        return "<%s: %s%s>" % (
                self.__class__.__name__,
                self.path,
                marks)


class Step(object):
    def __init__(self, edge, callstack, from_state):
        assert isinstance(edge, KwEdge)
        assert isinstance(callstack, list)
        assert isinstance(from_state, State)

        self._edge = edge
        self.from_state = from_state
        self.to_state = None
        self.callstack = callstack
        self.runtime = from_state.runtime

        assert from_state.to_step is None
        from_state.to_step = self

    @property
    def name(self):
        return self._edge.name

    @property
    def path(self):
        from_ = self.from_state.name
        if self.to_state is None:
            to_ = "?"
        else:
            to_ = self.to_state.name
        return "[" + from_ + "]" + self.name + "[" + to_ + "]"

    @property
    def keyword(self):
        return self._edge.keyword

    @property
    def joins_objs(self):
        return self._edge.joins_objs

    @property
    def joined_objs(self):
        return self._edge.joined_objs

    @property
    def joined_interface(self):
        return self._edge.joined_interface

    @property
    def joins_interface(self):
        return self._edge.joins_interface

    def __repr__(self):
        return "<%s: %s, `%s`, %d-%d jo, %s-%s ji>" % (
                self.__class__.__name__,
                self.path,
                self.keyword,
                len(self.joins_objs),
                len(self.joined_objs),
                "1" if self.joins_interface else "0",
                "1" if self.joined_interface else "0")


class Token(object):
    def __init__(self, start_node, master):
        assert isinstance(start_node, TdNode)
        assert start_node.is_start
        assert isinstance(master, Master)

        self.runtime = GraphRuntime(start_node.graph)
        self.callstack = []
        self.step = None
        self.state = State(start_node, self.callstack, runtime=self.runtime)
        self.master = master

    @property
    def is_complete(self):
        return self.state.is_thread_end

    @property
    def thread_graph(self):
        return self.runtime.thread_graph

    def _step_edge(self, edge):
        self.master.seen_edges.add(edge)
        while isinstance(edge, FnEdge):
            self.callstack = self.callstack[:]
            self.callstack.append(edge)
            edge = edge.func_graph.start_edge
            self.master.seen_edges.add(edge)
        assert isinstance(edge, KwEdge)
        self.step = Step(edge, self.callstack, self.state)

        node = edge.node
        while isinstance(node, FnNode) and node.is_end:
            self.callstack = self.callstack[:]
            f_edge = self.callstack.pop()
            node = f_edge.node
        self.state = State(node, self.callstack, self.step)

    @classmethod
    def new(cls, master, keyword, component):
        assert isinstance(master, Master)
        assert isinstance(keyword, str)
        assert isinstance(component, Component)

        threadgraphs = master.threadgraphs_by_component.get(component, OrderedSet())
        for t_g in threadgraphs:
            for s_node in t_g.start_nodes:
                edge = s_node.decide_edge(keyword)
                if edge:
                    token = cls(s_node, master)
                    token._step_edge(edge)
                    return token
        return None

    def do_step(self, keyword):
        assert isinstance(keyword, str)
        edge = self.state._node.decide_edge(keyword)
        if edge:
            self._step_edge(edge)
            return True
        else:
            return False

    def __repr__(self):
        progress=""
        if self.step:
            progress+=self.step.name
        else:
            progress+="|"
        progress+="["+self.state.name+"]"
        if self.callstack:
            where = "~".join(f.name for f in reversed(self.callstack))
            where += "~"
        where += self.thread_graph.name
        return "<%s: %s, stack %s, %d states>" % (
                self.__class__.__name__,
                progress,
                where,
                len(self.runtime.states))
