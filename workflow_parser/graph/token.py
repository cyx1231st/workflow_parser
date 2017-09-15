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

from orderedset import OrderedSet

from ..service_registry import Component
from . import (ClEdge,
               FnNode,
               KwEdge,
               Master,
               NodeBase,
               ReqNode,
               TdNode,
               ThreadGraph)


# Refer to a Node
class State(object):
    def __init__(self, node, callstack, from_step, token):
        assert isinstance(node, NodeBase)
        assert isinstance(callstack, list)
        assert isinstance(token, Token)
        if from_step is not None:
            assert isinstance(from_step, Step)
            assert token is from_step.token
        else:
            assert isinstance(node, TdNode)
            assert node.is_start

        self._node = node
        self._callstack = callstack
        self.from_step = from_step
        self.to_step = None
        self.token = token

    @property
    def nodename(self):
        return self._node.name

    @property
    def path(self):
        if self.from_step is None:
            from_ = "|["
        else:
            from_ = "%s[" % self.from_step.edgename
        if self.to_step is None:
            to_ = "]|"
        else:
            to_ = "]%s" % self.to_step.edgename
        return from_ + self.nodename + to_

    @property
    def callstack(self):
        ret = self.token.threadgraph_name
        if self._callstack:
            ret += "(%d.%s@%s)" % (
                len(self._callstack),
                self._callstack[-1].func_name,
                self.nodename)
        else:
            ret += "(@%s)" % self.nodename
        return ret

    @property
    def marks(self):
        return self._node.marks

    @property
    def is_thread_start(self):
        if isinstance(self._node, TdNode):
            return self._node.is_start
        else:
            return False

    @property
    def is_thread_end(self):
        if isinstance(self._node, TdNode):
            return self._node.is_end
        else:
            return False

    @property
    def is_request_start(self):
        if isinstance(self._node, ReqNode):
            return True
        else:
            return False

    @property
    def request_type(self):
        if self.is_request_start:
            return self._node.request_type
        else:
            return None

    @property
    def is_request_end(self):
        if isinstance(self._node, TdNode):
            return self._node.is_request_end
        else:
            return False

    @property
    def request_state(self):
        if isinstance(self._node, TdNode):
            return self._node.request_state
        return None

    @property
    def vis_weight(self):
        return self._node.vis_weight

    def __repr__(self):
        marks = ""
        if self.is_thread_start:
            marks += ", s"
        if self.is_thread_end:
            marks += ", e"
        marks += self._node.__repr_marks__()
        return "<%s: %s, %s%s>" % (
                self.__class__.__name__,
                self.path,
                self.callstack,
                marks)


# Refer to a KwEdge
class Step(object):
    def __init__(self, kwedge, callstack, from_state):
        assert isinstance(kwedge, KwEdge)
        assert isinstance(callstack, list)
        assert isinstance(from_state, State)

        self._kwedge = kwedge
        self._callstack = callstack
        self.from_state = from_state
        self.to_state = None
        self.token = from_state.token

        assert from_state.to_step is None
        from_state.to_step = self

    @property
    def edgename(self):
        return self._kwedge.name

    @property
    def joinable(self):
        return self._kwedge

    @property
    def path(self):
        if self.to_state is None:
            to_ = "?"
        else:
            to_ = self.to_state.nodename
        return "["+self.from_state.nodename+"]"\
               +self.edgename\
               +"["+to_+"]"

    @property
    def keyword(self):
        return self._kwedge.keyword

    @property
    def callstack(self):
        ret = self.token.threadgraph_name
        if self._callstack:
            ret += "(%d.%s@%s-`%s`)" % (
                    len(self._callstack),
                    self._callstack[-1].func_name,
                    self.edgename,
                    self.keyword)
        else:
            ret += "(@%s-`%s`)" % (
                    self.edgename, self.keyword)
        return ret

    def __repr__(self):
        return "<%s: %s, %s%s>" % (
                self.__class__.__name__,
                self.path,
                self.callstack,
                self._kwedge.__repr_marks__())


# Refer to a ThreadGraph
class Token(object):
    # private
    def __init__(self, start_node, master):
        assert isinstance(start_node, TdNode)
        assert isinstance(start_node.graph, ThreadGraph)
        assert start_node.is_start
        assert isinstance(master, Master)

        self._master = master
        self._callstack = []

        self._thread_graph = start_node.graph
        self.start_state = State(start_node, self._callstack, None, self)
        self.to_step = None
        self.to_state = self.start_state
        self.len_states = 1
        self.len_steps = 0

    @property
    def threadgraph_name(self):
        return self._thread_graph.name

    @property
    def component(self):
        return self._thread_graph.component

    @property
    def is_complete(self):
        if self.to_state.is_thread_end:
            assert not self._callstack
            return True
        else:
            return False

    def _step_edge(self, edge):
        self._master.seen_edges.add(edge)
        while isinstance(edge, ClEdge):
            self._callstack = self._callstack[:]
            self._callstack.append(edge)
            edge = edge.func_startedge
            self._master.seen_edges.add(edge)
        assert isinstance(edge, KwEdge)
        self.to_step = Step(edge, self._callstack, self.to_state)
        self.to_state.to_step = self.to_step
        self.to_state = None
        self.len_steps += 1

        node = edge.node
        while isinstance(node, FnNode) and node.is_end:
            self._callstack = self._callstack[:]
            f_edge = self._callstack.pop()
            node = f_edge.node
        self.to_state = State(node, self._callstack, self.to_step, self)
        self.to_step.to_state = self.to_state
        self.len_states += 1

    @classmethod
    def new(cls, master, keyword, component):
        assert isinstance(master, Master)
        assert isinstance(keyword, str)
        assert isinstance(component, Component)

        threadgraphs = master.threadgraphs_bycomponent.get(component, OrderedSet())
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
        edge = self.to_state._node.decide_edge(keyword)
        if edge:
            self._step_edge(edge)
            return True
        else:
            return False

    def iter_steps(self):
        state = self.start_state
        while state:
            to_step = state.to_step
            if to_step:
                yield to_step
            else:
                break
            state = to_step.to_state

    def __repr__(self):
        if self.is_complete:
            marks = ", COMPLETE"
        else:
            marks = ", ~COMPLETE"
        if self.to_state:
            end = self.to_state.nodename
        else:
            end = self.to_step.edgename
        return "<%s#%s: %s, %s-->%s, %d states, %d steps%s>" % (
                self.__class__.__name__,
                self.threadgraph_name,
                self.component,
                self.start_state.nodename,
                end,
                self.len_states,
                self.len_steps,
                marks)

    def __str__(self):
        ret = repr(self)+":"
        state = self.start_state
        while state:
            ret += "\n  "+repr(state)
            step = state.to_step
            if step:
                ret += "\n  "+repr(step)
            else:
                break
            state = step.to_state
        return ret
