from __future__ import print_function

from abc import ABCMeta
from collections import defaultdict
from functools import total_ordering

from ... import reserved_vars as rv
from ...target import Line
from ...target import Thread
from ...graph import Edge
from ...graph import JoinBase
from ...graph import MasterGraph
from ...graph import Node
from ...graph import Token


@total_ordering
class Pace(object):
    """ Pace is relative to transition. """
    def __init__(self, line_obj, from_node, edge, threadins):
        assert isinstance(line_obj, Line)
        assert line_obj._assigned is None
        assert isinstance(from_node, Node)
        assert isinstance(edge, Edge)
        assert isinstance(threadins, ThreadInstance)

        self.line_obj = line_obj
        self.from_node = from_node
        self.edge = edge
        self.threadins = threadins
        self.prv_int = None
        self.nxt_int = None

        self.joins_int = None
        self.joined_int = None
        self.joins_crossrequest_int = None
        self.joined_crossrequest_int = None

        line_obj._assigned = self
        assert self.target == self.target_obj.target
        assert self.thread == self.thread_obj.thread

    @property
    def to_node(self):
        return self.edge.node

    @property
    def request_state(self):
        return self.to_node.request_state

    @property
    def marks(self):
        return self.to_node.marks

    @property
    def thread_obj(self):
        return self.threadins.thread_obj

    @property
    def target_obj(self):
        return self.thread_obj.target_obj

    @property
    def prv_pace(self):
        prv_int = self.prv_int
        if prv_int is None:
            return None
        else:
            return prv_int.from_pace

    @property
    def nxt_pace(self):
        nxt_int = self.nxt_int
        if nxt_int is None:
            return None
        else:
            return nxt_int.to_pace

    @property
    def joins_pace(self):
        if self.joins_int and self.joins_int is not empty_join:
            # assert isinstance(self.joins_int, InnerjoinIntervalBase)
            return self.joins_int.to_pace
        else:
            return None

    @property
    def joined_pace(self):
        if self.joined_int and self.joined_int is not empty_join:
            # assert isinstance(self.joined_int, InnerjoinIntervalBase)
            return self.joined_int.from_pace
        else:
            return None

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

    # total ordering
    __eq__ = lambda self, other: self.seconds == other.seconds
    __lt__ = lambda self, other: self.seconds < other.seconds

    def __getattribute__(self, item):
        assert isinstance(item, str)

        if item in rv.ALL_VARS:
            ret = getattr(self.line_obj, item)
            if ret is None and item == rv.REQUEST:
                ret = getattr(self.threadins, "request")
            return ret
        else:
            return super(Pace, self).__getattribute__(item)

    def __getitem__(self, item):
        assert isinstance(item, str)

        if item in rv.ALL_VARS:
            return getattr(self, item)
        elif item in self.line_obj:
            return self.line_obj[item]
        elif item in self.threadins.thread_vars:
            return self.threadins.thread_vars[item]
        elif item in self.threadins.thread_vars_dup:
            raise StateError("(Pace) got multiple %s: %s" %
                    (item, self.threadins.thread_vars_dup[item]))
        else:
            raise StateError("(Pace) key %s not exist!" % item)

    def _mark_str(self):
        mark_str = ""
        if self.edge.joins_objs:
            mark_str += ", join(%d):" % len(self.edge.joins_objs)
            if self.joins_int:
                mark_str += self.joins_int.name
            else:
                mark_str += "?"
        if self.edge.joined_objs:
            mark_str += ", joined(%d):" % len(self.edge.joined_objs)
            if self.joined_int:
                mark_str += self.joined_int.name
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
                self.seconds,
                self.from_node.name,
                self.edge.name,
                self.to_node.name,
                self.keyword,
                len(self.line_obj.keys),
                self.target,
                self.thread,
                self.request,
                mark_str)

    def __str__thread__(self):
        mark_str = self._mark_str()

        return "%.3f {%s>%s>%s}, `%s`, %d lvars%s" % (
                self.seconds,
                self.from_node.name,
                self.edge.name,
                self.to_node.name,
                self.keyword,
                len(self.line_obj.keys),
                mark_str)


    def __repr__(self):
        ret_str = str(self)
        ret_str += "\n  >>%s" % self.line_obj.line
        return ret_str


class IntervalBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, from_pace, to_pace, entity):
        assert isinstance(from_pace, Pace)
        assert isinstance(to_pace, Pace)

        if entity:
            name_str = entity.name
        else:
            name_str = "Nah"

        self.from_pace = from_pace
        self.to_pace = to_pace
        self.entity = entity
        self.name = "%s(%s)%s" % (self.from_node.name,
                                  name_str,
                                  self.to_node.name)

        self.is_main = False

    @property
    def from_node(self):
        return self.from_pace.from_node

    @property
    def to_node(self):
        return self.to_pace.to_node

    @property
    def from_edge(self):
        return self.from_pace.edge

    @property
    def to_edge(self):
        return self.to_pace.edge

    @property
    def from_seconds(self):
        return self.from_pace.seconds

    @property
    def to_seconds(self):
        return self.to_pace.seconds

    @property
    def from_time(self):
        return self.from_pace.time

    @property
    def to_time(self):
        return self.to_pace.time

    @property
    def lapse(self):
        return self.to_seconds - self.from_seconds

    @property
    def is_violated(self):
        return self.from_seconds > self.to_seconds

    @property
    def path_name(self):
        return "%s(%s)%s" % (self.from_node.name,
                             self.entity.name,
                             self.to_node.name)

    @property
    def path_type(self):
        if self.is_main:
            return "main"
        else:
            return "branch"

    @property
    def int_type(self):
        return "base"

    def __repr__(self):
        return "<%s#%s: %s %s |--> %s %s>" % (
                self.__class__.__name__,
                self.name,
                self.from_seconds,
                self.from_edge.keyword,
                self.to_seconds,
                self.to_edge.keyword)


class ThreadIntervalBase(IntervalBase):
    __metaclass__ = ABCMeta

    def __init__(self, from_pace, to_pace, entity):
        super(ThreadIntervalBase, self).__init__(from_pace,
                                                 to_pace,
                                                 entity)
        assert from_pace.thread_obj is to_pace.thread_obj
        self.thread_obj = from_pace.thread_obj

        assert from_pace.nxt_int is None
        assert to_pace.prv_int is None
        from_pace.nxt_int = self
        to_pace.prv_int = self

    @property
    def target(self):
        return self.thread_obj.target

    @property
    def component(self):
        return self.thread_obj.component

    @property
    def host(self):
        return self.thread_obj.host

    @property
    def thread(self):
        return self.thread_obj.thread

    @property
    def target_obj(self):
        return self.thread_obj.target_obj


class BlankInterval(ThreadIntervalBase):
    def __init__(self, from_pace, to_pace):
        super(BlankInterval, self).__init__(from_pace,
                                            to_pace,
                                            None)
        assert self.from_threadins is not self.to_threadins

    @property
    def from_threadins(self):
        return self.from_pace.threadins

    @property
    def to_threadins(self):
        return self.to_pace.threadins


class ThreadInterval(ThreadIntervalBase):
    def __init__(self, from_pace, to_pace):
        super(ThreadInterval, self).__init__(from_pace,
                                             to_pace,
                                             from_pace.to_node)
        assert from_pace.threadins is to_pace.threadins

    @property
    def threadins(self):
        return self.from_pace.threadins

    @property
    def joins_int(self):
        return self.to_pace.joins_int

    @property
    def joined_int(self):
        return self.from_pace.joined_int

    @property
    def node(self):
        return self.entity

    @property
    def is_lock(self):
        return self.node.is_lock

    @property
    def requestins(self):
        return self.threadins.requestins

    @property
    def color(self):
        return self.component.color

    @property
    def prv_int(self):
        prv_int = self.from_pace.prv_int
        if isinstance(prv_int, ThreadInterval):
            return prv_int
        else:
            return None

    @property
    def nxt_int(self):
        nxt_int = self.to_pace.nxt_int
        if isinstance(nxt_int, ThreadInterval):
            return nxt_int
        else:
            return None

    @property
    def prv_main(self):
        assert self.is_main

        if self.prv_int and not self.prv_int.is_lock:
            if self.joined_int:
                e = StateError("Both prv_int and joined_int are accetable")
                e.where = self.node.name
                raise e
            return self.prv_int
        elif self.prv_int:
            if self.joined_int:
                ret = self.joined_int
            else:
                ret = self.prv_int
        else:
            ret = self.joined_int
        if not ret or ret is empty_join:
            if self.is_request_start:
                return None
            else:
                e = StateError("The thread path backward is empty")
                e.where = self.node.name
                raise e
        return ret

    @property
    def nxt_main(self):
        assert self.is_main
        if self.nxt_int and self.nxt_int.is_main:
            # self.joins_int cannot be main
            assert not self.joins_int or not self.joins_int.is_main
            return self.nxt_int
        elif self.nxt_int:
            if self.joins_int and self.joins_int.is_main:
                return self.joins_int
            elif self.joins_int:
                # self.nxt_int nor self.joins_int is main
                assert False
            else:
                assert self.is_request_end
                return None
        else:
            if self.joins_int and self.joins_int.is_main:
                return self.joins_int
            elif self.joins_int:
                assert self.is_request_end
                return None
            else:
                assert self.is_request_end
                return None

    # inheritance
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

    @property
    def path_type(self):
        if self.is_lock:
            return "lock"
        else:
            return super(ThreadInterval, self).path_type

    @property
    def int_type(self):
        return "thread"


class ThreadInstance(object):
    def __init__(self, thread_obj, token, line_obj):
        assert isinstance(thread_obj, Thread)
        assert isinstance(token, Token)
        assert isinstance(line_obj, Line)

        self.thread_obj = thread_obj
        self.token = token

        self.thread_vars = {}
        self.thread_vars_dup = defaultdict(set)

        self.paces = []
        self.paces_by_mark = defaultdict(list)

        self.intervals = []

        self.joined_paces = set()
        self.joins_paces = set()
        self.leftinterface_paces = set()
        self.rightinterface_paces = set()

        self.joined_ints = set()
        self.joins_ints = set()
        self.interfacejoined_ints = set()
        self.interfacejoins_ints = set()
        self.leftinterface_ints = set()
        self.rightinterface_ints = set()

        self.request = None
        self.requestins = None

        # init
        self._apply_token(line_obj)
        assert self.paces

    @property
    def thread(self):
        return self.thread_obj.thread

    @property
    def target(self):
        return self.thread_obj.target

    @property
    def component(self):
        return self.thread_obj.component

    @property
    def host(self):
        return self.thread_obj.host

    @property
    def threadgraph(self):
        return self.token.thread_graph

    @property
    def target_obj(self):
        return self.thread_obj.target_obj

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

    @property
    def start_seconds(self):
        return self.paces[0].seconds

    @property
    def end_seconds(self):
        return self.paces[-1].seconds

    @property
    def lapse(self):
        return self.end_seconds - self.start_seconds

    @property
    def start_time(self):
        return self.paces[0].time

    @property
    def end_time(self):
        return self.paces[-1].time

    def __str__(self):
        mark_str = ""
        if not self.is_complete:
            mark_str += ", INCOMPLETE"
        if self.is_request_start:
            mark_str += ", RSTART"
        if self.is_request_end:
            mark_str += ", REND"
        if self.paces_by_mark:
            mark_str += ", %dMARKS" % len(self.paces_by_mark)

        return "<ThIns#%s-%s: log#%d, comp#%s, host#%s, graph#%s, "\
               "req#%s, %d(dup%d) vars, %d(act%d) joined_p, %d(act%d) joins_p%s>" % (
                self.target,
                self.thread,
                self.len_paces,
                self.component,
                self.host,
                self.threadgraph.name,
                self.request,
                len(self.thread_vars),
                len(self.thread_vars_dup),
                len(self.joined_paces),
                len(self.joined_ints),
                len(self.joins_paces),
                len(self.joins_ints),
                mark_str)

    def __repr__(self):
        ret_str = str(self)
        ret_str += "\n  %s" % self.threadgraph
        ret_str += "\n  Paces:"
        for pace in self.paces:
            ret_str += "\n  | %s" % pace.__str__thread__()
        return ret_str

    @classmethod
    def create(cls, master_graph, line_obj, thread_obj):
        assert isinstance(master_graph, MasterGraph)
        assert isinstance(line_obj, Line)
        assert isinstance(thread_obj, Thread)

        token = Token.new(master_graph, line_obj.keyword, thread_obj.component)
        if token:
            threadins = ThreadInstance(thread_obj, token, line_obj)
            if thread_obj.threadinss:
                BlankInterval(thread_obj.threadinss[-1].end_pace,
                              threadins.start_pace)
            return threadins
        else:
            return None

    def report(self, reason):
        print("(ThreadInstance) report: %s" % reason)
        print("-------- Thread --------")
        print("%r" % self)
        # report_loglines(self.loglines, self.s_index, self.f_index)
        print("-------- end -----------")

    def _apply_token(self, line_obj):
        assert isinstance(line_obj, Line)
        assert line_obj.keyword == self.token.keyword

        from_node = self.token.from_node
        edge = self.token.edge
        pace = Pace(line_obj, from_node, edge, self)
        for mark in edge.marks:
            self.paces_by_mark[mark].append(pace)
        if edge.joins_objs:
            self.joins_paces.add(pace)
        if edge.joined_objs:
            self.joined_paces.add(pace)
        if edge.left_interface:
            self.leftinterface_paces.add(pace)
        if edge.right_interface:
            self.rightinterface_paces.add(pace)
        for key in line_obj.keys:
            if key in ("keyword", "time", "seconds"):
                continue

            new_val = line_obj[key]
            if key in ("component", "target", "host", "thread"):
                val = getattr(self, key)
                if val != line_obj[key]:
                    import pdb; pdb.set_trace()
                    self.report("Thread var %s=%s, conflicts %s"
                            % (key, val, new_val))
                    raise StateError("(ThreadInstance) parse error: variable mismatch")
                else:
                    pass
            elif key == "request":
                if new_val is None:
                    pass
                elif self.request is None:
                    self.request = new_val
                elif self.request != new_val:
                    self.report("Thread request=%s, conflicts %s"
                            % (self.request, new_val))
                    raise StateError("(ThreadInstance) thread is not shared!")
                else:
                    pass
            else:
                if key in self.thread_vars_dup:
                    self.thread_vars_dup[key].add(new_val)
                else:
                    val = self.thread_vars.get(key)
                    if val is None:
                        self.thread_vars[key] = new_val
                    elif val != new_val:
                        self.thread_vars_dup[key].add(val)
                        self.thread_vars_dup[key].add(new_val)
                        self.thread_vars.pop(key)
                    else:
                        pass

        if self.paces:
            prv_pace = self.paces[-1]
            interval = ThreadInterval(prv_pace, pace)
            self.intervals.append(interval)

        self.paces.append(pace)

    def step(self, line_obj):
        assert isinstance(line_obj, Line)

        if self.token.step(line_obj.keyword):
            self._apply_token(line_obj)
            return True
        else:
            return False


class JoinIntervalBase(IntervalBase):
    __metaclass__ = ABCMeta

    joinobj_type = JoinBase
    entity_joins_int = "Error not assigned"
    entity_joined_int = "Error not assigned"

    def __init__(self, join_obj, from_entity, to_entity):
        if isinstance(from_entity, Pace):
            from_pace = from_entity
        else:
            from_pace = from_entity.from_pace
        if isinstance(to_entity, Pace):
            to_pace = to_entity
        else:
            to_pace = to_entity.to_pace
        super(JoinIntervalBase, self).__init__(from_pace, to_pace, join_obj)

        assert isinstance(join_obj, self.joinobj_type)
        if not self.is_remote:
            assert self.from_targetobj is self.to_targetobj
        assert self.from_threadobj is not self.to_threadobj

        assert getattr(from_entity, self.entity_joins_int) is None
        assert getattr(to_entity, self.entity_joined_int) is None
        setattr(from_entity, self.entity_joins_int, self)
        setattr(to_entity, self.entity_joined_int, self)

    @property
    def join_obj(self):
        return self.entity

    @property
    def is_remote(self):
        return self.join_obj.is_remote

    @property
    def from_threadins(self):
        return self.from_pace.threadins

    @property
    def to_threadins(self):
        return self.to_pace.threadins

    @property
    def from_host(self):
        return self.from_pace.host

    @property
    def to_host(self):
        return self.to_pace.host

    @property
    def from_component(self):
        return self.from_pace.component

    @property
    def to_component(self):
        return self.to_pace.component

    @property
    def from_target(self):
        return self.from_pace.target

    @property
    def to_target(self):
        return self.to_pace.target

    @property
    def from_threadobj(self):
        return self.from_pace.thread_obj

    @property
    def to_threadobj(self):
        return self.to_pace.thread_obj

    @property
    def from_targetobj(self):
        return self.from_pace.target_obj

    @property
    def to_targetobj(self):
        return self.to_pace.target_obj

    @property
    def join_type(self):
        if self.is_remote and self.from_host != self.to_host:
            return "remote"
        elif self.is_remote:
            return "local_remote"
        else:
            return "local"

    @classmethod
    def assert_emptyjoin(cls, pace, is_joins, is_forced):
        if is_joins:
            if is_forceed:
                assert getattr(pace, cls.entity_joins_int) is None
                setattr(pace, cls.entity_joins_int, empty_join)
            else:
                assert getattr(pace, cls.entity_joins_int) is not empty_join
                if getattr(pace, cls.entity_joins_int) is None:
                    setattr(pace, cls.entity_joins_int, empty_join)
        else:
            if is_forced:
                assert getattr(pace, cls.entity_joined_int) is None
                setattr(pace, cls.entity_joined_int, empty_join)
            else:
                assert getattr(pace, cls.entity_joined_int) is not empty_join
                if getattr(pace, cls.entity_joined_int) is None:
                    setattr(pace, cls.entity_joined_int, empty_join)

    def __str__marks__(self):
        return ""

    def __repr__(self):
        return "<%s#%s %f -> %f, %s -> %s%s>" % (
                self.__class__.__name__,
                self.name,
                self.from_seconds, self.to_seconds,
                self.from_host, self.to_host,
                self.__str__marks__())


class EmptyJoin(JoinIntervalBase):
    def __init__(self):
        self.name = "EMPTY"

    def __repr__(self):
        return "<EMPTYJOIN>"


empty_join = EmptyJoin()
