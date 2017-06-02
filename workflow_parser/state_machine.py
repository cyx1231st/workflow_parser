from __future__ import print_function

from abc import ABCMeta
from abc import abstractmethod
from collections import defaultdict
from functools import total_ordering

from workflow_parser import reserved_vars as rv
from workflow_parser.exception import WFException
from workflow_parser.log_parser import LogLine
from workflow_parser.state_graph import Edge
from workflow_parser.state_graph import InterfaceJoin
from workflow_parser.state_graph import InnerJoin
from workflow_parser.state_graph import JoinBase
from workflow_parser.state_graph import MasterGraph
from workflow_parser.state_graph import Node
from workflow_parser.state_graph import RequestInterface
from workflow_parser.state_graph import Token
from workflow_parser.state_runtime import Thread
from workflow_parser.utils import report_loglines


empty_join = object()


class StateError(WFException):
    pass


@total_ordering
class Pace(object):
    """ Pace is relative to transition. """
    def __init__(self, logline, from_node, edge, threadins):
        assert isinstance(logline, LogLine)
        assert logline.pace is None
        assert isinstance(from_node, Node)
        assert isinstance(edge, Edge)
        assert isinstance(threadins, ThreadInstance)

        self.logline = logline
        self.from_node = from_node
        self.edge = edge
        self.threadins = threadins

        self.prv_int = None
        self.nxt_int = None

        self.prv_blank_int = None
        self.nxt_blank_int = None

        self.joins_int = None
        self.joined_int = None

        self.joins_crossrequest_int = None
        self.joined_crossrequest_int = None

        logline.pace = self

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
            assert isinstance(self.joins_int, InnerjoinIntervalBase)
            return self.joins_int.to_pace
        else:
            return None

    @property
    def joined_pace(self):
        if self.joined_int and self.joined_int is not empty_join:
            assert isinstance(self.joined_int, InnerjoinIntervalBase)
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
            ret = getattr(self.logline, item)
            if ret is None and item == rv.REQUEST:
                ret = getattr(self.threadins, "request")
            return ret
        else:
            return super(Pace, self).__getattribute__(item)

    def __getitem__(self, item):
        assert isinstance(item, str)

        if item in rv.ALL_VARS:
            return getattr(self, item)
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
        if self.edge.joins_objs:
            mark_str += ", join(%d):" % len(self.edge.joins_objs)
            if self.joins_int:
                if self.joins_int is empty_join:
                    mark_str += "EMPTY"
                else:
                    mark_str += self.joins_int.name
            else:
                mark_str += "?"
        if self.edge.joined_objs:
            mark_str += ", joined(%d):" % len(self.edge.joined_objs)
            if self.joined_int:
                if self.joined_int is empty_join:
                    mark_str += "EMPTY"
                else:
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
                len(self.logline.get_keys()),
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
                len(self.logline.get_keys()),
                mark_str)


    def __repr__(self):
        ret_str = str(self)
        ret_str += "\n  >>%s" % self.logline.line
        return ret_str


class IntervalBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, from_pace, to_pace, entity):
        if from_pace is not None:
            assert isinstance(from_pace, Pace)
            self.from_pace = from_pace

        if to_pace is not None:
            assert isinstance(to_pace, Pace)
            self.to_pace = to_pace

        self.entity = entity

        if self.entity:
            name_str = self.entity.name
        else:
            name_str = "Nah"

        if from_pace is not None and to_pace is not None:
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

    def __str__(self):
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

    @property
    def prv_int(self):
        return self.from_pace.prv_int

    @property
    def nxt_int(self):
        return self.to_pace.nxt_int


class BlankInterval(ThreadIntervalBase):
    def __init__(self, from_pace, to_pace):
        super(BlankInterval, self).__init__(from_pace,
                                            to_pace,
                                            None)
        assert self.from_threadins is not self.to_threadins
        self.from_pace.nxt_blank_int = self
        self.to_pace.prv_blank_int = self

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
        self.from_pace.nxt_int = self
        self.to_pace.prv_int = self

    @property
    def threadins(self):
        return self.from_pace.threadins

    @property
    def prv_blank_int(self):
        return self.from_pace.prv_blank_int

    @property
    def nxt_blank_int(self):
        return self.to_pace.nxt_blank_int

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


class JoinIntervalBase(IntervalBase):
    __metaclass__ = ABCMeta

    joinobj_type = JoinBase
    entity_joins_int = "Error not assigned"
    entity_joined_int = "Error not assigned"

    def __init__(self, join_obj, from_entity, to_entity):
        from_pace = None
        if isinstance(from_entity, Pace):
            from_pace = from_entity
        to_pace = None
        if isinstance(to_entity, Pace):
            to_pace = to_entity
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


class InnerjoinIntervalBase(JoinIntervalBase):
    joinobj_type = InnerJoin
    entity_joins_int = "joins_int"
    entity_joined_int = "joined_int"

    def __init__(self, join_obj, from_pace, to_pace):
        super(InnerjoinIntervalBase, self).__init__(join_obj, from_pace, to_pace)

    @property
    def joined_int(self):
        return self.from_pace.prv_int

    @property
    def joins_int(self):
        return self.to_pace.nxt_int

    @property
    def requestins(self):
        from_ = self.from_pace.threadins.requestins
        to_ = self.to_pace.threadins.requestins
        assert from_ is to_
        return from_

    @property
    def prv_main(self):
        assert self.is_main

        if not self.joined_int:
            e = StateError("ERROR joined_int is empty")
            e.where = self.join_obj.name
        return self.joined_int

    @property
    def nxt_main(self):
        assert self.is_main
        assert self.joins_int and self.joins_int.is_main
        return self.joins_int

    @classmethod
    def create(cls, join_obj, from_item, to_item):
        if isinstance(join_obj, RequestInterface):
            return InterfaceInterval(join_obj, from_item, to_item)
        else:
            assert isinstance(join_obj, InnerJoin)
            return InnerjoinInterval(join_obj, from_item, to_item)


class InnerjoinInterval(InnerjoinIntervalBase):
    def __init__(self, join_obj, from_pace, to_pace):
        super(InnerjoinInterval, self).__init__(join_obj, from_pace, to_pace)

        self.from_threadins.joins_ints.add(self)
        self.to_threadins.joined_ints.add(self)

    # @property
    # def color(self):
    #     if self.is_remote:
    #         return "#fa8200"
    #     else:
    #         return "#fade00"

    # @property
    # def color_jt(self):
    #     j_type = self.join_type
    #     if j_type == "remote":
    #         return "#fa8200"
    #     elif j_type == "local_remote":
    #         return "#fab300"
    #     else:
    #         return "#fade00"

    # @property
    # def int_type(self):
    #     return "join"


class InterfaceInterval(InnerjoinIntervalBase):
    joinobj_type = RequestInterface

    def __init__(self, join_obj, from_pace, to_pace):
        super(InterfaceInterval, self).__init__(join_obj, from_pace, to_pace)
        self.joins_crossrequest_int = None
        self.joined_crossrequest_int = None

        self.from_threadins.interfacejoins_ints.add(self)
        self.to_threadins.interfacejoined_ints.add(self)

    def __str__marks__(self):
        str_marks = super(InterfaceInterval, self).__str__marks__()
        if self.joins_crossrequest_int is not None:
            str_marks += ", [%s, %s, %s | %s, %s, %s]" %\
                (self.joins_crossrequest_int.name,
                 self.joins_crossrequest_int.to_seconds,
                 self.joins_crossrequest_int.to_host,
                 self.joined_crossrequest_int.name,
                 self.joined_crossrequest_int.from_seconds,
                 self.joined_crossrequest_int.from_host)
        return str_marks


class InterfacejoinInterval(JoinIntervalBase):
    joinobj_type = InterfaceJoin
    entity_joins_int = "joins_crossrequest_int"
    entity_joined_int = "joined_crossrequest_int"

    def __init__(self, join_obj, from_entity, to_entity):
        if join_obj.is_left:
            assert isinstance(from_entity, InterfaceInterval)
            self.interface_int = from_entity
            self.name = "%s(%s)%s" % (from_entity.join_obj.name,
                                      join_obj.name,
                                      to_entity.to_node.name)
        else:
            assert isinstance(to_entity, InterfaceInterval)
            self.interface_int = to_entity
            self.name = "%s(%s)%s" % (from_entity.from_node.name,
                                      join_obj.name,
                                      to_entity.join_obj.name)
        super(InterfacejoinInterval, self).__init__(join_obj, from_entity, to_entity)

        if join_obj.is_left:
            self.to_threadins.leftinterface_ints.add(self)
        else:
            self.from_threadins.rightinterface_ints.add(self)

    @property
    def is_left(self):
        return self.join_obj.is_left

    @property
    def from_pace(self):
        if self.is_left:
            return self.interface_int.from_pace
        else:
            from_pace = self.__dict__["from_pace"]
            assert from_pace
            return from_pace

    @from_pace.setter
    def from_pace(self, val):
        self.__dict__["from_pace"] = val

    @property
    def to_pace(self):
        if self.is_left:
            to_pace = self.__dict__["to_pace"]
            assert to_pace
            return to_pace
        else:
            return self.interface_int.to_pace

    @to_pace.setter
    def to_pace(self, val):
        self.__dict__["to_pace"] = val

    @property
    def pair(self):
        if self.is_left:
            return self.interface_int.joined_crossrequest_int
        else:
            return self.interface_int.joins_crossrequest_int

    @classmethod
    def create(cls, join_obj, from_item, to_item):
        return cls(join_obj, from_item, to_item)


class ThreadInstance(object):
    def __init__(self, thread_obj, token, logline):
        assert isinstance(thread_obj, Thread)
        assert isinstance(token, Token)
        assert isinstance(logline, LogLine)

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
        self._apply_token(logline)
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
    def create(cls, master_graph, logline, thread_obj):
        assert isinstance(master_graph, MasterGraph)
        assert isinstance(logline, LogLine)
        assert isinstance(thread_obj, Thread)

        token = Token.new(master_graph, logline.keyword, thread_obj.component)
        if token:
            threadins = ThreadInstance(thread_obj, token, logline)
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
        report_loglines(self.loglines, self.s_index, self.f_index)
        print("-------- end -----------")

    def _apply_token(self, logline):
        assert isinstance(logline, LogLine)
        assert logline.keyword == self.token.keyword

        from_node = self.token.from_node
        edge = self.token.edge
        pace = Pace(logline, from_node, edge, self)
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
        for key in logline.get_keys(True):
            if key in ("keyword", "time", "seconds"):
                continue

            new_val = logline[key]
            if key in ("component", "target", "host", "thread"):
                val = getattr(self, key)
                if val != logline[key]:
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

    def step(self, logline):
        assert isinstance(logline, LogLine)

        if self.token.step(logline.keyword):
            self._apply_token(logline)
            return True
        else:
            return False


class RequestInstance(object):
    _index_dict = defaultdict(lambda: 0)

    def __init__(self, mastergraph, request, threadinss):
        assert isinstance(mastergraph, MasterGraph)
        assert isinstance(threadinss, set)

        self.request = request
        self.mastergraph = mastergraph
        self.request_vars = defaultdict(set)

        self.threadinss = []
        self.start_threadins = None
        self.end_threadins = None
        self.last_threadins = None

        self.paces_by_mark = defaultdict(list)
        self.len_paces = 0

        # interval
        self.join_ints = set()
        self.interface_ints = set()
        self.l_interface_joins = set()
        self.r_interface_joins = set()
        self.td_ints = set()
        self.intervals_extended = []

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
        tis = set()
        for threadins in threadinss:
            assert isinstance(threadins, ThreadInstance)
            tis.add(threadins)
            threadins.requestins = self
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
            return

        seen_tis = set()
        def _process(threadins):
            assert isinstance(threadins, ThreadInstance)
            if threadins in seen_tis:
                return
            seen_tis.add(threadins)

            # threadinss
            self.threadinss.append(threadins)
            self.td_ints.update(threadins.intervals)

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

            # last_theadins
            if self.last_threadins is None \
                    or self.last_threadins.end_seconds < threadins.end_seconds:
                self.last_threadins = threadins

            # paces_by_mark
            for mark, paces in threadins.paces_by_mark.iteritems():
                self.paces_by_mark[mark].extend(paces)

            # request vars
            self.request_vars["thread"].add(threadins.target+":"+threadins.thread)
            self.request_vars["host"].add(threadins.host)
            self.request_vars["component"].add(threadins.component)
            self.request_vars["target"].add(threadins.target)
            for key, val in threadins.thread_vars.iteritems():
                self.request_vars[key].add(val)
            for key, vals in threadins.thread_vars_dup.iteritems():
                self.request_vars[key].update(vals)
            self.len_paces += threadins.len_paces

            for joins_pace in threadins.joins_paces:
                relation = joins_pace.joins_int
                if relation is empty_join:
                    # cnt_unjoined_paces_by_edgename
                    self.e_unjoins_paces_by_edge[joins_pace.edge].add(joins_pace)
                else:
                    assert isinstance(relation, InnerjoinIntervalBase)
                    assert joins_pace.joins_pace.joined_pace is joins_pace
                    if isinstance(relation, InnerjoinInterval):
                        pass
                    else:
                        assert isinstance(relation, InterfaceInterval)
                        self.interface_ints.add(relation)
                        reqjoins = relation.joins_crossrequest_int
                        if reqjoins is empty_join:
                            self.e_unjoins_paces_by_edge[relation.join_obj].add(relation)
                    self.join_ints.add(relation)
                    _process(joins_pace.joins_pace.threadins)
            for ljoin_pace in threadins.leftinterface_paces:
                relation = ljoin_pace.joined_crossrequest_int
                if relation is empty_join:
                    self.e_unjoins_paces_by_edge[ljoin_pace.edge].add(ljoin_pace)
                else:
                    assert isinstance(relation, InterfacejoinInterval)
                    self.l_interface_joins.add(relation)
            for rjoin_pace in threadins.rightinterface_paces:
                relation = rjoin_pace.joins_crossrequest_int
                if relation is empty_join:
                    self.e_unjoins_paces_by_edge[rjoin_pace.edge].add(rjoin_pace)
                else:
                    assert isinstance(relation, InterfacejoinInterval)
                    self.r_interface_joins.add(relation)


        _process(self.start_threadins)

        self.threadinss.sort(key=lambda ti: ti.start_pace.seconds)

        # error: incomplete threads
        if self.e_incomplete_threadinss:
            err_str = "\n".join("%r" % ti for it in self.e_incomplete_threadinss)
            self.errors["Has incomplete threads"] = err_str

        # error: multiple end threads
        if self.e_extra_e_threadinss:
            self.e_extra_e_threadinss.add(self.end_threadins)
            err_str = "\n".join("%r" % ti for ti in self.e_extra_e_threadinss)
            self.errors["Has multiple end threads"] = err_str

        _main_tis = set()
        # error: no end thread
        if not self.end_threadins:
            self.errors["Contains no end thread"] = ""
        else:
            int_extended = []
            int_ = self.end_interval
            assert isinstance(int_, ThreadInterval)
            to_pace = None
            try:
                while int_:
                    int_.is_main = True
                    if isinstance(int_, ThreadInterval):
                        ti = int_.threadins
                        if ti not in _main_tis:
                            _main_tis.add(ti)
                        if to_pace is None:
                            to_pace = int_.to_pace
                    else:
                        if to_pace is not None:
                            int_extended.append(
                                    ThreadInterval(int_.to_pace,
                                                   to_pace))
                            to_pace = int_.from_pace
                        else:
                            assert False
                        int_extended.append(int_)
                    int_ = int_.prv_main
            except StateError as e:
                self.errors["Main route parse error"] = (int_, e)
            else:
                if to_pace is not None:
                    int_extended.append(
                            ThreadInterval(self.start_interval.from_pace,
                                           to_pace))
                for tint in reversed(int_extended):
                    if self.intervals_extended:
                        assert self.intervals_extended[-1].to_pace is tint.from_pace
                    else:
                        assert tint.from_pace is self.start_interval.from_pace
                    self.intervals_extended.append(tint)
                assert self.intervals_extended[-1].to_pace\
                        is self.end_interval.to_pace

        # error: stray thread instances
        self.e_stray_threadinss = tis - seen_tis
        if self.e_stray_threadinss:
            err_str = "\n".join("%r" % ti for ti in self.e_stray_threadinss)
            self.warns["Has stray threads"] = err_str

        # error: unjoins paces
        if self.e_unjoins_paces_by_edge:
            self.warns["Has unjoins paces"] = "%d joins edges" % len(self.e_unjoins_paces_by_edge)

        if not self.errors and self.request is None:
            self._index_dict[self.request_type] += 1
            self.request = "%s%d" % (self.request_type,
                    self._index_dict[self.request_type])

    @property
    def request_type(self):
        return self.start_threadins.start_pace.from_node.request_name

    @property
    def components(self):
        ret = set()
        for ti in self.threadinss:
            ret.add(ti.component)
        return ret

    @property
    def hosts(self):
        ret = set()
        for ti in self.threadinss:
            ret.add(ti.host)
        return ret

    @property
    def target_objs(self):
        ret = set()
        for ti in self.threadinss:
            ret.add(ti.target_obj)
        return ret

    @property
    def thread_objs(self):
        ret = set()
        for ti in self.threadinss:
            ret.add(ti.thread_obj)
        return ret

    @property
    def start_interval(self):
        return self.start_threadins.intervals[0]

    @property
    def end_interval(self):
        return self.end_threadins.intervals[-1]

    @property
    def request_state(self):
        return self.end_threadins.request_state

    @property
    def start_seconds(self):
        return self.start_threadins.start_seconds

    @property
    def end_seconds(self):
        return self.end_threadins.end_seconds

    @property
    def last_seconds(self):
        return self.last_threadins.end_seconds

    @property
    def start_time(self):
        return self.start_threadins.start_time

    @property
    def end_time(self):
        return self.end_threadins.end_time

    @property
    def last_time(self):
        return self.last_threadins.end_time

    @property
    def lapse(self):
        return self.end_seconds - self.start_seconds

    def __str__(self):
        return "RIns %s: lapse:%.3f[%.3f,%.3f], @%s, %d paces, "\
               "%d hosts, %d targets, %d threads, %d threadinss"\
               % (self.request,
                  self.lapse,
                  self.start_seconds,
                  self.end_seconds,
                  self.request_state,
                  self.len_paces,
                  len(self.hosts),
                  len(self.target_objs),
                  len(self.thread_objs),
                  len(self.threadinss))

    def __repr__(self):
        return "<RIns#%s: lapse:%.3f[%.3f,%.3f], @%s, %d paces, "\
               "%d hosts, %d threads>" % (self.request,
                                          self.lapse,
                                          self.start_seconds,
                                          self.end_seconds,
                                          self.request_state,
                                          self.len_paces,
                                          len(self.hosts),
                                          len(self.threadinss))

    def trace(self):
        print("%r" % self)
        int_ = self.end_interval
        print("  %s" % int_.to_pace)
        while(int_):
            print("  %s" % int_.from_pace)
            int_ = int_.prv_main
