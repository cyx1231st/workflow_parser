from abc import ABCMeta
from collections import defaultdict
from functools import total_ordering

from ... import reserved_vars as rv
from ...target import Line
from ...target import Thread
from ...graph import State
from ...graph import Step
from ...graph import ThreadGraph


@total_ordering
class Pace(object):
    """ Pace is relative to transition. """
    def __init__(self, line_obj, step, threadins):
        assert isinstance(line_obj, Line)
        assert line_obj._assigned is None
        assert isinstance(step, Step)
        assert isinstance(threadins, ThreadInstance)
        assert line_obj.thread_obj is threadins.thread_obj

        self.line_obj = line_obj
        self.step = step
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
    def path(self):
        return self.step.path

    @property
    def step_name(self):
        return self.step.name

    @property
    def marks(self):
        return self.step.to_state.marks

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
        if self.joins_int:
            # assert isinstance(self.joins_int, InnerjoinIntervalBase)
            return self.joins_int.to_pace
        else:
            return None

    @property
    def joined_pace(self):
        if self.joined_int:
            # assert isinstance(self.joined_int, InnerjoinIntervalBase)
            return self.joined_int.from_pace
        else:
            return None

    @property
    def request_state(self):
        return self.step.to_state.request_state

    @property
    def request_type(self):
        return self.step.from_state.request_type

    @property
    def is_request_start(self):
        return self.step.from_state.is_request_start

    @property
    def is_request_end(self):
        return self.step.to_state.is_request_end

    @property
    def is_thread_start(self):
        return self.step.from_state.is_thread_start

    @property
    def is_thread_end(self):
        return self.step.to_state.is_thread_end and\
                not isinstance(self.nxt_int, ThreadInterval)

    @property
    def joins_objs(self):
        return self.step.joins_objs

    @property
    def joined_objs(self):
        return self.step.joined_objs

    @property
    def joins_interface(self):
        return self.step.joins_interface

    @property
    def joined_interface(self):
        return self.step.joined_interface

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
        if self.step.joins_objs:
            mark_str += ", join:"
            if self.joins_int:
                mark_str += "[%s->%.3f,%s]" %\
                        (self.joins_int.path,
                         self.joins_pace.seconds,
                         self.joins_pace.step.name)
            else:
                mark_str += "?"
        if self.step.joined_objs:
            mark_str += ", joined:"
            if self.joined_int:
                mark_str += "[%s<-%.3f,%s]" %\
                        (self.joined_int.path,
                         self.joined_pace.seconds,
                         self.joined_pace.step_name)
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
        if self.request_state:
            mark_str += "@%s" % self.request_state
        return mark_str

    def __str__(self):
        mark_str = self._mark_str()
        return "<Pace %.3f {%s}, `%s`, %d lvars, %s-%s %s%s>" % (
                self.seconds,
                self.step.path,
                self.keyword,
                len(self.line_obj.keys),
                self.target,
                self.thread,
                self.request,
                mark_str)

    def __str_thread__(self):
        mark_str = self._mark_str()

        return "%.3f {%s}, `%s`, %d lvars%s" % (
                self.seconds,
                self.step.path,
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

        self.from_pace = from_pace
        self.to_pace = to_pace
        # state or join_obj or None
        self.entity = entity
        self.is_main = False

    @property
    def fromstep_name(self):
        return self.from_pace.step_name if self.from_pace else "Nah"

    @property
    def tostep_name(self):
        return self.to_pace.step_name if self.to_pace else "Nah"

    @property
    def state_name(self):
        return self.entity.name if self.entity else "Nah"

    @property
    def path(self):
        return "%s[%s]%s" % (
                self.fromstep_name, self.state_name, self.tostep_name)

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
    def marks(self):
        if isinstance(self.entity, State):
            return self.entity.marks
        else:
            return []

    def __repr__(self):
        return "<%s#%s: %s %s |--> %s %s>" % (
                self.__class__.__name__,
                self.path,
                self.from_seconds,
                self.from_pace.step.keyword,
                self.to_seconds,
                self.to_pace.step.keyword)


class ThreadIntervalBase(IntervalBase):
    __metaclass__ = ABCMeta

    def __init__(self, from_pace, to_pace, entity):
        super(ThreadIntervalBase, self).__init__(from_pace,
                                                 to_pace,
                                                 entity)
        assert from_pace.thread_obj is to_pace.thread_obj
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
    def thread_obj(self):
        return self.from_pace.thread_obj

    @property
    def target_obj(self):
        return self.thread_obj.target_obj

    @property
    def prv_int(self):
        prv_int = self.from_pace.prv_int
        if prv_int:
            assert isinstance(prv_int, ThreadIntervalBase)
        return prv_int

    @property
    def nxt_int(self):
        nxt_int = self.to_pace.nxt_int
        if nxt_int:
            assert isinstance(nxt_int, ThreadIntervalBase)
        return nxt_int


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
        state = from_pace.step.to_state
        assert to_pace.step.from_state is state
        super(ThreadInterval, self).__init__(from_pace,
                                             to_pace,
                                             state)
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
    def state(self):
        return self.entity

    @property
    def requestins(self):
        return self.threadins.requestins

    @property
    def request(self):
        return self.requestins.request

    @property
    def request_type(self):
        return self.requestins.request_type

    # inheritance
    @property
    def is_request_start(self):
        return self.from_pace.is_request_start

    @property
    def is_request_end(self):
        return self.to_pace.is_request_end

    @property
    def is_thread_start(self):
        return self.from_pace.is_thread_start

    @property
    def is_thread_end(self):
        if self.to_pace.is_thread_end:
            assert self is self.threadins.intervals[-1]
            return True
        else:
            return False

    @property
    def request_state(self):
        return self.to_pace.request_state


class ThreadInstance(object):
    def __init__(self, thread_obj, threadgraph):
        assert isinstance(thread_obj, Thread)
        assert isinstance(threadgraph, ThreadGraph)
        assert thread_obj.component is threadgraph.component

        self.thread_obj = thread_obj
        self.threadgraph = threadgraph

        self.thread_vars = {}
        self.thread_vars_dup = defaultdict(set)

        self._start_pace = None
        self.intervals = []
        self.intervals_by_mark = defaultdict(list)

        self.joinedints_by_type = defaultdict(set)
        self.joinsints_by_type = defaultdict(set)
        self.joinedinterfaceints_by_type = defaultdict(set)
        self.joinsinterfaceints_by_type = defaultdict(set)

        self.request = None
        self.requestins = None

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
    def target_obj(self):
        return self.thread_obj.target_obj

    @property
    def start_interval(self):
        return self.intervals[0]

    @property
    def end_interval(self):
        return self.intervals[-1]

    @property
    def request_state(self):
        return self.end_interval.request_state

    @property
    def is_request_start(self):
        return self.start_interval.is_request_start

    @property
    def is_request_end(self):
        return self.end_interval.is_request_end

    @property
    def is_complete(self):
        return self.end_interval.is_thread_end

    @property
    def start_seconds(self):
        return self.start_interval.from_seconds

    @property
    def end_seconds(self):
        return self.end_interval.to_seconds

    @property
    def lapse(self):
        return self.end_seconds - self.start_seconds

    @property
    def start_time(self):
        return self.start_interval.from_time

    @property
    def end_time(self):
        return self.end_interval.to_time

    @property
    def prv_threadins(self):
        prv_int = self.start_interval.prv_int
        if prv_int is not None:
            assert isinstance(prv_int, BlankInterval)
            return prv_int.from_threadins
        else:
            return None

    @property
    def nxt_threadins(self):
        nxt_int = self.end_interval.nxt_int
        if nxt_int is not None:
            assert isinstance(nxt_int, BlankInterval)
            return nxt_int.to_threadins
        else:
            return None

    def __repr__(self):
        mark_str = ""
        if not self.is_complete:
            mark_str += ", INCOMPLETE"
        if self.is_request_start:
            mark_str += ", RSTART"
        if self.is_request_end:
            mark_str += ", REND"
        if self.intervals_by_mark:
            mark_str += ", %dMARKS" % len(self.intervals_by_mark)

        return "<ThIns#%s-%s: len#%d, lapse#%f, comp#%s, host#%s, graph#%s, "\
               "req#%s, %d(dup%d) vars, %d %dinnerj, %d %dcrossj%s>" % (
                self.target,
                self.thread,
                len(self.intervals),
                self.lapse,
                self.component,
                self.host,
                self.threadgraph.name,
                self.request,
                len(self.thread_vars),
                len(self.thread_vars_dup),
                sum(len(ints) for ints in self.joinsints_by_type.itervalues()),
                sum(len(ints) for ints in self.joinedints_by_type.itervalues()),
                sum(len(ints) for ints in self.joinsinterfaceints_by_type.itervalues()),
                sum(len(ints) for ints in self.joinedinterfaceints_by_type.itervalues()),
                mark_str)

    def __str__(self):
        ret_str = repr(self)
        ret_str += "\n  %s" % self.threadgraph
        ret_str += "\n  Paces:"
        for pace in self._iter_paces():
            ret_str += "\n  | %s" % pace.__str_thread__()
        return ret_str

    def _iter_paces(self, reverse=False):
        assert isinstance(reverse, bool)
        if self.intervals:
            if reverse is False:
                yield self.intervals[0].from_pace
                for interval in self.intervals:
                    yield interval.to_pace
            else:
                yield self.intervals[-1].to_pace
                for interval in reversed(self.intervals):
                    yield interval.from_pace
