from collections import defaultdict
from itertools import chain

from ...graph import Master
from ..exc import StateError
from .join import EmptyJoin
from .join import InnerjoinInterval
from .join import InterfacejoinInterval
from .join import InterfaceInterval
from .join import JoinIntervalBase
from .join import NestedrequestInterval
from .threadins import ThreadInterval
from .threadins import ThreadIntervalBase


class RequestInterval(ThreadInterval):
    def __init__(self, from_pace, to_pace):
        assert from_pace.threadins is to_pace.threadins
        super(ThreadIntervalBase, self).__init__(
                from_pace, to_pace, from_pace.threadins.component)

    @property
    def path(self):
        ret = super(RequestInterval, self).path
        if self.is_request_end:
            ret = "-"+ret
        if self.is_request_start:
            ret = "+"+ret
        return ret


class RequestInstance(object):
    _index_dict = defaultdict(lambda: 0)

    def __init__(self, mastergraph, request, builder):
        assert isinstance(mastergraph, Master)

        self.request = request
        self.mastergraph = mastergraph
        self.request_vars = defaultdict(set)
        self.len_paces = 0
        self.thread_objs = set()
        self.target_objs = set()

        self.threadinss = []

        # interval
        self.start_interval = None
        self.end_interval = None
        self.last_interval = None
        self.thread_ints = set()
        self.intervals_by_mark = defaultdict(list)

        self.joinints_by_type = defaultdict(set)
        self.joinedinterfaceints_by_type = defaultdict(set)
        self.joinsinterfaceints_by_type = defaultdict(set)

        self.extended_ints = []

        self._builder = builder

    @property
    def join_ints(self):
        return chain(self.joinints_by_type[InnerjoinInterval],
                     self.joinints_by_type[InterfacejoinInterval],
                     self.joinints_by_type[NestedrequestInterval])

    @property
    def request_type(self):
        ret = self.start_interval.from_pace.request_type
        assert ret
        return ret

    @property
    def components(self):
        return set(t.component for t in self.target_objs)

    @property
    def hosts(self):
        return set(t.host for t in self.target_objs)

    @property
    def request_state(self):
        if not self.end_interval:
            return "NO END INTERVAL"
        return self.end_interval.request_state

    @property
    def start_seconds(self):
        if not self.start_interval:
            return -1
        return self.start_interval.from_seconds

    @property
    def end_seconds(self):
        if not self.end_interval:
            return -1
        return self.end_interval.to_seconds

    @property
    def last_seconds(self):
        if not self.last_interval:
            return -1
        return self.last_interval.to_seconds

    @property
    def start_time(self):
        return self.start_interval.from_time

    @property
    def end_time(self):
        return self.end_interval.to_time

    @property
    def last_time(self):
        return self.last_interval.to_time

    @property
    def lapse(self):
        return self.end_seconds - self.start_seconds

    @property
    def cnt_nested(self):
        return len(self.joinedinterfaceints_by_type[InterfacejoinInterval])

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

    def iter_mainpath(self, reverse=False):
        assert isinstance(reverse, bool)
        if reverse:
            interval = self.end_interval
            while interval:
                interval.is_main = True
                yield interval

                next_i = None
                if isinstance(interval, ThreadInterval):
                    if isinstance(interval.prv_int, ThreadInterval):
                        if interval.joined_int:
                            next_i = interval.joined_int
                        else:
                            next_i = interval.prv_int
                    else:
                        next_i = interval.joined_int
                else:
                    assert isinstance(interval, JoinIntervalBase)
                    next_i = interval.joined_int

                if isinstance(next_i, InterfaceInterval):
                    next_i.is_main = True
                    next_i = next_i.joined_crossrequest_int

                if (next_i is None and not interval.is_request_start) or\
                        isinstance(next_i, EmptyJoin):
                    e = StateError("The path backward is empty")
                    e.where = interval.state_name
                    raise e
                interval = next_i
        else:
            interval = self.start_interval
            while interval:
                assert interval.is_main
                yield interval

                next_i = None
                if isinstance(interval, ThreadInterval):
                    if isinstance(interval.nxt_int, ThreadInterval) and\
                            interval.nxt_int.is_main:
                        assert not interval.joins_int or not interval.joins_int.is_main
                        next_i = interval.nxt_int
                    elif interval.joins_int and interval.joins_int.is_main:
                        next_i = interval.joins_int
                    else:
                        next_i = None
                else:
                    assert isinstance(interval, JoinIntervalBase)
                    assert interval.joins_int and interval.joins_int.is_main
                    next_i = interval.joins_int

                if isinstance(next_i, InterfaceInterval):
                    next_i = next_i.joins_crossrequest_int

                if next_i is None:
                    assert interval.is_request_end
                interval = next_i
