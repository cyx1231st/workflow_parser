from collections import defaultdict

from ...graph import MasterGraph
from ..engine.exc import StateError
from .join import EmptyJoin
from .join import InnerjoinIntervalBase
from .threadins import IntervalBase
from .threadins import ThreadInterval


class RequestInterval(IntervalBase):
    def __init__(self, from_pace, to_pace):
        super(RequestInterval, self).__init__(from_pace, to_pace, None)


class RequestInstance(object):
    _index_dict = defaultdict(lambda: 0)

    def __init__(self, mastergraph, request, builder):
        assert isinstance(mastergraph, MasterGraph)

        self.request = request
        self.mastergraph = mastergraph
        self.request_vars = defaultdict(set)
        self.len_paces = 0

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
    def request_type(self):
        return self.start_interval.from_node.request_name

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
                        if interval.prv_int.is_lock:
                            if interval.joined_int:
                                next_i = interval.joined_int
                            else:
                                next_i = interval.prv_int
                        else:
                            if interval.joined_int:
                                e = StateError("Both prv_int and joined_int are accetable")
                                e.where = interval.node.name
                                raise e
                            next_i = interval.prv_int
                    else:
                        next_i = interval.joined_int
                else:
                    assert isinstance(interval, InnerjoinIntervalBase)
                    next_i = interval.joined_int

                if (next_i is None and not interval.is_request_start) or\
                        isinstance(next_i, EmptyJoin):
                    e = StateError("The path backward is empty")
                    e.where = interval.entity.name
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
                    assert isinstance(interval, InnerjoinIntervalBase)
                    assert interval.joins_int and interval.joins_int.is_main
                    next_i = interval.joins_int

                if next_i is None:
                    assert interval.is_request_end
                interval = next_i
