from collections import defaultdict

from ...graph import MasterGraph
from ..engine.exc import StateError
from .join import EmptyJoin
from .join import InnerjoinInterval
from .join import InnerjoinIntervalBase
from .join import InterfaceInterval
from .join import InterfacejoinInterval
from .threadins import IntervalBase
from .threadins import ThreadInstance
from .threadins import ThreadInterval


class RequestInterval(IntervalBase):
    def __init__(self, from_pace, to_pace):
        super(RequestInterval, self).__init__(from_pace, to_pace, None)


class RequestInstance(object):
    _index_dict = defaultdict(lambda: 0)

    def __init__(self, mastergraph, request, threadinss):
        assert isinstance(mastergraph, MasterGraph)
        assert isinstance(threadinss, set)

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
                if self.start_interval is not None:
                    self.e_extra_s_threadinss.add(threadins)
                else:
                    self.start_interval = threadins.start_interval

        # error: multiple start threads
        if self.e_extra_s_threadinss:
            self.e_extra_s_threadinss.add(self.start_interval.threadins)
            error_str = "\n".join("%r" % ti for ti in self.e_extra_s_threadinss)
            self.errors["Has multiple start intervals"] = error_str

        # error: no start thread
        if self.start_interval is None:
            self.errors["Contains no start interval"] = ""
            return

        seen_tis = set()
        def _process(threadins):
            assert isinstance(threadins, ThreadInstance)
            if threadins in seen_tis:
                return
            seen_tis.add(threadins)

            # threadinss
            self.threadinss.append(threadins)
            self.thread_ints.update(threadins.intervals)

            # incomplete_threadinss
            if not threadins.is_complete:
                self.e_incomplete_threadinss.add(threadins)

            # end_interval
            r_state = threadins.request_state
            if r_state:
                assert threadins.is_request_end is True
                if self.end_interval:
                    self.e_extra_e_threadinss.add(threadins)
                else:
                    self.end_interval = threadins.end_interval

            # last_theadins
            if self.last_interval is None \
                    or self.last_interval.to_seconds < threadins.end_seconds:
                self.last_interval = threadins.end_interval

            # intervals_by_mark
            for mark, intervals in threadins.intervals_by_mark.iteritems():
                self.intervals_by_mark[mark].extend(intervals)

            # request vars
            self.request_vars["thread"].add(threadins.target+":"+threadins.thread)
            self.request_vars["host"].add(threadins.host)
            self.request_vars["component"].add(threadins.component)
            self.request_vars["target"].add(threadins.target)
            for key, val in threadins.thread_vars.iteritems():
                self.request_vars[key].add(val)
            for key, vals in threadins.thread_vars_dup.iteritems():
                self.request_vars[key].update(vals)
            self.len_paces += len(threadins.intervals)+1

            for joins_int in threadins.joinsints_by_type[EmptyJoin]:
                self.joinints_by_type[EmptyJoin].add(joins_int)
                unjoins_pace = joins_int.from_pace
                self.e_unjoins_paces_by_edge[unjoins_pace.edge].add(unjoins_pace)
            for joins_int in threadins.joinsints_by_type[InnerjoinInterval]:
                self.joinints_by_type[InnerjoinInterval].add(joins_int)
                _process(joins_int.to_threadins)
            for joins_int in threadins.joinsints_by_type[InterfaceInterval]:
                reqjoins = joins_int.joins_crossrequest_int
                if isinstance(reqjoins, EmptyJoin):
                    self.e_unjoins_paces_by_edge[joins_int.join_obj].add(joins_int)
                self.joinints_by_type[InterfaceInterval].add(joins_int)
                _process(joins_int.to_threadins)

            for joins_int in threadins.joinsinterfaceints_by_type[InterfacejoinInterval]:
                self.joinsinterfaceints_by_type[InterfacejoinInterval].add(joins_int)
            for joins_int in threadins.joinsinterfaceints_by_type[EmptyJoin]:
                self.joinsinterfaceints_by_type[EmptyJoin].add(joins_int)
                unjoins_pace = joins_int.from_pace
                self.e_unjoins_paces_by_edge[unjoins_pace.edge].add(unjoins_pace)

            for joined_int in threadins.joinedinterfaceints_by_type[InterfacejoinInterval]:
                self.joinedinterfaceints_by_type[InterfacejoinInterval].add(joined_int)
            for joined_int in threadins.joinedinterfaceints_by_type[EmptyJoin]:
                self.joinedinterfaceints_by_type[EmptyJoin].add(joined_int)

        _process(self.start_interval.threadins)

        self.threadinss.sort(key=lambda ti: ti.start_seconds)

        # error: incomplete threads
        if self.e_incomplete_threadinss:
            err_str = "\n".join("%r" % ti for it in self.e_incomplete_threadinss)
            self.errors["Has incomplete threads"] = err_str

        # error: multiple end threads
        if self.e_extra_e_threadinss:
            self.e_extra_e_threadinss.add(self.end_interval.threadins)
            err_str = "\n".join("%r" % ti for ti in self.e_extra_e_threadinss)
            self.errors["Has multiple end intervals"] = err_str

        # error: no end thread
        if not self.end_interval:
            self.errors["Contains no end interval"] = ""
        else:
            int_extended = []
            try:
                last_int = None
                for int_ in self.iter_mainpath(reverse=True):
                    if isinstance(int_, ThreadInterval):
                        if last_int is None:
                            last_int = int_
                    else:
                        assert isinstance(int_, InnerjoinIntervalBase)
                        if last_int is not None:
                            int_extended.append(
                                    RequestInterval(int_.to_pace,
                                                    last_int.to_pace))
                            last_int = None
                        else:
                            assert False
                        int_extended.append(int_)
                int_extended.append(
                        RequestInterval(self.start_interval.from_pace,
                                        last_int.to_pace))
            except StateError as e:
                self.errors["Main route parse error"] = (int_, e)
            else:
                for tint in reversed(int_extended):
                    if self.extended_ints:
                        assert self.extended_ints[-1].to_pace is tint.from_pace
                    else:
                        assert tint.from_pace is self.start_interval.from_pace
                    self.extended_ints.append(tint)
                assert self.extended_ints[-1].to_pace is self.end_interval.to_pace

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
