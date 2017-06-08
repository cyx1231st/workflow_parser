from collections import defaultdict

from ...graph import MasterGraph
from ..engine.exc import StateError
from .join import InnerjoinInterval
from .join import InnerjoinIntervalBase
from .join import InterfaceInterval
from .join import InterfacejoinInterval
from .threadins import empty_join
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

        self.threadinss = []
        self.start_threadins = None
        self.end_threadins = None
        self.last_threadins = None

        self.intervals_by_mark = defaultdict(list)
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

        self.threadinss.sort(key=lambda ti: ti.start_seconds)

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
                                    RequestInterval(int_.to_pace,
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
                            RequestInterval(self.start_interval.from_pace,
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
        return self.start_threadins.start_interval.from_node.request_name

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
