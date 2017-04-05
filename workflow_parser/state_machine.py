import abc
from collections import defaultdict
from functools import total_ordering

from workflow_parser import reserved_vars as rv
from workflow_parser.exception import WFException
from workflow_parser.state_graph import ThreadGraph
from workflow_parser.state_graph import MasterGraph
from workflow_parser.state_graph import Node
from workflow_parser.state_graph import Edge
from workflow_parser.log_parser import LogLine
from workflow_parser.state_graph import Join
from workflow_parser.utils import report_loglines


empty_join = object()


class StateError(WFException):
    pass


@total_ordering
class Pace(object):
    """ Pace is relative to transition. """
    def __init__(self, logline, from_node, edge, threadins):
        assert isinstance(logline, LogLine)
        assert isinstance(from_node, Node)
        assert isinstance(edge, Edge)
        assert isinstance(threadins, ThreadInstance)

        self.logline = logline
        self.from_node = from_node
        self.to_node = edge.node
        self.edge = edge
        self.threadins = threadins

        self.prv_pace = None
        self.nxt_pace = None

        self.prv_int = None
        self.nxt_int = None

        self.joins_int = None
        self.joined_int = None

    # total ordering
    __eq__ = lambda self, other: self.seconds == other.seconds
    __lt__ = lambda self, other: self.seconds < other.seconds

    @property
    def request_state(self):
        return self.to_node.request_state

    @property
    def marks(self):
        return self.to_node.marks

    @property
    def joins_pace(self):
        if self.joins_int and self.joins_int is not empty_join:
            return self.joins_int.to_pace
        else:
            return None

    @property
    def joined_pace(self):
        if self.joined_int and self.joined_int is not empty_join:
            return self.joined_int.from_pace
        else:
            return None

    @property
    def joins_objs(self):
        return self.edge.joins_objs

    @property
    def joined_objs(self):
        return self.edge.joined_objs

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
        if self.joins_objs:
            mark_str += ", join(%d):" % len(self.joins_objs)
            if self.joins_int:
                if self.joins_int is empty_join:
                    mark_str += "EMPTY"
                else:
                    mark_str += self.joins_int.name
            else:
                mark_str += "?"
        if self.joined_objs:
            mark_str += ", joined(%d):" % len(self.joined_objs)
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

        req_str = ""
        if self.threadins.is_shared:
            req_str = ", req#%s" % self.request

        return "%.3f {%s>%s>%s}, `%s`, %d lvars%s%s" % (
                self.seconds,
                self.from_node.name,
                self.edge.name,
                self.to_node.name,
                self.keyword,
                len(self.logline.get_keys()),
                req_str,
                mark_str)


    def __repr__(self):
        ret_str = str(self)
        ret_str += "\n  >>%s" % self.logline.line
        return ret_str

    def join_pace(self, next_pace, join_obj):
        assert isinstance(next_pace, Pace)
        assert isinstance(join_obj, Join)
        assert join_obj in self.edge.joins_objs
        assert self.joins_int is None
        assert next_pace.joined_int is None

        relation = JoinInterval(join_obj, self, next_pace)
        self.joins_int = relation
        next_pace.joined_int = relation
        next_pace.nxt_int.joined_int = relation
        return relation


class IntervalBase(object):
    def __init__(self, from_pace, to_pace, entity):
        assert isinstance(from_pace, Pace)
        assert isinstance(to_pace, Pace)

        self.from_pace = from_pace
        self.to_pace = to_pace

        self.from_node = from_pace.from_node
        self.to_node = to_pace.to_node
        self.entity = entity

        self.from_edge = from_pace.edge
        self.to_edge = to_pace.edge

        self.name = "%s(%s)%s" % (self.from_node.name,
                                  self.entity.name,
                                  self.to_node.name)

        self.is_main = False


    @property
    def from_seconds(self):
        return self.from_pace.seconds

    @property
    def to_seconds(self):
        return self.to_pace.seconds

    @property
    def lapse(self):
        return self.to_seconds - self.from_seconds

    @property
    def is_violated(self):
        return self.from_seconds > self.to_seconds


class ThreadInterval(IntervalBase):
    def __init__(self, from_pace, to_pace, threadins):
        assert isinstance(threadins, ThreadInstance)

        self.prv_int = None
        self.nxt_int = None
        self.joins_int = None
        self.joined_int = None

        self.target = threadins.target
        self.component = threadins.component
        self.host = threadins.host
        self.threadins = threadins

        super(ThreadInterval, self).__init__(from_pace,
                                             to_pace,
                                             from_pace.to_node)

    @property
    def node(self):
        return self.entity

    @property
    def is_lock(self):
        return self.node.is_lock

    @property
    def color(self):
        return self.component.color

    @property
    def is_request_start(self):
        return self.from_node.is_request_start

    @property
    def is_request_end(self):
        return self.to_node.is_request_end

    @property
    def is_thread_end(self):
        return self.to_node.is_thread_end

    @property
    def prv_main(self):
        assert self.is_main

        if self.prv_int and not self.prv_int.is_lock:
            assert not self.joined_int
            return self.prv_int
        elif self.prv_int:
            ret = self.joined_int
        else:
            ret = self.joined_int
        if not ret:
            assert self.is_request_start
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


class JoinInterval(IntervalBase):
    def __init__(self, join_obj, from_pace, to_pace):
        assert isinstance(join_obj, Join)
        assert isinstance(from_pace, Pace)
        assert isinstance(to_pace, Pace)

        self.joined_int = from_pace.prv_int
        self.joins_int = to_pace.nxt_int

        super(JoinInterval, self).__init__(from_pace, to_pace, join_obj)

        if not self.is_remote:
            assert self.from_target == self.to_target
            assert self.from_host == self.to_host

    @property
    def join_obj(self):
        return self.entity

    @property
    def color(self):
        j_type = self.join_type
        if j_type == "remote":
            return "#f57405"
        elif j_type == "local_remote":
            return "#ccb704"
        else:
            return self.from_component.color

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
    def is_shared(self):
        return self.join_obj.is_shared

    @property
    def is_remote(self):
        return self.join_obj.is_remote

    @property
    def join_type(self):
        if self.is_remote and self.from_host != self.to_host:
            return "remote"
        elif self.is_remote:
            return "local_remote"
        else:
            return "local"

    @property
    def prv_main(self):
        assert self.is_main
        assert self.joined_int
        return self.joined_int

    @property
    def nxt_main(self):
        assert self.is_main
        assert self.joins_int and self.joins_int.is_main
        return self.joins_int

    def __repr__(self):
        return "<JoinR#%s %f -> %f, %s -> %s>" % (
                self.name,
                self.from_seconds, self.to_seconds,
                self.from_host, self.to_host)


class ThreadInstance(object):
    def __init__(self, thread, threadgraph, loglines, s_index):
        assert isinstance(thread, str)
        assert isinstance(threadgraph, ThreadGraph)
        assert isinstance(loglines, list)
        assert isinstance(s_index, int)

        self.thread = thread
        self.threadgraph = threadgraph
        self.thread_vars = {}
        self.thread_vars_dup = {}

        self.paces = []
        self.paces_by_mark = defaultdict(list)

        self.intervals = []

        self.s_index = s_index
        self.f_index = None
        self.loglines = loglines

        self.component = None
        self.host = None
        self.target = None

        self.joined_paces = []
        self.joins_paces = []
        self.joined_ints = set()
        self.joins_ints = set()

        self.is_shared = threadgraph.is_shared
        if self.is_shared:
            self.requests = set()
            self.requestinss = set()
        else:
            self.request = None
            self.requestins = None

        # init
        s_log = loglines[s_index]
        node = threadgraph.decide_node(s_log)
        index = s_index
        while index != len(loglines):
            log = loglines[index]
            edge = node.decide_edge(log)
            if not edge:
                break
            pace = Pace(log, node, edge, self)
            marks = edge.marks
            for mark in marks:
                self.paces_by_mark[mark].append(pace)
            if edge.joins_objs:
                self.joins_paces.append(pace)
            if edge.joined_objs:
                self.joined_paces.append(pace)
            self.paces.append(pace)
            node = edge.node
            index += 1

        self.f_index = index

        # check
        if not self.paces or not self.is_complete:
            self.report("Thread is not complete")
            raise StateError("(ThreadInstance) parse error")

        # check and set thread vars
        v_dict = defaultdict(set)
        for pace in self.paces:
            logline = pace.logline
            keys = logline.get_keys(True)
            for key in keys:
                v_dict[key].add(logline[key])

        for name in ("keyword", "time", "seconds"):
            v_dict.pop(name)

        for name in ("component", "target", "host"):
            values = v_dict.pop(name)
            if len(values) == 1:
                setattr(self, name, values.pop())
            else:
                self.report("Thread key %s contains: %s" % (name, values))
                raise StateError("(ThreadInstance) parse error: variable mismatch")

        threads = v_dict.pop("thread")
        if len(threads) == 1:
            thread = threads.pop()
            if thread != self.thread:
                self.report("Thread %s collides: %s" % (self.thread, thread))
                raise StateError("(ThreadInstance) thread mismatch")
        else:
            self.report("Thread %s collides: %s" % (self.thread, threads))
            raise StateError("(ThreadInstance) thread mismatch")

        requests = v_dict.pop("request")
        if None in requests:
            requests.discard(None)
        if self.is_shared:
            self.requests.update(requests)
        else:
            if len(requests) == 0:
                pass
            elif len(requests) == 1:
                self.request = requests.pop()
            else:
                self.report("Thread has multiple requests %s" % requests)
                raise StateError("(ThreadInstance) thread is not shared!")

        # generate vars
        for k, v in v_dict.iteritems():
            if len(v) == 0:
                pass
            elif len(v) == 1:
                self.thread_vars[k] = v.pop()
            else:
                self.thread_vars_dup[k] = v

        # NOTE: must be the last, intervals and link
        prv = None
        prv_int = None
        for pace in self.paces:
            pace.prv_pace = prv
            if prv:
                prv.nxt_pace = pace
                interval = ThreadInterval(prv, pace, self)
                prv.nxt_int = interval
                pace.prv_int = interval
                self.intervals.append(interval)
                interval.prv_int = prv_int
                if prv_int:
                    prv_int.nxt_int = interval
                prv_int = interval
            prv = pace


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

    def __str__(self):
        mark_str = ""
        if self.is_shared:
            mark_str += ", SHARED"
        if not self.is_complete:
            mark_str += ", INCOMPLETE"
        if self.is_request_start:
            mark_str += ", RSTART"
        if self.is_request_end:
            mark_str += ", REND"
        if self.paces_by_mark:
            mark_str += ", %dMARKS" % len(self.paces_by_mark)

        if self.is_shared:
            req_str = self.requests
        else:
            req_str = self.request

        return "<ThIns#%s-%s: log#%d[%d, %d) comp#%s, host#%s, graph#%s, "\
               "req#%s, %d-%d vars, %d(%d) joined_p, %d(%d) joins_p%s>" % (
                self.target,
                self.thread,
                self.len_paces,
                self.s_index,
                self.f_index,
                self.component,
                self.host,
                self.threadgraph.name,
                req_str,
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

    def report(self, reason):
        print "(ThreadInstance) report: %s" % reason
        print "-------- Thread --------"
        print "%r" % self
        report_loglines(self.loglines, self.s_index, self.f_index)
        print "-------- end -----------"


class NestedRequest(object):
    def __init__(self, threadins, requestins):
        assert isinstance(threadins, ThreadInstance)
        assert threadins.is_shared == True
        assert isinstance(requestins, RequestInstance)

        self.threadins = threadins

        self.paces = []
        self.paces_by_mark = {}

        self.joined_paces = []
        self.joins_paces = []

        self.joined_ints = set()
        self.joins_ints = set()

        self.is_shared = False
        self.request = requestins.request
        self.requestins = requestins

        self.request_obj = request_obj
        self.request = request_obj.request

        dummy_pace1 = None
        for pace in threadins.joined_paces:
            if pace.joined_pace.requestins is requestins:
                dummy_pace1 = Pace(pace.logline, pace.from_node, pace.edge, self)
                relation = pace.joined_int
                relation.to_pace = dummy_pace1
                dummy_pace1.joined_int = relation

                self.joined_paces.append(dummy_pace1)
                self.joined_ints.add(dummy_relation)
                self.paces.append(dummy_pace1)
                break
        if not dummy_pace1:
            raise StateError("(NestedRequest) dummy_pace1 failed")

        dummy_pace2 = None
        for pace in threadins.joins_paces:
            if pace.joins_pace.requestins is requestins:
                dummy_pace2 = Pace(pace.logline, pace.from_node, pace.edge, self)
                relation = pace.joins_int
                relation.from_pace = dummy_pace2
                dummy_pace2.joins_int = relation

                self.joins_paces.append(dummy_pace2)
                self.joins_ints.add(dummy_relation)
                self.paces.append(dummy_pace2)

                dummy_pace1.nxt_pace = dummy_pace2
                dummy_pace2.prv_pace = dummy_pace1
                break
        if not dummy_pace2:
            raise StateError("(NestedRequest) dummy_pace2 failed")

    def __getattribute__(self, item):
        if item in self.__dict__:
            return self.__dict__[item]
        else:
            return getattr(self.threadins, name)


class RequestInstance(object):
    def __init__(self, mastergraph, request, threadinss):
        assert isinstance(mastergraph, MasterGraph)
        assert isinstance(request, str)
        assert isinstance(threadinss, set)

        self.request = request
        self.mastergraph = mastergraph
        self.request_vars = defaultdict(set)

        self.threadinss = []
        self.nestedreqs = set()
        self.start_threadins = None
        self.end_threadins = None
        self.last_threadins = None

        self.paces_by_mark = defaultdict(list)
        self.len_paces = 0

        self.join_ints = set()
        self.td_ints = set()

        # error report
        self.errors = {}
        self.warns = {}

        # collect others
        self.e_incomplete_threadinss = set()
        self.e_extra_s_threadinss = set()
        self.e_extra_e_threadinss = set()
        self.e_stray_threadinss = set()
        self.e_unjoins_paces_by_edge = defaultdict(set)

        # interval
        self.intervals = set()

        # init
        shared_tis = set()
        tis = set()
        for threadins in threadinss:
            assert isinstance(threadins, ThreadInstance)
            if threadins.is_shared:
                shared_tis.add(threadins)
                threadins.requestinss.add(self)
                threadins.requests.add(request)
            else:
                tis.add(threadins)
                threadins.request_obj = self
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

        for shared_ti in shared_tis:
            tis.add(NestedRequest(shared_ti, self))

        seen_tis = set()
        def _process(threadins):
            if threadins in seen_tis:
                return
            seen_tis.add(threadins)

            # threadinss
            self.threadinss.append(threadins)
            self.td_ints.update(threadins.intervals)

            # nestedreqs
            if isinstance(threadins, NestedRequest):
                self.nestedreqs.add(threadins)

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
            self.request_vars["thread"].add(threadins.target+"-"+threadins.thread)
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
                    assert joins_pace.joins_pace.joined_pace is joins_pace
                    self.join_ints.add(relation)
                    _process(joins_pace.joins_pace.threadins)

        _process(self.start_threadins)

        self.threadinss.sort(key=lambda ti: ti.start_pace.seconds)

        def _process_int(from_pace, to_pace):
            pass

        # error: incomplete threads
        if self.e_incomplete_threadinss:
            err_str = "\n".join("%r" % ti for it in self.e_incomplete_threadinss)
            self.errors["Has incomplete threads"] = err_str

        # error: multiple end threads
        if self.e_extra_e_threadinss:
            self.e_extra_e_threadinss.add(self.end_threadins)
            err_str = "\n".join("%r" % ti for ti in self.e_extra_e_threadinss)
            self.errors["Has multiple end threads"] = err_str

        # error: no end thread
        if not self.end_threadins:
            self.errors["Contains no end thread"] = ""
        else:
            int_ = self.end_interval
            while int_:
                int_.is_main = True
                try:
                    int_ = int_.prv_main
                except AssertionError:
                    self.errors["Main route parse error"] = int_
                    break

        # error: stray thread instances
        self.e_stray_threadinss = tis - seen_tis
        if self.e_stray_threadinss:
            err_str = "\n".join("%r" % ti for ti in self.e_stray_threadinss)
            self.warns["Has stray threads"] = err_str

        # error: unjoins paces
        if self.e_unjoins_paces_by_edge:
            self.warns["Has unjoins paces"] = "%d joins edges" % len(self.e_unjoins_paces_by_edge)

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
    def len_threadinss(self):
        return len(self.threadinss)

    @property
    def len_hosts(self):
        return len(self.request_vars["host"])

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
    def lapse(self):
        return self.end_seconds - self.start_seconds

    def __str__(self):
        return "Request %s: lapse:%.3f[%.3f,%.3f], @%s, %d paces, "\
               "%d hosts, %d threads" % (self.request,
                                          self.lapse,
                                          self.start_seconds,
                                          self.end_seconds,
                                          self.request_state,
                                          self.len_paces,
                                          self.len_hosts,
                                          self.len_threadinss)

    def __repr__(self):
        return "<Rins#%s: lapse:%.3f[%.3f,%.3f], @%s, %d paces, "\
               "%d hosts, %d threads>" % (self.request,
                                          self.lapse,
                                          self.start_seconds,
                                          self.end_seconds,
                                          self.request_state,
                                          self.len_paces,
                                          self.len_hosts,
                                          self.len_threadinss)
