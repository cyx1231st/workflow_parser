import numbers

from .log_entities import LogFile
from .log_entities import LogLine


class Thread(object):
    _repr_lines_lim = 10

    def __init__(self, id_, target_obj, thread):
        assert isinstance(target_obj, Target)
        assert isinstance(thread, str)
        assert isinstance(id_, int)

        self.id_ = id_
        self.thread = thread
        self.target_obj = target_obj
        # all the "correct" loglines in this thread
        self.loglines = []

        # after thread instances are built
        self.threadinss = []
        self.ignored_loglines = []

    @property
    def name(self):
        return "%s|td%d" % (self.target, self.id_)

    @property
    def target(self):
        return self.target_obj.target

    @property
    def component(self):
        return self.target_obj.component

    @property
    def host(self):
        return self.target_obj.host

    @property
    def lapse(self):
        assert self.threadinss
        return sum(ti.lapse for ti in self.threadinss)

    def __str__(self):
        return "<Thread#%s: %s, %d loglines, %d ignored, %d threadinss>" %\
                (self.name,
                 self.thread,
                 len(self.loglines),
                 len(self.ignored_loglines),
                 len(self.threadinss))

    def __repr__(self):
        ret = str(self)
        lim = self._repr_lines_lim
        ret += "\n^ ---- last ----"
        for line in reversed(self.loglines):
            ret += "\n| %s" % line.__str_thread__()
            lim -= 1
            if lim <= 0:
                ret += "\n ......"
        ret += "\n  ---- first ---"
        return ret

    def append_logline(self, logline):
        assert isinstance(logline, LogLine)
        assert logline.thread == self.thread
        assert logline.target == self.target

        if self.loglines:
            assert self.loglines[-1] <= logline
        self.loglines.append(logline)


class Target(object):
    _repr_lines_lim = 10

    def __init__(self, logfile):
        self.logfile = logfile
        # NOTE: filtered incorrect loglines
        self.loglines = []
        self.thread_objs = {}
        self.threadobjs_list = []
        self.requests = set()

        index_thread = 0
        for logline in logfile.yield_logs():
            assert isinstance(logline, LogLine)
            assert logline.logfile is logfile
            assert logline.thread is not None

            thread = logline.thread
            if logline.request is not None:
                self.requests.add(logline.request)
            self.loglines.append(logline)
            thread_obj = self.thread_objs.get(thread)
            if thread_obj is None:
                thread_obj = Thread(index_thread, self, thread)
                self.thread_objs[thread] = thread_obj
                self.threadobjs_list.append(thread_obj)
                index_thread += 1
            thread_obj.append_logline(logline)

        assert self.component is not None
        assert self.host is not None
        assert self.target is not None

    @property
    def component(self):
        return self.logfile.component

    @property
    def host(self):
        return self.logfile.host

    @property
    def target(self):
        return self.logfile.target

    @property
    def offset(self):
        return self.logfile.offset

    @offset.setter
    def offset(self, val):
        assert isinstance(val, numbers.Real)
        self.logfile.offset = val

    def __str__(self):
        return "<Target#%s: from %s, comp=%s, host=%s, off=%d, %d lines, %d threads>" % (
               self.target,
               self.logfile.name,
               self.component,
               self.host,
               self.offset,
               len(self.loglines),
               len(self.thread_objs))

    def __repr__(self):
        ret = str(self)
        lim = self._repr_lines_lim
        ret += "\n^ ---- last ----"
        for line in reversed(self.loglines):
            ret += "\n| %s" % line.__str_target__()
            lim -= 1
            if lim <= 0:
                ret += "\n ......"
        ret += "\n  ---- first ---"
        return ret
