from __future__ import print_function

import heapq


def report_loglines(loglines, from_, to_=None, reason=None, blanks=0, printend=False):
    if to_ is None:
        to_ = from_
    from_t = from_ - 3
    to_t = to_ + 7

    blanks_str = " "*blanks

    print(blanks_str + "-------- thread --------")
    if reason:
        print(blanks_str + "reason: %s" % reason)

    if from_t < 0:
        print(blanks_str + "  <start>")
        from_t = 0
    else:
        print(blanks_str + "  ...more...")

    for i in range(from_t, from_):
        print(blanks_str + "  %s" % loglines[i])
    for i in range(from_, to_):
        print(blanks_str + "| %s" % loglines[i])
    print(blanks_str + "> %s" % loglines[to_])
    for i in range(to_+1, min(to_t, len(loglines))):
        print(blanks_str + "  %s" % loglines[i])
    if to_t >= len(loglines):
        print(blanks_str + "  <end>")
    else:
        print(blanks_str + "  ...more...")

    if printend:
        print(blanks_str + "-------- end -----------")


class Heap(object):
    def __init__(self, key=lambda a:a):
        self._index = 0
        self._heap = []
        self.largest = None
        self.f_key = key

    @property
    def peeked(self):
        if not self._heap:
            return None
        else:
            return self._heap[0]

    @property
    def distance(self):
        if not self._heap:
            return None
        else:
            return self.largest[0] - self.peeked[0]

    def __len__(self):
        return len(self._heap)

    def push(self, item):
        key = self.f_key(item)
        item_tup = (key, self._index, item)
        heapq.heappush(self._heap, item_tup)
        self._index += 1
        if self.largest is None or key > self.largest[0]:
            self.largest = item_tup

    def pop(self):
        ret = heapq.heappop(self._heap)
        if not self._heap:
            self.largest = None
        return ret[2]

    def __repr__(self):
        peeked_str = ""
        if self.peeked is not None:
            peeked_str = ", peeked %s" % self.peeked[0]
        largest_str = ""
        if self.largest is not None:
            largest_str = ", largest %s" % self.largest[0]
        return "<Heap: %d items, index %d%s%s>"\
                % (len(self),
                   self._index,
                   peeked_str,
                   largest_str)


class Report(object):
    def __init__(self):
        self._steps = []
        self.lines = []
        self.components = []
        self.hosts = []
        self.targets = []
        self.threads = []
        self.requests = []
        self.threadinss = []
        self.joins = []
        self.joineds = []

    def step(self, name, line="x", component="x", host="x", target="x",
             thread="x", request="x", threadins="x", join="x", joined="x"):
        self._steps.append(name)
        self.lines.append(line)
        self.components.append(component)
        self.hosts.append(host)
        self.targets.append(target)
        self.threads.append(thread)
        self.requests.append(request)
        self.threadinss.append(threadins)
        self.joins.append(join)
        self.joineds.append(joined)

    def __repr__(self):
        head = " "*7 +" line/pace,component,     host,   target,   thread,  request,  thd_ins,    joins,   joined"
        fmt = "\n%7s" + ",%9s"*9
        ret = head
        for i, name in enumerate(self._steps):
            ret += fmt % (name, self.lines[i], self.components[i],
                          self.hosts[i], self.targets[i], self.threads[i],
                          self.requests[i], self.threadinss[i],
                          self.joins[i], self.joineds[i])
        return ret
