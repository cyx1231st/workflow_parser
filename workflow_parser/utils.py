# Copyright (c) 2017 Yingxin Cheng
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

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
        self.innerjoins = []
        self.innerjoineds = []
        self.interfacejoins = []
        self.interfacejoineds = []
        self.leftinterfaces = []
        self.rightinterfaces = []

    def step(self, name, line="x", component="x", host="x", target="x",
             thread="x", request="x", threadins="x",
             innerjoin="x", innerjoined="x",
             interfacejoin="x", interfacejoined="x",
             leftinterface="x", rightinterface="x"):
        self._steps.append(name)
        self.lines.append(line)
        self.components.append(component)
        self.hosts.append(host)
        self.targets.append(target)
        self.threads.append(thread)
        self.requests.append(request)
        self.threadinss.append(threadins)
        self.innerjoins.append(innerjoin)
        self.innerjoineds.append(innerjoined)
        self.interfacejoins.append(interfacejoin)
        self.interfacejoineds.append(interfacejoined)
        self.leftinterfaces.append(leftinterface)
        self.rightinterfaces.append(rightinterface)

    def __repr__(self):
        head = " "*7 +"      line,component,     host,   target,"\
               "   thread,  request,  thd_ins,"\
               " innjoins,innjoined,"\
               " infjoins,infjoined,"\
               " l_interf, r_interf"
        fmt = "\n%7s" + ",%9s"*13
        ret = head
        for i, name in enumerate(self._steps):
            ret += fmt % (name, self.lines[i], self.components[i],
                          self.hosts[i], self.targets[i], self.threads[i],
                          self.requests[i], self.threadinss[i],
                          self.innerjoins[i], self.innerjoineds[i],
                          self.interfacejoins[i], self.interfacejoineds[i],
                          self.leftinterfaces[i], self.rightinterfaces[i])
        return ret
