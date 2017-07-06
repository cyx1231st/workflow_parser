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

from collections import OrderedDict
from numbers import Real


class HostConstraint(object):
    def __init__(self, name):
        assert isinstance(name, str)

        self.hostname = name

        self.low = float("-inf")
        self.high = float("inf")
        # history
        self.last_low = None
        self.last_high = None

        # how many host connected?
        self.relationcons = []
        # how many joins related?
        self.cnt_joins = 0

    @property
    def determined(self):
        if self.low == self.high:
            return self.low
        else:
            return None

    @property
    def ready(self):
        if self.low != float("-inf") and self.high != float("inf") \
                and self.low != self.high:
            return True
        else:
            return False

    def change(self, low, high):
        assert self.low <= low\
            and low <= high\
            and high <= self.high

        if self.low != low or self.high != high:
            self.last_low = self.low
            self.last_high = self.high
            self.low = low
            self.high = high
            return True
        else:
            return False

    def adjust(self, distance):
        assert isinstance(distance, float)

        if self.ready:
            # This may introduce offsets even in single-host mode
            if self.low + 2 * distance > self.high:
                result = (self.low + self.high)/2
                self.change(result, result)
                return True
            guess = self.low + distance
            if guess > 0:
                assert guess + distance <= self.high
                self.change(guess, guess)
                return True
            guess = self.high - distance
            if guess < 0:
                assert guess - distance >= self.low
                self.low = guess
                self.high = guess
                return True
            self.change(0, 0)
            return True
        else:
            return False

    def adjust_side(self, distance):
        if self.low == float("-inf") and self.high == float("inf"):
            return False
        elif self.low == float("-inf"):
            if self.high - distance >= 0:
                target = 0
            else:
                target = self.high - distance
            self.change(target, target)
            return True
        elif self.high == float("inf"):
            if self.low + distance <= 0:
                target = 0
            else:
                target = self.low + distance
            self.change(target, target)
            return True
        else:
            return False

    def adjust_none(self):
        assert self.low == float("-inf")
        assert self.high == float("inf")
        self.change(0, 0)

    def __str__(self):
        if self.determined is not None:
            return "%s:%.3f" \
                   % (self.hostname, self.determined)
        else:
            return "%s:(%.3f, %.3f)" \
                   % (self.hostname, self.low, self.high)

    def __repr__(self):
        if self.determined is not None:
            return "<Host %s: %.3f, connects %d>" \
                   % (self.hostname, self.determined, len(self.relationcons))
        else:
            return "<Host %s: (%.3f, %.3f), connects %d>" \
                   % (self.hostname, self.low, self.high, len(self.relationcons))


class RelationConstraint(object):
    def __init__(self, from_hostc, to_hostc):
        assert isinstance(from_hostc, HostConstraint)
        assert isinstance(to_hostc, HostConstraint)

        self.from_hostc = from_hostc
        self.to_hostc = to_hostc

        self.low = float("-inf")
        self.high = float("inf")

        # how many joins related?
        self.cnt_joins = 0

    @property
    def distance(self):
        if self.high < self.low:
            assert False
        else:
            return self.high - self.low

    @property
    def is_violated(self):
        if self.high < 0 or self.low > 0:
            return True
        else:
            return False

    def setoffset(self, from_hostc, to_hostc, from_seconds, to_seconds):
        if from_hostc is self.from_hostc:
            assert to_hostc is self.to_hostc
            self.high = min(self.high, to_seconds - from_seconds)
        else:
            assert from_hostc is self.to_hostc
            assert to_hostc is self.from_hostc
            self.low = max(self.low, from_seconds - to_seconds)
        assert self.high >= self.low
        self.cnt_joins += 1
        from_hostc.cnt_joins += 1
        to_hostc.cnt_joins += 1


    def relax(self):
        changed_hostcs = []
        low = max(self.low, -self.to_hostc.high + self.from_hostc.low)
        high = min(self.high, -self.to_hostc.low + self.from_hostc.high)
        if low > high:
            orig_str = self._to_str()
            self.low = low
            self.high = high
            curr_str = self._to_str()
            raise RuntimeError("Relation constraint violated(relationc):\n%s\n%s"
                    % (orig_str, curr_str))
        self.low = low
        self.high = high

        from_l = max(self.from_hostc.low, self.to_hostc.low + self.low)
        from_h = min(self.from_hostc.high, self.to_hostc.high + self.high)
        if from_l > from_h:
            orig_str = self._to_str()
            self.from_hostc.low = from_l
            self.from_hostc.high = from_h
            curr_str = self._to_str()
            raise RuntimeError("Relation constraint violated(from_hc):\n%s\n%s"
                    % (orig_str, curr_str))
        try:
            if self.from_hostc.change(from_l, from_h):
                changed_hostcs.append(self.from_hostc)
        except AssertionError:
            raise RuntimeError("Constraint from_hostc %s violated!" % self)

        to_l = max(self.to_hostc.low, self.from_hostc.low - self.high)
        to_h = min(self.to_hostc.high, self.from_hostc.high - self.low)
        if to_l > to_h:
            orig_str = self._to_str()
            self.to_hostc.low = to_l
            self.to_hostc.high = to_h
            curr_str = self._to_str()
            raise RuntimeError("Relation constraint violated(to_hc):\n%s\n%s"
                    % (orig_str, curr_str))
        try:
            if self.to_hostc.change(to_l, to_h):
                changed_hostcs.append(self.to_hostc)
        except AssertionError:
            raise RuntimeError("Constraint to_hostc %s violated!" % self)

        return changed_hostcs

    def __repr__(self):
        return "<RCon %s --[%.3f, %.3f]--> %s>" % (
                self.from_hostc,
                self.low,
                self.high,
                self.to_hostc)


class CausalEngine(object):
    def __init__(self):
        self.hosts = {}
        self.unknown_hostcs = []
        self.determined_hostcs = set()

        self.relationcons = set()
        self._relationcon_by_from_to = {}

        self.distance = float("inf")

        self.violated_joinrelations = set()
        self.violated_relationcons = set()
        self.relax_counter = 0

    def _create_hostc(self, hostname):
        hostc = self.hosts.get(hostname)
        if hostc is None:
            hostc = HostConstraint(hostname)
            self.hosts[hostname] = hostc
            self.unknown_hostcs.append(hostc)
        return hostc

    def register(self, from_host, to_host, from_seconds, to_seconds):
        assert isinstance(from_host, str)
        assert isinstance(to_host, str)
        assert isinstance(from_seconds, Real)
        assert isinstance(to_seconds, Real)

        from_hostc = self._create_hostc(from_host)
        to_hostc = self._create_hostc(to_host)

        r_key = (from_host, to_host)
        relationcon = self._relationcon_by_from_to.get(r_key)
        if not relationcon:
            relationcon = RelationConstraint(from_hostc, to_hostc)
            self.relationcons.add(relationcon)

            self._relationcon_by_from_to[r_key] = relationcon
            r_rkey = (to_host, from_host)
            assert r_rkey not in self._relationcon_by_from_to
            self._relationcon_by_from_to[r_rkey] = relationcon

            from_hostc.relationcons.append(relationcon)
            to_hostc.relationcons.append(relationcon)
        relationcon.setoffset(from_hostc, to_hostc,
                              from_seconds, to_seconds)

    def relax(self):
        self.unknown_hostcs.sort(key=lambda hc: -len(hc.relationcons))
        h_odict = OrderedDict()
        for hc in self.unknown_hostcs:
            h_odict[hc] = None
        self.unknown_hostcs = h_odict

        for relationcon in self.relationcons:
            self.distance = min(self.distance, relationcon.distance/2)
            if relationcon.is_violated:
                self.violated_relationcons.add(relationcon)
        if self.distance == float("inf"):
            self.distance = 0.0

        if not self.unknown_hostcs:
            return False
        while True:
            next_hostc = None
            # ready hostc first
            for hostc in self.unknown_hostcs.iterkeys():
                if hostc.adjust(self.distance):
                    next_hostc = hostc
                    break
            # sided hostc second
            if next_hostc is None:
                for hostc in self.unknown_hostcs.iterkeys():
                    if hostc.adjust_side(self.distance):
                        next_hostc = hostc
                        break
            # isolated hostc
            if next_hostc is None:
                next_hostc = self.unknown_hostcs.iterkeys().next()
                next_hostc.adjust_none()

            self.unknown_hostcs.pop(next_hostc)
            self.determined_hostcs.add(next_hostc)

            if not self.unknown_hostcs:
                break

            changed_hostcs = {next_hostc}
            while changed_hostcs:
                next_changed_hostcs = set()
                for hostc in changed_hostcs:
                    for relationcon in hostc.relationcons:
                        hostcs = relationcon.relax()
                        next_changed_hostcs.update(hostcs)
                        self.relax_counter += 1
                changed_hostcs = next_changed_hostcs
        return True
