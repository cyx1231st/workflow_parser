from collections import OrderedDict

from workflow_parser.log_engine import TargetsCollector
from workflow_parser.state_engine import PacesCollector
from workflow_parser.state_machine import JoinInterval


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
    def __init__(self, relations):
        self.hosts = {}
        self.unknown_hostcs = []
        self.determined_hostcs = set()

        self.relationcons = set()

        self.distance = float("inf")

        self.violated_joinrelations = set()
        self.violated_relationcons = set()
        self.relax_counter = 0

        def _create_hostc(hostname):
            hostc = self.hosts.get(hostname)
            if hostc is None:
                hostc = HostConstraint(hostname)
                self.hosts[hostname] = hostc
                self.unknown_hostcs.append(hostc)
            return hostc

        _relationcon_by_from_to = {}
        def _create_relationcon(from_hostc, to_hostc):
            assert isinstance(from_hostc, HostConstraint)
            assert isinstance(to_hostc, HostConstraint)

            from_host = from_hostc.hostname
            to_host = to_hostc.hostname
            relationcon = _relationcon_by_from_to.get(
                    (from_host, to_host))
            if not relationcon:
                relationcon = RelationConstraint(from_hostc, to_hostc)
                self.relationcons.add(relationcon)
                _relationcon_by_from_to[(from_host, to_host)] = relationcon
                assert (to_host, from_host) not in _relationcon_by_from_to
                _relationcon_by_from_to[(to_host, from_host)] = relationcon
                from_hostc.relationcons.append(relationcon)
                to_hostc.relationcons.append(relationcon)
            return relationcon

        for relation in relations:
            assert isinstance(relation, JoinInterval)
            assert relation.is_remote is True
            assert relation.from_host != relation.to_host

            if relation.is_violated:
                self.violated_joinrelations.add(relation)

            from_hostc = _create_hostc(relation.from_host)
            to_hostc = _create_hostc(relation.to_host)
            relationcon = _create_relationcon(from_hostc, to_hostc)
            relationcon.setoffset(from_hostc, to_hostc,
                                  relation.from_seconds, relation.to_seconds)

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

    def relax(self):
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


def relation_parse(pcs_collector, tgs_collector):
    assert isinstance(pcs_collector, PacesCollector)
    assert isinstance(tgs_collector, TargetsCollector)

    relations = pcs_collector.join_intervals_by_type["remote"]
    if not relations:
        print("No relations detected, skip relation engine.\n")
        return

    print("Preparing constraints...")
    causal_engine = CausalEngine(relations)
    print("total %d host constraints" % len(causal_engine.hosts))
    print("total %d relation constraints" % len(causal_engine.relationcons))
    print("distance %f" % causal_engine.distance)
    print("%d violated joinrelations" %
            len(causal_engine.violated_joinrelations))
    print("%d violated relation constraints:" %
            len(causal_engine.violated_relationcons))
    for relationcon in causal_engine.violated_relationcons:
        print("  %s" % relationcon)
    print("ok\n")

    print("Correcting...")
    if causal_engine.relax():
        print("%d relax attempts" % causal_engine.relax_counter)
        hosts = causal_engine.hosts.values()
        hosts.sort(key=lambda hostc: hostc.hostname)
        for hostc in hosts:
            if hostc.low != 0:
                print "adjust %r" % hostc
                for target in tgs_collector.itervalues(tgs_collector.targets_by_host[hostc.hostname]):
                    target.offset = hostc.low
        for relation in relations:
            assert not relation.is_violated
    else:
        print("No need to correct clocks")
    print("ok\n")
