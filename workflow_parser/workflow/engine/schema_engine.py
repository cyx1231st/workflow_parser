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

from abc import ABCMeta
from abc import abstractmethod
from functools import total_ordering
from itertools import chain
import pandas as pd
import numpy as np

from ...graph.joinables import JoinBase
from ..exc import StateError


debug = True
debug_more = True


ONE = "ONE"
ALL = "ALL"
ANY = "ANY"
JoinTypes = {ONE, ALL, ANY}


class JoiningProject(object):
    def __init__(self, name, join_objs):
        assert isinstance(name, str)

        self.name = name
        self.works_byjo = {}

        self.cnt_tojoin = 0
        self.cnt_bejoin = 0

        self.from_items = []
        self.to_items = []

        for jo in join_objs:
            assert jo not in self.works_byjo
            self.works_byjo[jo] = PandasIndexer(join_obj=jo)

    def load_fromitem(self, join_objs, **kwds):
        assert join_objs

        from_item = JoinItem(join_objs=join_objs, **kwds)
        self.from_items.append(from_item)
        for jo in join_objs:
            join_work = self.works_byjo.get(jo)
            if not join_work:
                raise RuntimeError("Invalid join_obj %r from project %s" % (
                    join_obj, self.name))
            join_work.load_fromitem(from_item)

    def load_toitem(self, join_objs, **kwds):
        assert join_objs

        to_item = JoinItem(strategy=ONE, join_objs=join_objs, **kwds)
        self.to_items.append(to_item)
        for jo in join_objs:
            join_work = self.works_byjo.get(jo)
            if not join_work:
                raise RuntimeError("Invalid join_obj %r from project %s" % (
                    join_obj, self.name))
            join_work.load_toitem(to_item)

    def yield_results(self, target_byname):
        for jo_work in self.works_byjo.itervalues():
            for jo, from_, to_ in jo_work.yield_results(target_byname):
                yield jo, from_, to_
        for jo, from_, to_ in self.yield_empty():
            yield jo, from_, to_

    def yield_empty(self):
        print(self.name+":")
        cnt_success = 0
        cnt_fail = 0
        for item in self.from_items:
            if item.is_success:
                cnt_success += 1
            else:
                cnt_fail += 1
                for jo in item.yield_empty():
                    yield jo, item.item, None
        print("  from: %d success, %d failed" % (
            cnt_success, cnt_fail))

        cnt_success = 0
        cnt_fail = 0
        for item in self.to_items:
            if item.is_success:
                cnt_success += 1
            else:
                cnt_fail += 1
                for jos in item.yield_empty():
                    yield jos, None, item.item
        print("  to: %d success, %d failed" % (
            cnt_success, cnt_fail))


# NOTE: no total_ordering because it will be grouped
class JoinItem(object):
    def __init__(self, strategy, seconds, env, item, join_objs):
        assert strategy in JoinTypes
        assert isinstance(seconds, float)

        self.strategy = strategy
        self.seconds = seconds
        self.env = env
        self.item = item

        self._peers = {}
        self._has_peer = False

        for jo in join_objs:
            self._peers[jo] = None

    @property
    def is_success(self):
        if self.strategy in {ONE, ANY}:
            if self._has_peer:
                return True
            else:
                return False
        else:
            for _, peer in self._peers.iteritems():
                if peer is None:
                    return False
            return True

    # NOTE: only yield when is_success is False!
    def yield_empty(self):
        if self.strategy in {ONE, ANY}:
            if len(self._peers) > 1:
                yield self._peers.keys()
            else:
                for jo in self._peers.iterkeys():
                    yield [jo]
        elif self.strategy == ALL:
            for jo, peer in self._peers.iteritems():
                if peer is None:
                    yield [jo]

    def set_peer(self, join_obj, peer):
        assert isinstance(join_obj, JoinBase)
        assert isinstance(peer, JoinItem)
        if not self.strategy == ANY:
            assert self._peers[join_obj] is None

        self._has_peer = True
        self._peers[join_obj] = peer

    def is_joinable(self, join_obj):
        if self.strategy == ONE:
            if self._has_peer:
                return False
        elif self.strategy is ALL:
            if self._peers[join_obj]:
                return False
        return True


class IndexerBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, join_obj):
        assert isinstance(join_obj, JoinBase)
        self.join_obj = join_obj

    @property
    def name(self):
        return "%sIndexer" % self.join_obj.name

    @property
    def schemas(self):
        return self.join_obj.schemas

    @abstractmethod
    def load_fromitem(self, from_item):
        assert isinstance(from_item, JoinItem)
        for from_, _ in self.schemas:
            try:
                from_item.env[from_]
            except StateError as e:
                raise StateError(
                        "Env error, incomplete from schema: %s from %r of %s!"
                        % (from_, from_item.item, self.join_obj.name), e)

    @abstractmethod
    def load_toitem(self, to_item):
        assert isinstance(to_item, JoinItem)
        for _, to_ in self.schemas:
            try:
                to_item.env[to_]
            except StateError as e:
                raise StateError(
                        "Env error, incomplete schema: %s from %r of %s!"
                        % (to_, to_item.item, self.join_obj.name), e)

    @abstractmethod
    def yield_results(self):
        pass


class PandasIndexer(IndexerBase):
    def __init__(self, **kwds):
        super(PandasIndexer, self).__init__(**kwds)

        self.from_items = []
        self.to_items = []

        self.cnt_success = 0

        self.from_cnt_ignored = 0
        self.from_cnt_nomatch = 0
        self.from_cnt_novalidmatch = 0
        self.from_cntmax_permatch = 0
        self.from_total_matches = 0
        self.from_occur_matches = 0

        self.to_cnt_ignored = 0
        self.to_cnt_nomatch = 0
        self.to_cnt_novalidmatch = 0
        self.to_cntmax_permatch = 0
        self.to_total_matches = 0
        self.to_occur_matches = 0

        self.max_negative_offset = 0
        self.total_negative_offset = 0
        self.occur_negateve_offset = 0

    def load_fromitem(self, from_item):
        super(PandasIndexer, self).load_fromitem(from_item)

        self.from_items.append(from_item)

    def load_toitem(self, to_item):
        super(PandasIndexer, self).load_toitem(to_item)

        self.to_items.append(to_item)

    def yield_results(self, target_byname):
        print(self.join_obj.name+
              "(%d -> %d): "%(len(self.from_items), len(self.to_items))+
              repr(self.join_obj))

        str_schema = self.join_obj.str_schema
        columns = ["seconds", "_item", str_schema]

        # translation from *target* to target_alias
        def get_value(item, schema, other):
            ret = str(item.env[schema])
            if "target" != schema and "target" == other:
                if ret not in target_byname:
                    raise StateError("Cannot translate target %s" % ret)
                ret = target_byname[ret].target
            return ret

        # index from_items, count ignored
        self.from_items.sort(key=lambda i:i.seconds)
        def generate_from_rows():
            for item in self.from_items:
                if item.is_joinable(self.join_obj):
                    yield (item.seconds, item,
                           ",".join(get_value(item, schema, other)
                               for schema, other in self.schemas))
                else:
                    self.from_cnt_ignored += 1
        from_indexer = pd.DataFrame(
                generate_from_rows(),
                index=None,
                columns=columns)

        # index to_items, count ignored
        self.to_items.sort(key=lambda i:i.seconds)
        def generate_to_rows():
            for item in self.to_items:
                if item.is_joinable(self.join_obj):
                    yield (item.seconds, item,
                           ",".join(get_value(item, schema, other)
                               for other, schema in self.schemas))
                else:
                    self.to_cnt_ignored += 1
        to_indexer = pd.DataFrame(
                generate_to_rows(),
                index=None,
                columns=columns)

        # join by schemas
        join_result = pd.merge(
                from_indexer, to_indexer,
                on=[str_schema],
                suffixes=("_from", "_to"),
                how="outer")
        matches = join_result[(join_result["_item_from"].notnull()) &
                              (join_result["_item_to"].notnull())]
        matches_byto = matches.groupby("_item_to")

        # match items, evaluate offsets
        matches_byto_list = []
        for to_item, to_matches in matches_byto:
            matches_byto_list.append((to_item,
                                      [item for item in to_matches["_item_from"]]))
        matches_byto_list.sort(key=lambda item:item[0])
        for to_item, to_matches in matches_byto_list:
            to_matches.sort(key=lambda i:i.seconds)
            for match in to_matches:
                if match.is_joinable(self.join_obj):
                    match.set_peer(self.join_obj, to_item)
                    to_item.set_peer(self.join_obj, match)

                    offset = match.seconds - to_item.seconds
                    self.max_negative_offset = max(offset, self.max_negative_offset)
                    if offset > 0:
                        self.total_negative_offset += offset
                        self.occur_negateve_offset += 1
                    self.cnt_success += 1
                    yield self.join_obj, match.item, to_item.item
                    break

        if debug:
            # evaluate nomatch
            from_nomatch = join_result[join_result["_item_to"].isnull()]["_item_from"]
            to_nomatch = join_result[join_result["_item_from"].isnull()]["_item_to"]
            self.from_cnt_nomatch = len(from_nomatch)
            self.to_cnt_nomatch = len(to_nomatch)

            # evaluate multiple matches
            for _, to_matches in matches_byto:
                len_m = len(to_matches)
                self.to_cntmax_permatch = max(self.to_cntmax_permatch, len_m)
                if len_m > 1:
                    self.to_occur_matches += 1
                    self.to_total_matches += len_m
            matches_byfrom = matches.groupby("_item_from")
            for _, from_matches in matches_byfrom:
                len_m = len(from_matches)
                self.from_cntmax_permatch = max(self.from_cntmax_permatch, len_m)
                if len_m > 1:
                    self.from_occur_matches += 1
                    self.from_total_matches += len_m
            # evaluate novalidmatches
            for from_item, _ in matches_byfrom:
                if from_item._peers[self.join_obj] is None:
                    self.from_cnt_novalidmatch += 1
            for to_item, _ in matches_byto:
                if to_item._peers[self.join_obj] is None:
                    self.to_cnt_novalidmatch += 1
            if debug_more:
                # print diagnose details
                matches_byschema = matches.groupby(str_schema)
                to_print = []
                for s_content, lines in matches_byschema:
                    if len(lines) > 1:
                        for index, line in lines.iterrows():
                            to_print.append((line["seconds_from"],
                                             line[str_schema],
                                             line["_item_from"],
                                             line["_item_to"]))
                to_print.sort(key=lambda l:l[0])
                for l in to_print:
                    if l[2]._peers[self.join_obj] is not l[3]:
                        label="!"
                    else:
                        label=""
                    if l[2]._peers[self.join_obj] is None:
                        from_label="!"
                    else:
                        from_label=""
                    if l[3]._peers[self.join_obj] is None:
                        to_label="!"
                    else:
                        to_label=""
                    print("  %s%s: %s`%s`%s -> %s`%s`%s" % (
                        label, l[1],
                        l[2].seconds, l[2].item.keyword, from_label,
                        l[3].seconds, l[3].item.keyword, to_label))

        self.report()

    def report(self):
        print("  success: %d" % self.cnt_success)

        if self.from_cnt_ignored:
            print("  fromitems ignored: %d" % self.from_cnt_ignored)
        if self.from_cnt_nomatch:
            print("  fromitems nomatch: %d" % self.from_cnt_nomatch)
        if self.from_cnt_novalidmatch:
            print("  fromitems novalidmatch: %d" % self.from_cnt_novalidmatch)
        if self.from_occur_matches:
            print("  fromitems MULTI-MATCH: %d(max %d, evg %.5f)" % (
                self.from_occur_matches,
                self.from_cntmax_permatch,
                self.from_total_matches/float(self.from_occur_matches)))

        if self.to_cnt_ignored:
            print("  toitems ignored: %d" % self.to_cnt_ignored)
        if self.to_cnt_nomatch:
            print("  toitems nomatch: %d" % self.to_cnt_nomatch)
        if self.to_cnt_novalidmatch:
            print("  toitems novalidmatch: %d" % self.to_cnt_novalidmatch)
        if self.to_occur_matches:
            print("  toitems MULTI-MATCH: %d(max %d, evg %.5f)" % (
                self.to_occur_matches,
                self.to_cntmax_permatch,
                self.to_total_matches/float(self.to_occur_matches)))

        if self.occur_negateve_offset:
            print("  -OFFSET: %d(max %.5f, evg %.5f)" % (
                self.occur_negateve_offset,
                self.max_negative_offset,
                self.total_negative_offset/float(self.occur_negateve_offset)))
