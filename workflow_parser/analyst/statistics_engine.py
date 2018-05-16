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

from collections import defaultdict
from itertools import chain
from itertools import zip_longest
import pandas as pd

from ..workflow.entities.request import RequestInstance
from .statistic_helper import Workflow


def _reset_starttime(requestinss, targetobjs_by_target, do_reset=True):
    first_req = None
    end_req = None
    last_req = None
    for requestins in requestinss.values():
        if first_req is None:
            first_req = requestins
        elif first_req.from_seconds > requestins.from_seconds:
            first_req = requestins

        if end_req is None:
            end_req = requestins
        elif end_req.to_seconds < requestins.to_seconds:
            end_req = requestins

        if last_req is None:
            last_req = requestins
        elif last_req.last_seconds < requestins.last_seconds:
            last_req = requestins

    start_s = first_req.from_seconds
    start_t = first_req.from_time
    end_s = end_req.to_seconds
    end_t = end_req.to_time
    last_s = last_req.last_seconds
    last_t = last_req.last_time
    print("lapse: %.4f, (%.4f -> %.4f,%.4f), (%s -> %s,%s)" % (
            end_s - start_s,
            start_s,
            end_s,
            last_s,
            start_t,
            end_t,
            last_t))

    if do_reset:
        for tg in targetobjs_by_target.values():
            tg.offset -= start_s
        end_s = end_s - start_s
        last_s = last_s - start_s
        start_s = 0
        print("zero: %.4f -> %.4f,%.4f" % (start_s, end_s, last_s))

    return {"seconds": {"start": start_s,
                        "end":   end_s,
                        "last":  last_s},
            "time":    {"start": start_t,
                        "end":   end_t,
                        "last":  last_t}}


def _convert_to_dataframe(objs, index, columns):
    # objs: objs to be converted
    # index: the index attr
    # columns: appended "_entity" column
    if index is None:
        index_vals = None
    else:
        objs = [obj for obj in objs]
        index_vals = (getattr(obj, index) for obj in objs)
    f_getvals = []
    cols = []
    for col in columns:
        if isinstance(col, str):
            def _make_getattr(col):
                return lambda o: getattr(o, col)
            cols.append(col)
            f_getvals.append(_make_getattr(col))
        else:
            assert isinstance(col, tuple)
            assert len(col) == 2
            assert isinstance(col[0], str)
            cols.append(col[0])
            f_getvals.append(col[1])
    f_getvals.append(lambda e:e)
    cols.append("_entity")

    df = pd.DataFrame((tuple(f(o) for f in f_getvals) for o in objs),
                       index=index_vals,
                       columns=cols)
    return df


def generate_dataframes(requestinss):
    targetobjs_by_target = {t.target: t
                            for r in requestinss.values()
                            for t in r.target_objs}
    requestinss_by_type = defaultdict(list)
    for r in requestinss.values():
        assert isinstance(r, RequestInstance)
        requestinss_by_type[r.request_type].append(r)

    ## adjust offset
    start_end = _reset_starttime(requestinss, targetobjs_by_target)

    workflow_by_type = {}
    for r_type, reqinss in requestinss_by_type.items():
        requestins_iters = [r.iter_mainints() for r in reqinss]
        workflow = Workflow(r_type)
        for intervals in zip_longest(*requestins_iters):
            workflow.build(intervals)
        workflow.reduce()
        workflow.ready()
        workflow_by_type[r_type] = workflow

    ## prepare dataframes
    targets_df = _convert_to_dataframe(
            targetobjs_by_target.values(),
            "target",
            ("component",
             "host"))

    join_intervals_df = _convert_to_dataframe(
            chain(chain.from_iterable(req.iter_joins() for req in requestinss.values())),
            None,
            ("request",
             "request_type",
             "int_name",
             "lapse",
             "path",
             "from_seconds",
             "to_seconds",
             "from_time",
             "to_time",
             "from_keyword",
             "to_keyword",
             ("int_type", lambda i: i.__class__.__name__),

             ("desc", lambda i: "%s: %s -> %s" % (i.path,
                                                  i.from_keyword,
                                                  i.to_keyword)),
             "order",
             "is_main",
             "remote_type",
             "from_target",
             "to_target",
             "from_host",
             "to_host",
             ("hosts", lambda i: "%s -> %s" % (i.from_host,
                                               i.to_host)),
             "from_component",
             "to_component",
             "from_thread",
             "to_thread"))

    td_intervals_df = _convert_to_dataframe(
            chain.from_iterable(req.iter_threadints() for req in requestinss.values()),
            None,
            ("request",
             "request_type",
             "int_name",
             "lapse",
             "path",
             "from_seconds",
             "to_seconds",
             "from_time",
             "to_time",
             "from_keyword",
             "to_keyword",
             ("int_type", lambda i: i.__class__.__name__),

             ("desc", lambda i: "%s: %s -> %s" % (i.path,
                                                  i.from_keyword,
                                                  i.to_keyword)),
             "order",
             "is_main",
             "target",
             "host",
             "component",
             "thread"))

    request_df = _convert_to_dataframe(
            requestinss.values(),
            "request",
            ("request_type",
             "int_name",
             "lapse",
             "path",
             "from_seconds",
             "to_seconds",
             "from_time",
             "to_time",
             "from_keyword",
             "to_keyword",
             ("int_type", lambda i: i.__class__.__name__),

             "request_state",
             "last_seconds",
             "last_time",
             "len_paces",
             "len_main_paces",
             ("len_threads", lambda r: len(r.thread_objs)),
             ("len_threadinss", lambda r: len(r.threadinss)),
             ("len_targets", lambda r: len(r.target_objs)),
             ("len_hosts", lambda r: len(r.hosts))))

    #vars
    invalid_keys = set()
    valid_keys = set()
    for req in requestinss.values():
        for k, v in req.request_vars.items():
            if len(v) > 1:
                valid_keys.discard(k)
                invalid_keys.add(k)
            elif len(v) == 1:
                if k not in invalid_keys:
                    valid_keys.add(k)

    def _get_val(k):
        return lambda r: list(r.request_vars.get(k, [None]))[0]
    request_vars_df = _convert_to_dataframe(
            requestinss.values(),
            "request",
            ((k, _get_val(k)) for k in valid_keys))

    return (requestinss_by_type,
            targetobjs_by_target,
            workflow_by_type,
            start_end,
            join_intervals_df, td_intervals_df,
            request_df, targets_df,
            request_vars_df)
