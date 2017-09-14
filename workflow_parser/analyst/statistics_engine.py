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
import pandas as pd

from ..workflow.entities.request import RequestInstance
from .automated_suite import general_purpose_analysis


def _reset_starttime(requestinss, targetobjs_by_target, do_reset=True):
    first_req = None
    last_req = None
    for requestins in requestinss.itervalues():
        if first_req is None:
            first_req = requestins
        elif first_req.from_seconds > requestins.from_seconds:
            first_req = requestins

        if last_req is None:
            last_req = requestins
        elif last_req.last_seconds < requestins.last_seconds:
            last_req = requestins

    start_s = first_req.from_seconds
    start_t = first_req.from_time
    end_s = last_req.last_seconds
    end_t = last_req.last_time
    print("lapse: %.4f, (%.4f -> %.4f), (%s -> %s)" % (
            end_s - start_s,
            start_s,
            end_s,
            start_t,
            end_t))

    if do_reset:
        for tg in targetobjs_by_target.itervalues():
            tg.offset -= start_s
        end_s = end_s - start_s
        start_s = 0
        print("zero: %.4f -> %.4f" % (start_s, end_s))

    return {"seconds": {"start": start_s,
                        "end":   end_s},
            "time":    {"start": start_t,
                        "end":   end_t}}


def _convert_to_dataframe(objs, index, columns):
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


def do_statistics(master_graph, requestinss, d_engine, report):
    if not requestinss:
        print("No requests available, abort!")
        return

    print("Preparing relations...")
    targetobjs_by_target = {t.target: t
                            for r in requestinss.itervalues()
                            for t in r.target_objs}
    requestinss_by_type = defaultdict(list)
    for r in requestinss.itervalues():
        assert isinstance(r, RequestInstance)
        requestinss_by_type[r.request_type].append(r)

    ## adjust offset
    start_end = _reset_starttime(requestinss, targetobjs_by_target)

    ## prepare dataframes
    targets_df = _convert_to_dataframe(
            targetobjs_by_target.itervalues(),
            "target",
            ("component",
             "host"))

    join_intervals_df = _convert_to_dataframe(
            chain(chain.from_iterable(req.iter_joins() for req in requestinss.itervalues())),
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

             "is_main",
             ("int_type", lambda i: i.__class__.__name__),
             "remote_type",
             "from_target",
             "to_target",
             "from_host",
             "to_host",
             "from_component",
             "to_component",
             "from_thread",
             "to_thread"))

    td_intervals_df = _convert_to_dataframe(
            chain.from_iterable(req.iter_threadints() for req in requestinss.itervalues()),
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

             "is_main",
             ("int_type", lambda i: i.__class__.__name__),
             "target",
             "host",
             "component",
             "thread"))

    extendedints_df = _convert_to_dataframe(
            chain.from_iterable(req.iter_mainints(extended=True) for req in requestinss.itervalues()),
            None,
            ("request",
             "request_type",
             "int_name",
             "lapse",
             "path",
             "from_seconds",
             "to_seconds",
             "from_time",
             "to_time"))

    request_df = _convert_to_dataframe(
            requestinss.itervalues(),
            "request",
            ("request_type",
             "int_name",
             "lapse",
             "path",
             "from_seconds",
             "to_seconds",
             "from_time",
             "to_time",

             "request_state",
             "last_seconds",
             "last_time",
             "len_paces",
             "len_main_paces",
             ("len_threads", lambda r: len(r.thread_objs)),
             ("len_threadinss", lambda r: len(r.threadinss)),
             ("len_targets", lambda r: len(r.target_objs)),
             ("len_hosts", lambda r: len(r.hosts))))

    general_purpose_analysis(master_graph,
                             requestinss, requestinss_by_type,
                             targetobjs_by_target, start_end,
                             join_intervals_df, td_intervals_df,
                             extendedints_df, request_df, targets_df,
                             d_engine, report)

    report.export()
