from __future__ import print_function

from collections import defaultdict
from itertools import chain
import pandas as pd

from ..workflow.entities.request import RequestInstance
from ..workflow.entities.request import RequestInterval
from .automated_suite import general_purpose_analysis
from .statistic_helper import get_extended_pathtype


def _reset_starttime(requestinss, targetobjs_by_target, do_reset=True):
    first_req = None
    last_req = None
    for requestins in requestinss.itervalues():
        if first_req is None:
            first_req = requestins
        elif first_req.start_seconds > requestins.start_seconds:
            first_req = requestins

        if last_req is None:
            last_req = requestins
        elif last_req.last_seconds < requestins.last_seconds:
            last_req = requestins

    start_s = first_req.start_seconds
    start_t = first_req.start_time
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
            chain(chain.from_iterable(req.join_ints for req in requestinss.itervalues())),
            None,
            ("request",
             ("request_type", lambda i: requestinss[i.request].request_type),
             ("entity", lambda i: i.entity.name),
             "lapse",
             "path_name",
             "is_main",
             ("int_type", lambda i: i.__class__.__name__),
             "from_seconds",
             "to_seconds",
             "from_time",
             "to_time",
             ("from_node", lambda i: i.from_node.name),
             ("to_node", lambda i: i.to_node.name),
             ("from_edge", lambda i: i.from_edge.name),
             ("to_edge", lambda i: i.to_edge.name),
             #join_int
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
            chain.from_iterable(req.thread_ints for req in requestinss.itervalues()),
            None,
            ("request",
             ("request_type", lambda i: requestinss[i.request].request_type),
             ("entity", lambda i: i.entity.name),
             "lapse",
             "path_name",
             "is_main",
             ("int_type", lambda i: i.__class__.__name__),
             "from_seconds",
             "to_seconds",
             "from_time",
             "to_time",
             ("from_node", lambda i: i.from_node.name),
             ("to_node", lambda i: i.to_node.name),
             ("from_edge", lambda i: i.from_edge.name),
             ("to_edge", lambda i: i.to_edge.name),
             #thread_int
             "target",
             "host",
             "component",
             "thread"))

    def _get_extended_pathtype(interval):
        return "%s->%s->%s" % (interval.from_edge.name,
                               get_extended_pathtype(interval, True),
                               interval.to_edge.name)
    extendedints_df = _convert_to_dataframe(
            chain.from_iterable(req.extended_ints for req in requestinss.itervalues()),
            None,
            ("request",
             ("request_type", lambda i: i.requestins.request_type),
             "from_seconds",
             "to_seconds",
             "lapse",
             ("path_type", get_extended_pathtype),
             ("path", _get_extended_pathtype)))

    request_df = _convert_to_dataframe(
            requestinss.itervalues(),
            "request",
            ("request_type",
             "request_state",
             "lapse",
             "start_seconds",
             "end_seconds",
             "last_seconds",
             "start_time",
             "end_time",
             "last_time",
             "len_paces",
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
