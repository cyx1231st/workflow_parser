from __future__ import print_function

from collections import defaultdict
from itertools import chain
import pandas as pd

from ..workflow.entities.request import RequestInstance
from ..workflow.entities.request import RequestInterval
from .automated_suite import general_purpose_analysis
from .statistic_helper import get_path_type


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

    def _get_path_type(interval):
        return "%s->%s->%s" % (interval.from_edge.name,
                               get_path_type(interval, True),
                               interval.to_edge.name)
    extendedints_df = _convert_to_dataframe(
            chain.from_iterable(req.extended_ints for req in requestinss.itervalues()),
            None,
            ("request",
             ("request_type", lambda i: i.requestins.request_type),
             "from_seconds",
             "to_seconds",
             "lapse",
             ("path_type", _get_path_type),
             ("path", get_path_type)))

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


    """
    join_intervals = set()
    thread_intervals = set()
    extended_intervals = set()
    for requestins in requestinss.itervalues():
        assert isinstance(requestins, RequestInstance)
        for ti in requestins.threadinss:
            targetobjs_by_target[ti.target] = ti.target_obj
        join_intervals.update(requestins.join_ints)
        thread_intervals.update(requestins.td_ints)
        extended_intervals.update(requestins.intervals_extended)

    ## join intervals
    relations = join_intervals
    t_dict = {relation.from_target:(str(relation.from_component), relation.from_host)
              for relation in relations}
    t_dict.update({relation.to_target:(str(relation.to_component), relation.to_host)
                  for relation in relations})
    t_index = [k for k in t_dict.iterkeys()]
    t_values = ((t_dict[k][0], t_dict[k][1]) for k in t_index)

    # targets
    targets_df = pd.DataFrame(t_values,
                              index=t_index,
                              columns=["component", "host"])

    # target relations
    join_intervals_df = pd.DataFrame(((relation.requestins.request,
                                       relation.lapse,
                                       relation.from_seconds,
                                       relation.to_seconds,
                                       relation.entity.name,
                                       relation.from_node.name,
                                       relation.to_node.name,
                                       relation.path_name,
                                       relation.path_type,
                                       relation.int_type,
                                       relation.color,
                                       relation.color_jt,
                                       relation.from_target,
                                       relation.to_target,
                                       relation.join_type)
                                      for relation in relations),
                                     columns=["request",
                                              "lapse", "from_sec", "to_sec",
                                              "entity", "from_node", "to_node",
                                              "path",
                                              "pathtype", "inttype",
                                              "color",
                                              # join specific
                                              "color_jt",
                                              "from_target", "to_target",
                                              "join_type"])

    # full relations
    relations_df = join_intervals_df\
            .join(targets_df, on="from_target")\
            .join(targets_df, on="to_target", rsuffix="1")

    # host relations
    host_relations_df = relations_df\
            .loc[relations_df["join_type"] != "local"]\
            .groupby(["host", "host1"])\
            .size()\
            .unstack()\
            .fillna(0)\
            .astype("int")
    host_relations_df.index.name = "from_host"
    host_relations_df.columns.name = "to_host"

    # component local relations
    ca_local_relations_df = relations_df\
            .loc[relations_df["join_type"] == "local"]\
            .groupby(["from_target", "to_target"])\
            .size()\
            .reset_index()\
            .join(targets_df, on="from_target")\
            .join(targets_df, on="to_target", rsuffix="1")\
            .groupby(["component", "component1"])\
            .mean()\
            .unstack()
    ca_local_relations_df.index.name = "from_component"
    ca_local_relations_df.columns = ca_local_relations_df.columns.droplevel()
    ca_local_relations_df.columns.name = "to_component"

    # component remote relations
    ca_relations_df = relations_df\
            .loc[relations_df["join_type"] != "local"]\
            .groupby(["from_target", "to_target"])\
            .size()\
            .reset_index()\
            .join(targets_df, on="from_target")\
            .join(targets_df, on="to_target", rsuffix="1")\
            .groupby(["component", "component1"])\
            .mean()\
            .unstack()
    ca_relations_df.index.name = "from_component"
    ca_relations_df.columns = ca_relations_df.columns.droplevel()
    ca_relations_df.columns.name = "to_component"

    ## requests
    rqs_index = [ri for ri in requestinss]
    rqs_df = pd.DataFrame(((requestinss[rq], requestinss[rq].len_paces,
                            len(requestinss[rq].hosts),
                            len(requestinss[rq].threadinss),
                            requestinss[rq].start_seconds, requestinss[rq].end_seconds,
                            requestinss[rq].lapse)
                           for rq in rqs_index),
                          index=rqs_index,
                          columns=["requestins", "paces", "hosts",
                                   "threadinss", "start", "end",
                                   "lapse"])

    longest_lapse_requestins = rqs_df.loc[rqs_df["lapse"].idxmax()]["requestins"]
    print("longest lapse request instance: %s" % longest_lapse_requestins)
    longest_paces_requestins = rqs_df.loc[rqs_df["paces"].idxmax()]["requestins"]
    print("longest paces_request instance: %s" % longest_paces_requestins)

    ## thread intervals
    threadints_df = pd.DataFrame(((int_.requestins.request,
                                   int_.lapse,
                                   int_.from_seconds, int_.to_seconds,
                                   int_.entity.name,
                                   int_.from_node.name, int_.to_node.name,
                                   int_.path_name,
                                   int_.path_type, int_.int_type,
                                   int_.color,
                                   int_.host, str(int_.component), int_.target)
                                 for int_ in thread_intervals),
                                columns=["request",
                                         "lapse", "from_sec", "to_sec",
                                         "entity", "from_node", "to_node",
                                         "path",
                                         "pathtype", "inttype",
                                         "color",
                                         # threadins specific
                                         "host", "component", "target"
                                         ])

    ## general intervals
    intervals_df = pd.concat((join_intervals_df, threadints_df), join="inner")
    lock_intervals_df = intervals_df[intervals_df["pathtype"]=="lock"]
    all_intervals_df = intervals_df[intervals_df["pathtype"]!="lock"]
    main_intervals_df = intervals_df[intervals_df["pathtype"]=="main"]

    def _get_type(int_):
        if isinstance(int_, ThreadInterval):
            return str(int_.component)
        else:
            assert isinstance(int_, JoinIntervalBase)
            if int_.is_remote:
                return "remote_join"
            else:
                return "local_join"
    print("----------------------")

    if d_engine:
        assert isinstance(d_engine, DrawEngine)

        # d_engine.draw_threadobj(targetobjs_by_target["osd.1"]
        #         .threadobjs_list[0])
        # d_engine.draw_threadobj(targetobjs_by_target["osd.1"]
        #         .threadobjs_list[5])
        # d_engine.draw_threadobj(targetobjs_by_target["osd.1"]
        #         .threadobjs_list[2])
        # d_engine.draw_target(targetobjs_by_target["osd.1"])

        d_engine.draw_relation_heatmap(host_relations_df, "host_relations")
        d_engine.draw_relation_heatmap(ca_local_relations_df, "component_local_relations", "f")
        d_engine.draw_relation_heatmap(ca_relations_df, "component_remote_relations", "f")
        if len(requestinss) > 1:
            d_engine.draw_distplot(rqs_df["lapse"], "request_with_lapse")
            d_engine.draw_countplot(rqs_df["paces"], "request_with_paces")
            d_engine.draw_countplot(rqs_df["hosts"], "request_with_hosts")
            d_engine.draw_countplot(rqs_df["threadinss"], "request_with_threads")
        d_engine.draw_requestins(longest_lapse_requestins,
                "longest_lapse_requestins", start_end)
        d_engine.draw_requestins(longest_paces_requestins,
                "longest_paces_requestins", start_end)
        d_engine.draw_boxplot(join_intervals_df, "join_intervals",
                              x="path", y="lapse",
                              hue="join_type", color_column="color_jt")
        d_engine.draw_violinplot(join_intervals_df, "join_intervals",
                                 x="path", y="lapse",
                                 hue="join_type", color_column="color_jt")
        ordered_x = threadints_df.groupby("path")["lapse"]\
                    .median()
        ordered_x.sort(ascending=False)
        ordered_x = ordered_x.keys()
        lim = min(len(ordered_x), 5)
        for i in range(lim):
            d_engine.draw_boxplot(threadints_df[threadints_df["path"]==ordered_x[i]],
                                  "thread_interval_%s_by_host" % ordered_x[i],
                                  x="host", y="lapse")
        d_engine.draw_boxplot(threadints_df, "thread_intervals",
                              x="path", y="lapse")
        d_engine.draw_boxplot(lock_intervals_df, "lock_intervals",
                              x="path", y="lapse")
        d_engine.draw_violinplot(lock_intervals_df, "lock_intervals",
                              x="path", y="lapse")
        d_engine.draw_boxplot(all_intervals_df, "all_intervals",
                              x="path", y="lapse",
                              nth=20)
        d_engine.draw_violinplot(all_intervals_df, "all_intervals",
                                 x="path", y="lapse",
                                 nth=20)
        d_engine.draw_boxplot(main_intervals_df, "main_intervals",
                              x="path", y="lapse",
                              nth=20)
        d_engine.draw_violinplot(main_intervals_df, "main_intervals",
                                 x="path", y="lapse",
                                 nth=20)
        d_engine.draw_stacked_intervals(start_end, requestinss, main_intervals_df, "stacked_main_paths")
        """
