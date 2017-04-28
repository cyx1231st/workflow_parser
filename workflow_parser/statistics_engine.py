from __future__ import print_function

from collections import OrderedDict
import pandas as pd
import numpy as np

from workflow_parser.draw_engine import DrawEngine
from workflow_parser.log_engine import TargetsCollector
from workflow_parser.state_engine import StateEngine
from workflow_parser.state_machine import JoinInterval
from workflow_parser.state_machine import ThreadInterval


def do_statistics(tgs, s_engine, d_engine):
    assert isinstance(tgs, TargetsCollector)
    assert isinstance(s_engine, StateEngine)
    pcs = s_engine.pcs
    tis = s_engine.tis
    rqs = s_engine.rqs
    if d_engine:
        assert isinstance(d_engine, DrawEngine)

    print("Preparing relations...")

    if not rqs.requestinss:
        print("No requests available, abort!")
        return

    ## adjust offset
    first_req = None
    last_req = None
    for requestins in rqs.requestinss.itervalues():
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
    print("lapse: %.4f, (%.4f, %.4f), (%s, %s)" % (
            end_s - start_s,
            start_s,
            end_s,
            start_t,
            end_t))

    for tg in tgs.target_objs:
        tg.offset -= start_s
    end_s = end_s - start_s
    start_s = 0
    start_end = ((start_s, end_s), (start_t, end_t))

    ## join intervals
    relations = pcs.join_intervals
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
    requestinss = rqs.requestinss

    rqs_index = [ri for ri in requestinss]
    rqs_df = pd.DataFrame(((requestinss[rq], requestinss[rq].len_paces,
                            requestinss[rq].len_hosts, requestinss[rq].len_threadinss,
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
                                 for int_ in pcs.thread_intervals),
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
            assert isinstance(int_, JoinInterval)
            if int_.is_remote:
                return "remote_join"
            else:
                return "local_join"
    ## extended intervals
    extendedints_df = pd.DataFrame(((int_.requestins.request,
                                     _get_type(int_),
                                     int_.lapse,
                                     int_.from_seconds,
                                     int_.to_seconds,
                                     int_.from_time,
                                     int_.to_time,
                                     int_.color)
                                    for int_ in pcs.extended_intervals),
                                   columns=["request", "type",
                                            "lapse", "from_sec", "to_sec",
                                            "from_time", "to_time",
                                            "color"
                                           ])
    print("----------------------")

    if d_engine:
        d_engine.draw_threadobj(tgs.targetobjs_by_target["osd.1"].threadobjs_list[0])

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
        d_engine.draw_stacked_intervals(start_end, extendedints_df, main_intervals_df, "stacked_main_paths")
