from __future__ import print_function

from collections import OrderedDict
import pandas as pd
import numpy as np

from workflow_parser.draw_engine import DrawEngine
from workflow_parser.log_engine import TargetsCollector
from workflow_parser.state_engine import PacesCollector
from workflow_parser.state_engine import ThreadInssCollector
from workflow_parser.state_engine import RequestsCollector
from workflow_parser.state_machine import JoinInterval
from workflow_parser.state_machine import ThreadInterval


def do_statistics(tgs, pcs, tis, rqs, d_engine):
    assert isinstance(tgs, TargetsCollector)
    assert isinstance(pcs, PacesCollector)
    assert isinstance(tis, ThreadInssCollector)
    assert isinstance(rqs, RequestsCollector)
    assert isinstance(d_engine, DrawEngine)

    print("(Statistics) preparing relations...")

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
                                              "pathtype", "inttype",
                                              "color",
                                              # join specific
                                              "color_jt",
                                              "from_target", "to_target",
                                              "join_type"])

    # full relations
    relations_df = join_intervals_df.join(targets_df, on="from_target")\
            .join(targets_df, on="to_target", rsuffix="1")

    # host relations
    host_relations_df = relations_df.groupby(["host", "host1"])\
                .size()\
                .unstack()\
                .fillna(0)\
                .astype("int")
    host_relations_df.index.name = "from_host"
    host_relations_df.columns.name = "to_host"

    # component relations
    component_relations_df = relations_df.groupby(["component", "component1"])\
                .size()\
                .unstack()\
                .fillna(0)\
                .astype("int")
    component_relations_df.index.name = "from_component"
    component_relations_df.columns.name = "to_component"

    # component average relations
    ca_relations_df = relations_df.groupby(["from_target", "to_target"])\
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
                                   int_.path_type, int_.int_type,
                                   int_.color,
                                   int_.host, str(int_.component), int_.target)
                                 for int_ in pcs.thread_intervals),
                                columns=["request",
                                         "lapse", "from_sec", "to_sec",
                                         "entity", "from_node", "to_node",
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
    print("ok")

    d_engine.draw_relation_heatmap(host_relations_df, "host_relations")
    d_engine.draw_relation_heatmap(component_relations_df, "component_relations")
    d_engine.draw_relation_heatmap(ca_relations_df, "component_mean_relations", "f")
    d_engine.draw_distplot(rqs_df["lapse"], "requestlapse")
    d_engine.draw_countplot(rqs_df["paces"], "requestpaces")
    d_engine.draw_countplot(rqs_df["hosts"], "requesthosts")
    d_engine.draw_countplot(rqs_df["threadinss"], "requesttis")
    d_engine.draw_requestins(longest_lapse_requestins, "longest_lapse_requestins")
    d_engine.draw_requestins(longest_paces_requestins, "longest_paces_requestins")
    d_engine.draw_boxplot(join_intervals_df, "join_intervals",
                          x="entity", y="lapse",
                          hue="join_type", color_column="color_jt")
    d_engine.draw_violinplot(join_intervals_df, "join_intervals",
                             x="entity", y="lapse",
                             hue="join_type", color_column="color_jt")
    ordered_x = threadints_df.groupby("entity")["lapse"]\
                .median()
    ordered_x.sort(ascending=False)
    ordered_x = ordered_x.keys()
    lim = min(len(ordered_x), 5)
    for i in range(lim):
        d_engine.draw_boxplot(threadints_df[threadints_df["entity"]==ordered_x[i]],
                              "intervals_%s_by_host" % ordered_x[i],
                              x="host", y="lapse")
    d_engine.draw_boxplot(lock_intervals_df, "lock_intervals",
                          x="entity", y="lapse")
    d_engine.draw_violinplot(lock_intervals_df, "lock_intervals",
                          x="entity", y="lapse")
    d_engine.draw_boxplot(all_intervals_df, "all_intervals",
                          x="entity", y="lapse")
    d_engine.draw_violinplot(all_intervals_df, "all_intervals",
                             x="entity", y="lapse")
    d_engine.draw_boxplot(main_intervals_df, "main_intervals",
                          x="entity", y="lapse")
    d_engine.draw_violinplot(main_intervals_df, "main_intervals",
                             x="entity", y="lapse")
    d_engine.draw_stacked_intervals(extendedints_df, main_intervals_df, "stacked_main_paths")
