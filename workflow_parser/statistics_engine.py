from __future__ import print_function

from collections import OrderedDict
import pandas as pd
import numpy as np

from workflow_parser.draw_engine import DrawEngine
from workflow_parser.log_engine import TargetsCollector
from workflow_parser.state_engine import PacesCollector
from workflow_parser.state_engine import ThreadInssCollector
from workflow_parser.state_engine import RequestsCollector


def do_statistics(tgs, pcs, tis, rqs, d_engine):
    assert isinstance(tgs, TargetsCollector)
    assert isinstance(pcs, PacesCollector)
    assert isinstance(tis, ThreadInssCollector)
    assert isinstance(rqs, RequestsCollector)
    assert isinstance(d_engine, DrawEngine)

    print("(Statistics) preparing relations...")
    ## relations
    relations = pcs.join_intervals
    t_dict = {relation.from_target:(str(relation.from_component), relation.from_host)
              for relation in relations}
    t_dict.update({relation.to_target:(str(relation.to_component), relation.to_host)
                  for relation in relations})
    t_index = [k for k in t_dict.iterkeys()]
    t_values = ((t_dict[k][0], t_dict[k][1]) for k in t_index)

    t_df = pd.DataFrame(t_values,
                        index=t_index,
                        columns=["component", "host"])
    r_df = pd.DataFrame(((relation.from_target,
                          relation.to_target,
                          relation.join_type,
                          relation.to_seconds-relation.from_seconds,
                          "%s(%s)%s" % (relation.from_node.name,
                                        relation.join_obj.name,
                                        relation.to_node.name))
                         for relation in relations),
                        columns=["from_target", "to_target",
                                 "join_type", "lapse",
                                 "join_nodes"])
    r1_df = r_df.join(t_df, on="from_target")\
                .join(t_df, on="to_target", rsuffix="1")
    # 1
    h_df = r1_df.groupby(["host", "host1"])\
                .size()\
                .unstack()\
                .fillna(0)\
                .astype("int")
    h_df.index.name = "from_host"
    h_df.columns.name = "to_host"
    # 2
    c_df = r1_df.groupby(["component", "component1"])\
                .size()\
                .unstack()\
                .fillna(0)\
                .astype("int")
    c_df.index.name = "from_component"
    c_df.columns.name = "to_component"
    # 3
    ca_df = r1_df.groupby(["from_target", "to_target"])\
                 .size()\
                 .reset_index()\
                 .join(t_df, on="from_target")\
                 .join(t_df, on="to_target", rsuffix="1")\
                 .groupby(["component", "component1"])\
                 .mean()\
                 .unstack()
    ca_df.index.name = "from_component"
    ca_df.columns = ca_df.columns.droplevel()
    ca_df.columns.name = "to_component"
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
    print("ok")

    # d_engine.draw_relation_heatmap(h_df, "host_relations")
    # d_engine.draw_relation_heatmap(c_df, "component_relations")
    # d_engine.draw_relation_heatmap(ca_df, "component_mean_relations", "f")
    # d_engine.draw_boxplot(r_df, "lapse_nodes_jtype",
    #                       x="join_nodes", y="lapse",
    #                       hue="join_type")
    # d_engine.draw_violinplot(r_df, "lapse_nodes_jtype_scaled",
    #                          x="join_nodes", y="lapse",
    #                          hue="join_type")
    # d_engine.draw_distplot(rqs_df["lapse"], "requestlapse")
    # d_engine.draw_countplot(rqs_df["paces"], "requestpaces")
    # d_engine.draw_countplot(rqs_df["hosts"], "requesthosts")
    # d_engine.draw_countplot(rqs_df["threadinss"], "requesttis")
    d_engine.draw_requestins(longest_lapse_requestins, "longest lapse requestins")
    d_engine.draw_requestins(longest_paces_requestins, "longest paces requestins")

    # hosts = set()
    # components = set()
    # for relation in relations:
    #     hosts.add(relation.from_host)
    #     hosts.add(relation.to_host)
    #     components.add(str(relation.from_component))
    #     components.add(str(relation.to_component))
    # hosts = sorted(list(hosts))
    # components = sorted(list(components))
    # df = pd.DataFrame(0, index=hosts, columns=hosts)
    # df_c = pd.DataFrame(0, index=components, columns=components)
    # for relation in relations:
    #     df.ix[relation.from_host, relation.to_host] += 1
    #     df_c.ix[str(relation.from_component),
    #             str(relation.to_component)] += 1
    # print("ok")
    # d_engine.draw_relation_heatmap(df, "host_relations")
    # d_engine.draw_relation_heatmap(df_c, "component_relations")

