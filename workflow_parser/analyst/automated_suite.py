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
import numpy as np

from ..graph import Master
from ..workflow.entities.join import CrossjoinActivity
from ..workflow.entities.join import InnerjoinActivity
from ..workflow.entities.join import RequestjoinActivity
from .draw_engine import DrawEngine
from .statistic_helper import projection_time
from .statistics_engine import generate_dataframes
from .draw_engine import (REMOTE_C,
                          LOCALREMOTE_C,
                          LOCAL_C,
                          INTERFACE_C,
                          NESTED_C,
                          getcolor_byint)
from .report import Report


def f_dist(iterable, lim=None):
    i_list = list(iterable)
    if not i_list:
        return "=0"
    i_min = np.min(i_list)
    i_max = np.max(i_list)
    if i_min == i_max:
        return "=%s" % i_min

    i_25, i_50, i_75 = np.percentile(i_list, [25, 50, 75])

    if lim is not None:
        i_min = round(i_min, lim)
        i_max = round(i_max, lim)
        i_25 = round(i_25, lim)
        i_50 = round(i_50, lim)
        i_75 = round(i_75, lim)

    return "%s[%s|%s|%s]%s" % (i_min, i_25, i_50, i_75, i_max)


def generate_reports(name, master_graph,
                     requestinss_byrtype,
                     targetobjs_by_target,
                     workflow_byrtype,
                     start_end,
                     join_intervals_df, td_intervals_df,
                     request_df, targets_df):
    assert isinstance(name, str)
    assert isinstance(master_graph, Master)

    #####  request statistics  #####
    report_r = Report(name)
    r_types = master_graph.request_types
    comps = master_graph.components

    targetobjs_by_component = defaultdict(list)
    for t in targetobjs_by_target.values():
        targetobjs_by_component[t.component].append(t)
    for comp in comps:
        report_r.register("targets %s" % comp,
                len(targetobjs_by_component.get(comp, [])))

    report_r.register("hosts",
            len(set(t.host for t in targetobjs_by_target.values())))

    for comp in comps:
        report_r.register("hosts %s" % comp,
                len(set(t.host for t in
                        targetobjs_by_component.get(comp, []))))

    report_r.register("threads",
            sum(len(t.thread_objs) for t in targets_df._entity))

    for comp in comps:
        report_r.register("threads %s dist" % comp,
                f_dist([len(t.thread_objs) for t in
                        targetobjs_by_component.get(comp, [])]))

    report_r.register("threadinss",
            sum(len(r.threadinss) for r in request_df._entity))

    report_r.register("paces",
            sum(r.len_paces for r in request_df._entity))

    lapse_total = start_end["seconds"]["end"] - start_end["seconds"]["start"]
    report_r.register("lapse", lapse_total)

    report_r.blank()

    for r_type in r_types:
        report_r.register("%s reqs" % r_type,
                len(requestinss_byrtype.get(r_type, [])))

        report_r.register("%s targets dist" % r_type,
                f_dist([len(r.target_objs) for r in
                        requestinss_byrtype.get(r_type, [])]))

        report_r.register("%s hosts dist" % r_type,
                f_dist([len(r.hosts) for r in
                        requestinss_byrtype.get(r_type, [])]))

        report_r.register("%s threads dist" % r_type,
                f_dist([len(r.thread_objs) for r in
                        requestinss_byrtype.get(r_type, [])]))

        report_r.register("%s threadinss dist" % r_type,
                f_dist([len(r.threadinss) for r in
                        requestinss_byrtype.get(r_type, [])]))

        report_r.register("%s paces dist" % r_type,
                f_dist([r.len_paces for r in
                        requestinss_byrtype.get(r_type, [])]))
        report_r.blank()

    innerjoinints_df = join_intervals_df\
            .loc[join_intervals_df["int_type"] == InnerjoinActivity.__name__]
    crossjoinints_df = join_intervals_df\
            .loc[join_intervals_df["int_type"] == CrossjoinActivity.__name__]
    reqjoinints_df = join_intervals_df\
            .loc[join_intervals_df["int_type"] == RequestjoinActivity.__name__]
    assert len(innerjoinints_df) + len(crossjoinints_df) + len(reqjoinints_df) == len(join_intervals_df)

    mainjoins_df = join_intervals_df[join_intervals_df["is_main"] == True]

    _nested_index = (mainjoins_df["int_type"] == RequestjoinActivity.__name__)
    mainnested_df = mainjoins_df[_nested_index]
    mainjoins_df = mainjoins_df[~_nested_index]

    mainlocaljoins_df = mainjoins_df[mainjoins_df["remote_type"]=="local"]
    mainremoteljoins_df = mainjoins_df[mainjoins_df["remote_type"]=="local_remote"]
    mainremotejoins_df = mainjoins_df[mainjoins_df["remote_type"]=="remote"]

    for r_type in r_types:
        report_r.register("%s td_ints" % r_type,
                td_intervals_df["request_type"].value_counts().get(r_type, 0))

        report_r.register("%s innerjoin_ints" % r_type,
                innerjoinints_df["request_type"].value_counts().get(r_type, 0))

        report_r.register("%s crossjoin_ints" % r_type,
                crossjoinints_df["request_type"].value_counts().get(r_type, 0))

        report_r.register("%s requestjoin_ints" % r_type,
                reqjoinints_df["request_type"].value_counts().get(r_type, 0))

        report_r.register("%s lapse dist" % r_type,
                "100.00% " +
                f_dist([r.lapse for r in
                        requestinss_byrtype.get(r_type, [])], 5))

        cumulated = sum(r.lapse for r in requestinss_byrtype.get(r_type, []))

        maintdints_df = td_intervals_df[td_intervals_df["is_main"]==True]
        for comp in comps:
            _df = maintdints_df[maintdints_df["component"] == comp]["lapse"]
            report_r.register("%s %s dist:" % (r_type, comp),
                    ("%6.2f%% " % (_df.sum()/cumulated*100))
                    + f_dist([v for v in _df], 9))

        _df = mainlocaljoins_df[mainlocaljoins_df["request_type"]==r_type]["lapse"]
        report_r.register("%s local" % r_type,
                ("%6.2f%% " % (_df.sum()/cumulated*100))
                + f_dist([v for v in _df], 9))

        _df = mainremoteljoins_df[mainremoteljoins_df["request_type"]==r_type]["lapse"]
        report_r.register("%s remote_l" % r_type,
                ("%6.2f%% " % (_df.sum()/cumulated*100))
                + f_dist([v for v in _df], 9))

        _df = mainremotejoins_df[mainremotejoins_df["request_type"]==r_type]["lapse"]
        report_r.register("%s remote_r" % r_type,
                ("%6.2f%% " % (_df.sum()/cumulated*100))
                + f_dist([v for v in _df], 9))

        _df = mainnested_df[mainnested_df["request_type"]==r_type]["lapse"]
        report_r.register("%s nest" % r_type,
                ("%6.2f%% " % (_df.sum()/cumulated*100))
                + f_dist([v for v in _df], 9))

    #####  interval statistics  #####
    report_i = Report("Intervals")

    all_intervals_df = pd.concat([td_intervals_df, join_intervals_df],
                                 join="inner", ignore_index=True)
    main_intervals_df = all_intervals_df[all_intervals_df["is_main"]==True]
    intsbypath_df = main_intervals_df.groupby("path")
    desc_bypath = {}
    for p_type, e_df in intsbypath_df:
        if len(e_df):
            desc = "%s -> %s" % (
                    e_df["from_keyword"].iloc[0],
                    e_df["to_keyword"].iloc[0])
            desc_bypath[p_type] = desc

    addedup_total = main_intervals_df["lapse"].sum()
    report_i.register("Addedup total", "%.5f %.2f(rate)" % (
        addedup_total, addedup_total/lapse_total))
    addedup_df = intsbypath_df["lapse"]\
                 .sum()\
                 .sort_values(ascending=False)
    addedup_by_ptype = {}
    for name, lapse in addedup_df.items():
        addedup_by_ptype[name] = lapse
        report_i.register("added %s" % name, "%.5f %.3f%% %s" %
            (lapse, lapse/addedup_total*100, desc_bypath[name]))

    report_i.register("Average", None)
    average_df = intsbypath_df["lapse"]\
                 .mean()\
                 .sort_values(ascending=False)
    average_by_ptype = {}
    for name, lapse in average_df.items():
        average_by_ptype[name] = lapse
        report_i.register(
                "avg %s" % name, "%.5f %s" % (
                    lapse, desc_bypath[name]))

    report_i.register("Projection", None)
    projection_result = []
    for p_type, e_df in intsbypath_df:
        from_tos = [(r[1]["from_seconds"],
                     r[1]["to_seconds"])
                    for r in e_df.iterrows()]
        p_t = projection_time(from_tos)
        concurrent_ratio = addedup_by_ptype[p_type]/p_t
        projection_result.append((concurrent_ratio, p_t, p_type))
    projection_result.sort(reverse=True)
    for proj_ratio, proj_t, p_type in projection_result:
        report_i.register("proj %s" % p_type,
                          "%.5f %.2f %s" % (proj_t, proj_ratio, desc_bypath[p_type]))

    return report_r, report_i


def do_statistics(name, master_graph, requestinss, d_engine, out_file):
    if not requestinss:
        print("No requests available, abort!")
        return

    print("Preparing dataframes...")
    ret = generate_dataframes(requestinss)

    print("Generate reports...")
    report_r, report_i = generate_reports(
            name, master_graph, *ret)
    if out_file:
        report_r.set_outfile(out_file, True)
    print()
    report_r.export()
    print()
    report_i.export()
    for r_type, workflow in ret[2].items():
        print()
        print("%s" % workflow)

    #####  visualization  #####
    if d_engine:
        alljoins_df = join_intervals_df[join_intervals_df["int_type"]!=RequestjoinActivity.__name__]
        # 1. host remote relations heatmap
        crosshost_relations_df = alljoins_df\
                .loc[alljoins_df["remote_type"] != "local"]\
                .groupby(["from_host", "to_host"])\
                .size()\
                .unstack()\
                .fillna(0)\
                .astype("int")
        crosshost_relations_df.index.name = "from_host"
        crosshost_relations_df.columns.name = "to_host"
        d_engine.draw_relation_heatmap(crosshost_relations_df,
                "host_remote_relations")

        # 2. component local relations per target heatmap
        componentlocal_relations_df = alljoins_df\
                [alljoins_df["remote_type"]=="local"]\
                .groupby(["from_target", "to_target"])\
                .size()\
                .reset_index()\
                .join(targets_df, on="from_target")\
                .join(targets_df, on="to_target",
                      lsuffix="_from", rsuffix="_to")\
                .groupby(["component_from", "component_to"])\
                .mean()\
                .unstack()
        componentlocal_relations_df.index.name = "from_component"
        componentlocal_relations_df.columns =\
                componentlocal_relations_df.columns.droplevel()
        componentlocal_relations_df.columns.name = "to_component"
        d_engine.draw_relation_heatmap(componentlocal_relations_df,
                "component_localrelations_pertarget", "f")

        # 3. component remote relations per target heatmap
        componentremote_relations_df = alljoins_df\
                [alljoins_df["remote_type"]!="locol"]\
                .groupby(["from_target", "to_target"])\
                .size()\
                .reset_index()\
                .join(targets_df, on="from_target")\
                .join(targets_df, on="to_target",
                        lsuffix="_from", rsuffix="_to")\
                .groupby(["component_from", "component_to"])\
                .mean()\
                .unstack()
        componentremote_relations_df.index.name = "from_component"
        componentremote_relations_df.columns =\
                componentremote_relations_df.columns.droplevel()
        componentremote_relations_df.columns.name = "to_component"
        d_engine.draw_relation_heatmap(componentremote_relations_df,
                "component_remoterelations_pertarget", "f")

        if len(request_df) > 1:
            # 4. lapse distplot
            d_engine.draw_distplot(request_df["lapse"],
                    "request_lapse")

            # 5. paces distplot
            d_engine.draw_distplot(request_df["len_paces"],
                    "request_paces")

            # 6. hosts distplot
            d_engine.draw_distplot(request_df["len_hosts"],
                    "request_hosts")

            # 7. threadinss distplot
            d_engine.draw_distplot(request_df["len_threadinss"],
                    "request_threadinss")

        # 8. longest lapse requestins plot
        longestlapse_req = request_df\
                .loc[request_df["lapse"].idxmax()]\
                ["_entity"]
        d_engine.draw_requestins(longestlapse_req,
                "longest_lapse_requestins", start_end)

        # 9. longest paces requestins plot
        longestpaces_req = request_df\
                .loc[request_df["len_paces"].idxmax()]\
                ["_entity"]
        d_engine.draw_requestins(longestpaces_req,
                "longest_paces_requestins", start_end)

        palette = {"remote": REMOTE_C,
                   "local_remote": LOCALREMOTE_C,
                   "local": LOCAL_C}
        # 10. join intervals lapse by remote type box/violinplot
        if not len(alljoins_df):
            print("No joins to print")
        else:
            d_engine.draw_boxplot(alljoins_df, "joinintervals_lapse_bytype",
                    "path", "lapse", hue="remote_type", palette=palette)
            d_engine.draw_violinplot(alljoins_df, "joinintervals_lapse_bytype",
                    "path", "lapse", hue="remote_type", palette=palette)

        # 11. top 5 slowest thread intervals by host boxplot
        ordered_x = td_intervals_df.groupby("path")["lapse"].median()
        ordered_x.sort_values(ascending=False, inplace=True)
        ordered_x = ordered_x.keys()
        lim = min(len(ordered_x), 5)
        for i in range(lim):
            to_draw = td_intervals_df[td_intervals_df["path"]==ordered_x[i]]
            d_engine.draw_boxplot(to_draw, "tdint_%s_byhost" % ordered_x[i],
                                  x="host", y="lapse",
                                  color=to_draw.iloc[0]["component"].color)

        palette_path = {}
        for r in chain(td_intervals_df.iterrows(),
                       join_intervals_df.iterrows()):
            r = r[1]
            palette_path[r["path"]] = getcolor_byint(r["_entity"],
                    ignore_lr=True)
        # 12. thread intervals lapse by path box/violinplot
        d_engine.draw_boxplot(td_intervals_df, "tdints_lapse_bypath",
                "path", "lapse", palette=palette_path)
        d_engine.draw_violinplot(td_intervals_df, "tdints_lapse_bypath",
                "path", "lapse", palette=palette_path)

        # 13. all intervals lapse by path box/violinplot
        d_engine.draw_boxplot(all_intervals_df, "allints_lapse_bypath",
                              "path", "lapse", palette=palette_path, nth=20)
        d_engine.draw_violinplot(all_intervals_df, "allints_lapse_bypath",
                                 "path", "lapse", palette=palette_path, nth=20)

        # 14. main intervals lapse by path box/violinplot
        d_engine.draw_boxplot(main_intervals_df, "mainints_lapse_bypath",
                              "path", "lapse", palette=palette_path)
        d_engine.draw_violinplot(main_intervals_df, "mainints_lapse_bypath",
                              "path", "lapse", palette=palette_path)

        for r_type in requestinss_byrtype.keys():
            # 15. workflow plot
            d_engine.draw_workflow(start_end,
                    workflow_byrtype[r_type],
                    r_type)
            # 16. stacked intervals plot
            d_engine.draw_profiling(start_end,
                    requestinss_byrtype[r_type],
                    r_type)
