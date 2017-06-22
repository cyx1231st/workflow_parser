from __future__ import print_function

from collections import defaultdict
from itertools import chain
from itertools import izip_longest
import pandas as pd
import numpy as np

from ..graph import MasterGraph
from ..workflow.entities.join import InnerjoinInterval
from ..workflow.entities.join import InterfaceInterval
from ..workflow.entities.join import InterfacejoinInterval
from ..workflow.entities.join import NestedrequestInterval
from .draw_engine import DrawEngine
from .statistic_helper import projection_time
from .statistic_helper import Workflow
from .draw_engine import (REMOTE_C,
                          LOCALREMOTE_C,
                          LOCAL_C,
                          INTERFACE_C,
                          NESTED_C,
                          getcolor_byint)
from . import Report


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


def general_purpose_analysis(master_graph,
                             requestinss_by_request, requestinss_byrtype,
                             targetobjs_by_target, start_end,
                             join_intervals_df, td_intervals_df,
                             extendedints_df, request_df, targets_df,
                             d_engine, report):
    assert isinstance(master_graph, MasterGraph)
    assert isinstance(requestinss_by_request, dict)
    assert isinstance(requestinss_byrtype, dict)
    assert isinstance(targetobjs_by_target, dict)
    assert isinstance(start_end, dict)
    assert isinstance(join_intervals_df, pd.DataFrame)
    assert isinstance(td_intervals_df, pd.DataFrame)
    assert isinstance(extendedints_df, pd.DataFrame)
    assert isinstance(request_df, pd.DataFrame)
    assert isinstance(targets_df, pd.DataFrame)
    assert isinstance(d_engine, DrawEngine)
    assert isinstance(report, Report)

    r_types = master_graph.request_types
    comps = master_graph.components

    #####  recorded statistics  #####
    targetobjs_by_component = defaultdict(list)
    for t in targetobjs_by_target.itervalues():
        targetobjs_by_component[t.component].append(t)
    for comp in comps:
        report.register("targets %s" % comp,
                len(targetobjs_by_component.get(comp, [])))

    report.register("hosts",
            len(set(t.host for t in targetobjs_by_target.itervalues())))

    for comp in comps:
        report.register("hosts %s" % comp,
                len(set(t.host for t in
                        targetobjs_by_component.get(comp, []))))

    report.register("threads",
            sum(len(t.thread_objs) for t in targetobjs_by_target.itervalues()))

    for comp in comps:
        report.register("threads %s dist" % comp,
                f_dist([len(t.thread_objs) for t in
                        targetobjs_by_component.get(comp, [])]))

    report.register("threadinss",
            sum(len(r.threadinss) for r in requestinss_by_request.itervalues()))

    report.register("paces",
            sum(r.len_paces for r in requestinss_by_request.itervalues()))

    lapse_total = start_end["seconds"]["end"] - start_end["seconds"]["start"]
    report.register("lapse", lapse_total)

    report.blank()

    for r_type in r_types:
        report.register("%s reqs" % r_type,
                len(requestinss_byrtype.get(r_type, [])))

        report.register("%s targets dist" % r_type,
                f_dist([len(r.target_objs) for r in
                        requestinss_byrtype.get(r_type, [])]))

        report.register("%s hosts dist" % r_type,
                f_dist([len(r.hosts) for r in
                        requestinss_byrtype.get(r_type, [])]))

        report.register("%s threads dist" % r_type,
                f_dist([len(r.thread_objs) for r in
                        requestinss_byrtype.get(r_type, [])]))

        report.register("%s threadinss dist" % r_type,
                f_dist([len(r.threadinss) for r in
                        requestinss_byrtype.get(r_type, [])]))

        report.register("%s paces dist" % r_type,
                f_dist([r.len_paces for r in
                        requestinss_byrtype.get(r_type, [])]))
        report.blank()

    innerjoinints_df = join_intervals_df\
            .loc[join_intervals_df["int_type"] == InnerjoinInterval.__name__]
    interfacejints_df = join_intervals_df\
            .loc[join_intervals_df["int_type"] == InterfacejoinInterval.__name__]
    nestedreqints_df = join_intervals_df\
            .loc[join_intervals_df["int_type"] == NestedrequestInterval.__name__]
    mainjoins_df = join_intervals_df[join_intervals_df["is_main"] == True]
    nested_index = mainjoins_df["int_type"]==NestedrequestInterval.__name__
    mainnested_df = mainjoins_df[nested_index]
    mainjoins_df = mainjoins_df[~nested_index]
    mainlocaljoins_df = mainjoins_df[mainjoins_df["remote_type"]=="local"]
    mainremoteljoins_df = mainjoins_df[mainjoins_df["remote_type"]=="local_remote"]
    mainremotejoins_df = mainjoins_df[mainjoins_df["remote_type"]=="remote"]
    for r_type in r_types:
        report.register("%s td_ints" % r_type,
                td_intervals_df["request_type"].value_counts().get(r_type, 0))

        report.register("%s innerjoin_ints" % r_type,
                innerjoinints_df["request_type"].value_counts().get(r_type, 0))

        report.register("%s interfacejoin_ints" % r_type,
                interfacejints_df["request_type"].value_counts().get(r_type, 0))

        report.register("%s nestedreq_ints" % r_type,
                nestedreqints_df["request_type"].value_counts().get(r_type, 0))

        report.register("%s lapse dist" % r_type,
                "100.00% " +
                f_dist([r.lapse for r in
                        requestinss_byrtype.get(r_type, [])], 5))

        cumulated = sum(r.lapse for r in requestinss_byrtype.get(r_type, []))

        comp_df = extendedints_df[extendedints_df["request_type"] == r_type]
        for comp in comps:
            _df = comp_df[comp_df["path"] == str(comp)]["lapse"]
            report.register("%s %s dist:" % (r_type, comp),
                    ("%6.2f%% " % (_df.sum()/cumulated*100))
                    + f_dist([v for v in _df], 9))

        _df = mainlocaljoins_df[mainlocaljoins_df["request_type"]==r_type]["lapse"]
        report.register("%s local" % r_type,
                ("%6.2f%% " % (_df.sum()/cumulated*100))
                + f_dist([v for v in _df], 9))

        _df = mainremoteljoins_df[mainremoteljoins_df["request_type"]==r_type]["lapse"]
        report.register("%s remote_l" % r_type,
                ("%6.2f%% " % (_df.sum()/cumulated*100))
                + f_dist([v for v in _df], 9))

        _df = mainremotejoins_df[mainremotejoins_df["request_type"]==r_type]["lapse"]
        report.register("%s remote_r" % r_type,
                ("%6.2f%% " % (_df.sum()/cumulated*100))
                + f_dist([v for v in _df], 9))

        _df = mainnested_df[mainnested_df["request_type"]==r_type]["lapse"]
        report.register("%s nest" % r_type,
                ("%6.2f%% " % (_df.sum()/cumulated*100))
                + f_dist([v for v in _df], 9))

        report.blank()

    #####  printed statistics  #####
    report_i = Report("Intervals")

    addedup_total = extendedints_df["lapse"].sum()
    report_i.register("Addedup total", "%.5f %.2f(rate)" % (
        addedup_total, addedup_total/lapse_total))
    addedup_df = extendedints_df\
                 .groupby("path_type")["lapse"]\
                 .sum()\
                 .sort_values(ascending=False)
    addedup_by_ptype = {}
    for name, lapse in addedup_df.iteritems():
        addedup_by_ptype[name] = lapse
        report_i.register("added %s" % name, "%.5f %.3f%%" % (
            lapse, lapse/addedup_total*100))

    report_i.register("Average", None)
    average_df = extendedints_df\
                 .groupby("path_type")["lapse"]\
                 .mean()\
                 .sort_values(ascending=False)
    average_by_ptype = {}
    for name, lapse in average_df.iteritems():
        average_by_ptype[name] = lapse
        report_i.register("avg %s" % name, "%.5f" % lapse)

    report_i.register("Projection", None)
    extendedints_by_type = extendedints_df.groupby("path_type")
    projection_result = []
    for p_type, e_df in extendedints_by_type:
        from_tos = [(r[1]["from_seconds"],
                     r[1]["to_seconds"])
                    for r in e_df.iterrows()]
        p_t = projection_time(from_tos)
        concurrent_ratio = addedup_by_ptype[p_type]/p_t
        projection_result.append((concurrent_ratio, p_t, p_type))
    projection_result.sort(reverse=True)
    for proj_ratio, proj_t, p_type in projection_result:
        report_i.register("proj %s" % p_type,
                          "%.5f %.2f" % (proj_t, proj_ratio))

    report_i.export()

    workflow_byrtype = {}
    for r_type in r_types:
        _requestinss = requestinss_byrtype.get(r_type, [])
        requestins_iters = [iter(r.extended_ints) for r in _requestinss]
        workflow = Workflow(r_type)
        for intervals in izip_longest(*requestins_iters):
            workflow.build(intervals)
        workflow_byrtype[r_type] = workflow
        print()
        print("%s" % workflow)

    #####  visualization  #####
    if d_engine:
        alljoins_df = join_intervals_df[
                join_intervals_df["int_type"]!=\
                        NestedrequestInterval.__name__]
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

        if len(requestinss_by_request) > 1:
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
        d_engine.draw_boxplot(alljoins_df, "joinintervals_lapse_bytype",
                "path_name", "lapse", hue="remote_type", palette=palette)
        d_engine.draw_violinplot(alljoins_df, "joinintervals_lapse_bytype",
                "path_name", "lapse", hue="remote_type", palette=palette)

        # 11. top 5 slowest thread intervals by host boxplot
        ordered_x = td_intervals_df.groupby("path_name")["lapse"].median()
        ordered_x.sort(ascending=False)
        ordered_x = ordered_x.keys()
        lim = min(len(ordered_x), 5)
        for i in range(lim):
            to_draw = td_intervals_df[td_intervals_df["path_name"]==ordered_x[i]]
            d_engine.draw_boxplot(to_draw, "tdint_%s_byhost" % ordered_x[i],
                                  x="host", y="lapse",
                                  color=to_draw.iloc[0]["component"].color)

        palette_path = {}
        for r in chain(td_intervals_df.iterrows(),
                       join_intervals_df.iterrows()):
            r = r[1]
            palette_path[r["path_name"]] = getcolor_byint(r["_entity"],
                    ignore_lr=True)
        # 12. thread intervals lapse by path box/violinplot
        d_engine.draw_boxplot(td_intervals_df, "tdints_lapse_bypath",
                "path_name", "lapse", palette=palette_path)
        d_engine.draw_violinplot(td_intervals_df, "tdints_lapse_bypath",
                "path_name", "lapse", palette=palette_path)

        # 13. all intervals lapse by path box/violinplot
        all_intervals_df = pd.concat([td_intervals_df, join_intervals_df],
                                     ignore_index=True)
        d_engine.draw_boxplot(all_intervals_df, "allints_lapse_bypath",
                              "path_name", "lapse", palette=palette_path, nth=20)
        d_engine.draw_violinplot(all_intervals_df, "allints_lapse_bypath",
                                 "path_name", "lapse", palette=palette_path, nth=20)

        # 14. main intervals lapse by path box/violinplot
        main_intervals_df = all_intervals_df[all_intervals_df["is_main"]==True]
        d_engine.draw_boxplot(main_intervals_df, "mainints_lapse_bypath",
                              "path_name", "lapse", palette=palette_path)
        d_engine.draw_violinplot(main_intervals_df, "mainints_lapse_bypath",
                              "path_name", "lapse", palette=palette_path)

        for r_type in requestinss_byrtype.keys():
            # 15. workflow plot
            d_engine.draw_workflow(start_end,
                    workflow_byrtype[r_type],
                    r_type+"_workflow")
            # 16. stacked intervals plot
            d_engine.draw_profiling(start_end,
                    requestinss_byrtype[r_type],
                    r_type+"_profiling")
