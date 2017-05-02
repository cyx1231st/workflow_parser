from __future__ import print_function

from collections import defaultdict
from collections import OrderedDict
import heapq
from matplotlib import patches as mpatches
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sys import maxint

from workflow_parser.state_machine import RequestInstance
from workflow_parser.state_machine import ThreadInterval
from workflow_parser.state_machine import JoinInterval
from workflow_parser.state_runtime import Target


def patch_violinplot():
    """Patch seaborn's violinplot in current axis to workaround
    matplotlib's bug ##5423."""
    from matplotlib.collections import PolyCollection
    ax = plt.gca()
    for art in ax.get_children():
        if isinstance(art, PolyCollection):
            art.set_linewidth(0)


class DrawEngine(object):
    def __init__(self, out_path):
        assert isinstance(out_path, str)

        self.out_path = out_path

    def draw_target(self, target_obj):
        assert isinstance(target_obj, Target)

        print("(DrawEngine) drawing %s..." % target_obj.target)
        df = pd.DataFrame(((to.lapse,
                            to.name)
                           for to in target_obj.thread_objs.itervalues()),
                          columns=["lapse", "thread"])

        fig = plt.figure(figsize=(15, len(target_obj.thread_objs)*.5+2))
        fig.clear()
        fig.suptitle("%s barplot" % target_obj, fontsize=14)

        sns.barplot(x="lapse", y="thread", data=df)

        fig.savefig(self.out_path + target_obj.target + "_targetfig.png")
        print("ok")


    def draw_relation_heatmap(self, relation_df, name, fmt="d"):
        assert isinstance(relation_df, pd.DataFrame)
        assert isinstance(name, str)

        print("(DrawEngine) drawing %s..." % name)
        relation_df = relation_df.ix[(relation_df.T!=0).any(),
                                     (relation_df!=0).any()]
        relation_df.reindex_axis(sorted(relation_df.columns), axis=1)
        relation_df.reindex_axis(sorted(relation_df.index))
        mask = relation_df[:].isin([0, np.NaN])
        l_col = len(relation_df.columns)
        l_row = len(relation_df)
        f_wid = l_col + 1
        f_hig = l_row + 1

        fig = plt.figure(figsize=(l_col/2+5, l_row/2+5))
        fig.clear()

        ax_m = plt.subplot2grid((f_hig,f_wid), (1,1), colspan=f_wid-1, rowspan=f_hig-1)
        ax_t = plt.subplot2grid((f_hig,f_wid), (0,1), colspan=f_wid-1, rowspan=1)
        ax_l = plt.subplot2grid((f_hig,f_wid), (1,0), colspan=1, rowspan=f_hig-1)

        sns.heatmap(relation_df,
                    ax=ax_m,
                    annot=True,
                    mask=mask,
                    annot_kws={"size":8},
                    fmt=fmt,
                    linewidths=.5,
                    cbar=False)
        ax_m.set_xticklabels([""]*l_col)
        ax_m.set_yticklabels([""]*l_row)
        ax_m.set_xlabel("")
        ax_m.set_ylabel("")
        sns.heatmap(pd.DataFrame(relation_df.sum(0)).T,
                    ax=ax_t,
                    annot=True,
                    annot_kws={"size":8},
                    fmt=fmt,
                    linewidths=.5,
                    cbar=False)
        ax_t.xaxis.tick_top()
        ax_t.xaxis.set_label_position('top')
        ax_t.set_xticklabels(relation_df.columns,rotation=40)
        ax_t.set_yticklabels([""]*l_row)
        ax_t.set_ylabel("")
        sns.heatmap(pd.DataFrame(relation_df.sum(1)),
                    ax=ax_l,
                    annot=True,
                    annot_kws={"size":8},
                    fmt=fmt,
                    linewidths=.5,
                    cbar=False)
        ax_l.set_xticklabels([""]*l_col)
        ax_l.set_xlabel("")

        fig.savefig(self.out_path + name + "_heatmap.png")
        print("ok")

    def draw_violinplot(self, to_draw, name,
                        x=None, y=None, hue=None,
                        nth=None, color_column="color",
                        scale=True):
        print("(DrawEngine) drawing %s..." % name)

        x_groups = to_draw.groupby(x)[y, color_column]
        ordered_x = x_groups.median().sort_values(y, ascending=False)
        if nth is not None:
            ordered_x = ordered_x[:nth]

        if scale:
            s = "count"
            sh = False
        else:
            s = "width"
            sh = True

        kwargs = dict(split="True",
                      inner="box",
                      scale=s,
                      scale_hue=sh,
                      cut=0,
                      bw=.1)

        if isinstance(to_draw, pd.Series) or isinstance(to_draw, list):
            pass
        elif isinstance(to_draw, pd.DataFrame):
            kwargs.update({"x":x, "y":y, "hue":hue})
        else:
            assert False

        fig_wid = len(ordered_x)*0.3
        if hue is not None:
            palette = to_draw.groupby(hue)[color_column].nth(0)
            fig_wid *= len(palette)
        else:
            palette = x_groups.nth(0)[color_column].reindex_axis(ordered_x.index)

        fig = plt.figure(figsize=(fig_wid+3, 5))
        fig.clear()
        fig.suptitle("%s violinplot" % name, fontsize=14)

        ax = sns.violinplot(data=to_draw, order=ordered_x.index, palette=palette, **kwargs)
        ax.set_xticklabels(ordered_x.index,rotation=90)
        patch_violinplot()

        fig.tight_layout(rect=[0, 0.03, 1, 0.95])
        fig.savefig(self.out_path + name + "_violinplot.png")
        print("ok")

    def draw_boxplot(self, to_draw, name,
                     x, y, hue=None,
                     nth=None, color_column="color",
                     if_swarm=False):
        print("(DrawEngine) drawing %s..." % name)

        x_groups = to_draw.groupby(x)[y, color_column]
        ordered_x = x_groups.median().sort_values(y, ascending=False)
        if nth is not None:
            ordered_x = ordered_x[:nth]

        kwargs = dict(data=to_draw,
                      order=ordered_x.index)

        if isinstance(to_draw, pd.Series) or isinstance(to_draw, list):
            pass
        elif isinstance(to_draw, pd.DataFrame):
            kwargs.update({"x":x, "y":y, "hue":hue})
        else:
            assert False

        fig_wid = len(ordered_x)*0.15
        if hue is not None:
            palette = to_draw.groupby(hue)[color_column].nth(0)
            fig_wid *= len(palette)
        else:
            palette = x_groups.nth(0)[color_column].reindex_axis(ordered_x.index)

        fig = plt.figure(figsize=(fig_wid+3, 5))
        fig.clear()
        fig.suptitle("%s boxplot" % name, fontsize=14)

        ax = sns.boxplot(palette=palette, **kwargs)
        ax.set_xticklabels(ordered_x.index,rotation=90)
        if if_swarm:
            sns.swarmplot(**kwargs)

        fig.tight_layout(rect=[0, 0.03, 1, 0.95])
        fig.savefig(self.out_path + name + "_boxplot.png")
        print("ok")

    def draw_distplot(self, to_draw, name):
        print("(DrawEngine) drawing %s..." % name)

        fig = plt.figure()
        fig.clear()
        fig.suptitle("%s distplot" % name, fontsize=14)

        sns.distplot(to_draw, kde=False, color="#86d7f5")

        fig.savefig(self.out_path + name + "_distplot.png")
        print("ok")

    def draw_countplot(self, to_draw, name):
        print("(DrawEngine) drawing %s..." % name)

        fig = plt.figure()
        fig.clear()
        fig.suptitle("%s countplot" % name, fontsize=14)

        ax = sns.countplot(to_draw, color="#86d7f5")

        total = len(to_draw)
        for p in ax.patches:
            height = p.get_height()
            ax.text(p.get_x() + p.get_width()/2.,
                    height + 3,
                    '{:.2f}%'.format(height/total*100),
                    ha="center")

        fig.savefig(self.out_path + name + "_countplot.png")
        print("ok")

    def _prepare_thread_indexes(self, threadinss, plot_main=False):
        y_indexes_l = [""]
        y_indexes_r = [""]
        y_index = 1
        if plot_main:
            y_indexes_l.append("MAIN")
            y_indexes_r.append("MAIN")
            y_index += 1
        host_dict = defaultdict(
                        lambda: defaultdict(
                            lambda: defaultdict(
                                dict)))
        for ti in threadinss:
            # thread_object index
            host_dict[ti.host][str(ti.component)][ti.target][ti.thread_obj.id_] = 0

        hosts = sorted(host_dict.keys())
        for host in hosts:
            c_dict = host_dict[host]
            comps = sorted(c_dict.keys())
            for comp in comps:
                tg_dict = c_dict[comp]
                tgs = sorted(tg_dict.keys())
                for tg in tgs:
                    td_dict = tg_dict[tg]
                    tdids = sorted(td_dict.keys())
                    for tdid in tdids:
                        td_dict[tdid] = y_index
                        y_indexes_l.append("%s|td%s" % (tg, tdid))
                        y_indexes_r.append("%s|%s" % (host, comp))
                        y_index += 1

        threadins_y = {}
        for ti in threadinss:
            threadins_y[ti] = host_dict[ti.host][str(ti.component)]\
                                       [ti.target][ti.thread_obj.id_]
        return y_indexes_l, y_indexes_r, threadins_y

    def draw_requestins(self, requestins, name, start_end=None):
        assert isinstance(requestins, RequestInstance)
        print("(DrawEngine) drawing %s..." % name)

        # prepare requests
        if start_end is not None:
            (start, last), (start_t, last_t) = start_end
        else:
            start = requestins.start_seconds
            last = requestins.last_seconds
        all_lapse = last - start
        plot_main = requestins.start_interval.is_main

        threadinss = requestins.threadinss
        y_indexes_l, y_indexes_r, tiy =\
                self._prepare_thread_indexes(threadinss, plot_main)

        ## settings ##
        figsize = (30, .5*len(y_indexes_l))
        annot_off_y = 0.18
        annot_mark_lim = 0.006
        annot_pres_lim = 0.020
        markersize = 10
        node_markersize = 5
        int_style = "-"
        int_lock_style = ":"
        main_linewidth = 2
        other_linewidth = 1
        ##############

        #### sns #####
        sns.set_style("whitegrid")
        # marker bug
        sns.set_context(rc={'lines.markeredgewidth': 0.5})
        ##############

        #### args ####
        annot_kw = dict(color="k",
                        horizontalalignment="center",
                        verticalalignment="center",
                        size=8)
        ##############

        # figure and axes
        fig = plt.figure(figsize=figsize)
        fig.clear()
        fig.suptitle("%s: %s" % (name, requestins), fontsize=14)
        ax = fig.add_subplot(1,1,1)

        # xy axis labels
        ax.set_xlabel("lapse (seconds)")
        ax.set_ylabel("target | thread")
        ax.set_yticks(range(len(y_indexes_l)))
        ax.set_ylim(.5, len(y_indexes_l)-0.5)
        ax.set_yticklabels(y_indexes_l)
        # ax2 = ax.twinx()
        # ax2.set_ylabel("component by hosts")
        # ax2.set_yticks(range(len(y_indexes_r)))
        # ax2.set_ylim(0.5, len(y_indexes_r)-0.5)
        # ax2.set_yticklabels(y_indexes_r)

        # ratios for markers
        mark_lim = all_lapse * annot_mark_lim
        pres_lim = all_lapse * annot_pres_lim

        for ti in threadinss:
            ti_color = ti.component.color
            # draw start point
            if ti.is_request_start:
                t_marker=node_markersize
                color = "k"
            else:
                t_marker = "."
                color = "k"
            ax.plot(ti.start_seconds-start, tiy[ti],
                     marker=t_marker,
                     color=color,
                     markersize=markersize)

            # annotate start edge
            ax.annotate("%s" % ti.intervals[0].from_edge.name,
                         (ti.start_seconds-start, tiy[ti]),
                         (ti.start_seconds-start, tiy[ti]+annot_off_y),
                         **annot_kw)

            marker=7
            for int_ in ti.intervals:
                from_x = int_.from_seconds - start
                from_y = tiy[int_.threadins]
                to_x = int_.to_seconds - start
                to_y = from_y

                # draw interval
                if int_.is_lock:
                    linestyle = int_lock_style
                else:
                    linestyle = int_style
                if int_.is_main:
                    linewidth = main_linewidth
                    assert plot_main
                    ax.plot([from_x, to_x],
                            [1, 1],
                            linestyle=linestyle,
                            color=ti_color,
                            linewidth=linewidth)
                else:
                    linewidth = other_linewidth
                ax.plot([from_x, to_x],
                         [from_y, to_y],
                         linestyle=linestyle,
                         color=ti_color,
                         label=int_.node.name,
                         linewidth=linewidth)

                # mark interval
                if marker == 6:
                    annot_off = annot_off_y
                else:
                    annot_off = -annot_off_y

                if int_.lapse >= pres_lim:
                    ax.annotate("%s=%.2f" % (int_.entity.name, int_.lapse),
                                 ((from_x + to_x)/2, to_y),
                                 ((from_x + to_x)/2, to_y+annot_off),
                                 **annot_kw)
                elif int_.lapse >= mark_lim:
                    ax.annotate("%s" % int_.entity.name,
                                 ((from_x + to_x)/2, to_y),
                                 ((from_x + to_x)/2, to_y+annot_off),
                                 **annot_kw)

                # draw end point
                if int_.is_request_end:
                    t_marker = 4
                    color = "k"
                    t_markersize=markersize
                elif int_.is_thread_end:
                    t_marker = "3"
                    color="k"
                    t_markersize=markersize
                else:
                    t_marker = marker
                    color=ti_color
                    t_markersize=node_markersize
                ax.plot(int_.to_seconds-start, tiy[ti],
                         marker=t_marker,
                         color=color,
                         markersize=t_markersize)

                # draw thread end
                if int_.is_thread_end:
                    ax.annotate("%s" % int_.to_edge.name,
                                 (to_x, to_y),
                                 (to_x, to_y+annot_off_y),
                                 **annot_kw)

                marker = 6 if marker==7 else 7

        ax = fig.axes[0]
        for int_ in requestins.join_ints:
            from_x = int_.from_seconds - start
            from_y = tiy[int_.from_threadins]
            to_x = int_.to_seconds - start
            to_y = tiy[int_.to_threadins]
            ti_color = int_.color_jt
            if to_y > from_y:
                connstyle="arc3,rad=0"
            else:
                connstyle="arc3,rad=0"
            if int_.is_main:
                width = main_linewidth
                assert plot_main
                ax.plot([from_x, to_x],
                        [1, 1],
                        linestyle=int_style,
                        color=ti_color,
                        linewidth=linewidth)
            else:
                width = other_linewidth
            ax.annotate("",
                        (to_x, to_y),
                        (from_x, from_y),
                        arrowprops=dict(arrowstyle="->",
                                        shrinkA=0.5,
                                        shrinkB=0.5,
                                        color=ti_color,
                                        connectionstyle=connstyle,
                                        lw=width))
            ax.annotate("%s=%.2f" % (int_.entity.name, int_.lapse),
                        ((from_x+to_x)/2, (from_y+to_y)/2+.5),
                        **annot_kw)

        ax.annotate(start_t, xy=(0, 0), xytext=(0, 0))
        ax.annotate(last_t, xy=(last-start, 0), xytext=(last-start, 0))

        fig.tight_layout(rect=[0, 0.03, 1, 0.95])
        fig.savefig(self.out_path + name + "_requestplot.png")
        print("ok")

    def _convert_stack(self, stack):
        stack.sort(key=lambda tup: tup[0])

        x_list = []
        y_list = []
        x, y = None, 0
        prv_y = None
        for data in stack:
            if x is None:
                # init
                x = data[0]
                prv_y = y
            elif x != data[0]:
                # x changes
                x_list.append(x)
                y_list.append(prv_y)
                x_list.append(x+0.00000001)
                y_list.append(y)
                x = data[0]
                prv_y = y
            else:
                # x not change
                pass
            y += data[1]
        if x is not None:
            x_list.append(x)
            y_list.append(prv_y)
            x_list.append(x+0.00000001)
            y_list.append(y)

        return x_list, y_list

    def _convert_intervals_to_stack1(self, rows):
        stack = [(round(row.from_seconds, 7), 1) for row in rows]
        stack.extend((round(row.to_seconds, 7), -1) for row in rows)
        return self._convert_stack(stack)

    def _convert_intervals_to_stack(self, df):
        stack = [(round(sec, 7), 1) for sec in df["from_sec"]]
        stack.extend((round(sec, 7), -1) for sec in df["to_sec"])
        return self._convert_stack(stack)

    class SNode(object):
        def __init__(self, type_=None, color=None):
            self.tonode_by_type = {}
            self.type_ = type_
            self.color = color
            self.content = []
            self.key = maxint

        def get_tonode(self, type_, color):
            assert type_ is not None
            tonode = self.tonode_by_type.get(type_)
            if tonode is None:
                tonode = DrawEngine.SNode(type_, color)
                self.tonode_by_type[type_] = tonode
            return tonode

        def append(self, item, key):
            self.content.append(item)
            self.key = min(self.key, key)


    def draw_stacked_intervals(self, start_end, requestinss, main_ints_df, name):
        print("(DrawEngine) drawing %s..." % name)

        (start_s, end_s), (start_t, end_t) = start_end

        ## main_stack
        # main_x_list
        # main_ys_list

        def _get_type(int_):
            if isinstance(int_, ThreadInterval):
                return str(int_.component)
            else:
                assert isinstance(int_, JoinInterval)
                if int_.is_remote:
                    return "remote_join"
                else:
                    return "local_join"

        component_workflow = self.SNode()
        for requestins in requestinss.itervalues():
            snode = component_workflow
            for int_ in requestins.intervals_extended:
                snode = snode.get_tonode(_get_type(int_), int_.color)
                snode.append(int_, int_.from_seconds)

        nodes = []
        for node in component_workflow.tonode_by_type.itervalues():
            heapq.heappush(nodes, (node.key, node))

        main_stacked_results = []
        while nodes:
            node = heapq.heappop(nodes)[1]
            x_list, y_list = self._convert_intervals_to_stack1(node.content)
            main_stacked_results.append((node.type_, node.color, x_list, y_list))
            for node_ in node.tonode_by_type.itervalues():
                heapq.heappush(nodes, (node_.key, node_))

        # type_ = None
        # color = None
        # same_type = []
        # same_types = []
        # for row_tup in thread_ints_ordered.itertuples():
        #     r_type = row_tup.type
        #     r_color = row_tup.color
        #     if type_ is not None and type_ == r_type:
        #         same_type.append(row_tup)
        #     else:
        #         assert r_type is not None
        #         type_ = r_type
        #         color = r_color
        #         same_type = [row_tup]
        #         same_types.append((type_, color, same_type))

        # main_stacked_results = []
        # for type_, color, rows in same_types:
        #     x_list, y_list = self._convert_intervals_to_stack1(rows)
        #     main_stacked_results.append((type_, color, x_list, y_list))

        # thread_ints_by_type = thread_ints_df.groupby("type")
        # main_stacked_results = []
        # for type_, df in thread_ints_by_type:
        #     color = df.iloc[0]["color"]
        #     x_list, y_list = self._convert_intervals_to_stack(df)
        #     main_stacked_results.append((type_, color, x_list, y_list))

        types = (tup[0] for tup in main_stacked_results)
        colors = (tup[1] for tup in main_stacked_results)
        main_x_list = set()
        for tup in main_stacked_results:
            main_x_list.update(tup[2])
        main_x_list = list(main_x_list)
        main_x_list.sort()

        main_ys_list = []
        for tup in main_stacked_results:
            stacked_x = tup[2]
            stacked_y = tup[3]
            len_stacked = len(stacked_x)
            assert len_stacked == len(stacked_y)
            i_stacked = 0
            y_list = []
            y = 0
            for x in main_x_list:
                if i_stacked < len(stacked_y) and x == stacked_x[i_stacked]:
                    y = stacked_y[i_stacked]
                    i_stacked += 1
                y_list.append(y)
            assert i_stacked == len(stacked_y)
            main_ys_list.append(y_list)

        y_max = 0
        y_stacked = [0] * len(main_x_list)
        for y_list in main_ys_list:
            for y_i, val in enumerate(y_list):
                y_stacked[y_i] += val
                y_max = max(y_max, y_stacked[y_i])

        main_x_list = [x for x in main_x_list]

        ## individual stacks
        ints_by_entity = main_ints_df.groupby("path")
        entity_stack_results = []
        for entity_, df in ints_by_entity:
            color = df.iloc[0]["color"]
            x_list, y_list = self._convert_intervals_to_stack(df)
            entity_stack_results.append((entity_, color, x_list, y_list))

        ## settings ##
        figsize = (30, 2*(len(entity_stack_results)+1))
        y_plots = len(entity_stack_results) + 1
        annot_off_y = 0.18
        annot_mark_lim = 0.006
        annot_pres_lim = 0.020
        markersize = 10
        node_markersize = 5
        int_style = "-"
        int_lock_style = ":"
        main_linewidth = 2
        other_linewidth = 1
        ##############

        #### sns #####
        sns.set_style("whitegrid")
        # marker bug
        sns.set_context(rc={'lines.markeredgewidth': 0.5})
        ##############

        fig = plt.figure(figsize=figsize)
        fig.clear()
        fig.suptitle("%s" % name, fontsize=14)

        a_index = 1
        for entity, color, x_list, y_list in entity_stack_results:
            ax = fig.add_subplot(y_plots, 1, a_index)
            ax.set_xticklabels([])
            ax.set_ylabel(entity)
            x_list = [x for x in x_list]
            ax.stackplot(x_list, y_list, colors=[color], linewidth=0)
            ax.set_xlim(0, end_s*1.05)
            ax.set_ylim(0, y_max)
            a_index += 1

        ax = fig.add_subplot(y_plots,1,a_index)
        ax.set_xlabel("lapse (seconds)")
        ax.set_xlim(0, end_s*1.05)
        ax.set_ylim(0, y_max)
        ax.set_ylabel("MAIN")

        ax.annotate(start_t, xy=(0, 0), xytext=(0, 0))
        ax.annotate(end_t, xy=(end_s, 0), xytext=(end_s, 0))
        ax.plot([0, end_s], [0, 0], 'r*')

        ax.stackplot(main_x_list, *main_ys_list, colors=colors, linewidth=0)
        # ax.legend((mpatches.Patch(color=color) for color in colors), types)

        fig.tight_layout(rect=[0, 0.03, 1, 0.95])
        fig.savefig(self.out_path + name + "_stackedplot.png")
        print("ok")


    def draw_threadobj(self, thread_obj):
        print("(DrawEngine) drawing %s..." % thread_obj.name)

        ## settings ##
        figsize = (60, 8)
        ##############

        #### sns #####
        sns.set_style("whitegrid")
        # marker bug
        sns.set_context(rc={'lines.markeredgewidth': 0.5})
        ##############

        # figure and axes
        fig = plt.figure(figsize=figsize)
        fig.clear()
        fig.suptitle("%s" % thread_obj, fontsize=14)
        ax = fig.add_subplot(1,1,1)

        ax.set_xlabel("lapse (seconds)")
        ax.set_ylabel("wait time (seconds)")

        for threadins in thread_obj.threadinss:
            ax.plot([threadins.start_seconds, threadins.end_seconds],
                     [0, 0],
                     linestyle="-",
                     color=threadins.component.color,
                     label=None)
            for joined_int in threadins.joined_ints:
                ax.plot([joined_int.to_seconds, joined_int.to_seconds],
                        [-joined_int.lapse, 0],
                        linestyle="-",
                        color=joined_int.color_jt,
                        label=joined_int.join_obj.name)
            for joins_int in threadins.joins_ints:
                ax.plot([joins_int.from_seconds, joins_int.from_seconds],
                        [0, joins_int.lapse],
                        linestyle="-",
                        color=joins_int.color_jt,
                        label=joins_int.join_obj.name)

        fig.tight_layout(rect=[0, 0.03, 1, 0.95])
        fig.savefig(self.out_path + thread_obj.name + "_threadplot.png")
        print("ok")


    def draw_debug_groups(self, requests, threadgroup):
        print("(DrawEngine) drawing %s..." % requests)

        ## settings ##
        figsize = (200, 30)
        annot_off_y = 0.18
        annot_mark_lim = 0.006
        annot_pres_lim = 0.020
        markersize = 10
        node_markersize = 5
        int_style = "-"
        int_lock_style = ":"
        main_linewidth = 2
        other_linewidth = 1
        ##############

        #### sns #####
        sns.set_style("whitegrid")
        # marker bug
        sns.set_context(rc={'lines.markeredgewidth': 0.5})
        ##############

        #### args ####
        annot_kw = dict(color="k",
                        horizontalalignment="center",
                        verticalalignment="center",
                        size=8)
        ##############

        # figure and axes
        fig = plt.figure(figsize=figsize)
        fig.clear()
        fig.suptitle("Debug: %s" % requests, fontsize=14)
        ax = fig.add_subplot(1,1,1)

        # prepare requests
        start = maxint
        last = 0
        for ti in threadgroup:
            start = min(start, ti.start_seconds)
            last = max(last, ti.end_seconds)
        start = 0
        all_lapse = last - start
        plot_main = False

        threadinss = threadgroup
        y_indexes_l, y_indexes_r, tiy =\
                self._prepare_thread_indexes(threadinss, plot_main)

        # xy axis labels
        ax.set_xlabel("lapse (seconds)")
        ax.set_ylabel("thread by targets")
        ax.set_yticks(range(len(y_indexes_l)))
        ax.set_ylim(.5, len(y_indexes_l)-0.5)
        ax.set_yticklabels(y_indexes_l)
        ax2 = ax.twinx()
        ax2.set_ylabel("component by hosts")
        ax2.set_yticks(range(len(y_indexes_r)))
        ax2.set_ylim(0.5, len(y_indexes_r)-0.5)
        ax2.set_yticklabels(y_indexes_r)

        # ratios for markers
        mark_lim = all_lapse * annot_mark_lim
        pres_lim = all_lapse * annot_pres_lim

        for ti in threadinss:
            ti_color = ti.component.color
            # draw start point
            if ti.is_request_start:
                t_marker=node_markersize
                color = "k"
            else:
                t_marker = "."
                color = "k"
            ax.plot(ti.start_seconds-start, tiy[ti],
                     marker=t_marker,
                     color=color,
                     markersize=markersize)

            # annotate start edge
            ax.annotate("%s" % ti.intervals[0].from_edge.name,
                         (ti.start_seconds-start, tiy[ti]),
                         (ti.start_seconds-start, tiy[ti]+annot_off_y),
                         **annot_kw)

            marker=7
            for int_ in ti.intervals:
                from_x = int_.from_seconds - start
                from_y = tiy[int_.threadins]
                to_x = int_.to_seconds - start
                to_y = from_y

                # draw interval
                if int_.is_lock:
                    linestyle = int_lock_style
                else:
                    linestyle = int_style
                if int_.is_main:
                    linewidth = main_linewidth
                    assert plot_main
                    ax.plot([from_x, to_x],
                            [1, 1],
                            linestyle=linestyle,
                            color=ti_color,
                            linewidth=linewidth)
                else:
                    linewidth = other_linewidth
                ax.plot([from_x, to_x],
                         [from_y, to_y],
                         linestyle=linestyle,
                         color=ti_color,
                         label=int_.node.name,
                         linewidth=linewidth)

                # mark interval
                if marker == 6:
                    annot_off = annot_off_y
                else:
                    annot_off = -annot_off_y

                if int_.lapse >= pres_lim:
                    ax.annotate("%s=%.2f" % (int_.node.name, int_.lapse),
                                 ((from_x + to_x)/2, to_y),
                                 ((from_x + to_x)/2, to_y+annot_off),
                                 **annot_kw)
                elif int_.lapse >= mark_lim:
                    ax.annotate("%s" % int_.node.name,
                                 ((from_x + to_x)/2, to_y),
                                 ((from_x + to_x)/2, to_y+annot_off),
                                 **annot_kw)

                # draw end point
                if int_.is_request_end:
                    t_marker = 4
                    color = "k"
                    t_markersize=markersize
                elif int_.is_thread_end:
                    t_marker = "3"
                    color="k"
                    t_markersize=markersize
                else:
                    t_marker = marker
                    color=ti_color
                    t_markersize=node_markersize
                ax.plot(int_.to_seconds-start, tiy[ti],
                         marker=t_marker,
                         color=color,
                         markersize=t_markersize)

                # draw thread end
                if int_.is_thread_end:
                    ax.annotate("%s" % int_.to_edge.name,
                                 (to_x, to_y),
                                 (to_x, to_y+annot_off_y),
                                 **annot_kw)

                marker = 6 if marker==7 else 7

        join_ints = set()
        for ti in threadinss:
            join_ints.update(ti.joined_ints)
            join_ints.update(ti.joins_ints)

        ax = fig.axes[0]
        for int_ in join_ints:
            from_x = int_.from_seconds - start
            from_y = tiy[int_.from_threadins]
            to_x = int_.to_seconds - start
            to_y = tiy[int_.to_threadins]
            ti_color = int_.color_jt
            if to_y > from_y:
                connstyle="arc3,rad=0"
            else:
                connstyle="arc3,rad=0"
            if int_.is_main:
                width = main_linewidth
                assert plot_main
                ax.plot([from_x, to_x],
                        [1, 1],
                        linestyle=int_style,
                        color=ti_color,
                        linewidth=linewidth)
            else:
                width = other_linewidth
            ax.annotate("",
                        (to_x, to_y),
                        (from_x, from_y),
                        arrowprops=dict(arrowstyle="->",
                                        shrinkA=0.5,
                                        shrinkB=0.5,
                                        color=ti_color,
                                        connectionstyle=connstyle,
                                        lw=width))
            ax.annotate("%s=%.2f" % (int_.join_obj.name, int_.lapse),
                        ((from_x+to_x)/2, (from_y+to_y)/2+.5),
                        **annot_kw)

        fig.tight_layout(rect=[0, 0.03, 1, 0.95])
        fig.savefig(self.out_path + "debuggroupplot.png")
        print("ok")
