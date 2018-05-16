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
from collections import OrderedDict
from contextlib import contextmanager
import heapq
from itertools import chain
import matplotlib
matplotlib.use('Agg')
from matplotlib import patches as mpatches
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sys import maxsize

from ..datasource import Target
from ..workflow.entities.join import JoinActivityBase
from ..workflow.entities.join import RequestjoinActivity
from ..workflow.entities.request import ExtendedInterval
from ..workflow.entities.request import RequestInstance
from ..workflow.entities.threadins import ThreadActivity
from .statistic_helper import Workflow


REMOTE_C = "#fa8200"
LOCALREMOTE_C = "#fab300"
LOCAL_C = "#fade00"
INTERFACE_C = "#a82511"
NESTED_C = "#ff3719"


def getcolor_byint(interval, ignore_lr=False):
    if isinstance(interval, ThreadActivity) or \
       isinstance(interval, ExtendedInterval):
        return interval.component.color
    elif isinstance(interval, RequestjoinActivity):
        if interval.is_nest:
            return NESTED_C
        else:
            return INTERFACE_C
    elif isinstance(interval, JoinActivityBase):
        r_type = interval.remote_type
        if r_type == "local_remote":
            if ignore_lr:
                return REMOTE_C
            else:
                return LOCALREMOTE_C
        elif r_type == "remote":
            return REMOTE_C
        else:
            return LOCAL_C
    else:
        raise RuntimeError("%r" % interval)


def _patch_violinplot():
    """Patch seaborn's violinplot in current axis to workaround
    matplotlib's bug ##5423."""
    from matplotlib.collections import PolyCollection
    ax = plt.gca()
    for art in ax.get_children():
        if isinstance(art, PolyCollection):
            art.set_linewidth(0)


def _convertfromtos_to_stack(from_tos):
    stack = []
    for from_to in from_tos:
        stack.append((round(from_to[0], 7), 1))
        stack.append((round(from_to[1], 7), -1))
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


def _convert_tostacked(xylists_l):
        xylists_l = list(xylists_l)

        main_x_list = set()
        for x_list, _ in xylists_l:
            main_x_list.update(x_list)
        main_x_list = list(main_x_list)
        main_x_list.sort()

        main_ys_list = []
        for x_list, y_list in xylists_l:
            len_stacked = len(x_list)
            assert len_stacked == len(y_list)
            i_stacked = 0
            r_ylist = []
            y = 0
            for x in main_x_list:
                if i_stacked < len(y_list) and x == x_list[i_stacked]:
                    y = y_list[i_stacked]
                    i_stacked += 1
                r_ylist.append(y)
            assert i_stacked == len(y_list)
            main_ys_list.append(r_ylist)

        return main_x_list, main_ys_list


def _generate_stackeddata(type_color_fromtos_list):
    types = [tup[0] for tup in type_color_fromtos_list]
    colors = [tup[1] for tup in type_color_fromtos_list]

    main_x_list, main_ys_list = _convert_tostacked(
            _convertfromtos_to_stack(tup[2])
            for tup in type_color_fromtos_list)
    return types, colors, main_x_list, main_ys_list


def _prepare_thread_indexes(thread_objs, plot_main=False):
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
    for to in thread_objs:
        # thread_object index
        host_dict[to.host][str(to.component)][to.target][to.id_] = 0

    targets_y = []
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
                targets_y.append(y_index-.5)

    threadobjs_y = {}
    for to in thread_objs:
        threadobjs_y[to] = host_dict[to.host][str(to.component)]\
                                   [to.target][to.id_]
    return y_indexes_l, y_indexes_r, threadobjs_y, targets_y


class DrawEngine(object):
    def __init__(self, out_path):
        if not out_path:
            self.out_path = None
        else:
            assert isinstance(out_path, str)
            self.out_path = out_path

    @contextmanager
    def _build_fig(self, figtype, figname,
            figsize=None, title=None, tight=True):
        if self.out_path:
            print("(DrawEngine) drawing %s..." % figname)

        #### sns #####
        sns.set_style("whitegrid")
        # marker bug
        sns.set_context(rc={'lines.markeredgewidth': 0.5})

        fig = plt.figure(figsize=figsize)
        fig.clear()
        if title is None:
            title = figname
        else:
            title = "%s: %s" % (figname, title)
        fig.suptitle("%s %s" % (title, figtype), fontsize=14)

        yield fig

        if tight:
            fig.tight_layout(rect=[0, 0.03, 1, 0.95])

        if self.out_path:
            fig.savefig(self.out_path + figname + "_" + figtype + ".png")
        else:
            from IPython.display import display_png
            from IPython.core.pylabtools import print_figure
            data = print_figure(fig, "png")
            display_png(data, raw=True)
        plt.close(fig)

        if self.out_path:
            print("ok")


    def draw_relation_heatmap(self, relation_df, name, fmt="d"):
        assert isinstance(relation_df, pd.DataFrame)
        assert isinstance(name, str)

        if relation_df.empty:
            print("EMPTY INPUT! stop drawing %s" % name)
            return

        relation_df = relation_df.ix[(relation_df.T!=0).any(),
                                     (relation_df!=0).any()]
        relation_df.reindex(sorted(relation_df.columns), axis=1)
        relation_df.reindex(sorted(relation_df.index))
        mask = relation_df[:].isin([0, np.NaN])
        l_col = len(relation_df.columns)
        l_row = len(relation_df)
        f_wid = l_col + 1
        f_hig = l_row + 1

        with self._build_fig("heatmap", name,
                figsize=(l_col/2+7, l_row/2+7), tight=False) as fig:
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

    def draw_boxplot(self, to_draw, name, x, y,
                     hue=None,
                     palette=None, color=None,
                     violin=True):
        x_groups = to_draw.groupby(x)[y]
        ordered_x = x_groups.median().sort_values(ascending=False).index
        desc_max = 0
        for _x in ordered_x:
            desc_max = max(desc_max, len(str(_x)))
        kwargs = dict(data=to_draw,
                      color=color,
                      palette=palette)

        if isinstance(to_draw, pd.Series) or isinstance(to_draw, list):
            pass
        elif isinstance(to_draw, pd.DataFrame):
            kwargs.update({"x":x, "y":y, "hue":hue})
        else:
            assert False

        fig_wid = len(ordered_x)*0.3
        if hue is not None:
            fig_wid *= len(to_draw.groupby(hue))

        def _interact(x, draw_violin):
            with self._build_fig("boxplot", name,
                    figsize=(fig_wid+2, desc_max*0.07+7.5)) as fig:
                _orderedx = []
                for i in x:
                    _orderedx.append(ordered_x[i])
                _kwargs = kwargs.copy()
                _kwargs["order"] = _orderedx

                if draw_violin:
                    _kwargs.update(dict(split=False,
                                        inner="box",
                                        scale="width", # or "count"
                                        scale_hue=True, # or False
                                        cut=0,
                                        bw=.1))
                    ax = sns.violinplot(**_kwargs)
                    _patch_violinplot()
                else:
                    ax = sns.boxplot(**_kwargs)

                x_labels = []
                for k in _orderedx:
                    x_labels.append("%s (%d)" % (k, len(x_groups.get_group(k))))
                ax.set_xticklabels(x_labels,rotation=90)

        if self.out_path:
            _interact(tuple(i for i in range(min(10, len(ordered_x)))), violin)
        else:
            from ipywidgets import widgets, interactive_output, Layout
            from IPython.display import display

            layout = Layout(width="99%")
            w_violin = widgets.Checkbox(
                    value=violin,
                    description="plot violin")
            options = [(v, i) for i, v in enumerate(ordered_x)]
            w_range = widgets.SelectMultiple(
                    options=options,
                    index=tuple(i for i in range(min(10, len(ordered_x)))),
                    rows=min(10, len(ordered_x)),
                    description="x range",
                    layout=layout)

            w_box = widgets.VBox([w_violin, w_range])
            out = widgets.interactive_output(_interact,
                                             {'x': w_range,
                                              'draw_violin': w_violin})
            display(w_box, out)


    def draw_distplot(self, to_draw, name):
        with self._build_fig("distplot", name) as fig:
            ax = fig.add_subplot(1,1,1)
            ax.set_ylabel("count")
            sns.distplot(to_draw, ax=ax, kde=False, color="#86d7f5")

    def draw_countplot(self, to_draw, name):
        with self._build_fig("countplot", name) as fig:
            ax = sns.countplot(to_draw, color="#86d7f5")
            total = len(to_draw)
            for p in ax.patches:
                height = p.get_height()
                ax.text(p.get_x() + p.get_width()/2.,
                        height + 3,
                        '{:.2f}%'.format(height/total*100),
                        ha="center")

    def draw_kdeplot(self, s_x, s_y, name):
        with self._build_fig("kdeplot", name, figsize=(15, 7)) as fig:
            cmap = sns.cubehelix_palette(
                    as_cmap=True, dark=0, light=1, reverse=True)
            ax = sns.kdeplot(s_x, s_y, cmap=cmap, shade=True, n_levels=100)
            ax.set_xlim(s_x.min(), s_x.max())
            ax.set_ylim(0, s_y.max())

    def draw_requestins(self, requestins, name, start_end=None):
        assert isinstance(requestins, RequestInstance)

        if start_end is None:
            start_end = {"seconds": {"start": requestins.from_seconds,
                                     "end": requestins.to_seconds,
                                     "last": requestins.last_seconds},
                         "time":    {"start": requestins.from_time,
                                     "end": requestins.to_time,
                                     "last": requestins.last_time}}

        self.draw_intervalvisual(requestins.threadinss,
                                 requestins.iter_joins(),
                                 name, start_end,
                                 plot_main=True,
                                 title=repr(requestins))

    def draw_intervalvisual(self, threadinss, joinints, name, start_end,
                            plot_main=False, title=None):
        # prepare requests
        start = start_end["seconds"]["start"]
        last = start_end["seconds"]["last"]
        start_t = start_end["time"]["start"]
        last_t = start_end["time"]["last"]
        all_lapse = last - start
        last_plot = last + all_lapse * 0.05

        threadinss = list(threadinss)
        joinints = list(joinints)
        thread_objs = set()
        for ti in threadinss:
            thread_objs.add(ti.thread_obj)
        for ji in joinints:
            thread_objs.add(ji.from_threadobj)
            thread_objs.add(ji.to_threadobj)

        y_indexes_l, y_indexes_r, tos_y, targets_y =\
                _prepare_thread_indexes(thread_objs, plot_main)

        #### settings ####
        annot_off_x_lim = 0.005
        annot_off_y = 0.18
        annot_mark_lim = 0.007
        annot_pres_lim = 0.020
        markersize = 10
        node_markersize = 5
        int_style = "-"
        main_linewidth = 3
        other_linewidth = 1

        #### others ####
        annot_kw = dict(color="k",
                        horizontalalignment="center",
                        verticalalignment="center",
                        size=8)
        # ratios for markers
        mark_lim = all_lapse * annot_mark_lim
        pres_lim = all_lapse * annot_pres_lim
        annot_off_x = all_lapse * annot_off_x_lim

        # figure and axes
        with self._build_fig("intplot", name,
                figsize=(30, .5*len(y_indexes_l)), title=title) as fig:
            ax = fig.add_subplot(1,1,1)

            # xy axis labels
            ax.set_xlabel("lapse (seconds)")
            ax.set_ylabel("target | thread")
            ax.set_yticks(range(len(y_indexes_l)))
            ax.set_xlim(start, last_plot)
            ax.set_ylim(.5, len(y_indexes_l)-0.5)
            ax.set_yticklabels(y_indexes_l)
            # ax2 = ax.twinx()
            # ax2.set_ylabel("component by hosts")
            # ax2.set_yticks(range(len(y_indexes_r)))
            # ax2.set_ylim(0.5, len(y_indexes_r)-0.5)
            # ax2.set_yticklabels(y_indexes_r)

            # 1. draw horizonal divide lines
            if plot_main:
                ax.plot([start, last_plot],
                        [1.5, 1.5],
                        color="#d0d0d0",
                        linewidth=10)
            for t_y in targets_y:
                ax.plot([start, last_plot],
                        [t_y, t_y],
                        linestyle=":",
                        color="#d0d0d0",
                        linewidth=10)

            def _process_main(int_):
                if plot_main and int_.is_main:
                    width = main_linewidth
                    ax.plot([int_.from_seconds, int_.to_seconds],
                            [1, 1],
                            linestyle=int_style,
                            color=getcolor_byint(int_),
                            linewidth=width)
                else:
                    width = other_linewidth
                return width

            # 2. draw join intervals/main intervals
            for int_ in joinints:
                from_x = int_.from_seconds
                from_y = tos_y[int_.from_threadobj]
                to_x = int_.to_seconds
                to_y = tos_y[int_.to_threadobj]
                width = _process_main(int_)
                if to_y > from_y:
                    connstyle="arc3,rad=0"
                else:
                    connstyle="arc3,rad=0"
                ax.annotate("",
                            (to_x, to_y),
                            (from_x, from_y),
                            arrowprops=dict(arrowstyle="->",
                                            shrinkA=0.5,
                                            shrinkB=0.5,
                                            color=getcolor_byint(int_),
                                            connectionstyle=connstyle,
                                            lw=width))

            # 3. draw thread intervals/main intervals
            for ti in threadinss:
                color = ti.component.color
                plot_y = tos_y[ti.thread_obj]
                for int_ in ti.iter_ints():
                    from_x = int_.from_seconds
                    to_x = int_.to_seconds
                    linestyle = int_style
                    linewidth = _process_main(int_)
                    ax.plot([from_x, to_x],
                             [plot_y, plot_y],
                             linestyle=linestyle,
                             color=color,
                             label=int_.int_name,
                             linewidth=linewidth)

            # 4. annotate thread intervals
            for ti in threadinss:
                ti_color = ti.component.color
                plot_y = tos_y[ti.thread_obj]
                # draw start point
                if ti.is_request_start:
                    t_marker=node_markersize
                    color = "k"
                else:
                    t_marker = "."
                    color = "k"
                ax.plot(ti.from_seconds, plot_y,
                         marker=t_marker,
                         color=color,
                         markersize=markersize)

                # annotate start edge
                ax.annotate("%s" % ti.from_edgename,
                             (ti.from_seconds, plot_y),
                             (ti.from_seconds-annot_off_x, plot_y),
                             **annot_kw)

                marker=7
                for int_ in ti.iter_ints():
                    from_x = int_.from_seconds
                    to_x = int_.to_seconds

                    # annotate thread interval
                    if marker == 6:
                        annot_off = annot_off_y
                    else:
                        annot_off = -annot_off_y
                    if int_.lapse >= mark_lim:
                        a_str = "%s" % int_.int_name
                        if int_.lapse >= pres_lim:
                            a_str += "=%.3f" % int_.lapse
                        ax.annotate(a_str,
                                    ((from_x + to_x)/2, plot_y),
                                    ((from_x + to_x)/2, plot_y+annot_off),
                                    **annot_kw)

                    # draw end point
                    if int_.nxt_thread_activity.is_request_end:
                        t_marker = 4
                        color = "k"
                        t_markersize=markersize
                    elif int_.nxt_thread_activity.is_thread_end:
                        t_marker = "3"
                        color="k"
                        t_markersize=markersize
                    else:
                        t_marker = marker
                        color=ti_color
                        t_markersize=node_markersize
                    ax.plot(int_.to_seconds, plot_y,
                             marker=t_marker,
                             color=color,
                             markersize=t_markersize)

                    # annotate end step
                    if int_.nxt_thread_activity.is_thread_end:
                        ax.annotate("%s" % int_.to_edgename,
                                     (to_x, plot_y),
                                     (to_x+annot_off_x, plot_y),
                                     **annot_kw)
                    elif int_.lapse >= mark_lim:
                        ax.annotate("%s" % int_.to_edgename,
                                     (to_x, plot_y),
                                     (to_x, plot_y+annot_off),
                                     **annot_kw)

                    marker = 6 if marker==7 else 7

            # 5. annotate join intervals
            for int_ in joinints:
                from_x = int_.from_seconds
                from_y = tos_y[int_.from_threadobj]
                to_x = int_.to_seconds
                to_y = tos_y[int_.to_threadobj]

                annot_str = "%s=%.2f" % (int_.int_name, int_.lapse)
                anot_x = (from_x+to_x)/2.0
                anot_y = (from_y+to_y)/2.0
                if isinstance(int_, RequestjoinActivity):
                    annot_str += "(%d)" % int_.cnt_nested
                ax.annotate(annot_str,
                            (anot_x, anot_y),
                            **annot_kw)

            ax.annotate(start_t, xy=(start, .5), xytext=(start, 0.5))
            ax.annotate(last_t, xy=(last, .5), xytext=(last, 0.5))
            ax.plot([start, last], [.5, .5], 'r*')


    def draw_workflow(self, start_end, workflow, name):
        assert isinstance(workflow, Workflow)

        start_s = start_end["seconds"]["start"]
        last_s = start_end["seconds"]["last"]
        start_t = start_end["time"]["start"]
        last_t = start_end["time"]["last"]

        # 1. get main stacked axis list
        main_stacked_results = []

        main_stacked_results.append((
            "BLANK",
            "#eeeeee",
            [(0, c.from_seconds)
                for c in chain.from_iterable(s.intervals
                for s in workflow.start_group.iter_nxtgroups())]))

        for group in workflow.sort_topologically():
            _name = group.desc
            color = getcolor_byint(group.intervals[0], ignore_lr=True)
            main_stacked_results.append((
                _name,
                color,
                [(c.from_seconds, c.to_seconds)
                    for c in group.intervals]))

        # 2. calc main_x_list from main_stacked_results
        types, colors, main_x_list, main_ys_list =\
                _generate_stackeddata(main_stacked_results)

        # 3. calc and check the arguments
        x_start_default = 0
        y_start_default = 0
        x_end_default = last_s*1.05
        y_end_default = workflow.len_reqs

        def _interact(x, y, selects, color):
            with self._build_fig("workflowplot", name,
                    figsize=(30, 6)) as fig:
                ax = fig.add_subplot(1 ,1, 1)
                ax.set_xlabel("lapse (seconds)")
                ax.set_ylabel("requests")
                ax.set_xlim(x[0], x[1])
                ax.set_ylim(y[0], y[1])
                plot_colors = colors[:]
                if color:
                    for s in selects:
                        plot_colors[s] = color
                ax.annotate(start_t, xy=(0, 0), xytext=(0, 0))
                ax.annotate(last_t, xy=(last_s, 0), xytext=(last_s, 0))
                ax.plot([0, last_s], [0, 0], 'r*')

                ax.stackplot(main_x_list, *main_ys_list, colors=plot_colors,
                             edgecolor="black", linewidth=.1)
                # ax.legend((mpatches.Patch(color=color) for color in colors), types)

        if self.out_path:
            _interact((x_start_default, x_end_default),
                      (y_start_default, y_end_default),
                      [],
                      None)
        else:
            from ipywidgets import widgets, interactive_output, fixed, Layout
            from IPython.display import display

            layout = Layout(width="99%")
            w_lapse = widgets.FloatRangeSlider(
                value=[x_start_default, x_end_default],
                min=x_start_default,
                max=x_end_default,
                step=0.0001,
                description='x-lapse:',
                continuous_update=False,
                readout_format='.4f',
                layout=layout
            )
            w_requests = widgets.IntRangeSlider(
                value=[y_start_default, y_end_default],
                min=y_start_default,
                max=y_end_default,
                description='y-requests:',
                continuous_update=False,
                layout=layout
            )
            w_range = widgets.VBox([w_lapse, w_requests])

            w_color = widgets.ColorPicker(
                concise=True,
                description='Highlight:',
                value='#ff40ff',
                layout=layout
            )
            options = [types[i] for i in range(1, len(types)-1)]
            dedup = defaultdict(lambda: -1)
            for i, o in enumerate(options):
                dedup[o] += 1
                if dedup[o]:
                    options[i] = ("%s (%d)" % (options[i], dedup[o]))
            options = [(v, i+1) for i, v in enumerate(options)]
            w_select = widgets.SelectMultiple(
                options=options,
                rows=min(10, len(options)),
                description='Steps:',
                layout=layout
            )
            w_highlight = widgets.VBox([w_color, w_select])

            w_tab = widgets.Tab()
            w_tab.children = [w_range, w_highlight]
            w_tab.set_title(0, "range")
            w_tab.set_title(1, "highlight")

            out = widgets.interactive_output(_interact,
                                             {'x': w_lapse,
                                              'y': w_requests,
                                              'selects': w_select,
                                              'color': w_color})
            display(w_tab, out)


    def draw_profiling(self, start_end, reqinss, name):
        start_s = start_end["seconds"]["start"]
        last_s = start_end["seconds"]["last"]
        start_t = start_end["time"]["start"]
        last_t = start_end["time"]["last"]
        y_max = len(reqinss)

        # 1. collect entity interval lists
        intlists_byentity = defaultdict(list)
        for req in reqinss:
            assert isinstance(req, RequestInstance)
            counter_byentity = defaultdict(lambda: 0)
            for int_ in req.iter_mainints():
                path_ = int_.path
                ilist = intlists_byentity[path_]
                counter = counter_byentity[path_]
                counter_byentity[path_] += 1

                if counter > len(ilist):
                    raise RuntimeError()
                elif counter == len(ilist):
                    ilist.append([])
                else:
                    pass
                ilist[counter].append(int_)

        # 2. pad entity interval lists
        paddedintlists_todraw = []
        for p_name, intlists in intlists_byentity.items():
            padded_intlists = []

            assert len(intlists) > 0
            len_req = len(intlists[0])
            prv_seconds = [0]*len_req
            color = getcolor_byint(intlists[0][0])

            for intlist in intlists:
                assert len(intlist) <= len(prv_seconds)

                paddings = []
                from_secs = sorted(int_.from_seconds
                        for int_ in intlist)
                paddings.extend(zip(prv_seconds, from_secs))
                paddings.extend((prv_seconds[i], last_s)
                        for i in range(len(intlist), len(prv_seconds)))
                padded_intlists.append((
                        "blank",
                        "#eeeeee",
                        paddings))

                intervals = [(i.from_seconds, i.to_seconds)
                            for i in intlist]
                padded_intlists.append((
                        "occupied",
                        getcolor_byint(intlist[0], ignore_lr=True),
                        intervals))

                len_req = len(intlist)
                prv_seconds = sorted([i.to_seconds for i in intlist])

            min_seconds = min(tup.from_seconds for tup in intlists[0])
            types, colors, main_x_list, main_ys_list =\
                    _generate_stackeddata(padded_intlists)
            paddedintlists_todraw.append((min_seconds,
                                          main_x_list,
                                          main_ys_list,
                                          colors,
                                          p_name))

        paddedintlists_todraw.sort(key=lambda i: i[0])

        ## settings ##
        num_plots = len(paddedintlists_todraw)

        with self._build_fig("profilingplot", name,
                figsize=(30, 3*(num_plots+1))) as fig:
            ax = None
            for index, (_, main_x_list, main_ys_list, colors, p_name)\
                    in enumerate(paddedintlists_todraw):
                ax = fig.add_subplot(num_plots, 1, index+1)
                # ax.set_xticklabels([])
                ax.set_ylabel(p_name)
                ax.stackplot(main_x_list, *main_ys_list, colors=colors,
                             edgecolor="black", linewidth=.1)
                ax.set_xlim(0, last_s*1.05)
                ax.set_ylim(0, y_max)

            ax.annotate(start_t, xy=(0, 0), xytext=(0, 0))
            ax.annotate(last_t, xy=(last_s, 0), xytext=(last_s, 0))
            ax.plot([0, last_s], [0, 0], 'r*')
            ax.set_xlabel("lapse (seconds)")

    #### others ####
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
            ax.plot([threadins.from_seconds, threadins.to_seconds],
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
        start = maxsize
        last = 0
        for ti in threadgroup:
            start = min(start, ti.from_seconds)
            last = max(last, ti.to_seconds)
        start = 0
        all_lapse = last - start
        plot_main = False

        threadinss = threadgroup
        y_indexes_l, y_indexes_r, tiy =\
                _prepare_thread_indexes(threadinss, plot_main)

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
            if ti.prv_thread_activity.is_request_start:
                t_marker=node_markersize
                color = "k"
            else:
                t_marker = "."
                color = "k"
            ax.plot(ti.from_seconds-start, tiy[ti],
                     marker=t_marker,
                     color=color,
                     markersize=markersize)

            # annotate start edge
            ax.annotate("%s" % ti.from_edgename,
                         (ti.from_seconds-start, tiy[ti]),
                         (ti.from_seconds-start, tiy[ti]+annot_off_y),
                         **annot_kw)

            marker=7
            for int_ in ti.iter_ints():
                from_x = int_.from_seconds - start
                from_y = tiy[int_.threadins]
                to_x = int_.to_seconds - start
                to_y = from_y

                # draw interval
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
                         label=int_.int_name,
                         linewidth=linewidth)

                # mark interval
                if marker == 6:
                    annot_off = annot_off_y
                else:
                    annot_off = -annot_off_y

                if int_.lapse >= pres_lim:
                    ax.annotate("%s=%.2f" % (int_.int_name, int_.lapse),
                                 ((from_x + to_x)/2, to_y),
                                 ((from_x + to_x)/2, to_y+annot_off),
                                 **annot_kw)
                elif int_.lapse >= mark_lim:
                    ax.annotate("%s" % int_.int_name,
                                 ((from_x + to_x)/2, to_y),
                                 ((from_x + to_x)/2, to_y+annot_off),
                                 **annot_kw)

                # draw end point
                if int_.nxt_thread_activity.is_request_end:
                    t_marker = 4
                    color = "k"
                    t_markersize=markersize
                elif int_.nxt_thread_activity.is_thread_end:
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
                if int_.nxt_thread_activity.is_thread_end:
                    ax.annotate("%s" % int_.to_edgename,
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

    def draw_target(self, target_obj):
        assert isinstance(target_obj, Target)

        print("(DrawEngine) drawing %s..." % target_obj.target)
        df = pd.DataFrame(((to.lapse,
                            to.name)
                           for to in target_obj.thread_objs.values()),
                          columns=["lapse", "thread"])

        fig = plt.figure(figsize=(15, len(target_obj.thread_objs)*.5+2))
        fig.clear()
        fig.suptitle("%s barplot" % target_obj, fontsize=14)

        sns.barplot(x="lapse", y="thread", data=df)

        fig.savefig(self.out_path + target_obj.target + "_targetfig.png")
        print("ok")
