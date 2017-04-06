from __future__ import print_function

from collections import defaultdict
from collections import OrderedDict
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from workflow_parser.state_machine import RequestInstance


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

    def draw_violinplot(self, to_draw, name, x=None, y=None, hue=None, scale=True):
        print("(DrawEngine) drawing %s..." % name)

        x_groups = to_draw.groupby(x)[y, "color"]
        ordered_x = x_groups.median().sort_values(y, ascending=False)

        fig = plt.figure()
        fig.clear()
        fig.suptitle("%s violinplot" % name, fontsize=14)

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

        if hue is not None:
            palette = to_draw.groupby(hue)["color"].nth(0)
        else:
            palette = x_groups.nth(0)["color"].reindex_axis(ordered_x.index)

        ax = sns.violinplot(data=to_draw, order=ordered_x.index, palette=palette, **kwargs)
        patch_violinplot()

        fig.savefig(self.out_path + name + "_violinplot.png")
        print("ok")

    def draw_boxplot(self, to_draw, name, x, y, hue=None, if_swarm=False):
        print("(DrawEngine) drawing %s..." % name)

        x_groups = to_draw.groupby(x)[y, "color"]
        ordered_x = x_groups.median().sort_values(y, ascending=False)

        fig = plt.figure()
        fig.clear()
        fig.suptitle("%s boxplot" % name, fontsize=14)

        kwargs = dict(data=to_draw,
                      order=ordered_x.index)

        if isinstance(to_draw, pd.Series) or isinstance(to_draw, list):
            pass
        elif isinstance(to_draw, pd.DataFrame):
            kwargs.update({"x":x, "y":y, "hue":hue})
        else:
            assert False

        if hue is not None:
            palette = to_draw.groupby(hue)["color"].nth(0)
        else:
            palette = x_groups.nth(0)["color"].reindex_axis(ordered_x.index)

        ax = sns.boxplot(palette=palette, **kwargs)
        if if_swarm:
            sns.swarmplot(**kwargs)

        # if hue is None:
        #     colors = 
        #     for i, box in enumerate(ax.artists):
        #         box.set_facecolor(colors[i])

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
            # comp index, y index
            host_dict[ti.host][ti.component][ti.target][ti.thread] = 0

        hosts = sorted(host_dict.keys())
        tg_index = 1
        for host in hosts:
            c_dict = host_dict[host]
            comps = sorted(map(lambda k: (str(k), k), c_dict.keys()))
            for comp in comps:
                tg_dict = c_dict[comp[1]]
                tgs = sorted(tg_dict.keys())
                for tg in tgs:
                    td_dict = tg_dict[tg]
                    tds = sorted(td_dict.keys())
                    td_index = 1
                    for td in tds:
                        td_dict[td] = y_index
                        y_indexes_l.append("%s|td%d" % (tg, td_index))
                        y_indexes_r.append("%s|%s" % (comp[0], host))
                        y_index += 1
                        td_index += 1
                    tg_index += 1

        for ti in threadinss:
            ti.plot_y = host_dict[ti.host][ti.component][ti.target][ti.thread]

        return y_indexes_l, y_indexes_r

    def draw_requestins(self, requestins, name):
        assert isinstance(requestins, RequestInstance)
        print("(DrawEngine) drawing %s..." % name)

        ## settings ##
        figsize = (30, 5)
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

        # prepare requests
        start = requestins.start_seconds
        last = requestins.last_seconds
        all_lapse = last - start
        plot_main = requestins.start_interval.is_main

        threadinss = requestins.threadinss
        y_indexes_l, y_indexes_r = self._prepare_thread_indexes(threadinss,
                                                                plot_main)

        # xy axis labels
        ax.set_xlabel("lapse (seconds)")
        ax.set_ylabel("thread by targets")
        ax.set_yticks(range(len(y_indexes_l)))
        ax.set_ylim(.5, len(y_indexes_l)-0.5)
        ax.set_yticklabels(y_indexes_l)
        # ax2 = ax.twinx()
        # ax2.set_ylabel("component by hosts")
        # ax2.set_ylim(0.5, len(y_indexes_r)-0.5)
        # ax2.set_yticks(range(len(y_indexes_r)))
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
            ax.plot(ti.start_seconds-start, ti.plot_y,
                     marker=t_marker,
                     color=color,
                     markersize=markersize)

            # annotate start edge
            ax.annotate("%s" % ti.intervals[0].from_edge.name,
                         (ti.start_seconds-start, ti.plot_y),
                         (ti.start_seconds-start, ti.plot_y+annot_off_y),
                         **annot_kw)

            marker=7
            for int_ in ti.intervals:
                from_x = int_.from_seconds - start
                from_y = int_.threadins.plot_y
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
                    ax.annotate("%s=%.2f" % (int_.node.id_, int_.lapse),
                                 ((from_x + to_x)/2, to_y),
                                 ((from_x + to_x)/2, to_y+annot_off),
                                 **annot_kw)
                elif int_.lapse >= mark_lim:
                    ax.annotate("%s" % int_.node.id_,
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
                ax.plot(int_.to_seconds-start, ti.plot_y,
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
            from_y = int_.from_threadins.plot_y
            to_x = int_.to_seconds - start
            to_y = int_.to_threadins.plot_y
            ti_color = int_.color
            if to_y > from_y:
                connstyle="arc3,rad=-0.05"
            else:
                connstyle="arc3,rad=0.05"
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
        fig.savefig(self.out_path + name + "_requestplot.png")
        print("ok")
