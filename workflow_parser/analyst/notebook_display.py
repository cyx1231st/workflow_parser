import pandas as pd
from ipywidgets import interact
from ipywidgets import fixed
from ipywidgets import widgets
from itertools import chain

from ..workflow.entities.join import RequestjoinActivity
from ..workflow.entities.request import RequestInstance

from .automated_suite import generate_reports
from .statistics_engine import generate_dataframes
from .draw_engine import DrawEngine
from .draw_engine import getcolor_byint
from .draw_engine import (REMOTE_C, LOCALREMOTE_C, LOCAL_C)


class Requests_D(object):
    def __init__(self, name, requestins_byreq, graph):
        self.name = "reqs(%s)<<%s" % (graph.name, name)
        self.graph = graph
        self._draw_engine = DrawEngine(None)
        self._requestins_byreq = requestins_byreq

        ret = generate_dataframes(requestins_byreq)
        (requestinss_bytype,
         targetobjs_bytarget,
         self._workflow_bytype,
         # seconds -> start/end/last
         # time -> start/end/last
         self._start_end,
         df_ints_join_all,
         df_ints_thread,
         # *"request",
         # "request_type",
         # "int_name",
         # "lapse",
         # "path",
         # "from_seconds",
         # "to_seconds",
         # "from_time",
         # "to_time",
         # "from_keyword",
         # "to_keyword",
         # "int_type",
         # ---------
         # "request_state",
         # "last_seconds",
         # "last_time",
         # "len_paces",
         # "len_main_paces",
         # "len_threads",
         # "len_threadinss",
         # "len_targets",
         # "len_hosts"
         self.df_requests,
         # *"target",
         # "component",
         # "host"
         self.df_targets) = ret
        df_ints_join = df_ints_join_all[df_ints_join_all["int_type"]!=RequestjoinActivity.__name__]
        self._intervals_d = Intervals_D(
                "Ints<<"+self.name, self, None, df_ints_thread, df_ints_join)
        (self._report_r, self._report_i) = generate_reports(name, graph, *ret)

    @property
    def Intervals(self):
        return self._intervals_d

    def __repr__(self):
        ret = "<Requests %s: num_reqs=%d, graph=%s, lapse=[%s, %s]>" % (
                self.name, len(self._requestins_byreq),
                self.graph.name,
                self._start_end["time"]["start"],
                self._start_end["time"]["end"])
        return ret

    def __str__(self):
        ret = repr(self)
        ret += "\n\n" + str(self._report_i)
        for rtype, workflow in self._workflow_bytype.items():
            ret += "\n\n" + str(workflow)
        ret += "\n\n" + str(self._report_r)
        return ret

    def _ipython_display_(self):
        print(repr(self))
        self.display_profiling()

    def display_profiling(self):
        for r_type, workflow in self._workflow_bytype.items():
            self._draw_engine.draw_workflow(self._start_end,
                                            workflow,
                                            r_type)

    def display_dist_hosts(self):
        self._draw_engine.draw_countplot(
                self.df_requests["len_hosts"],
                "request_hosts")

    def display_dist_targets(self):
        self._draw_engine.draw_countplot(
                self.df_requests["len_targets"],
                "request_targets")

    def display_dist_lapse(self):
        self._draw_engine.draw_distplot(
                self.df_requests["lapse"],
                "request_lapse")

    def display_dist_paces(self):
        self._draw_engine.draw_countplot(
                self.df_requests["len_paces"],
                "request_paces")

    def display_dist_paces_main(self):
        self._draw_engine.draw_countplot(
                self.df_requests["len_main_paces"],
                "request_paces_main")

    def display_dist_threadinss(self):
        self._draw_engine.draw_countplot(
                self.df_requests["len_threadinss"],
                "request_threadinss")

    def display_dist_threads(self):
        self._draw_engine.draw_countplot(
                self.df_requests["len_threads"],
                "request_threads")

    def find_req_byname(self, name):
        try:
            req = self.df_requests.loc[name]["_entity"]
        except KeyError:
            raise KeyError("Cannot find request %s in %s" % (name, self.name))
        return Request_D(req, "%s<<%s" % (name, self.name))

    def find_req_longest(self):
        req = self.df_requests\
                .loc[self.df_requests["lapse"].idxmax()]\
                ["_entity"]
        return Request_D(req, "longest<<%s" % self.name)

    def find_req_shortest(self):
        req = self.df_requests\
                .loc[self.df_requests["lapse"].idxmin()]\
                ["_entity"]
        return Request_D(req, "shortest<<%s" % self.name)

    def find_req_mostcomplex(self):
        req = self.df_requests\
                .loc[self.df_requests["len_paces"].idxmax()]\
                ["_entity"]
        return Request_D(req, "mostcomplex<<%s" % self.name)

    def find_req_simplest(self):
        req = self.df_requests\
                .loc[self.df_requests["len_paces"].idxmin()]\
                ["_entity"]
        return Request_D(req, "simplest<<%s" % self.name)

    def list_req_names(self):
        return self.df_requests.index


class Request_D(object):
    def __init__(self, requestins, name):
        assert isinstance(requestins, RequestInstance)
        self.name = name
        self.requestins = requestins
        self._draw_engine = DrawEngine(None)

    def __repr__(self):
        return self.name + ": " + repr(self.requestins)

    def __str__(self):
        return self.name + ": " + str(self.requestins)

    def _ipython_display_(self):
        print(self.name + ":")
        print(repr(self.requestins))
        for k, v in self.requestins.request_vars.items():
            print("    %s: %s" % (k, v))
        self._draw_engine.draw_requestins(self.requestins, self.name)


class BaseIntervals_D(object):
    # "request",
    # "request_type",
    # "int_name",
    # "lapse",
    # "path",
    # "from_seconds",
    # "to_seconds",
    # "from_time",
    # "to_time",
    # "from_keyword",
    # "to_keyword",
    # "int_type",
    # "is_main",
    # "order"
    def __init__(self, name, requests_d, palettes, df_ints):
        if palettes is None:
            palettes = {}
            palettes_desc = {}
            for r in df_ints.iterrows():
                row = r[1]
                palettes_desc[row["desc"]] = getcolor_byint(
                        row["_entity"], ignore_lr=True)
            palettes["desc"] = palettes_desc
            palettes_iname = {}
            for r in df_ints.iterrows():
                row = r[1]
                palettes_iname[row["int_name"]] = getcolor_byint(
                        row["_entity"], ignore_lr=True)
            palettes["iname"] = palettes_iname
            palettes_rtype = {}
            palettes_rtype["local"] = LOCAL_C
            palettes_rtype["remote"] = REMOTE_C
            palettes_rtype["local_remote"] = LOCALREMOTE_C
            palettes["rtype"] = palettes_rtype

        self.name = name
        self._requests_d = requests_d
        self._draw_engine = requests_d._draw_engine
        self._palettes = palettes
        self.df_ints = df_ints

    @property
    def Mains(self):
        return self.__class__(
                "mains<<"+self.name,
                self._requests_d,
                self._palettes,
                self.df_ints[self.df_ints["is_main"]==True])

    def __repr__(self):
        return "<%s %s: num_ints=%d>" % (self.__class__.__name__,
                                         self.name,
                                         len(self.df_ints))

    def _ipython_display_(self):
        print(repr(self))
        self.display_lapse_bypath()

    def display_lapse_bypath(self):
        self._draw_engine.draw_boxplot(
                self.df_ints,
                "%s lapse_bypath" % self.name,
                "desc",
                "lapse",
                palette=self._palettes["desc"])

    def display_lapse_byiname(self):
        self._draw_engine.draw_boxplot(
                self.df_ints,
                "%s lapse_byiname" % self.name,
                "int_name",
                "lapse",
                palette=self._palettes["iname"])

    def display_lapse_bytime(self):
        s_x = (self.df_ints.from_seconds + self.df_ints.to_seconds)/2
        s_x.name = "time(s)"
        self._draw_engine.draw_kdeplot(
                s_x, self.df_ints.lapse,
                "%s lapse_bytime" % self.name)

    def display_lapse_byorder(self):
        self._draw_engine.draw_boxplot(
                self.df_ints,
                "%s lapse_byorder" % self.name,
                "order",
                "lapse",
                palette=None)

    def filter_bypath(self, path):
        assert isinstance(path, str)
        return self.__class__(
                ("ints(%s)<<"%path)+self.name,
                self._requests_d,
                self._palettes,
                self.df_ints[self.df_ints["path"] == path])

    def filter_byiname(self, iname):
        assert isinstance(iname, str)
        return self.__class__(
                ("ints(%s)<<"%iname)+self.name,
                self._requests_d,
                self._palettes,
                self.df_ints[self.df_ints["int_name"] == iname])

    def filter_byorder(self, order):
        assert isinstance(order, int)
        return self.__class__(
                ("ints_order(%d)<<"%order)+self.name,
                self._requests_d,
                self._palettes,
                self.df_ints[self.df_ints["order"] == order])

    def find_req_longest(self):
        req_name = self.df_ints\
                .loc[self.df_ints["lapse"].idxmax()]\
                ["request"]
        if req_name:
            req = self._requests_d.find_req_byname(req_name)
            req.name = "shortest<<%s" % self.name
            return req
        else:
            return None

    def find_req_shortest(self):
        req_name = self.df_ints\
                .loc[self.df_ints["lapse"].idxmin()]\
                ["request"]
        if req_name:
            req = self._requests_d.find_req_byname(req_name)
            req.name = "shortest<<%s" % self.name
            return req
        else:
            return None

    def list_paths(self):
        return sorted(self.df_ints.path.unique())

    def list_inames(self):
        return sorted(self.df_ints.int_name.unique())

    def list_orders(self):
        return sorted(self.df_ints.order.unique())



class Intervals_D(BaseIntervals_D):
    def __init__(self, name, requests_d, palettes, df_ints_thread, df_ints_join):
        self._df_ints_thread = df_ints_thread
        self._df_ints_join = df_ints_join
        df_ints = pd.concat([self._df_ints_thread, self._df_ints_join],
                             join="inner", ignore_index=True)
        super(Intervals_D, self).__init__(name, requests_d, palettes, df_ints)

    @property
    def Mains(self):
        return Intervals_D(
                "mains<<"+self.name,
                self._requests_d,
                self._palettes,
                self._df_ints_thread[self._df_ints_thread["is_main"]==True],
                self._df_ints_join[self._df_ints_join["is_main"]==True])

    @property
    def Joins(self):
        return JoinIntervals_D(
                "joins<<"+self.name,
                self._requests_d,
                self._palettes,
                self._df_ints_join)

    @property
    def Threads(self):
        return TdIntervals_D(
                "tds<<"+self.name,
                self._requests_d,
                self._palettes,
                self._df_ints_thread)

    def filter_bypath(self, path):
        assert isinstance(path, str)
        df_ints_thread = self._df_ints_thread[
                self._df_ints_thread["path"] == path]
        df_ints_join = self._df_ints_join[
                self._df_ints_join["path"] == path]
        if len(df_ints_thread) and len(df_ints_join):
            raise RuntimeError("path %s contains both join/tdintervals!" % path)
        elif len(df_ints_thread):
            return TdIntervals_D(
                    ("ints(%s)<<"%path)+self.name,
                    self._requests_d,
                    self._palettes,
                    df_ints_thread)
        elif len(df_ints_join):
            return JoinIntervals_D(
                    ("ints(%s)<<"%path)+self.name,
                    self._requests_d,
                    self._palettes,
                    df_ints_join)
        else:
            return None

    def filter_byiname(self, iname):
        assert isinstance(iname, str)
        df_ints_thread = self._df_ints_thread[
                self._df_ints_thread["int_name"] == iname]
        df_ints_join = self._df_ints_join[
                self._df_ints_join["int_name"] == iname]
        if len(df_ints_thread) and len(df_ints_join):
            raise RuntimeError("int_name %s contains both join/tdintervals!" % iname)
        elif len(df_ints_thread):
            return TdIntervals_D(
                    ("ints(%s)<<"%iname)+self.name,
                    self._requests_d,
                    self._palettes,
                    df_ints_thread)
        elif len(df_ints_join):
            return JoinIntervals_D(
                    ("ints(%s)<<"%iname)+self.name,
                    self._requests_d,
                    self._palettes,
                    df_ints_join)
        else:
            return None

    def filter_byorder(self, order):
        assert isinstance(order, int)
        df_ints_thread = self._df_ints_thread[
                self._df_ints_thread["order"] == order]
        df_ints_join = self._df_ints_join[
                self._df_ints_join["order"] == order]
        if len(df_ints_thread) == 0 and len(df_ints_join) == 0:
            return None
        else:
            return self.__class__(
                    ("ints_order(%d)<<"%order)+self.name,
                    self._requests_d,
                    self._palettes,
                    df_ints_thread,
                    df_ints_join)


class JoinIntervals_D(BaseIntervals_D):
    # "remote_type",
    # "from_target",
    # "to_target",
    # "from_host",
    # "to_host",
    # "from_component",
    # "to_component",
    # "from_thread",
    # "to_thread"
    def __init__(self, name, requests_d, palettes, df_ints_join):
        self._df_rel_host = df_ints_join\
                .loc[df_ints_join["remote_type"] != "local"]\
                .groupby(["from_host", "to_host"])\
                .size()\
                .unstack()\
                .fillna(0)\
                .astype("int")
        self._df_rel_host.index.name = "from_host"
        self._df_rel_host.columns.name = "to_host"

        self._df_rel_component = df_ints_join\
                [df_ints_join["remote_type"]!="locol"]\
                .groupby(["from_target", "to_target"])\
                .size()\
                .reset_index()\
                .join(requests_d.df_targets, on="from_target")\
                .join(requests_d.df_targets, on="to_target",
                        lsuffix="_from", rsuffix="_to")\
                .groupby(["component_from", "component_to"])\
                .mean()\
                .unstack()
        self._df_rel_component.index.name = "from_component"
        self._df_rel_component.columns =\
                self._df_rel_component.columns.droplevel()
        self._df_rel_component.columns.name = "to_component"

        super(JoinIntervals_D, self).__init__(
                name, requests_d, palettes, df_ints_join)

    def display_lapse_byrtype(self):
        self._draw_engine.draw_boxplot(
                self.df_ints,
                "%s lapse_byrtype" % self.name,
                "remote_type",
                "lapse",
                palette=self._palettes["rtype"])

    def display_lapse_byhosts(self):
        self._draw_engine.draw_boxplot(
                self.df_ints,
                "%s lapse_byhosts" % self.name,
                "hosts",
                "lapse",
                palette=None)

    def display_rel_host(self):
        self._draw_engine.draw_relation_heatmap(
                self._df_rel_host,
                "host_remote_relations")

    def display_rel_comp(self):
        self._draw_engine.draw_relation_heatmap(
                self._df_rel_component,
                "component_remote_relations",
                "f")


class TdIntervals_D(BaseIntervals_D):
    # "target",
    # "host",
    # "component",
    # "thread"
    def __init__(self, name, requests_d, palettes, df_ints_thread):
        super(TdIntervals_D, self).__init__(
                name, requests_d, palettes, df_ints_thread)

    def display_lapse_byhost(self):
        self._draw_engine.draw_boxplot(
                self.df_ints,
                "%s lapse_byhost" % self.name,
                "host",
                "lapse",
                palette=None)
