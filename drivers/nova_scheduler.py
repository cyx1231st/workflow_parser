# Copyright (c) 2016 Yingxin Cheng
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

from workflow_parser.log_parser.driver import DriverBase
from workflow_parser.log_parser.driver import rv
from workflow_parser.log_parser.driver import ServiceRegistry


sr = ServiceRegistry()
sr.f_register("nova", "api", "conductor", "scheduler", "compute")

api = sr.nova.api
cond = sr.nova.conductor
sche = sr.nova.scheduler
comp = sr.nova.compute

# name -> id
relations = {}


class NovaScheduler(DriverBase):
    def build_graph(self, graph):
        build = graph.build_edge

        e  = build( 0,  1, api,  "received", True)
        e  = build( 1, 20, api,  "failed:")
        f1 = build( 1,  2, api,  "sent/retried", )
        e  = build( 2, 27, api,  "api returned")

        j1 = build(28,  3, cond, "received")
        e  = build( 3, 21, cond, "failed: attempt")
        e  = build( 3,  4, cond, "attempts")
        f2 = build( 4,  5, cond, "sent scheduler")

        # case 3000
        e  = build( 5, 40, cond, "failed: Timed out")
        f6 = build( 40, 5, cond, "sent scheduler")

        j3 = build( 5, 22, cond, "failed: NoValidHost")
        j4 = build( 5, 13, cond, "decided")
        f5 = build(13, 14, cond, "sent")

        j2 = build(29,  6, sche, "received")
        e  = build( 6,  7, sche, "start scheduling")
        e  = build( 7,  8, sche, "start_db")
        e  = build( 8,  9, sche, "finish_db")
        e  = build( 9, 10, sche, "finish scheduling")
        f4 = build(10, 11, sche, "selected")
        f3 = build(10, 12, sche, "failed:")

        j5 = build(30, 15, comp, "received")
        e  = build(15, 24, comp, "success")
        e  = build(24, 25, comp, "finished: active")
        e  = build(15, 16, comp, "fail: retry")
        f7 = build(16, 31, comp, "sent/retried")
        e  = build(31, 32, comp, "finished: rescheduled")
        e  = build(15, 23, comp, "fail:")
        e  = build(23, 26, comp, "finished:")

        f1.join_edge(j1)
        f6.join_edge(j2)
        f2.join_edge(j2)
        f3.join_edge(j3)
        f4.join_edge(j4, {("t_host", "t_host")})
        f5.join_edge(j5, {("t_host", "host")})
        f7.join_edge(j1)

        graph.set_state(20, "API FAIL")
        graph.set_state(21, "RETRY FAIL")
        graph.set_state(22, "NO VALID HOST")
        graph.set_state(25, "SUCCESS")
        graph.set_state(26, "COMPUTE FAIL")

    def filter_logfile(self, f_dir, f_name):
        if f_name.startswith("out"):
            return False
        if not f_name.startswith("BENCH-"):
            return False
        return True

    def parse_logfilename(self, f_name, var_dict):
        pieces = f_name.split("-")
        component = pieces[1]
        host = "-".join(pieces[2:])

        var_dict[rv.COMPONENT] = component
        var_dict[rv.HOST] = host
        var_dict[rv.TARGET] = component+"@"+host

    def filter_logline(self, line):
        if "BENCH-" not in line:
            return False
        if "Bench initiated!" in line:
            return False
        else:
            return True

    def parse_logline(self, line, var_dict):
        pieces = line.split()

        # seconds
        time_pieces = pieces[1].split(":")
        seconds = int(time_pieces[0]) * 3600 + \
            int(time_pieces[1]) * 60 + \
            float(time_pieces[2])
        var_dict[rv.TIME] = pieces[1]
        var_dict[rv.SECONDS] = seconds

        # component, host
        pieces7 = None
        index = 0
        for piece in pieces:
            if piece.startswith("BENCH-"):
                pieces7 = piece
                break
            index += 1
        pieces7 = pieces7.split('-')
        host_pieces = pieces7[2:]
        host_pieces[-1] = pieces7[-1][:-1]
        var_dict[rv.HOST] = '-'.join(host_pieces)
        var_dict[rv.COMPONENT] = pieces7[1]

        # instance_id, instance_name
        instance_info = pieces[index+1]
        if instance_info == "--":
            pass
        elif "," in instance_info:
            instance_info = instance_info.split(",")
            i_name = instance_info[0]
            i_id = instance_info[1]
            var_dict["instance_name"] = i_name
            var_dict["instance_id"] = i_id
            var_dict[rv.THREAD] = i_id
            var_dict[rv.REQUEST] = i_id

            other_id = relations.get(i_name)
            if other_id:
                assert other_id == i_id
            else:
                relations[i_name] = i_id
        elif len(instance_info) is 36:
            var_dict["instance_id"] = instance_info
            var_dict[rv.THREAD] = instance_info
            var_dict[rv.REQUEST] = instance_info
        else:
            var_dict["instance_name"] = instance_info

        # request_id, action
        var_dict["request_id"] = pieces[index-3][5:]
        keyword = " ".join(pieces[index+2:])
        var_dict[rv.KEYWORD] = keyword

        if "selected" in keyword:
            var_dict["t_host"] = " ".join(keyword.split(" ")[1:])
        if "decided" in keyword:
            var_dict["t_host"] = " ".join(keyword.split(" ")[1:])

    def preprocess_logline(self, logline):
        if "start_db" in logline.keyword:
            assert "start scheduling" in logline.prv_logline.keyword
            assert "instance_id" not in logline
            i_id = logline.prv_logline["instance_id"]
            logline.request = i_id
            logline.thread = i_id
            logline["instance_id"] = i_id
        elif "finish_db" in logline.keyword:
            assert "finish scheduling" in logline.nxt_logline.keyword
            assert "instance_id" not in logline
            i_id = logline.nxt_logline["instance_id"]
            logline.request = i_id
            logline.thread = i_id
            logline["instance_id"] = i_id

        if not logline.request:
            assert "instance_id" not in logline
            i_name = logline["instance_name"]
            i_id = relations.get(i_name)
            if not i_id:
                if str(logline.component) != "api":
                    return i_name
                logline.request = i_name
                logline.thread = i_name
            else:
                logline.request = i_id
                logline.thread = i_id
        return True

    def build_statistics(self, s_engine, report):
        _i_apif = frozenset([(0, 20)])
        _i_api = frozenset([(0, 20), (0, 2)])
        _i_apis = frozenset([(0, 2)])
        _i_atc = frozenset([(1, 3)])
        _i_con1 = frozenset([(2, 5), (2, 21)])
        _i_cts = frozenset([(4, 6)])
        _i_sch = frozenset([(5, 11), (5, 12)])
        _i_stc = frozenset([(10, 13), (10, 22)])
        _i_con2 = frozenset([(11, 14)])
        _i_contc = frozenset([(13, 15)])
        _i_com = frozenset([(14, 2), (14, 23), (14, 25), (14, 26)])
        _i_con = frozenset(_i_con1 | _i_con2)

        _i_fil = frozenset([(6, 10)])
        _i_cac = frozenset([(7, 9)])
        _i_gap = frozenset([(8, 16), (8, 23), (8, 24)])
        _i_sus = frozenset([(0, 25)])
        _i_nvh = frozenset([(0, 22)])
        _i_ret = frozenset([(0, 16)])

        _i_all = [_i_api, _i_con, _i_sch, _i_com]
        _i_cut = [_i_api, _i_con1, _i_con2, _i_sch, _i_com, _i_atc, _i_cts,
                  _i_stc, _i_contc, _i_fil, _i_cac, _i_gap, _i_sus, _i_apif,
                  _i_nvh, _i_ret, _i_apis]
        cut_edge = s_engine.graph.get_edge(16, 2)

        all_, cut, cutted = s_engine.extract_intervals(_i_all, _i_cut, cut_edge)

        report.register("active schedulers",
                        s_engine.active_by_service.get("scheduler", 0))
        report.register("active computes",
                        s_engine.active_by_service.get("compute", 0))
        report.blank()
        report.register("total requests",
                        s_engine.total_requests)
        report.register("success requests",
                        s_engine.requests_by_state.get("SUCCESS", 0))
        report.register("nvh requests",
                        s_engine.requests_by_state.get("NO VALID HOST", 0))
        report.register("rtf requests",
                        s_engine.requests_by_state.get("RETRY FAIL", 0))
        report.register("api fail requests",
                        s_engine.requests_by_state.get("API FAIL", 0))
        report.register("compute fail requests",
                        s_engine.requests_by_state.get("COMPUTE FAIL", 0))
        report.register("error requests",
                        len(s_engine.error_instances))
        report.blank()
        report.register("total valid queries",
                        s_engine.count(3))
        report.register("direct successful queries",
                        len(cut[_i_sus]))
        report.register("direct nvh queries",
                        len(cut[_i_nvh]))
        report.register("direct retried queries",
                        len(cut[_i_ret]))
        report.register("retry successful queries",
                        s_engine.count(25) - len(cut[_i_sus]))
        report.register("retry nvh queries",
                        s_engine.count(22) - len(cut[_i_nvh]))
        report.register("retry retried queries",
                        s_engine.count(16) - len(cut[_i_ret]))
        report.blank()
        report.register("wallclock total",
                        s_engine.intervals_requests.wall_time())
        report.register("wallclock api",
                        s_engine.intervals_by_services["api"].wall_time())
        report.register("wallclock conductor",
                        s_engine.intervals_by_services["conductor"].wall_time())
        report.register("wallclock scheduler",
                        s_engine.intervals_by_services["scheduler"].wall_time())
        report.register("wallclock compute",
                        s_engine.intervals_by_services["compute"].wall_time())
        report.blank()
        report.register("time query avg", cut[_i_sus].average())
        report.register("time inapi avg", cut[_i_api].average())
        report.register("time a-con avg", cut[_i_atc].average())
        report.register("time cond1 avg", cut[_i_con1].average())
        report.register("time c-sch avg", cut[_i_cts].average())
        report.register("time sched avg", cut[_i_sch].average())
        report.register("time s-con avg", cut[_i_stc].average())
        report.register("time cond2 avg", cut[_i_con2].average())
        report.register("time c-com avg", cut[_i_contc].average())
        report.register("time compu avg", cut[_i_com].average())
        report.blank()
        report.register("time filter avg", cut[_i_fil].average())
        report.register("time refresh avg", cut[_i_cac].average())
        report.register("time gap avg", cut[_i_gap].average())
        report.blank()
        sum_query_avg = cut[_i_api].average()\
                        + cut[_i_atc].average()\
                        + cut[_i_con1].average()\
                        + cut[_i_cts].average()\
                        + cut[_i_sch].average()\
                        + cut[_i_stc].average()\
                        + cut[_i_con2].average()\
                        + cut[_i_contc].average()\
                        + cut[_i_com].average()
        report.register("percent api part",
                        cut[_i_api].average() / sum_query_avg * 100)
        report.register("percent cond part",
                        (cut[_i_con1].average() + cut[_i_con2].average())
                        / sum_query_avg * 100)
        report.register("percent sch part",
                        cut[_i_sch].average() / sum_query_avg * 100)
        report.register("percent comp part",
                        cut[_i_com].average() / sum_query_avg * 100)
        report.register("percent msg part",
                        (cut[_i_atc].average() + cut[_i_cts].average()
                         + cut[_i_stc].average() + cut[_i_contc].average())
                        / sum_query_avg * 100)
        report.register("percent filter part",
                        cut[_i_fil].average() / sum_query_avg * 100)
        report.register("percent refresh part",
                        cut[_i_cac].average() / sum_query_avg * 100)
        report.register("percent gap part",
                        cut[_i_gap].average() / sum_query_avg * 100)
        report.blank()
        report.register("request per sec",
                        (s_engine.total_requests -
                         s_engine.requests_by_state.get("API FAIL", 0))
                        / s_engine.intervals_requests.wall_time())
        report.register("query per sec",
                        s_engine.count(3)
                        / s_engine.intervals_requests.wall_time())
        report.register("success per sec",
                        s_engine.requests_by_state.get("SUCCESS", 0)
                        / s_engine.intervals_requests.wall_time())
        report.blank()
        report.register("percent query retry",
                        s_engine.count(16)
                        / float(s_engine.count(1) - s_engine.count(20)) * 100)
        report.register("percent api fail",
                        s_engine.count(20) / float(s_engine.count(1)) * 100)


if __name__ == "__main__":
    driver = NovaScheduler(sr)
    driver.cmdrun()
