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

from workflow_parser.driver import DriverBase
from workflow_parser.driver import rv
from workflow_parser.driver import ServiceRegistry


sr = ServiceRegistry()
sr.f_register("ceph", "client")

client = sr.ceph.client

client.color = "#54c0e8"


class CephRbdobjectreq(DriverBase):
    def build_graph(self, graph):
#### functions ####
        # function aio-operate send
        e1 , n1 , f_write = graph.build_func(
                               1, "oreq_aiooperate_entry", "write")
        e  , _   =  n1.build_endf("oreq_aiooperate_exit")

        # function copyup send
        e2 , n1 , f_copyup = graph.build_func(
                               1, "oreq_copyupsend_entry", "copyup")
        e  , _   =  n1.build_endf("oreq_copyupsend_exit")

        # function finish
        e  , n1 , f_finish = graph.build_func(
                               1, "oreq_finish_entry", "finish")
        e  , _   =  n1.build_endf("oreq_finish_exit")
        end1 = n1

        # function postomap
        e  , n1 , f_postom = graph.build_func(
                               1, "oreq_postomap_entry", "postom")
        ## finish
        e  , n2  =  n1.build(  2, f_finish)
        e3 , n3  =  n2.build(  3, "oreq_postomap_exit")
        ## postomap send
        e4 , n4  =  n1.build(  4, "oreq_postomap_send")
        ### success
        _  , _   =  n4.build( n3, e3)
        ### skip
        e  , n5  =  n4.build(  5, "oreq_postomap_skip")
        e  , n6  =  n5.build(  6, f_finish)
        _  , _   =  n6.build( n3, e3)

#### request objectreq ####
        # receive objectreq
        e11, n1  = graph.build_thread(client,
                               1, "oreq_send_entry", "objectreq")
        ## fail async
        e5 , n2  =  n1.build(  2, "oreq_send_fail")
        e6 , n3  =  n2.build(  3, "oreq_send_exit")
        ## write
        e  , n4  =  n1.build(  4, f_write)
        _  , _   =  n4.build( n3, e6)
        ## copyup
        e  , n5  =  n1.build(  5, f_copyup)
        _  , _   =  n5.build( n3, e6)
        ## send preomap
        e7 , n6  =  n1.build(  6, "oreq_preomap_send")
        ### success
        _  , _   =  n6.build( n3, e6)
        ### skip
        _  , n7  =  n6.build(  7, "oreq_preomap_skip")
        _  , n8  =  n7.build(  8, f_write)
        _  , _   =  n8.build( n3, e6)

        # handle preomap
        e8 , n10 = graph.build_thread(client,
                              10, "oreq_handlepreomap_entry")
        _  , n11 = n10.build( 11, f_write)
        e  , n12 = n11.build( 12, "oreq_handlepreomap_exit")

        # handle write
        e9 , n15 = graph.build_thread(client,
                              15, "oreq_handlewrite_entry")
        ## finish: fail
        e  , n16 = n15.build( 16, f_finish)
        e10, n17 = n16.build( 17, "oreq_handlewrite_exit")
        ## copyup
        e  , n18 = n15.build( 18, f_copyup)
        _  , _   = n18.build(n17, e10)
        ## postomap
        e  , n19 = n15.build( 19, f_postom)
        _  , _   = n19.build(n17, e10)

        # handle copyup
        e15, n25 = graph.build_thread(client,
                              25, "oreq_handlecopyup_entry")
        ## finish
        e  , n26 = n25.build( 26, f_finish)
        e16, n27 = n26.build( 27, "oreq_handlecopyup_exit")
        ## write
        e  , n28 = n25.build( 28, f_write)
        _  , _   = n28.build(n27, e16)
        ## postomap
        e  , n29 = n25.build( 29, f_postom)
        _  , _   = n29.build(n27, e16)

        # handle postomap
        e20, n35 = graph.build_thread(client,
                              35, "oreq_handlepostomap_entry")
        _  , n36 = n35.build( 36, f_finish)
        e  , n37 = n36.build( 37, "oreq_handlepostomap_exit")

        # finish thread
        e25, n40 = graph.build_thread(client,
                              40, "oreq_finish_entry")
        _  , n41 = n40.build( 41, "oreq_finish_exit")

#### request imagereq states ####
        n40.set_state("SUCCESS")
        end1.set_state("FAIL_ASYNC")
        e11.refresh_var("oreq")

#### relationship ####
        # preomap
        j1 =  e7.join_one( e8, False, ["oreq"])

        # write
        j2 =  e1.join_one( e9, False, ["oreq"])

        # copyup
        j3 =  e2.join_one(e15, False, ["oreq"])

        # postomap
        j4 =  e4.join_one(e20, False, ["oreq"])

        # finish async
        j5 =  e5.join_one(e25, False, ["oreq"])


    def filter_logfile(self, f_dir, f_name, var_dict):
        if f_name.startswith("out"):
            return False
        else:
            var_dict[rv.HOST] = f_name
            return True


    def filter_logline(self, line, var_dict):
        if " rbdobjectreq:" not in line:
            return False

        # time, seconds
        lines = line.split(" ", 1)
        line = lines[1]
        time = lines[0][1:-1]
        var_dict[rv.TIME] = time
        _time_s = time.split(":")
        seconds = int(_time_s[0]) * 3600 + int(_time_s[1]) * 60 + float(_time_s[2])
        var_dict[rv.SECONDS] = seconds

        # component, target
        lines = line.split(" ", 2)
        line = lines[2]
        _comp = lines[1].split(":")
        comp = _comp[1]
        if comp == "python":
            comp = client
        elif comp == "fio":
            comp = client
        else:
            raise RuntimeError("Unknown component: %s" % comp)
        var_dict[rv.COMPONENT] = comp
        target_alias = _comp[1] + ":" + _comp[2]
        var_dict[rv.TARGET] = target_alias

        # keyword
        lines = line.split(" ", 1)
        line = lines[1]
        var_dict[rv.KEYWORD] = lines[0].split(":", 1)[1][:-1]

        def _convert(dict_str):
            try:
                ret = {}
                dict_str = dict_str.strip()
                if dict_str:
                    items = dict_str.split(", ")
                    for item in items:
                        k, v = item.strip().split(" = ", 1)
                        k = k.strip()
                        ret[k] = eval(v.strip())
            except Exception:
                import pdb; pdb.set_trace()
                raise RuntimeError("Cannot evaluate %s" % dict_str)
            return ret

        # thread
        lines = line.split(" }, { ")
        # dict_ = _convert(lines[0].strip()[1:])
        # var_dict[rv.THREAD] = str(dict_["cpu_id"])
        dict1_ = _convert(lines[1])
        var_dict.update(dict1_)
        dict2_ = _convert(lines[2].strip()[:-1])
        var_dict.update(dict2_)
        var_dict[rv.THREAD] = str(var_dict["pthread_id"])

        return True

if __name__ == "__main__":
    driver = CephRbdobjectreq(sr, extensions=["ctraces"])
    driver.cmdrun()
