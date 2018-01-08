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


class CephRbdimagereq(DriverBase):
    def build_graph(self, graph):
#### functions ####
        # function oreq send
        e0 , n1 , f_osend = graph.build_func(
                               1, "imagerequest_sendoreq_entry", "osend")
        e  , _   =  n1.build_endf("imagerequest_sendoreq_exit")

        # function imagereq send
        e  , n1 , f_isend = graph.build_func(
                               1, "imagerequest_send_entry", "isend")
        ## fail
        e  , n2  =  n1.build(  2, "ireqcomp_fail_entry")
        e  , n3  =  n2.build(  3, "ireqcomp_complete")
        e  , n4  =  n3.build(  4, "ireqcomp_fail_exit")
        e1 , _   =  n4.build_endf("imagerequest_send_exit")
        ## not fail
        e  , n5  =  n1.build(  5, "imagerequest_toextents")
        ### send oreqs
        e2 , n6  =  n5.build(  6, f_osend)
        _  , _   =  n6.build( n6, e2)
        _  , _   =  n6.build_endf(e1)
        ### journal
        e3 , n7  =  n5.build(  7, "rbdjournal_enqueue")
        _  , _   =  n7.build_endf(e1)
        ### objectcacher
        e12, n8  =  n5.build(  8, "imagectx_writex_entry")
        e13, n9  =  n8.build(  9, "imagectx_writex_exit")
        _  , _   =  n9.build( n8, e12)
        _  , _   =  n9.build_endf(e1)

        end1 = n3

#### request imagereq ####
        # receive aio_write
        e  , n1  = graph.build_thread(client,
                               1, "imagerequestwq_entry", "imagereq")
        ## enqueue ireq
        e4 , n2  =  n1.build(  2, "imagerequestwq_enqueue")
        e5 , n3  =  n2.build(  3, "imagerequestwq_exit")
        ## send ireq directly
        e  , n4  =  n1.build(  4, f_isend)
        _  , _   =  n4.build( n3, e5)

        # dequeue ireq
        e6 , n5  = graph.build_thread(client,
                               5, "imagerequestwq_dequeue_entry")
        e  , n6  =  n5.build(  6, f_isend)
        e  , n7  =  n6.build(  7, "imagerequestwq_dequeue_exit")

        # dequeue journal
        e7 , n10 = graph.build_thread(client,
                              10, "rbdjournal_dequeue_entry")
        ## oreq fail
        e  , n11 = n10.build( 11, "rbdjournal_failoreq")
        e  , n12 = n11.build( 12, "ireqcomp_finish_entry")
        e  , n13 = n12.build( 13, "ireqcomp_complete")
        e9 , _   = n13.build(n10, "ireqcomp_finish_exit")
        _  , _   = n12.build(n10, e9)
        ## send oreqs
        e  , _   = n10.build(n10, f_osend)
        e  , n14 = n10.build( 14, "rbdjournal_dequeue_exit")

        # aio complete
        e10, n15 = graph.build_thread(client,
                              15, "ireqcomp_finish_entry")
        e  , n16 = n15.build( 16, "ireqcomp_complete")
        e  , n17 = n16.build( 17, "ireqcomp_finish_exit")

#### request imagereq states ####
        end1.set_state("FAIL")
        n13.set_state("FAIL_JOURNAL")
        n16.set_state("SUCCESS")

#### relationship ####
        # oreq completes
        j1 =  e0.join_one(e10, False, ["comp"])

        # journal enqueue
        j2 =  e3.join_one( e7, False, ["jtid"])

        # writex completes
        j3 = e12.join_one(e10, False, ["comp"])

        # ireq enqueue
        j4 =  e4.join_one( e6, False, ["ireq"])


    def filter_logfile(self, f_dir, f_name, var_dict):
        if f_name.startswith("out"):
            return False
        else:
            var_dict[rv.HOST] = f_name
            return True


    def filter_logline(self, line, var_dict):
        if " rbdimagereq:" not in line:
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
    driver = CephRbdimagereq(sr, extensions=["ctraces"])
    driver.cmdrun()
