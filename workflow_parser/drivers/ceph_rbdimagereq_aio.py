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

#TODO: remove imagerequestwq_fail

from workflow_parser.driver import init
from workflow_parser.driver import register_driver


sr, graph, rv = init("CephRbdimagereq")

#### services ####
sr.f_register("ceph", "client")
client = sr.ceph.client
client.color = "#54c0e8"


#### functions ####
# function comp_fail
e  , n1 , f_fail = graph.build_func(
                       1, "ireqcomp_fail_entry", "fail")
e  , n2  =  n1.build(  2, "ireqcomp_complete")
e30, n3  =  n2.build_endf("ireqcomp_fail_exit")
_  , _   =  n1.build_endf(e30)

end1 = n2

# function oreq dispatch
e0 , n1 , f_odisp = graph.build_func(
                       1, "imagerequest_sendoreq_entry", "osend")
## writex
e20, n2  =  n1.build(  2, "dispatch_writex_entry")
e24, n3  =  n2.build(  3, "dispatch_writex_exit")
e  , n8  =  n2.build(  8, "dispatch_write_entry")
e  , n9  =  n8.build(  9, "dispatch_write_exit")
_  , _   =  n9.build( n3, e24)
e21, _   =  n3.build_endf("imagerequest_sendoreq_exit")
## journal
e22, n4  =  n1.build(  4, "dispatch_journal_entry")
e  , n5  =  n4.build(  5, "dispatch_journal_exit")
_  , _   =  n5.build_endf(e21)
## write
e23, n6  =  n1.build(  6, "dispatch_write_entry")
e  , n7  =  n6.build(  7, "dispatch_write_exit")
_  , _   =  n7.build_endf(e21)

# function imagereq send
e  , n1 , f_isend = graph.build_func(
                       1, "imagerequest_entry", "isend")
## fail
e  , n2  =  n1.build(  2, f_fail)
e1 , _   =  n2.build_endf("imagerequest_exit")
## without imagecache
e  , n5  =  n1.build(  5, "imagerequest_toextents")
e2 , n6  =  n5.build(  6, f_odisp)
_  , _   =  n6.build( n6, e2)
_  , _   =  n6.build_endf(e1)
## with imagecache
e  , n7  =  n1.build(  7, "imagerequest_icache_entry")
e  , n8  =  n7.build(  8, "imagerequest_icache_exit")
_  , _   =  n8.build_endf(e1)

#### request imagereq ####

# receive aio_write
e  , n1  = graph.build_thread(client,
                       1, "imagerequestwq_entry", "imagereq")
## enqueue ireq
e4 , n2  =  n1.build(  2, "imagerequestwq_enqueue_entry")
e  , n3  =  n2.build(  3, "imagerequestwq_enqueue_exit")
e5 , n4  =  n3.build(  4, "imagerequestwq_exit")
## send ireq directly
e  , n5  =  n1.build(  5, f_isend)
_  , _   =  n5.build( n4, e5)
## fail
e  , n6  =  n1.build(  6, f_fail)
_  , _   =  n6.build( n4, e5)

# dequeue ireq
e6 , n7  = graph.build_thread(client,
                       7, "imagerequestwq_dequeue_entry")
e  , n8  =  n7.build(  8, f_isend)
e  , n9  =  n8.build(  9, "imagerequestwq_dequeue_exit")

# aio complete
e10, n15 = graph.build_thread(client,
                      15, "dispatch_comp_entry")
## dispatch failed
e  , n16 = n15.build( 16, "dispatch_comp_fail")
e25, n17 = n16.build( 17, "ireqcomp_finish_entry")
e  , n18 = n17.build( 18, "ireqcomp_complete")
e26, n19 = n18.build( 19, "ireqcomp_finish_exit")
_  , _   = n17.build(n19, e26)
e27, n20 = n19.build( 20, "dispatch_comp_exit")
## dispatch finish
e  , n21 = n15.build( 21, "dispatch_comp_fin")
_  , _   = n21.build(n17, e25)
## dispatch continue
e28, n22 = n15.build( 22, "dispatch_write_entry")
e  , n23 = n22.build( 23, "dispatch_write_exit")
_  , _   = n23.build(n20, e27)

#### request imagereq states ####
end1.set_state("FAIL")
n18.set_state("SUCCESS")
#e0.refresh_var("comp")
e20.refresh_var("spec_comp")
e22.refresh_var("spec_comp")
e23.refresh_var("spec_comp")
e4.refresh_var("ireq_spec")

#### relationship ####
# oreq dispatch
j1 = e20.join_one(e10, False, ["spec_comp"])
j2 = e22.join_one(e10, False, ["spec_comp"])
j3 = e23.join_one(e10, False, ["spec_comp"])

# ireq enqueue/dequeue
j4 =  e4.join_one( e6, False, ["ireq_spec"])


def filter_logfile(f_dir, f_name, var_dict):
    if f_name.startswith("out"):
        return False
    else:
        var_dict[rv.HOST] = f_name.rsplit(".", 1)[0]
        return True


def filter_logline(line, var_dict):
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
    elif comp == "qemu-system-x86":
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


register_driver(
        __name__, sr, graph,
        filter_logfile, filter_logline,
        ["ctraces"])
