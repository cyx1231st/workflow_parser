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

from workflow_parser.driver import init
from workflow_parser.driver import register_driver


sr, graph, rv = init("CephRadosWriteoperation")

#### services ####
sr.f_register("ceph", "osd", "client")
osd = sr.ceph.osd
client = sr.ceph.client
osd.color = "#9c51eb"
client.color = "#54c0e8"


#### functions ####
# function message send osd
e1 , n1 , f_sendosd = graph.build_func(
                       1, "rosd_sendosd_entry", "sdosd")
e  , _   =  n1.build_endf("rosd_sendosd_exit")

# function message send client
e5 , n1 , f_sendcli = graph.build_func(
                       1, "posd_sendcli_entry", "sdcli")
e  , _   =  n1.build_endf("posd_sendcli_exit")

# function main commit
e30, n1 , f_mcommit = graph.build_func(
                       1, "rosd_opcommit_entry", "mcommit")
e6 , _   =  n1.build_endf("rosd_opcommit_exit")
_  , n2  =  n1.build(  2, f_sendcli)
_  , _   =  n2.build_endf(e6)

# function main apply
e35, n1 , f_mapply = graph.build_func(
                       1, "rosd_opapply_entry", "mapply")
e7 , _   =  n1.build_endf("rosd_opapply_exit")
_  , n2  =  n1.build(  2, f_sendcli)
_  , _   =  n2.build_endf(e7)

# function replica commit
e40, n1 , f_rcommit = graph.build_func(
                       1, "rosd_repopcommit_entry", "rcommit")
_  , n2  =  n1.build(  2, f_sendosd)
e  , _   =  n2.build_endf("rosd_repopcommit_exit")

# function replica apply
e45, n1 , f_rapply = graph.build_func(
                       1, "rosd_repopapply_entry", "rapply")
e8 , _   =  n1.build_endf("rosd_repopapply_exit")
_  , n2  =  n1.build(  2, f_sendosd)
_  , _   =  n2.build_endf(e8)

#### request write_full ####
# thread client issue writefull
e  , n1  = graph.build_thread(client,
                       1, "ioctx_radoswrite_entry", "radoswrite")
e  , n2  =  n1.build(  2, "objecter_calculate_entry")
e  , n3  =  n2.build(  3, "objecter_calculate_exit")
e10, n4  =  n3.build(  4, "objecter_sendop_entry")
e  , n5  =  n4.build(  5, "objecter_sendop_exit")
e  , n6  =  n5.build(  6, "ioctx_radoswrite_exit")

# thread client receive message
e15, n10 = graph.build_thread(client,
                      10, "messenger_fastdispatch_entry")
e  , n11 = n10.build( 11, "objecter_complete")
e  , n12 = n11.build( 12, "messenger_fastdispatch_exit")

# thread osd recieive message
e20, n15 = graph.build_thread(osd,
                      15, "messenger_fastdispatch_entry")
e21, n16 = n15.build( 16, "osd_enqueueop_entry")
e  , n17 = n16.build( 17, "osd_enqueueop_exit")
e  , n18 = n17.build( 18, "messenger_fastdispatch_exit")

# thread osd dequeue op
e25, n20 = graph.build_thread(osd,
                      20, "osd_dequeueop_entry")
# main osd do_op
e  , n21 = n20.build( 21, "posd_doop_entry")
e  , n22 = n21.build( 22, "rosd_issueop_entry")
_  , _   = n22.build(n22, f_sendosd)
e  , n23 = n22.build( 23, "rosd_issueop_exit")
e26, n24 = n23.build( 24, "rosd_qtrans_entry")
e50, n25 = n24.build( 25, "rosd_qtrans_exit")
_  , n60 = n24.build( 60, f_mapply)
_  , _   = n60.build(n25, e50)
e52, n26 = n25.build( 26, "posd_doop_exit")
e27, n27 = n26.build( 27, "osd_dequeueop_exit")
# replica osd do_repop
e  , n30 = n20.build( 30, "rosd_dorepop_entry")
e28, n31 = n30.build( 31, "rosd_qtrans_entry")
e51, n32 = n31.build( 32, "rosd_qtrans_exit")
_  , n61 = n31.build( 61, f_rapply)
_  , _   = n61.build(n32, e51)
e  , n33 = n32.build( 33, "rosd_dorepop_exit")
_  , _   = n33.build(n27, e27)
# main osd do_repop_reply
e  , n35 = n20.build( 35, "rosd_dorepopreply_entry")
e29, n36 = n35.build( 36, "rosd_dorepopreply_exit")
_  , n37 = n35.build( 37, f_sendcli)
_  , _   = n37.build(n36, e29)
_  , _   = n36.build(n27, e27)
# direct exit
_  , _   = n21.build(n26, e52)

# thread main osd commit
_  , n40 = graph.build_thread(osd,
                      40, f_mcommit)

# thread main osd apply
_  , n45 = graph.build_thread(osd,
                      45, f_mapply)

# thread replica osd on commit
_  , n50 = graph.build_thread(osd,
                      50, f_rcommit)

# thread replica osd on applied
_  , n55 = graph.build_thread(osd,
                      55, f_rapply)

#### request imagereq states ####
n12.set_state("SUCCESS")

#### relationship ####
# client send osd
j1 = e10.join_one(e20, True, ["tid",
                              "msg_op",
                              ("target_t", "target_a"),
                              ("target_a", "target_s")])
# osd enqueue op
j2 = e21.join_any(e25, False, ["op_seq",
                               "msg_op",
                               "tid",
                               "pgid"])

# main osd send sub_op to replica osd
j3 = e1.join_one(e20, True, ["tid",
                             "msg_op",
                             ("target_t", "target_a"),
                             ("target_a", "target_s")])

# main osd queue transactions
j4 = e26.join_all(e30, False, ["tid",
                               "op_seq"])
j5 = e26.join_all(e35, False, ["tid",
                               "op_seq"])
# replica osd queue transactions
j6 = e28.join_all(e40, False, ["tid",
                               "op_seq"])
j7 = e28.join_all(e45, False, ["tid",
                               "op_seq"])

# main osd send reply
j8 = e5.join_one(e15, True, ["tid",
                             "msg_op",
                             ("target_t", "target_a"),
                             ("target_a", "target_s")])


def filter_logfile(f_dir, f_name, var_dict):
    if f_name.startswith("out"):
        return False
    else:
        var_dict[rv.HOST] = f_name.rsplit(".", 1)[0]
        return True


def filter_logline(line, var_dict):
    if " radoswrite:" not in line:
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
    if comp == "ceph-osd":
        comp = osd
    elif comp == "python":
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
                items = dict_str.split(",")
                for item in items:
                    k, v = item.strip().split("=", 1)
                    k = k.strip()
                    # NOTE: target is reinterpreted to target_a because one fio
                    # client can have multiple targets
                    if k == "target":
                        k = "target_a"
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
