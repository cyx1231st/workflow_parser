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
sr.f_register("ceph", "osd", "client")

osd = sr.ceph.osd
client = sr.ceph.client

osd.color = "#9c51eb"
client.color = "#54c0e8"


class CephWritefull(DriverBase):
    def build_graph(self, graph):
# TODO build_thread by function
#### functions ####
        # function message send osd
        e1 , n1 , f_sendosd = graph.build_func(
                               1, "rbackend send_message_osd_cluster start", "sdosd")
        e  , _   =  n1.build_endf("rbackend send_message_osd_cluster finish")

        # function message send client
        e5 , n1 , f_sendcli = graph.build_func(
                               1, "plogpg send_message_osd_client start", "sdcli")
        e  , _   =  n1.build_endf("plogpg send_message_osd_client finish")

#### request write_full ####
        # thread client issue writefull
        e  , n1  = graph.build_thread(client,
                               1, "IoCtx writefull start", "writefull")
        e  , n2  =  n1.build(  2, "objecter calculate start")
        e  , n3  =  n2.build(  3, "objecter calculate finish")
        e10, n4  =  n3.build(  4, "objecter send_op start")
        e  , n5  =  n4.build(  5, "objecter send_op finish")
        e  , n6  =  n5.build(  6, "IoCtx lock start")
        e11, n7  =  n6.build(  7, "IoCtx lock finish")
        e  , n8  =  n7.build(  8, "IoCtx writefull finish")
        n8.set_state("SUCCESS")

        # thread client receive message
        e15, n10 = graph.build_thread(client,
                              10, "messenger fast_dispatch start")
        e16, n11 = n10.build( 11, "objecter unlock start")
        e  , n12 = n11.build( 12, "objecter unlock finish")
        e  , n13 = n12.build( 13, "messenger fast_dispatch finish")

        # thread osd recieive message
        e20, n15 = graph.build_thread(osd,
                              15, "messenger fast_dispatch start")
        e21, n16 = n15.build( 16, "osd enqueue_op start")
        e  , n17 = n16.build( 17, "osd enqueue_op finish")
        e  , n18 = n17.build( 18, "messenger fast_dispatch finish")

        # thread osd dequeue op
        e25, n20 = graph.build_thread(osd,
                              20, "osd dequeue_op start")
        # main osd do_op
        e  , n21 = n20.build( 21, "plogpg do_op start")
        e  , n22 = n21.build( 22, "rbackend issue_op start")
        _  , _   = n22.build(n22, f_sendosd)
        e  , n23 = n22.build( 23, "rbackend issue_op finish")
        e26, n24 = n23.build( 24, "rbackend queue_transactions start")
        e  , n25 = n24.build( 25, "rbackend queue_transactions finish")
        e  , n26 = n25.build( 26, "plogpg do_op finish")
        e27, n27 = n26.build( 27, "osd dequeue_op finish")
        # replica osd do_repop
        e  , n30 = n20.build( 30, "rbackend do_repop start")
        e28, n31 = n30.build( 31, "rbackend queue_transactions start")
        e  , n32 = n31.build( 32, "rbackend queue_transactions finish")
        e  , n33 = n32.build( 33, "rbackend do_repop finish")
        _  , _   = n33.build(n27, e27)
        # main osd do_repop_reply
        e  , n35 = n20.build( 35, "rbackend do_repop_reply start")
        e29, n36 = n35.build( 36, "rbackend do_repop_reply finish")
        _  , n37 = n35.build( 37, f_sendcli)
        _  , _   = n37.build(n36, e29)
        _  , _   = n36.build(n27, e27)

        # thread main osd commit
        e30, n40 = graph.build_thread(osd,
                              40, "rbackend op_commit start")
        e31, n41 = n40.build( 41, "rbackend op_commit finish")
        _  , n42 = n40.build( 42, f_sendcli)
        _  , _   = n42.build(n41, e31)

        # thread main osd apply
        e35, n45 = graph.build_thread(osd,
                              45, "rbackend op_applied start")
        e36, n46 = n45.build( 46, "rbackend op_applied finish")
        _  , n47 = n45.build( 47, f_sendcli)
        _  , _   = n47.build(n46, e36)

        # thread replica osd on commit
        e40, n50 = graph.build_thread(osd,
                              50, "rbackend repop_commit start")
        _  , n51 = n50.build( 51, f_sendosd)
        e  , n52 = n51.build( 52, "rbackend repop_commit finish")

        # thread replica osd on applied
        e45, n55 = graph.build_thread(osd,
                              55, "rbackend repop_applied start")
        e46, n56 = n55.build( 56, "rbackend repop_applied finish")
        _  , n57 = n55.build( 57, f_sendosd)
        _  , _   = n57.build(n56, e46)

        # client send osd
        j1 = e10.join_one(e20, True, ["tid",
                                      "msg_op",
                                      ("target_t", "target"),
                                      ("target", "target_s")])
        # osd enqueue op
        j2 = e21.join_one(e25, False, ["tracked_op_seq",
                                       "msg_op",
                                       "tid",
                                       "pgid"])

        # main osd send sub_op to replica osd
        j3 = e1.join_one(e20, True, ["tid",
                                     "msg_op",
                                     ("target_t", "target"),
                                     ("target", "target_s")])

        # main osd queue transactions
        j4 = e26.join_all(e30, False, ["tid",
                                       "tracked_op_seq"])
        j5 = e26.join_all(e35, False, ["tid",
                                       "tracked_op_seq"])
        # replica osd queue transactions
        j6 = e28.join_all(e40, False, ["tid",
                                       "tracked_op_seq"])
        j7 = e28.join_all(e45, False, ["tid",
                                       "tracked_op_seq"])

        # main osd send reply
        j6 = e5.join_one(e15, True, ["tid",
                                     "msg_op",
                                     ("target_t", "target"),
                                     ("target", "target_s")])

        # client unlock ioctx
        j7 = e16.join_one(e11, False, ["tid",
                                       ("target_s", "target_t")])


    def filter_logfile(self, f_dir, f_name, var_dict):
        if f_name.startswith("out"):
            return False
        if f_name.startswith("client"):
            var_dict[rv.COMPONENT] = client
            # vstart 1 host mode
            var_dict[rv.HOST] = "HOST0"
            # cannot infer client target from filename
            return True
        elif f_name.startswith("osd"):
            var_dict[rv.COMPONENT] = osd
            var_dict[rv.HOST] = "HOST0"
            var_dict[rv.TARGET] = f_name
            return True
        else:
            return False

    def filter_logline(self, line, var_dict):
        if "^^^^" not in line:
            return False

        content = line.split("^^^^", 1)
        assert len(content) == 2

        content_0 = content[0].strip()
        splits_0 = content_0.split(" ")
        time = splits_0[1]
        time_s = time.split(":")
        seconds = int(time_s[0]) * 3600 + int(time_s[1]) * 60 + \
            float(time_s[2])
        thread = splits_0[2]

        content_1 = content[1].strip()
        action_s = content_1.split(">", 1)
        action = action_s[0][1:].strip()
        assert "=" not in action

        if len(action_s) > 1:
            assert len(action_s) == 2
            kw_str = action_s[1].strip()
            if kw_str.startswith(","):
                kw_str = kw_str[1:].strip()
            if kw_str.startswith("##"):
                pass
            else:
                kw_str = kw_str.split("##", 1)[0].strip()
                kvs = kw_str.split(",")
                for kv in kvs:
                    kv = kv.strip().split("=", 1)
                    if len(kv) == 2:
                        var_dict[kv[0].strip()] = kv[1].strip()

        var_dict[rv.KEYWORD] = action
        var_dict[rv.TIME] = time
        var_dict[rv.SECONDS] = seconds
        var_dict[rv.THREAD] = thread
        return True

    def preprocess_logline(self, logline):
        return True


if __name__ == "__main__":
    driver = CephWritefull(sr)
    driver.cmdrun()
