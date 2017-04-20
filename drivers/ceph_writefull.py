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
        # client issue writefull
        e1,  n1  = graph.build_thread(client,
                               1, "IoCtx writefull start", True)
        e2,  n2  =  n1.build(  2, "objecter _op_submit start")
        e3,  n3  =  n2.build(  3, "objecter calculate start")
        e4,  n4  =  n3.build(  4, "objecter calculate finish")
        e5,  n5  =  n4.build(  5, "objecter send_op start")
        e6,  n6  =  n5.build(  6, "objecter send_op finish")
        e7,  n7  =  n6.build(  7, "objecter _op_submit finish")
        e8,  n8  =  n7.build(  8, "IoCtx lock start")
        e9,  n9  =  n8.build(  9, "IoCtx lock finish")
        e10, n10 =  n9.build( 10, "IoCtx writefull finish")

        # client receive message
        e11, n11 = graph.build_thread(client,
                              11, "messenger fast_dispatch start")
        e12, n12 = n11.build( 12, "objecter ms_dispatch")
        e13, n13 = n12.build( 13, "messenger fast_dispatch finish")

        # osd recieive message
        e14, n14 = graph.build_thread(osd,
                              14, "messenger fast_dispatch start")
        e15, n15 = n14.build( 15, "osd dispatch_op_fast start")
        e16, n16 = n15.build( 16, "osd enqueue_op start")
        e17, n17 = n16.build( 17, "osd enqueue_op finish")
        e18, n18 = n17.build( 18, "osd dispatch_op_fast finish")
        e19, n19 = n18.build( 19, "messenger fast_dispatch finish")

        # osd dequeue op
        e20, n20 = graph.build_thread(osd,
                              20, "osd dequeue_op start")

        # main osd receive OSD_OP
        e21, n21 = n20.build( 21, "primarylogpg do_request start")
        e22, n22 = n21.build( 22, "primarylogpg execute_ctx start")
        e23, n23 = n22.build( 23, "primarylogpg prepare_transaction start")
        e24, n24 = n23.build( 24, "primarylogpg do_osd_ops start")
        e25, n25 = n24.build( 25, "primarylogpg do_osd_ops finish")
        e26, n26 = n25.build( 26, "primarylogpg prepare_transaction finish")
        e27, n27 = n26.build( 27, "primarylogpg submit_transaction start")
        e28, n28 = n27.build( 28, "replicatedbackend send_message_osd_cluster start")
        e29, _   = n28.build(n27, "replicatedbackend send_message_osd_cluster finish")
        e30, n29 = n27.build( 29, "replicatedbackend queue_transactions start")
        e31, n30 = n29.build( 30, "filestore queue_transactions")
        e32, n31 = n30.build( 31, "filejournal queue_writeq start")
        e33, n32 = n31.build( 32, "filejournal queue_writeq finish")
        e34, n33 = n32.build( 33, "replicatedbackend queue_transactions finish")
        e35, n34 = n33.build( 34, "primarylogpg submit_transaction finish")
        e36, n35 = n34.build( 35, "primarylogpg execute_ctx finish")
        e37, n36 = n35.build( 36, "primarylogpg do_request finish")

        e38, n37 = n36.build( 37, "osd dequeue_op finish")

        # main osd receive sub_op_reply
        e39, n38 = n20.build( 38, "replicatedbackend handle_message")
        e40, n39 = n38.build( 39, "replicatedbackend sub_op_modify_reply start")
        e41, _   = n39.build(n36, "replicatedbackend sub_op_modify_reply finish")

        # main osd applied 3 osds
        e42, n40 = n39.build( 40, "replicatedbackend execute_on_applied start")
        e43, n41 = n40.build( 41, "primarylogpg repop_all_applied start")
        e44, n42 = n41.build( 42, "primarylogpg repop_all_applied finish")
        e45, n43 = n42.build( 43, "replicatedbackend execute_on_applied finish")

        e46, n44 = n43.build( 44, "replicatedbackend execute_on_commit start")
        e47, n45 = n44.build( 45, "primarylogpg repop_all_committed start")
        e48, n46 = n45.build( 46, "primarylogpg register_on_commit start")
        e49, n47 = n46.build( 47, "primarylogpg send_message_osd_cluster start")
        e50, n48 = n47.build( 48, "primarylogpg send_message_osd_cluster finish")
        e51, n49 = n48.build( 49, "primarylogpg register_on_commit finish")
        e52, n50 = n49.build( 50, "primarylogpg register_on_success start")
        e53, n51 = n50.build( 51, "primarylogpg register_on_success finish")
        e54, n52 = n51.build( 52, "primarylogpg register_on_finish start")
        e55, n53 = n52.build( 53, "primarylogpg register_on_finish finish")
        e56, n54 = n53.build( 54, "primarylogpg repop_all_committed finish")
        e57, _   = n54.build(n39, "replicatedbackend execute_on_commit finish")

        # case on commit first
        _  , _   = n39.build(n44, e46)
        _  , _   = n49.build(n54, e56)

        # case all applied separatly
        _  , _   = n43.build(n36, e41)

        # case applied - success - finish
        _  , _   = n41.build(n50, e52)
        _  , _   = n53.build(n42, e44)

        # TODO: model call operations
        # replica osd 
        _  , _   = n38.build(n29, e30)
        _  , _   = n33.build(n37, e38)

        # main osd on applied
        e58, n55 = graph.build_thread(osd,
                              55, "replicatedbackend op_applied start")
        e59, n56 = n55.build( 56, "replicatedbackend op_applied finish")

        # case call all applied
        _  , _   = n55.build(n41, e43)
        _  , _   = n42.build(n56, e59)

        # main osd on commit
        e60, n57 = graph.build_thread(osd,
                              57, "replicatedbackend op_commit start")
        e61, n58 = n57.build( 58, "replicatedbackend op_commit finish")

        # case call all commit
        _  , _   = n57.build(n45, e47)
        _  , _   = n54.build(n58, e61)

        # journal write
        e62, n59 = graph.build_thread(osd,
                              59, "filejournal singlewrite")
        _  , _   = n59.build(n59, e62)
        e63, n60 = n59.build( 60, "filejournal queue_completions_thru")
        e64, n61 = n60.build( 61, "filejournal queue_finisher start")
        e65, n62 = n61.build( 62, "filejournal queue_finisher finish")
        _  , _   = n62.build(n60, e63)

        # journaled ahead
        e66, n63 = graph.build_thread(osd,
                              63, "filestore _journaled_ahead start")
        e67, n64 = n63.build( 64, "filestore queue_op start")
        e68, n65 = n64.build( 65, "filestore queue_op finish")
        e69, n66 = n65.build( 66, "filestore queue_ondisk_finishers start")
        e70, n67 = n66.build( 67, "filestore queue_ondisk_finishers finish")
        e71, n68 = n67.build( 68, "filestore _journaled_ahead finish")

        # disk write
        e72, n69 = graph.build_thread(osd,
                              69, "filestore _do_op start")
        e73, n70 = n69.build( 70, "filestore _do_op finish")
        e74, n71 = n70.build( 71, "filestore _finish_op start")
        e75, n72 = n71.build( 72, "filestore execute_onreadable_sync start")
        e76, n73 = n72.build( 73, "filestore execute_onreadable_sync finish")
        e77, n74 = n73.build( 74, "filestore queue_onreadable start")
        e78, n75 = n74.build( 75, "filestore queue_onreadable finish")
        e79, n76 = n75.build( 76, "filestore _finish_op finish")

        # replica osd disk write
        _  , _   = n71.build(n74, e77)

        ## replica osd on commit
        e80, n77 = graph.build_thread(osd,
                              77, "replicatedbackend sub_op_modify_commit start")
        e81, n78 = n77.build( 78, "replicatedbackend send_message_osd_cluster start")
        e82, n79 = n78.build( 79, "replicatedbackend send_message_osd_cluster finish")
        e83, n80 = n79.build( 80, "replicatedbackend sub_op_modify_commit finish")

        ## replica osd on applied
        e84, n81 = graph.build_thread(osd,
                              81, "replicatedbackend sub_op_modify_applied start")
        e85, n82 = n81.build( 82, "replicatedbackend sub_op_modify_applied finish")

        #! case  on applied sendmsg
        _  , _   = n81.build(n78, e81)
        _  , _   = n79.build(n82, e85)

        n3.set_lock()
        n5.set_lock()
        n8.set_lock()
        n16.set_lock()
        n28.set_lock()
        n31.set_lock()
        n39.set_lock()
        n47.set_lock()
        n61.set_lock()
        n64.set_lock()
        n66.set_lock()
        n74.set_lock()
        n78.set_lock()

        # client send osd
        j1 = e5.join_remote(e14, ["tid",
                                  "msg_op",
                                  ("target_t", "target"),
                                  ("target", "target_s")])
        # osd enqueue op
        j2 = e16.join_local(e20, ["tracked_op_seq",
                                  "pgid"])
        # main osd send sub_op to replica osd
        j3 = e28.join_remote(e14, ["tid",
                                   "msg_op",
                                   ("target_t", "target"),
                                   ("target", "target_s")])
        # filejournal queue writeq
        #TODO: nested request
        j4 = e32.join_local(e62, ["tracked_op_seq",
                                  "seq"],
                            is_shared=True)
        # filejournal journaled ahead
        j5 = e64.join_local(e66, ["tracked_op_seq",
                                  "seq"])
        # filejournal write disk
        j6 = e67.join_local(e72, ["tracked_op_seq",
                                  "seq"])
        # main osd on commit
        j7 = e69.join_local(e60, ["tracked_op_seq"])
        # replica osd on commit
        j9 = e69.join_local(e80, ["tracked_op_seq"])
        # replica osd reply main osd
        j10 = e81.join_remote(e14, ["tid",
                                    "msg_op",
                                    ("target_t", "target"),
                                    ("target", "target_s")])
        # main osd on apply
        j11 = e77.join_local(e58, ["tracked_op_seq"])
        # replica osd on apply
        j12 = e77.join_local(e84, ["tracked_op_seq"])
        # main osd send reply
        # NOTE: msg_op not match
        j13 = e49.join_remote(e11, ["tid",
                                    ("target_t", "target"),
                                    ("target", "target_s")])
        # main osd unlock ioctx
        j14 = e13.join_local(e9, ["tid",
                                  ("target_s", "target_t")])

        n10.set_state("SUCCESS")

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
