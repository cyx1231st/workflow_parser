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
#### functions ####
        # function queue transaction
        e30, n29, f_queuet = graph.build_func(
                              29, "replicatedbackend queue_transactions start", "queue_transactions")
        e31, n30 = n29.build( 30, "filestore queue_transactions")
        e32, n31 = n30.build( 31, "filejournal queue_writeq start")
        e33, n32 = n31.build( 32, "filejournal queue_writeq finish")
        e34, n33 = n32.build_endf("replicatedbackend queue_transactions finish")

        # function main osd receive OSD_OP
        e21, n21, f_doreq = graph.build_func(
                              21, "primarylogpg do_request start", "do_request")
        e22, n22 = n21.build( 22, "primarylogpg execute_ctx start")
        e23, n23 = n22.build( 23, "primarylogpg prepare_transaction start")
        e24, n24 = n23.build( 24, "primarylogpg do_osd_ops start")
        e25, n25 = n24.build( 25, "primarylogpg do_osd_ops finish")
        e26, n26 = n25.build( 26, "primarylogpg prepare_transaction finish")
        e27, n27 = n26.build( 27, "primarylogpg submit_transaction start")
        e28, n28 = n27.build( 28, "replicatedbackend send_message_osd_cluster start")
        e29, n93 = n28.build( 93, "replicatedbackend send_message_osd_cluster finish")
        _  , _   = n93.build(n28, e28)
        _  , n33 = n93.build( 33, f_queuet)
        e35, n34 = n33.build( 34, "primarylogpg submit_transaction finish")
        e36, n35 = n34.build( 35, "primarylogpg execute_ctx finish")
        e37, _   = n35.build_endf("primarylogpg do_request finish")

        # function osd handle message
        e39, n38, f_handlemsg = graph.build_func(
                              38, "replicatedbackend handle_message", "handle_message")
        # replica osd write disk
        e90, _   = n38.build_endf(f_queuet)
        # main osd receive sub_op_reply
        e40, n39 = n38.build( 39, "replicatedbackend sub_op_modify_reply start")
        e41, _   = n39.build_endf("replicatedbackend sub_op_modify_reply finish")
        _  , n90 = n39.build( 90, f_onapplied)
        e86, n91 = n39.build( 91, f_oncommit)
        _  , _   = n90.build_endf(e41)
        _  , _   = n90.build(n91, e86)
        _  , _   = n91.build_endf(e41)

        # function on success and finish
        e52, n50, f_success = graph.build_func(
                              50, "primarylogpg register_on_success start")
        e53, n51 = n50.build( 51, "primarylogpg register_on_success finish")
        e54, n52 = n51.build( 52, "primarylogpg register_on_finish start")
        e55, n53 = n52.build_endf("primarylogpg register_on_finish finish")

        # function all applied
        e43, n41, f_allapplied = graph.build_func(
                              41, "primarylogpg repop_all_applied start", "all_applied")
        e44, n42 = n41.build_endf("primarylogpg repop_all_applied finish")
        _  , n92 = n41.build( 92, f_success)
        _  , _   = n92.build_endf(e44)

        # function on applied callback
        e42, n40, f_onapplied = graph.build_func(
                              40, "replicatedbackend execute_on_applied start", "on_applied")
        _  , n42 = n40.build( 42, f_allapplied)
        e45, n43 = n42.build_endf("replicatedbackend execute_on_applied finish")

        # function all committed
        e47, n45, f_allcommitted = graph.build_func(
                              45, "primarylogpg repop_all_committed start", "all_committed")
        e48, n46 = n45.build( 46, "primarylogpg register_on_commit start")
        e49, n47 = n46.build( 47, "primarylogpg send_message_osd_cluster start")
        e50, n48 = n47.build( 48, "primarylogpg send_message_osd_cluster finish")
        e51, n49 = n48.build( 49, "primarylogpg register_on_commit finish")
        e56, _   = n49.build_endf("primarylogpg repop_all_committed finish")
        _  , n53 = n49.build( 53, f_success)
        _  , n54 = n53.build_endf(e56)

        # function on commit callback
        e46, n44, f_oncommit = graph.build_func(
                              44, "replicatedbackend execute_on_commit start", "on_commit")
        _  , n54 = n44.build( 54, f_allcommitted)
        e57, _   = n54.build_endf("replicatedbackend execute_on_commit finish")

        # function replica osd send reply
        e81, n78, f_sendrep = graph.build_func(
                              78, "replicatedbackend send_message_osd_cluster start", "send_reply")
        e82, _   = n78.build_endf("replicatedbackend send_message_osd_cluster finish")

#### nested request write_journal ####
        # thread osd journal write
        e62, n59 = graph.build_thread(osd,
                              59, "filejournal singlewrite", "write_journal")
        _  , _   = n59.build(n59, e62)
        e63, n60 = n59.build( 60, "filejournal queue_completions_thru")
        e64, n61 = n60.build( 61, "filejournal queue_finisher start")
        e65, n62 = n61.build( 62, "filejournal queue_finisher finish")
        _  , _   = n62.build(n60, e63)
        n62.set_state("SUCCESS")

#### request write_full ####
        # thread client issue writefull
        e1,  n1  = graph.build_thread(client,
                               1, "IoCtx writefull start", "write_full")
        e2,  n2  =  n1.build(  2, "objecter _op_submit start")
        e3,  n3  =  n2.build(  3, "objecter calculate start")
        e4,  n4  =  n3.build(  4, "objecter calculate finish")
        e5,  n5  =  n4.build(  5, "objecter send_op start")
        e6,  n6  =  n5.build(  6, "objecter send_op finish")
        e7,  n7  =  n6.build(  7, "objecter _op_submit finish")
        e8,  n8  =  n7.build(  8, "IoCtx lock start")
        e9,  n9  =  n8.build(  9, "IoCtx lock finish")
        e10, n10 =  n9.build( 10, "IoCtx writefull finish")

        # thread client receive message
        e11, n11 = graph.build_thread(client,
                              11, "messenger fast_dispatch start")
        e12, n12 = n11.build( 12, "objecter ms_dispatch")
        e13, n13 = n12.build( 13, "messenger fast_dispatch finish")

        # thread osd recieive message
        e14, n14 = graph.build_thread(osd,
                              14, "messenger fast_dispatch start")
        e15, n15 = n14.build( 15, "osd dispatch_op_fast start")
        e16, n16 = n15.build( 16, "osd enqueue_op start")
        e17, n17 = n16.build( 17, "osd enqueue_op finish")
        e18, n18 = n17.build( 18, "osd dispatch_op_fast finish")
        e19, n19 = n18.build( 19, "messenger fast_dispatch finish")

        # thread osd dequeue op
        e20, n20 = graph.build_thread(osd,
                              20, "osd dequeue_op start")
        _  , n36 = n20.build( 36, f_doreq)
        _  , _   = n20.build(n36, f_handlemsg)
        e38, n37 = n36.build( 37, "osd dequeue_op finish")

        # thread main osd on applied
        e58, n55 = graph.build_thread(osd,
                              55, "replicatedbackend op_applied start")
        e59, n56 = n55.build( 56, "replicatedbackend op_applied finish")
        _  , n94 = n55.build( 94, f_allapplied)
        _  , _   = n94.build(n56, e59)

        # thread main osd on commit
        e60, n57 = graph.build_thread(osd,
                              57, "replicatedbackend op_commit start")
        e61, n58 = n57.build( 58, "replicatedbackend op_commit finish")
        _  , n95 = n57.build( 95, f_allcommitted)
        _  , _   = n95.build(n58, e61)

        # thread journaled ahead
        e66, n63 = graph.build_thread(osd,
                              63, "filestore _journaled_ahead start")
        e67, n64 = n63.build( 64, "filestore queue_op start")
        e68, n65 = n64.build( 65, "filestore queue_op finish")
        e69, n66 = n65.build( 66, "filestore queue_ondisk_finishers start")
        e70, n67 = n66.build( 67, "filestore queue_ondisk_finishers finish")
        e71, n68 = n67.build( 68, "filestore _journaled_ahead finish")

        # thread disk write
        e72, n69 = graph.build_thread(osd,
                              69, "filestore _do_op start")
        e73, n70 = n69.build( 70, "filestore _do_op finish")
        e74, n71 = n70.build( 71, "filestore _finish_op start")
        # replica osd disk write
        e77, n74 = n71.build( 74, "filestore queue_onreadable start")
        e75, n72 = n71.build( 72, "filestore execute_onreadable_sync start")
        e76, n73 = n72.build( 73, "filestore execute_onreadable_sync finish")
        _  , _   = n73.build(n74, e77)
        e78, n75 = n74.build( 75, "filestore queue_onreadable finish")
        e79, n76 = n75.build( 76, "filestore _finish_op finish")

        # thread replica osd on commit
        e80, n77 = graph.build_thread(osd,
                              77, "replicatedbackend sub_op_modify_commit start")
        _  , n79 = n77.build( 79, f_sendrep)
        e83, n80 = n79.build( 80, "replicatedbackend sub_op_modify_commit finish")

        # thread replica osd on applied
        e84, n81 = graph.build_thread(osd,
                              81, "replicatedbackend sub_op_modify_applied start")
        e85, n82 = n81.build( 82, "replicatedbackend sub_op_modify_applied finish")
        _  , n96 = n81.build( 96, f_sendrep)
        _  , _   = n96.build(e85)

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

        # filejournal write disk
        j4 = e67.join_local(e72, ["tracked_op_seq",
                                  "seq"])
        # main osd on commit
        j5 = e69.join_local(e60, ["tracked_op_seq"])
        # replica osd on commit
        j6 = e69.join_local(e80, ["tracked_op_seq"])
        # replica osd reply main osd
        j7 = e81.join_remote(e14, ["tid",
                                    "msg_op",
                                    ("target_t", "target"),
                                    ("target", "target_s")])
        # main osd on apply
        j8 = e77.join_local(e58, ["tracked_op_seq"])
        # replica osd on apply
        j9 = e77.join_local(e84, ["tracked_op_seq"])
        # main osd send reply
        # NOTE: msg_op not match
        j10 = e49.join_remote(e11, ["tid",
                                    ("target_t", "target"),
                                    ("target", "target_s")])
        # main osd unlock ioctx
        j11 = e13.join_local(e9, ["tid",
                                  ("target_s", "target_t")])

        # filejournal queue writeq
        i1 = e32.join_local(e66, ["tracked_op_seq", "seq"], "journal")

        n10.set_state("SUCCESS")

        ########
        i1.call_req(e62, ["tracked_op_seq", "seq"], False,
                    e64, ["tracked_op_seq", "seq"], False)

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
