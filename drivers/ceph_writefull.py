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
        build = graph.build_edge

        #
        # client issue writefull
        e  = build(  0,  1, client, "IoCtx writefull start", True)
        e  = build(  1,  2, client, "objecter _op_submit start")
        e  = build(  2,  3, client, "objecter calculate start")
        e  = build(  3,  4, client, "objecter calculate finish")
        e3 = build(  4,  5, client, "objecter send_op start")
        e  = build(  5,  6, client, "objecter send_op finish")
        e  = build(  6,  7, client, "objecter _op_submit finish")
        e  = build(  7,  8, client, "IoCtx lock start")
        e1 = build(  8,  9, client, "IoCtx lock finish")
        e  = build(  9, 10, client, "IoCtx writefull finish")

        #
        # client receive message
        e35= build( 20, 21, client, "messenger fast_dispatch start")
        e  = build( 21, 22, client, "objecter ms_dispatch")
        e2 = build( 22, 23, client, "messenger fast_dispatch finish")

        #
        # osd recieive message
        e4 = build( 25, 26,    osd, "messenger fast_dispatch start")
        e  = build( 26, 27,    osd, "osd dispatch_op_fast start")
        e22= build( 27, 28,    osd, "osd enqueue_op start")
        e  = build( 28, 29,    osd, "osd enqueue_op finish")
        e  = build( 29, 30,    osd, "osd dispatch_op_fast finish")
        e  = build( 30, 31,    osd, "messenger fast_dispatch finish")

        #
        # osd dequeue op
        e23= build( 35, 36,    osd, "osd dequeue_op start")
        e8 = build( 52, 53,    osd, "osd dequeue_op finish")

        # main osd receive OSD_OP
        e  = build( 36, 37,    osd, "primarylogpg do_request start")
        e  = build( 37, 38,    osd, "primarylogpg execute_ctx start")
        e  = build( 38, 39,    osd, "primarylogpg prepare_transaction start")
        e  = build( 39, 40,    osd, "primarylogpg do_osd_ops start")
        e  = build( 40, 41,    osd, "primarylogpg do_osd_ops finish")
        e  = build( 41, 42,    osd, "primarylogpg prepare_transaction finish")
        e  = build( 42, 43,    osd, "primarylogpg submit_transaction start")
        e5 = build( 43, 44,    osd, "replicatedbackend send_message_osd_cluster start")
        e  = build( 44, 43,    osd, "replicatedbackend send_message_osd_cluster finish")
        e9 = build( 43, 45,    osd, "replicatedbackend queue_transactions start")
        e  = build( 45, 46,    osd, "filestore queue_transactions")
        e24= build( 46, 47,    osd, "filejournal queue_writeq start")
        e  = build( 47, 48,    osd, "filejournal queue_writeq finish")
        e  = build( 48, 49,    osd, "replicatedbackend queue_transactions finish")
        e  = build( 49, 50,    osd, "primarylogpg submit_transaction finish")
        e  = build( 50, 51,    osd, "primarylogpg execute_ctx finish")
        e  = build( 51, 52,    osd, "primarylogpg do_request finish")

        # main osd receive sub_op_reply
        e  = build( 36, 55,    osd, "replicatedbackend handle_message")
        e  = build( 55, 56,    osd, "replicatedbackend sub_op_modify_reply start")
        e13= build( 56, 52,    osd, "replicatedbackend sub_op_modify_reply finish")

        # main osd applied 3 osds
        e  = build( 56, 65,    osd, "replicatedbackend execute_on_applied start")
        e17= build( 65, 66,    osd, "primarylogpg repop_all_applied start")
        e16= build( 66, 67,    osd, "primarylogpg repop_all_applied finish")
        e  = build( 67, 68,    osd, "replicatedbackend execute_on_applied finish")

        e11= build( 68, 69,    osd, "replicatedbackend execute_on_commit start")
        e20= build( 69, 70,    osd, "primarylogpg repop_all_committed start")
        e  = build( 70, 71,    osd, "primarylogpg register_on_commit start")
        e34= build( 71, 72,    osd, "primarylogpg send_message_osd_cluster start")
        e  = build( 72, 73,    osd, "primarylogpg send_message_osd_cluster finish")
        e  = build( 73, 74,    osd, "primarylogpg register_on_commit finish")
        e15= build( 74, 75,    osd, "primarylogpg register_on_success start")
        e  = build( 75, 76,    osd, "primarylogpg register_on_success finish")
        e  = build( 76, 77,    osd, "primarylogpg register_on_finish start")
        e  = build( 77, 78,    osd, "primarylogpg register_on_finish finish")
        e12= build( 78, 79,    osd, "primarylogpg repop_all_committed finish")
        e  = build( 79, 56,    osd, "replicatedbackend execute_on_commit finish")

        # case on commit first
        graph.build_by_edge( 56, e11)
        graph.build_by_edge( 74, e12)

        # case all applied separatly
        graph.build_by_edge( 68, e13)

        # case applied - success - finish
        graph.build_by_edge( 66, e15)
        graph.build_by_edge( 78, e16)

        # TODO: model call operations
        # replica osd 
        graph.build_by_edge( 55, e9)
        graph.build_by_edge( 49, e8)

        #
        # main osd on applied
        e32= build( 80, 81,    osd, "replicatedbackend op_applied start")
        e18= build( 81, 82,    osd, "replicatedbackend op_applied finish")

        # case call all applied
        graph.build_by_edge( 81, e17)
        graph.build_by_edge( 67, e18)

        #
        # main osd on commit
        e30= build( 85, 86,    osd, "replicatedbackend op_commit start")
        e21= build( 86, 87,    osd, "replicatedbackend op_commit finish")

        # case call all commit
        graph.build_by_edge( 86, e20)
        graph.build_by_edge( 79, e21)

        #
        # journal write
        # TODO: actually, 90->91 cannot create new thread instance
        e25= build( 90, 90,    osd, "filejournal singlewrite")
        e6 = build( 90, 91,    osd, "filejournal queue_completions_thru")
        e7 = build( 91, 92,    osd, "filejournal queue_finisher start")
        e  = build( 92, 93,    osd, "filejournal queue_finisher finish")
        graph.build_by_edge( 93, e6)

        #
        # journaled ahead
        e26= build( 95, 96,    osd, "filestore _journaled_ahead start")
        e27= build( 96, 97,    osd, "filestore queue_op start")
        e  = build( 97, 98,    osd, "filestore queue_op finish")
        e29= build( 98, 99,    osd, "filestore queue_ondisk_finishers start")
        e  = build( 99,100,    osd, "filestore queue_ondisk_finishers finish")
        e  = build(100,101,    osd, "filestore _journaled_ahead finish")

        #
        # disk write
        e28= build(105,106,    osd, "filestore _do_op start")
        e  = build(106,107,    osd, "filestore _do_op finish")
        e  = build(107,108,    osd, "filestore _finish_op start")
        e  = build(108,109,    osd, "filestore execute_onreadable_sync start")
        e  = build(109,110,    osd, "filestore execute_onreadable_sync finish")
        e10= build(110,111,    osd, "filestore queue_onreadable start")
        e  = build(111,112,    osd, "filestore queue_onreadable finish")
        e  = build(112,113,    osd, "filestore _finish_op finish")

        # replica osd disk write
        graph.build_by_edge(108, e10)

        #
        ## replica osd on commit
        e31= build(115,116,    osd, "eplicatedbackend sub_op_modify_commit start")
        e19= build(116,117,    osd, "replicatedbackend send_message_osd_cluster start")
        e  = build(117,118,    osd, "replicatedbackend send_message_osd_cluster finish")
        e  = build(118,119,    osd, "eplicatedbackend sub_op_modify_commit finish")

        #
        ## replica osd on applied
        e33= build(120,121,    osd, "replicatedbackend sub_op_modify_applied start")
        e14= build(121,122,    osd, "replicatedbackend sub_op_modify_applied finish")

        #? case  on applied sendmsg
        graph.build_by_edge(121, e19)
        graph.build_by_edge(118, e14)


        graph.set_lock(  3)
        graph.set_lock(  5)
        graph.set_lock(  8)
        graph.set_lock( 28)
        graph.set_lock( 44)
        graph.set_lock( 47)
        graph.set_lock( 56)
        graph.set_lock( 72)
        graph.set_lock( 92)
        graph.set_lock( 97)
        graph.set_lock( 99)
        graph.set_lock(111)
        graph.set_lock(117)


        # client send osd
        e3.join_edge(e4, {("tid", "tid"),
                          ("target_t", "target"),
                          ("target", "target_s"),
                          ("msg_op", "msg_op")})
        # osd enqueue op
        e22.join_edge(e23, {("tracked_op_seq", "tracked_op_seq"),
                            ("pgid", "pgid")}, is_remote=False)
        # main osd send sub_op to replica osd
        e5.join_edge(e4, {("tid", "tid"),
                          ("target_t", "target"),
                          ("target", "target_s"),
                          ("msg_op", "msg_op")})
        # filejournal queue writeq
        e24.join_edge(e25,
                      {("tracked_op_seq", "tracked_op_seq"),
                       ("seq", "seq")},
                      is_remote=False,
                      is_shared=True)
        # filejournal journaled ahead
        e7.join_edge(e26,
                     {("tracked_op_seq", "tracked_op_seq"),
                      ("seq", "seq")},
                     is_remote=False)
        # filejournal write disk
        e27.join_edge(e28,
                      {("tracked_op_seq", "tracked_op_seq"),
                       ("seq", "seq")},
                      is_remote=False)
        # main osd on commit
        e29.join_edge(e30,
                      {("tracked_op_seq", "tracked_op_seq")},
                      is_remote=False)
        # replica osd on commit
        e29.join_edge(e31,
                      {("tracked_op_seq", "tracked_op_seq")},
                      is_remote=False)
        # replica osd reply main osd
        e19.join_edge(e4,
                      {("tid", "tid"),
                       ("target_t", "target"),
                       ("target", "target_s"),
                       ("msg_op", "msg_op")})
        # main osd on apply
        e10.join_edge(e32,
                      {("tracked_op_seq", "tracked_op_seq")},
                      is_remote=False)
        # replica osd on apply
        e10.join_edge(e33,
                      {("tracked_op_seq", "tracked_op_seq")},
                      is_remote=False)
        # main osd send reply
        # NOTE: msg_op not match
        e34.join_edge(e35,
                      {("tid", "tid"),
                       ("target_t", "target"),
                       ("target", "target_s")})
        # main osd unlock ioctx
        e2.join_edge(e1, {("tid", "tid"), ("target_s", "target_t")}, is_remote=False)

        graph.set_state(10, "SUCCESS")

        # f1.join_edge(j1)
        # f6.join_edge(j2)
        # f2.join_edge(j2)
        # f3.join_edge(j3)
        # f4.join_edge(j4, {("t_host", "t_host")})
        # f5.join_edge(j5, {("t_host", "host")})
        # f7.join_edge(j1)

        # graph.set_state(20, "API FAIL")
        # graph.set_state(21, "RETRY FAIL")
        # graph.set_state(22, "NO VALID HOST")
        # graph.set_state(25, "SUCCESS")
        # graph.set_state(26, "COMPUTE FAIL")

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
