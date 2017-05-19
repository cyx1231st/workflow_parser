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

from __future__ import print_function

import argparse
import traceback

# from workflow_parser.draw_engine import DrawEngine
from workflow_parser.log_engine import LogEngine
from workflow_parser.state_engine import StateEngine
# from workflow_parser.statistics_engine import do_statistics
# from workflow_parser.relation_engine import relation_parse
# from workflow_parser.statistics import Report
from workflow_parser.utils import Report as ReportInternal


def main1(driver):
    from workflow_parser.driver import DriverBase
    assert isinstance(driver, DriverBase)

    parser = argparse.ArgumentParser()
    parser.add_argument('folder',
                        default=".",
                        help="The logs are in that folder.")
    parser.add_argument('--draw',
                        action="store_true",
                        help="Draw figures.")
    parser.add_argument('--drawfolder',
                        help="Folder to put figures.")
    parser.add_argument('--csv-print-header', action="store_true",
                        help="Write a row into the CSV file for the headers.")
    parser.add_argument('--outfile',
                        help="The output file of report, "
                        "valid only when --draw is set.")
    args = parser.parse_args()

    # build graph
    master = driver.graph
    master.check()

    report_i = ReportInternal()

    try:
        # build logs
        log_engine = LogEngine(driver.services, driver, report_i)
        logfiles = log_engine.loadfiles(args.folder)
        logfiles = log_engine.readfiles(logfiles)
        targetobjs = log_engine.preparethreads(logfiles)

        # build states
        state_engine = StateEngine(master, report_i)
        threadinss = state_engine.build_thread_instances(targetobjs)
        relations_list = state_engine.join_paces(threadinss)
        threadgroup_by_request = state_engine.group_threads(threadinss)
        requestinss = state_engine.build_requests(threadgroup_by_request)
    except Exception:
        print("\n%r\n" % report_i)
        raise
    print("%r" % report_i)
    print()

    # # correct clocks
    # relation_parse(requestinss)

    # # statistics
    # if args.draw:
    #     draw_engine = DrawEngine("/home/vagrant/cyxvagrant/tmp/png/")
    # else:
    #     draw_engine = None
    # do_statistics(requestinss, draw_engine)

    # build statistics
    # s_engine = Engine(master_graph, instances, log_collector)
    # if args.brief:
    #     s_engine.report(args.folder)
    # else:
    #     report = Report(args.folder)
    #     report.set_outfile(args.outfile, args.csv_print_header)
    #     driver_obj.build_statistics(s_engine, report)
    #     report.export()
    return True
