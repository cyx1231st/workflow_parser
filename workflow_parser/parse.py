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

from .analyst import Report
from .analyst.draw_engine import DrawEngine
from .analyst.statistics_engine import do_statistics
from .datasource.log_engine import LogEngine
from .workflow.engine import StateEngine
from .clockmaster import adjust_clock
from .utils import Report as ParserReport


def main1(driver):
    from workflow_parser.driver import DriverBase
    assert isinstance(driver, DriverBase)

    parser = argparse.ArgumentParser()
    parser.add_argument('folder',
                        default=".",
                        help="The logs are in that folder.")
    parser.add_argument('--brief',
                        action="store_true",
                        help="Don't export report and draw figures.")
    parser.add_argument('--outfolder',
                        help="Folder to put figures.",
                        default="/home/vagrant/cyxvagrant/tmp/png/")
    # parser.add_argument('--csv-print-header', action="store_true",
    #                     help="Write a row into the CSV file for the headers.")
    # parser.add_argument('--outfile',
    #                     help="The output file of report, "
    #                     "valid only when --draw is set.")
    args = parser.parse_args()

    # build graph
    master = driver.graph
    master.check()

    report_i = ParserReport()

    try:
        # build logs
        log_engine = LogEngine(driver.services, driver, report_i)
        targetobjs = log_engine.proceed(args.folder)

        # build states
        state_engine = StateEngine(master, report_i)
        requestinss = state_engine.proceed(targetobjs)
    except Exception:
        print("\n%r\n" % report_i)
        raise
    print("%r" % report_i)
    print()

    # correct clocks
    adjust_clock(requestinss)

    # statistics
    folders = args.folder.split("/")
    report = Report(folders[-1] or folders[-2])
    draw_engine = None
    if not args.brief:
        draw_engine = DrawEngine(args.outfolder)
        report.set_outfile(args.outfolder+"/report.csv", True)
    do_statistics(master, requestinss, draw_engine, report)




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
