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

from workflow_parser.log_parser.log_engine import TargetsEngine
from workflow_parser.log_parser.state_engine import parse


def main1(driver):
    from workflow_parser.log_parser.driver import DriverBase
    assert isinstance(driver, DriverBase)

    parser = argparse.ArgumentParser()
    parser.add_argument('folder',
                        default=".",
                        help="The logs are in that folder.")
    parser.add_argument('--brief',
                        action="store_true",
                        help="Supress verbose error report.")
    parser.add_argument('--csv-print-header', action="store_true",
                        help="Write a row into the CSV file for the headers.")
    parser.add_argument('--outfile',
                        help="The output file of report.")
    args = parser.parse_args()

    # build graph
    master = driver.graph
    master.check()

    # build targets
    tgs_engine = TargetsEngine(driver.services, driver)
    tgs_engine.loadfiles(args.folder)
    tgs_engine.readfiles()
    tgs_engine.buildthreads()


    # debug
    # for comp in driver.services.sr_components:
    #     print("%s" % log_collector.logfiles_by_component[comp][0])

    # build states
    parse(tgs_engine, master)



    # instances = engine.parse()

    # for ins in instances.itervalues():
    #     print(ins)

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
