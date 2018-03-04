from .analyst.report import Report
from .analyst.draw_engine import DrawEngine
from .analyst.automated_suite import do_statistics
from .clockmaster import adjust_clock
from .datasource.log_engine import proceed as l_proceed
from .driver import Driver
from .utils import Report as ParserReport
from .workflow.engine import proceed


def _load_data(data_path, driver):
    print("Load result from %s" % data_path)
    assert isinstance(driver, Driver)
    print("Load driver %s" % driver.name)

    print
    master = driver.graph
    print("graph:")
    print(str(master))

    report_i = ParserReport()
    requestinss = None
    try:
        # build logs
        targets_byname = l_proceed(data_path, driver.services, driver, report_i)

        # build states
        requestinss = proceed(targets_byname, master, report_i)
    except Exception:
        print("\n%r\n" % report_i)
        raise
    print("%r" % report_i)
    print()

    # correct clocks
    adjust_clock(requestinss)

    return requestinss


def execute(driver):
    import argparse
    import os

    parser = argparse.ArgumentParser()
    parser.add_argument('folder',
                        default=".",
                        help="The logs are in that folder.")
    parser.add_argument('--brief',
                        action="store_true",
                        help="Don't export report and draw figures.")
    # parser.add_argument('--outfolder',
    #                     help="Folder to put figures.",
    #                     default="/root/container/out/")
    # parser.add_argument('--csv-print-header', action="store_true",
    #                     help="Write a row into the CSV file for the headers.")
    # parser.add_argument('--outfile',
    #                     help="The output file of report, "
    #                     "valid only when --draw is set.")
    args = parser.parse_args()

    requestinss = _load_data(args.folder, driver)
    if requestinss:
        folders = args.folder.split("/")
        name = folders[-1] or folders[-2]
        draw_engine = None
        out_file = None
        if not args.brief:
            outfolder = args.folder + ("/out-%s/" % driver.name)
            if not os.path.exists(outfolder):
                os.makedirs(outfolder)
            draw_engine = DrawEngine(outfolder)
            out_file = outfolder+"/report.csv"
        do_statistics(name, driver.graph, requestinss, draw_engine, out_file)


def load(data_path, driver):
    requestinss = _load_data(data_path, driver)

    folders = data_path.split("/")
    name = folders[-1] or folders[-2]

    from .analyst.notebook_display import Requests_D
    return Requests_D(name, requestinss, driver.graph)


__all__ = ["load"]
