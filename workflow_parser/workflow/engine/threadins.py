from __future__ import print_function

from collections import defaultdict

from ...target import Target
from ...graph import MasterGraph
from ...utils import Report
from ..entities.threadins import ThreadInstance


def build_thread_instances(targetobjs, mastergraph, report):
    assert isinstance(mastergraph, MasterGraph)
    assert isinstance(report, Report)

    valid_lineobjs = 0
    thread_objs = []

    print("Build thread instances...")
    for target_obj in targetobjs:
        assert isinstance(target_obj, Target)
        for thread_obj in target_obj.thread_objs.itervalues():
            threadins = None
            cnt_valid_lineobjs = 0
            for line_obj in thread_obj.line_objs:
                if threadins is not None:
                    if threadins.step(line_obj):
                        # success!
                        pass
                    else:
                        threadins = None
                if threadins is None:
                    threadins = ThreadInstance.create(mastergraph,
                                                      line_obj,
                                                      thread_obj)
                    if threadins is not None:
                        thread_obj.threadinss.append(threadins)
                    else:
                        # error
                        # print("(ParserEngine) parse error: cannot decide graph")
                        # report_loglines(loglines, c_index)
                        # print "-------- end -----------"
                        # raise StateError("(ParserEngine) parse error: cannot decide graph")
                        import pdb; pdb.set_trace()
                if threadins is not None:
                    cnt_valid_lineobjs += 1
                    assert line_obj._assigned is not None
                else:
                    thread_obj.dangling_lineobjs.append(line_obj)
                    assert line_obj._assigned is None

            assert len(thread_obj.dangling_lineobjs) + cnt_valid_lineobjs\
                    == len(thread_obj.line_objs)

            thread_objs.append(thread_obj)
            valid_lineobjs += cnt_valid_lineobjs
    print("-------------------------")

    #### collect ####
    ignored_lineobjs_by_component = defaultdict(lambda: [])
    components = set()
    hosts = set()
    targets = set()
    threadinss = []
    incomplete_threadinss_by_graph = defaultdict(list)
    complete_threadinss_by_graph = defaultdict(list)
    start_threadinss = []
    duplicated_vars = set()
    cnt_innerjoins_paces = 0
    cnt_innerjoined_paces = 0
    cnt_leftinterface_paces = 0
    cnt_rightinterface_paces = 0

    for thread_obj in thread_objs:
        if thread_obj.dangling_lineobjs:
            ignored_lineobjs_by_component[thread_obj.component]\
                    .extend(thread_obj.dangling_lineobjs)
        if thread_obj.threadinss:
            components.add(thread_obj.component)
            hosts.add(thread_obj.host)
            targets.add(thread_obj.target)
        for threadins in thread_obj.threadinss:
            if not threadins.is_complete:
                incomplete_threadinss_by_graph[threadins.threadgraph.name]\
                        .append(threadins)
            else:
                complete_threadinss_by_graph[threadins.threadgraph.name]\
                        .append(threadins)
            if threadins.is_request_start:
                start_threadinss.append(threadins)
            threadinss.append(threadins)
            duplicated_vars.update(threadins.thread_vars_dup.keys())
            cnt_innerjoins_paces += len(threadins.joins_paces)
            cnt_innerjoined_paces += len(threadins.joined_paces)
            cnt_leftinterface_paces += len(threadins.leftinterface_paces)
            cnt_rightinterface_paces += len(threadins.rightinterface_paces)

    #### summary ####
    print("%d valid line_objs" % valid_lineobjs)
    print("%d thread instances" % len(threadinss))
    if complete_threadinss_by_graph:
        for gname, tis in complete_threadinss_by_graph.iteritems():
            print("  %s: %d inss" % (gname, len(tis)))

    print("%d request start t_instances" % len(start_threadinss))
    print()

    #### report #####
    report.step("build_t",
                line=valid_lineobjs,
                component=len(components),
                host=len(hosts),
                target=len(targets),
                thread=len(thread_objs),
                request=len(start_threadinss),
                threadins=len(threadinss),
                innerjoin=cnt_innerjoins_paces,
                innerjoined=cnt_innerjoined_paces,
                leftinterface=cnt_leftinterface_paces,
                rightinterface=cnt_rightinterface_paces)

    #### errors #####
    if ignored_lineobjs_by_component:
        def _report_ignored(tup):
            # (logline, loglines, index, thread, component, target)
            print("  example:")
            raise NotImplementedError()
            # report_loglines(tup[1], tup[2], blanks=4, printend=True)
        print("! WARN !")
        for comp, line_objs in ignored_lineobjs_by_component.iteritems():
            print("%s: %d ignored line_objs" % (comp, len(line_objs)))
            _report_ignored(line_objs[0])
        print()

    edges = mastergraph.edges - mastergraph.seen_edges
    if edges:
        print("! WARN !")
        print("Unseen graph edges: %s" %
                ",".join(edge.name for edge in edges))
        print()

    if duplicated_vars:
        print("! WARN !")
        print("Duplicated vars in t_instances: %s" %
                ",".join(duplicated_vars))
        print()

    if incomplete_threadinss_by_graph:
        print("! WARN !")
        print("Incompleted t_instances:")
        for gname, tis in incomplete_threadinss_by_graph.iteritems():
            print("  %s: %d t_instances" % (gname, len(tis)))
        print()

    return threadinss
