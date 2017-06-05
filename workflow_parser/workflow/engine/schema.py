from __future__ import print_function

from collections import defaultdict
from collections import Iterable
from collections import OrderedDict

from ...graph import InnerJoin
from ...graph import InterfaceJoin
from ...graph import MasterGraph
from ...graph import RequestInterface
from ...utils import Report
from ..entities.join import InnerjoinIntervalBase
from ..entities.join import InterfacejoinInterval
from ..entities.join import InterfaceInterval
from ..entities.threadins import empty_join
from ..entities.threadins import JoinIntervalBase
from ..entities.threadins import Pace
from ..entities.threadins import ThreadInstance


class SchemaEngine(object):
    def __init__(self, joininterval_type):
        assert issubclass(joininterval_type, JoinIntervalBase)

        self.name = joininterval_type.__name__

        self.joins_items = []

        self.joined_items = []
        self.joineditems_by_jo = defaultdict(list)
        self.joineditems_by_jo_host = defaultdict(lambda: defaultdict(list))
        self.joineditems_by_jo_target = defaultdict(lambda: defaultdict(list))

        self.joininterval_type = joininterval_type

        # results
        self.relations = None
        self.join_attempt_cnt = None

    def _convert_joinobj_to_list(self, join_objs):
        if isinstance(join_objs, self.joininterval_type.joinobj_type):
            return [join_objs]
        else:
            assert isinstance(join_objs, Iterable)
            return join_objs

    def register_fromitems(self, from_items, f_get_objs):
        for from_item in from_items:
            joins_objs = f_get_objs(from_item)
            joins_objs = self._convert_joinobj_to_list(joins_objs)
            for join_obj in joins_objs:
                assert isinstance(join_obj, self.joininterval_type.joinobj_type)

            if isinstance(from_item, Pace):
                from_pace = from_item
            else:
                from_pace = from_item.from_pace
                assert isinstance(from_pace, Pace)
            self.joins_items.append((from_pace, joins_objs, from_item))

    def register_toitems(self, to_items, f_get_objs):
        for to_item in to_items:
            join_objs = f_get_objs(to_item)
            join_objs = self._convert_joinobj_to_list(join_objs)
            for obj in join_objs:
                assert isinstance(obj, self.joininterval_type.joinobj_type)
                if isinstance(to_item, Pace):
                    to_pace = to_item
                else:
                    to_pace = to_item.to_pace
                    assert isinstance(to_pace, Pace)

                item = (to_pace, to_item)
                self.joined_items.append(item)
                self.joineditems_by_jo[obj].append(item)
                self.joineditems_by_jo_host[obj][to_pace.host].append(item)
                self.joineditems_by_jo_target[obj][to_pace.target].append(item)

    def proceed(self, target_objs):
        join_attempt_cnt = 0
        relations = []
        print("  Proceed %s..." % self.name)

        sort_key = lambda item: item[0]

        self.joins_items.sort(key=sort_key)
        self.joined_items.sort(key=sort_key)
        for jo, i_list in self.joineditems_by_jo.iteritems():
            i_list.sort(key=sort_key)
            self.joineditems_by_jo[jo] = OrderedDict.fromkeys(i_list)
        for jo, items_by_host in self.joineditems_by_jo_host.iteritems():
            for host, items in items_by_host.iteritems():
                items.sort(key=sort_key)
                self.joineditems_by_jo_host[jo][host] = OrderedDict.fromkeys(items)
        for jo, paces_by_target in self.joineditems_by_jo_target.iteritems():
            for target, items in paces_by_target.iteritems():
                items.sort(key=sort_key)
                self.joineditems_by_jo_target[jo][target] = OrderedDict.fromkeys(items)

        for joins_pace, join_objs, joins_item in self.joins_items:
            target_item = None

            for join_obj in join_objs:
                schemas = join_obj.schemas
                from_schema = {}
                care_host = None
                care_target = None
                for from_schema_key, to_schema_key in schemas:
                    if to_schema_key == "host":
                        care_host = joins_pace[from_schema_key]
                    elif to_schema_key == "target":
                        care_target = joins_pace[from_schema_key]
                    from_schema[from_schema_key] = joins_pace[from_schema_key]
                if care_target is not None:
                    if care_host is not None:
                        target_objs[care_target].host == care_host
                    target_items = self.joineditems_by_jo_target[join_obj][care_target]
                elif care_host is not None:
                    target_items = self.joineditems_by_jo_host[join_obj][care_host]
                else:
                    target_items = self.joineditems_by_jo[join_obj]
                if target_items:
                    assert isinstance(target_items, OrderedDict)

                to_schemas = defaultdict(set)
                for item_pair in target_items:
                    pace, item = item_pair
                    if getattr(pace, self.joininterval_type.entity_joined_int) is not None:
                        target_items.pop(item_pair)
                        continue

                    join_attempt_cnt += 1
                    match = True
                    for schema in schemas:
                        from_schema_key = schema[0]
                        from_schema_val = from_schema[from_schema_key]
                        to_schema_key = schema[1]
                        to_schema_val = pace[to_schema_key]

                        to_schemas[to_schema_key].add(to_schema_val)
                        if from_schema_key == "request":
                            assert to_schema_key == "request"
                            if from_schema_val and to_schema_val and from_schema_val != to_schema_val:
                                match = False
                                break
                        elif from_schema[from_schema_key] != to_schema_val:
                            assert from_schema_val is not None
                            assert to_schema_val is not None
                            match = False
                            break

                    if match:
                        target_item = item
                        target_items.pop(item_pair)
                        break
                if target_item:
                    break

            if target_item:
                relation = self.joininterval_type.create(join_obj,
                                                         joins_item,
                                                         target_item)
                relations.append(relation)
            else:
                # debug joins
                # print("from_schema: %s" % from_schema)
                # print("to_schemas:")
                # for k, v in to_schemas.iteritems():
                #     print("  %s: %s" % (k, v))
                # import pdb; pdb.set_trace()
                self.joininterval_type.assert_emptyjoin(joins_item, True, True)

        for pace, item in self.joined_items:
            self.joininterval_type.assert_emptyjoin(item, False, False)

        self.join_attempt_cnt = join_attempt_cnt
        self.relations = relations

        return relations

    def report(self, mastergraph):
        print("%s report:" % self.name)
        #### collect ####
        unjoinspaces_by_edge = defaultdict(list)
        unjoinedpaces_by_edge = defaultdict(list)
        relations = set()
        relation_by_jo = defaultdict(list)
        for pace, _, item in self.joins_items:
            joins_int = getattr(item, self.joininterval_type.entity_joins_int)
            if joins_int is empty_join:
                if isinstance(item, Pace):
                    unjoinspaces_by_edge[item.edge].append(item)
                else:
                    unjoinspaces_by_edge[item.join_obj].append(item)
            elif isinstance(joins_int, JoinIntervalBase):
                relations.add(joins_int)
                relation_by_jo[joins_int.join_obj].append(joins_int)
            else:
                assert False
        for pace, item in self.joined_items:
            joined_int = getattr(item, self.joininterval_type.entity_joined_int)
            if joined_int is empty_join:
                if isinstance(item, Pace):
                    unjoinedpaces_by_edge[item.edge].append(item)
                else:
                    unjoinedpaces_by_edge[item.join_obj].append(item)
            elif isinstance(joined_int, JoinIntervalBase):
                assert joined_int in relations
            else:
                assert False

        # #### summary ####
        print("  %d join attempts" % self.join_attempt_cnt)
        if relations:
            print("  %d relations:" % len(relations))
            for jo, _relations in relation_by_jo.iteritems():
                if isinstance(jo, InnerJoin):
                    print("    %s->%s: %d rels" % (jo.from_edge.name,
                                                   jo.to_edge.name,
                                                   len(_relations)))
                else:
                    if jo.is_left:
                        print("    %s->%s: %d rels" % (jo.interface.name,
                                                       jo.edge.name,
                                                       len(_relations)))
                    else:
                        print("    %s->%s: %d rels" % (jo.edge.name,
                                                       jo.interface.name,
                                                       len(_relations)))

        # #### errors #####
        jos = set(relation_by_jo.keys())
        if self.joininterval_type.joinobj_type is InnerJoin:
            g_jos = set(mastergraph.join_objs)
        elif self.joininterval_type.joinobj_type is RequestInterface:
            g_jos = set(mastergraph.interfaces.values())
        elif self.joininterval_type.joinobj_type is InterfaceJoin:
            g_jos = set()
            for interface in mastergraph.interfaces.itervalues():
                for pair in interface.join_pairs:
                    g_jos.update(pair)
        else:
            assert False
        unseen_jos = g_jos - jos
        if unseen_jos:
            print("  ! WARN !")
            for jo in unseen_jos:
                print("  %s unseen!" % jo)
            print()

        if len(relations) != len(self.joins_items):
            print("  ! WARN !")
            for edge, notjoins_paces in unjoinspaces_by_edge.iteritems():
                print("  %s: %d unjoins paces"
                        % (edge.name,
                           len(notjoins_paces)))
            print("  --------")
            for edge, notjoined_paces in unjoinedpaces_by_edge.iteritems():
                print("%s: %d unjoined paces"
                        % (edge.name,
                           len(notjoined_paces)))
            print()
        else:
            assert not unjoinedpaces_by_edge
            assert not unjoinspaces_by_edge


def join_paces(threadinss, mastergraph, report):
    assert isinstance(mastergraph, MasterGraph)
    assert isinstance(report, Report)

    join_attempt_cnt = 0

    print("Join paces...")
    target_objs = {}

    innerjoin_engine = SchemaEngine(InnerjoinIntervalBase)
    interfacejoin_engine = SchemaEngine(InterfacejoinInterval)

    for threadins in threadinss:
        assert isinstance(threadins, ThreadInstance)
        target_objs[threadins.target] = threadins.target_obj

        innerjoin_engine.register_fromitems(
                threadins.joins_paces,
                lambda p: p.edge.joins_objs)
        innerjoin_engine.register_toitems(
                threadins.joined_paces,
                lambda p: p.edge.joined_objs)

        interfacejoin_engine.register_fromitems(
                threadins.rightinterface_paces,
                lambda p: p.edge.right_interface)
        interfacejoin_engine.register_toitems(
                threadins.leftinterface_paces,
                lambda p: p.edge.left_interface)

    inner_relations = innerjoin_engine.proceed(target_objs)
    request_interfaces = [item for item in inner_relations
                          if isinstance(item, InterfaceInterval)]

    interfacejoin_engine.register_fromitems(
            request_interfaces,
            lambda i: i.join_obj.joins_interfaces)
    interfacejoin_engine.register_toitems(
            request_interfaces,
            lambda i: i.join_obj.joined_interfaces)

    interfacej_relations = interfacejoin_engine.proceed(target_objs)
    left_interfacejs = [item for item in interfacej_relations if
            item.is_left]
    print("-------------")

    # #### summary ####
    innerjoin_engine.report(mastergraph)
    interfacejoin_engine.report(mastergraph)
    print()

    #### report #####
    report.step("join_ps",
                innerjoin=len(inner_relations)-len(request_interfaces),
                innerjoined=len(inner_relations)-len(request_interfaces),
                interfacejoin=len(request_interfaces),
                interfacejoined=len(request_interfaces),
                leftinterface=len(left_interfacejs),
                rightinterface=len(interfacej_relations)-len(left_interfacejs))

    return inner_relations, request_interfaces, interfacej_relations
