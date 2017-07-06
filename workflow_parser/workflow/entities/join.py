# Copyright (c) 2017 Yingxin Cheng
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

from abc import ABCMeta
from abc import abstractproperty

from ...graph import JoinBase
from ...graph import InnerJoin
from ...graph import InterfaceJoin
from ...graph import RequestInterface
from .threadins import IntervalBase
from .threadins import Pace
from .threadins import ThreadInterval


class JoinIntervalBase(IntervalBase):
    __metaclass__ = ABCMeta

    joinobj_type = JoinBase
    entity_joins_int = "Error not assigned"
    entity_joined_int = "Error not assigned"

    def __init__(self, join_obj, from_entity, to_entity,
            from_pace=None, to_pace=None):
        if from_pace is None:
            assert isinstance(from_entity, Pace)
            from_pace = from_entity
        else:
            assert not isinstance(from_entity, Pace)
        if to_pace is None:
            assert isinstance(to_entity, Pace)
            to_pace = to_entity
        else:
            assert not isinstance(to_entity, Pace)
        super(JoinIntervalBase, self).__init__(from_pace, to_pace, join_obj)

        assert isinstance(join_obj, self.joinobj_type)
        if not self.is_remote:
            assert self.from_targetobj is self.to_targetobj
        if self.__class__ is not NestedrequestInterval:
            assert self.from_threadobj is not self.to_threadobj

        assert getattr(from_entity, self.entity_joins_int) is None
        assert getattr(to_entity, self.entity_joined_int) is None
        setattr(from_entity, self.entity_joins_int, self)
        setattr(to_entity, self.entity_joined_int, self)

    @property
    def join_obj(self):
        return self.entity

    @abstractproperty
    def requestins(self):
        raise NotImplementedError()

    @property
    def request(self):
        return self.requestins.request

    @property
    def request_type(self):
        return self.requestins.request_type

    @property
    def from_threadins(self):
        return self.from_pace.threadins

    @property
    def to_threadins(self):
        return self.to_pace.threadins

    @property
    def from_thread(self):
        return self.from_threadins.thread

    @property
    def to_thread(self):
        return self.to_threadins.thread

    @property
    def from_host(self):
        return self.from_pace.host

    @property
    def to_host(self):
        return self.to_pace.host

    @property
    def from_component(self):
        return self.from_pace.component

    @property
    def to_component(self):
        return self.to_pace.component

    @property
    def from_target(self):
        return self.from_pace.target

    @property
    def to_target(self):
        return self.to_pace.target

    @property
    def from_threadobj(self):
        return self.from_pace.thread_obj

    @property
    def to_threadobj(self):
        return self.to_pace.thread_obj

    @property
    def from_targetobj(self):
        return self.from_pace.target_obj

    @property
    def to_targetobj(self):
        return self.to_pace.target_obj

    @property
    def is_remote(self):
        return self.join_obj.is_remote

    @property
    def remote_type(self):
        if self.is_remote and self.from_host != self.to_host:
            return "remote"
        elif self.is_remote:
            return "local_remote"
        else:
            return "local"

    def __str__marks__(self):
        return ""

    def __repr__(self):
        return "<%s#%s %f -> %f, %s -> %s%s>" % (
                self.__class__.__name__,
                self.path,
                self.from_seconds, self.to_seconds,
                self.from_host, self.to_host,
                self.__str__marks__())


class EmptyJoin(JoinIntervalBase):
    def __init__(self, from_pace=None, to_pace=None):
        if from_pace:
            assert to_pace is None
            self._pace = from_pace
        else:
            assert to_pace
            self._pace = to_pace
        self.from_pace = from_pace
        self.to_pace = to_pace

    @property
    def requestins(self):
        return self._pace.threadins.requestins

    def __repr__(self):
        info = ""
        if self.from_pace:
            info += "%s ->|" % self.from_pace.keyword
        if self.to_pace:
            info += "|-> %s" % self.to_pace.keyword
        return "<EMPTYJOIN: %s>" % info


class InnerjoinIntervalBase(JoinIntervalBase):
    joinobj_type = InnerJoin
    entity_joins_int = "joins_int"
    entity_joined_int = "joined_int"

    def __init__(self, join_obj, from_pace, to_pace):
        super(InnerjoinIntervalBase, self).__init__(join_obj, from_pace, to_pace)

        self.from_threadins.joinsints_by_type[self.__class__].add(self)
        self.to_threadins.joinedints_by_type[self.__class__].add(self)

    @property
    def joined_int(self):
        joined_int = self.from_pace.prv_int
        assert isinstance(joined_int, ThreadInterval)
        return joined_int

    @property
    def joins_int(self):
        joins_int = self.to_pace.nxt_int
        assert isinstance(joins_int, ThreadInterval)
        return joins_int

    @property
    def requestins(self):
        from_ = self.from_pace.threadins.requestins
        to_ = self.to_pace.threadins.requestins
        assert from_ is to_
        return from_

    @classmethod
    def create(cls, join_obj, from_item, to_item):
        if isinstance(join_obj, RequestInterface):
            return InterfaceInterval(join_obj, from_item, to_item)
        else:
            assert isinstance(join_obj, InnerJoin)
            return InnerjoinInterval(join_obj, from_item, to_item)


class InnerjoinInterval(InnerjoinIntervalBase):
    def __init__(self, join_obj, from_pace, to_pace):
        super(InnerjoinInterval, self).__init__(join_obj, from_pace, to_pace)

    # @property
    # def color(self):
    #     if self.is_objremote:
    #         return "#fa8200"
    #     else:
    #         return "#fade00"

    # @property
    # def color_jt(self):
    #     j_type = self.join_type
    #     if j_type == "remote":
    #         return "#fa8200"
    #     elif j_type == "local_remote":
    #         return "#fab300"
    #     else:
    #         return "#fade00"


class InterfaceInterval(InnerjoinIntervalBase):
    joinobj_type = RequestInterface

    def __init__(self, join_obj, from_pace, to_pace):
        super(InterfaceInterval, self).__init__(join_obj, from_pace, to_pace)
        self.joins_crossrequest_int = None
        self.joined_crossrequest_int = None
        self.nestedrequest_int = None

    def __str__marks__(self):
        str_marks = super(InterfaceInterval, self).__str__marks__()
        if self.joins_crossrequest_int is not None:
            str_marks += ", [%s, %s, %s | %s, %s, %s]" %\
                (self.joins_crossrequest_int.path,
                 self.joins_crossrequest_int.to_seconds,
                 self.joins_crossrequest_int.to_host,
                 self.joined_crossrequest_int.path,
                 self.joined_crossrequest_int.from_seconds,
                 self.joined_crossrequest_int.from_host)
        return str_marks

    def build_nestedrequestinterval(self):
        self.nestedrequest_int = NestedrequestInterval(
                self.joins_crossrequest_int,
                self.joined_crossrequest_int)


class InterfacejoinInterval(JoinIntervalBase):
    joinobj_type = InterfaceJoin
    entity_joins_int = "joins_crossrequest_int"
    entity_joined_int = "joined_crossrequest_int"

    def __init__(self, join_obj, from_entity, to_entity):
        if join_obj.is_left:
            assert isinstance(from_entity, InterfaceInterval)
            interface_int = from_entity
            # name = "%s(%s)%s" % (from_entity.join_obj.name,
            #                      join_obj.name,
            #                      to_entity.to_edge.name)
            super(InterfacejoinInterval, self).__init__(
                    join_obj, from_entity, to_entity,
                    from_pace=from_entity.from_pace)
        else:
            assert isinstance(to_entity, InterfaceInterval)
            interface_int = to_entity
            # name = "%s(%s)%s" % (from_entity.from_edge.name,
            #                      join_obj.name,
            #                      to_entity.join_obj.name)
            super(InterfacejoinInterval, self).__init__(
                    join_obj, from_entity, to_entity,
                    to_pace=to_entity.to_pace)
        self.interface_int = interface_int
        self.join_nr_int = None

        if join_obj.is_left:
            assert self.from_pace is self.interface_int.from_pace
            self.to_threadins.joinedinterfaceints_by_type[self.__class__].add(self)
        else:
            assert self.to_pace is self.interface_int.to_pace
            self.from_threadins.joinsinterfaceints_by_type[self.__class__].add(self)

    @property
    def is_left(self):
        return self.join_obj.is_left

    @property
    def pair(self):
        if self.is_left:
            return self.interface_int.joined_crossrequest_int
        else:
            return self.interface_int.joins_crossrequest_int

    @property
    def from_requestins(self):
        return self.from_threadins.requestins

    @property
    def to_requestins(self):
        return self.to_threadins.requestins

    @property
    def requestins(self):
        return self.interface_int.requestins

    @property
    def joined_int(self):
        if self.is_left:
            return self.interface_int.joined_int
        else:
            return self.join_nr_int

    @property
    def joins_int(self):
        if self.is_left:
            return self.join_nr_int
        else:
            return self.interface_int.joins_int

    @classmethod
    def create(cls, join_obj, from_item, to_item):
        return cls(join_obj, from_item, to_item)


class NestedrequestInterval(JoinIntervalBase):
    joinobj_type = RequestInterface
    entity_joins_int = "join_nr_int"
    entity_joined_int = "join_nr_int"

    def __init__(self, left_cr_int, right_cr_int):
        assert isinstance(left_cr_int, InterfacejoinInterval)
        assert left_cr_int.is_left
        assert isinstance(right_cr_int, InterfacejoinInterval)
        assert not right_cr_int.is_left
        assert left_cr_int.to_requestins is right_cr_int.from_requestins
        assert left_cr_int.interface_int is right_cr_int.interface_int

        super(NestedrequestInterval, self).__init__(
                left_cr_int.interface_int.join_obj,
                left_cr_int, right_cr_int,
                from_pace=left_cr_int.to_pace,
                to_pace=right_cr_int.from_pace)

        self.left_cr_int = left_cr_int
        self.right_cr_int = right_cr_int

    @property
    def state_name(self):
        ret = super(NestedrequestInterval, self).state_name
        return "nest"+ret

    @property
    def interface_int(self):
        return self.left_cr_int.interface_int

    @property
    def nested_requestins(self):
        return self.left_cr_int.to_requestins

    @property
    def requestins(self):
        return self.left_cr_int.requestins

    @property
    def joined_int(self):
        return self.left_cr_int

    @property
    def joins_int(self):
        return self.right_cr_int

    @property
    def is_remote(self):
        return True

    @property
    def cnt_nested(self):
        return self.nested_requestins.cnt_nested
