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
from collections import OrderedDict
from orderedset import OrderedSet


def _defensive_check(cls, obj, *attrs):
    for attr in attrs:
        assert not hasattr(super(cls, obj), attr)


ONE = "ONE"
ALL = "ALL"
ANY = "ANY"
JoinTypes = {ONE, ALL, ANY}

##### JoinObjs #####

class JoinBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, name, from_item, to_item, is_remote, schemas):
        assert isinstance(name, str)
        assert isinstance(from_item, JoinMixinBase)
        assert isinstance(to_item, JoinMixinBase)
        assert isinstance(is_remote, bool)
        assert isinstance(schemas, list)

        self.name = "j"+name
        self.from_item = from_item
        self.to_item = to_item
        self.is_remote = is_remote
        self.schemas = OrderedSet()

        for schema in schemas:
            if isinstance(schema, str):
                self.schemas.add((schema, schema))
            else:
                assert isinstance(schema, tuple)
                assert isinstance(schema[0], str)
                assert isinstance(schema[1], str)
                self.schemas.add(schema)
        if is_remote:
            assert ("host", "host") not in self.schemas
            assert ("target", "target") not in self.schemas
        else:
            self.schemas.add(("target", "target"))
            self.schemas.add(("host", "host"))
        assert ("thread", "thread") not in self.schemas

    @property
    def from_name(self):
        return self.from_item._jm_name

    @property
    def to_name(self):
        return self.to_item._jm_name

    @property
    def from_keyword(self):
        return self.from_item._jm_keyword

    @property
    def to_keyword(self):
        return self.to_item._jm_keyword

    @property
    def strategy(self):
        return ALL

    def __repr_marks__(self):
        if self.is_remote:
            return ", REMOTE"
        else:
            return ", LOCAL"

    def __repr__(self):
        return "<%s#%s: %s->%s, %d schemas%s>" % (
                self.__class__.__name__,
                self.name,
                self.from_name,
                self.to_name,
                len(self.schemas),
                self.__repr_marks__())

    def __str__(self):
        ret_str = repr(self)
        ret_str += "\n  From: "+repr(self.from_item)
        ret_str += "\n  To: "+repr(self.to_item)
        ret_str += "\n  Schemas: "+",".join(l+"-"+r for l,r in self.schemas)
        return ret_str


class InnerJoin(JoinBase):
    def __init__(self, from_item, to_item, **kwds):
        assert isinstance(from_item, InnerjoinMixin)
        assert isinstance(to_item, InnerjoinMixin)

        super(InnerJoin, self).__init__(
                from_item=from_item,
                to_item=to_item,
                **kwds)

    @property
    def strategy(self):
        st = self.from_item._jm_inner_jstype
        assert st in JoinTypes
        return st

    def __repr_marks__(self):
        mark = super(InnerJoin, self).__repr_marks__()
        mark += ", [%s-->%s]" % (self.from_keyword,
                                  self.to_keyword)
        return mark



class CrossJoin(JoinBase):
    def __init__(self, is_left, caller, caller_index, callee, **kwds):
        assert isinstance(is_left, bool)
        assert isinstance(caller, RequestJoin)
        assert isinstance(caller_index, int)
        assert isinstance(callee, CrossCalleeMixin)

        if is_left:
            super(CrossJoin, self).__init__(
                    name="l_"+caller._jm_keyword,
                    from_item=caller,
                    to_item=callee,
                    **kwds)
        else:
            super(CrossJoin, self).__init__(
                    name="r_"+caller._jm_keyword,
                    from_item=callee,
                    to_item=caller,
                    **kwds)
        _defensive_check(CrossJoin, self,
                "is_left", "caller", "callee", "caller_index", "call_peer")

        self.is_left = is_left
        self.caller = caller
        self.caller_index = caller_index
        self.callee = callee
        self.call_peer = None

    @property
    def caller_real(self):
        if self.is_left:
            return self.caller.from_item
        else:
            return self.caller.to_item

    @property
    def from_keyword(self):
        if self.is_left:
            return self.caller_real._jm_keyword
        else:
            return self.callee._jm_keyword

    @property
    def to_keyword(self):
        if self.is_left:
            return self.callee._jm_keyword
        else:
            return self.caller_real._jm_keyword

    def __repr_marks__(self):
        mark = super(CrossJoin, self).__repr_marks__()
        if self.is_left:
            fmt_str = ", [%s~~>%s]"
        else:
            fmt_str = ", [%s<~~%s]"
        mark += fmt_str % (
            self.caller_real._jm_keyword,
            self.callee._jm_keyword)
        return mark

    def __str__(self):
        str_ = super(CrossJoin, self).__str__()
        str_ += "\n  Real caller: "+repr(self.caller_real)
        str_ += "\n  Peer: "+repr(self.call_peer)
        return str_

##### JoinMixins #####

class JoinMixinBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, jm_master, jm_name, jm_keyword, **kwds):
        assert isinstance(jm_master, JoinMasterMixin)
        assert isinstance(jm_name, str)
        assert isinstance(jm_keyword, str)

        super(JoinMixinBase, self).__init__(**kwds)
        _defensive_check(JoinMixinBase, self,
                "_jm_master", "_jm_name", "_jm_keyword")

        self._jm_master = jm_master
        self._jm_name = jm_name
        self._jm_keyword = jm_keyword


class InnerjoinMixin(JoinMixinBase):
    def __init__(self, **kwds):

        super(InnerjoinMixin, self).__init__(**kwds)
        _defensive_check(InnerjoinMixin, self,
                "_jm_inner_jstype",
                "_jm_inner_jsobjs",
                "_jm_inner_jedobjs")

        self._jm_inner_jstype = None
        self._jm_inner_jsobjs = OrderedSet()
        self._jm_inner_jedobjs = OrderedSet()

    def _join(self, type_, to_item, **kwds):
        assert type_ in JoinTypes
        assert isinstance(to_item, InnerjoinMixin)

        if self._jm_inner_jstype is None:
            self._jm_inner_jstype = type_
        else:
            if self._jm_inner_jstype != type_:
                raise RuntimeError(
                        "Expect join type %s, but got %s" % (
                            self._jm_inner_jstype, type_))

        join_obj = self._jm_master._create_inner_joinobj(
                from_item=self, to_item=to_item, **kwds)
        self._jm_inner_jsobjs.add(join_obj)
        to_item._jm_inner_jedobjs.add(join_obj)
        return join_obj

    def join_one(self, to_item, is_remote, schemas, reqname=None):
        return self._join(
                ONE,
                to_item=to_item,
                is_remote=is_remote,
                schemas=schemas,
                reqname=reqname)

    def join_all(self, to_item, is_remote, schemas, reqname=None):
        return self._join(
                ALL,
                to_item=to_item,
                is_remote=is_remote,
                schemas=schemas,
                reqname=reqname)

    def join_any(self, to_item, is_remote, schemas, reqname=None):
        return self._join(
                ANY,
                to_item=to_item,
                is_remote=is_remote,
                schemas=schemas,
                reqname=reqname)

    def __repr_marks__(self):
        mark_str = super(InnerjoinMixin, self).__repr_marks__()
        if self._jm_inner_jstype:
            mark_str += ", "+self._jm_inner_jstype+"("
            mark_str += ",".join("%s->%s"%(jo.name,jo.to_name)
                    for jo in self._jm_inner_jsobjs)
            mark_str += ")"
        else:
            assert not self._jm_inner_jsobjs
        for jo in self._jm_inner_jedobjs:
            mark_str += ", %s<-%s"%(jo.name,jo.from_name)
        return mark_str

    def __str__(self):
        str_ = super(InnerjoinMixin, self).__str__()
        if self._jm_inner_jstype:
            str_ += "\n  ijoins(%s):" % self._jm_inner_jstype
            for jo in self._jm_inner_jsobjs:
                str_ += "\n    " + repr(jo)
        else:
            assert not self._jm_inner_jsobjs
        if self._jm_inner_jedobjs:
            str_ += "\n  ijoined:"
            for jo in self._jm_inner_jedobjs:
                str_ += "\n    " + repr(jo)
        return str_


# CrossCaller
class RequestJoin(JoinMixinBase, InnerJoin):
    def __init__(self, reqname, **kwds):
        assert isinstance(reqname, str)

        super(RequestJoin, self).__init__(
                jm_name="jq_"+reqname,
                jm_keyword=reqname,
                name="q_"+reqname,
                **kwds)
        assert isinstance(self, JoinBase)
        _defensive_check(RequestJoin, self,
                "caller_jobjpeers", "reqname")

        self.caller_jobjpeers = []
        self.reqname = reqname

    @property
    def joins_objs(self):
        return [joins for joins, _ in self.caller_jobjpeers]

    @property
    def joined_objs(self):
        return [joined for _, joined in self.caller_jobjpeers]

    def __repr_marks__(self):
        mark = super(RequestJoin, self).__repr_marks__()
        for l, r in self.caller_jobjpeers:
            mark += ", [~>%s@%s~>~%s@%s~>]" % (
                    l.to_keyword, l.to_name,
                    r.from_keyword, r.from_name)
        return mark

    def __str__(self):
        str_ = super(RequestJoin, self).__str__()
        if not self.caller_jobjpeers:
            str_ += "\n  Calls: EMPTY!"
        else:
            str_ += "\n  Calls:"
            str_ += "\n  - - -".join(
                    "\n    "+repr(l)+"\n    "+repr(r)
                    for l, r in self.caller_jobjpeers)
        return str_

    def call_req(self, from_item, from_isremote, from_schemas,
                       to_item, to_isremote, to_schemas):
        index = len(self.caller_jobjpeers)
        if index:
            raise RuntimeError("call_req() failed, %s already has calling schema!"
                    % self.name)
        left_joinobj = self._jm_master._create_cross_joinobj(
                is_left=True,
                caller=self,
                caller_index=index,
                callee=from_item,
                is_remote=from_isremote,
                schemas=from_schemas)
        right_joinobj = self._jm_master._create_cross_joinobj(
                is_left=False,
                caller=self,
                caller_index=index,
                callee=to_item,
                is_remote=to_isremote,
                schemas=to_schemas)
        left_joinobj.call_peer = right_joinobj
        right_joinobj.call_peer = left_joinobj
        from_item._jm_callee_jedobjs.add(left_joinobj)
        to_item._jm_callee_jsobjs.add(right_joinobj)
        self.caller_jobjpeers.append((left_joinobj, right_joinobj))


class CrossCalleeMixin(JoinMixinBase):
    def __init__(self, **kwds):
        super(CrossCalleeMixin, self).__init__(**kwds)
        _defensive_check(CrossCalleeMixin, self,
                "_jm_callee_jedobjs", "_jm_callee_jsobjs")

        self._jm_callee_jedobjs = OrderedSet()
        self._jm_callee_jsobjs = OrderedSet()

    def __repr_marks__(self):
        mark_str = super(CrossCalleeMixin, self).__repr_marks__()
        for jo in self._jm_callee_jsobjs:
            mark_str += ", %s~~>%s"%(jo.name, jo.caller_real.name)
        for jo in self._jm_callee_jedobjs:
            mark_str += ", %s<~~%s"%(jo.name, jo.caller_real.name)
        return mark_str

    def __str__(self):
        str_ = super(CrossCalleeMixin, self).__str__()
        if self._jm_callee_jedobjs:
            str_ += "\n  callee joined:"
            for jo in self._jm_callee_jedobjs:
                str_ += "\n    " + repr(jo)
        if self._jm_callee_jsobjs:
            str_ += "\n  callee joins:"
            for jo in self._jm_callee_jsobjs:
                str_ += "\n    " + repr(jo)
        return str_

##### Indexer #####

class JoinMasterMixin(object):
    def __init__(self, **kwds):
        super(JoinMasterMixin, self).__init__(**kwds)
        _defensive_check(JoinMasterMixin, self,
                "_inner_joinobj_index",
                "inner_joinobjs",
                "cross_joinobjs",
                "joinobj_byname")

        self._inner_joinobj_index = 0
        self.inner_joinobjs = OrderedSet()
        self.cross_joinobjs = OrderedSet()
        self.joinobj_byname = OrderedDict()

    def _create_inner_joinobj(self, reqname, **kwds):
        if reqname is None:
            self._inner_joinobj_index += 1
            join_obj = InnerJoin(name=str(self._inner_joinobj_index), **kwds)
        else:
            join_obj = RequestJoin(jm_master=self, reqname=reqname, **kwds)

        if join_obj.name in self.joinobj_byname:
            raise RuntimeError("Join name %s already exists!" % reqname)
        self.joinobj_byname[join_obj.name] = join_obj
        self.inner_joinobjs.add(join_obj)

        return join_obj

    def _create_cross_joinobj(self, **kwds):
        join_obj = CrossJoin(**kwds)
        if join_obj.name in self.joinobj_byname:
            raise RuntimeError("Join name %s already exists!" % reqname)
        self.joinobj_byname[join_obj.name] = join_obj
        self.cross_joinobjs.add(join_obj)

        return join_obj

    def __repr_marks__(self):
        mark = super(JoinMasterMixin, self).__repr_marks__()
        mark += ", jm[%d innerjoins, %d crossjoins]" % (
                len(self.inner_joinobjs), len(self.cross_joinobjs))
        return mark

    def __str__(self):
        ret_str = super(JoinMasterMixin, self).__str__()
        if self.inner_joinobjs:
            ret_str += "\n  InnerJoins:"
            for jo in self.inner_joinobjs:
                ret_str += "\n    %r" % jo
        if self.cross_joinobjs:
            ret_str += "\n  CrossJoins:"
            for jo in self.cross_joinobjs:
                ret_str += "\n    %r" % jo
        return ret_str
