import sys
import cython
from cpython.ref cimport Py_XDECREF, Py_XINCREF, Py_DECREF, Py_INCREF, Py_CLEAR
from cpython.object cimport PyObject, PyObject_RichCompareBool, Py_EQ
from cpython.tuple cimport PyTuple_SetItem, PyTuple_GetItem, PyTuple_New, PyTuple_GET_SIZE, PyTuple_SET_ITEM, PyTuple_GET_ITEM, PyTuple_Pack
from cpython.module cimport PyImport_Import, PyModule_GetDict

from ._entity cimport EntityType, EntityBase, EntityAttribute, EntityAttributeImpl, EntityAttributeExt, EntityAttributeExtGroup, get_alias_target, NOTSET
from ._expression cimport Expression, Visitor, PathExpression, VirtualExpressionVal
from ._field cimport Field, ForeignKey
from ._factory cimport Factory, ForwardDecl, new_instance_from_forward, is_forward_decl
from ._visitors cimport replace_entity
from ._error cimport JoinError


cdef class Relation(EntityAttribute):
    def __cinit__(self, *args, join = None):
        self._default_ = join

    def __get__(self, instance, owner):
        if instance is None:
            try:
                self.update_join_expr()
            except JoinError as e:
                pass
            return self
        else:
            return super().__get__(instance, owner)

    cdef object bind(self):
        cdef RelationImpl impl
        if EntityAttribute.bind(self):
            impl = self._impl_
            if self._default_:
                impl = self._impl_
                if not impl.resolve_default(self):
                    return False


            # Ha megtudja csinálni jó, hanem akkor mind1, majd később
            # try:
            #     self.update_join_expr()
            # except:
            #     pass
            return True
        else:
            return False

    def __getattr__(self, name):
        cdef EntityType joined = self._impl_.joined
        cdef Expression expr = getattr(joined, name)

        if isinstance(expr, VirtualExpressionVal):
            return expr

        return PathExpression([self, expr])

    cpdef visit(self, Visitor visitor):
        return visitor.visit_relation(self)

    def __repr__(self):
        return "<Relation %s :: %s>" % (self._entity_, self._impl_)

    cpdef object clone(self):
        cdef Relation res = type(self)(self._impl_.clone())
        res._exts_ = self.clone_exts(res)
        res._default_ = self._default_
        res._key_ = self._key_
        res._name_ = self._name_
        res._deps_ = set(self._deps_)
        return res

    cdef object update_join_expr(self):
        cdef RelationImpl impl = self._impl_
        if not impl.join_expr:
            if not impl.determine_join_expr(self._entity_, self):
                raise JoinError("Can't initialize relation %s" % self)

        return True



@cython.final
cdef class RelatedAttribute(EntityAttribute):
    def __cinit__(self, Relation rel, *, str name, **kwargs):
        self.__relation__ = rel
        self._name_ = name
        self._impl_ = RelatedAttributeImpl()
        self._virtual_ = True

    def __getattribute__(self, key):
        if key in ("__repr__", "_virtual_", "clone", "bind", "visit"):
            return object.__getattribute__(self, key)
        else:
            return getattr(self.__rattr__, key)

    def __setattr__(self, name, value):
        setattr(self.__rattr__, name, value)

    def __getitem__(self, key):
        return self.__rpath__[key]

    def __get__(self, instance, owner):
        if instance is None:
            return self
        elif isinstance(instance, EntityBase):
            return getattr((<EntityBase>instance).__state__.get_value(self.__relation__), self._name_)
        else:
            raise RuntimeError("...")

    def __set__(self, EntityBase instance, value):
        related = instance.__state__.get_value(self.__relation__)
        if related is NOTSET:
            related = self.__relation__._impl_._joined()
            instance.__state__.set_value(self.__relation__, related)
        setattr(related, self._name_, value)

    def __delete__(self, EntityBase instance):
        cdef EntityBase related = instance.__state__.get_value(self.__relation__)
        if related is not None:
            delattr(related, self._name_)

    def __repr__(self):
        return "<RelatedAttribute %r>" % self.__relation__

    cpdef clone(self):
        return type(self)(self.__relation__.clone(), name=self._name_)

    cdef object bind(self):
        if self.__relation__.bind():
            if not isinstance(self.__relation__._impl_, ManyToOne):
                raise ValueError("RelatedAttribute only accepts ManyToOne type ralations")

            if self.__rattr__ is None:
                if not self.__relation__.update_join_expr():
                    return False

                self.__rattr__ = getattr(self.__relation__._impl_.joined, self._name_)
                self.__rpath__ = getattr(self.__relation__, self._name_)
            return True
        else:
            return False

    cpdef visit(self, Visitor visitor):
        return self.__rpath__.visit(visitor)


cdef class RelatedAttributeImpl(EntityAttributeImpl):
    cdef object state_init(self, object initial):
        return NOTSET

    cdef object state_set(self, object initial, object current, object value):
        return NOTSET

    cdef object state_get_dirty(self, object initial, object current):
        return NOTSET

cdef class RelationImpl(EntityAttributeImpl):
    def __cinit__(self, joined, state_impl, *args):
        self._joined = joined
        self.state_impl = state_impl

    cdef object determine_join_expr(self, EntityType entity, Relation attr):
        raise NotImplementedError()

    cdef object resolve_default(self, Relation attr):
        raise NotImplementedError()

    cpdef object clone(self):
        cdef RelationImpl c = type(self)(self._joined, self.state_impl)
        c.joined = self.joined
        return c

    cdef object state_init(self, object initial):
        return self.state_impl.state_init(initial)

    cdef object state_set(self, object initial, object current, object value):
        return self.state_impl.state_set(initial, current, value)

    cdef object state_get_dirty(self, object initial, object current):
        return self.state_impl.state_get_dirty(initial, current)

    cdef object _eval(self, Relation attr, str expr):
        module = PyImport_Import(attr._entity_.__module__)
        mdict = PyModule_GetDict(module)
        ldict = {attr._entity_.__qualname__.split(".").pop(): attr._entity_}
        ldict.update(attr._entity_.__registry__.locals)
        return eval(expr, <object>mdict, <object>ldict)



cdef class ManyToOne(RelationImpl):
    def __repr__(self):
        return "ManyToOne %r" % self._joined

    cdef object determine_join_expr(self, EntityType entity, Relation attr):
        if self._joined is not entity and not can_determine_join_cond(self._joined):
            return False

        if self.joined is None:
            self.joined = self._joined.alias()

        if attr._default_:
            self.join_expr = replace_entity(attr._default_, self._joined, self.joined)
        else:
            self.join_expr = determine_join_expr(entity, self.joined)

        entity_aliased = get_alias_target(entity)
        if entity_aliased is not entity:
            self.join_expr = replace_entity(self.join_expr, entity_aliased, entity)

        # attr._deps_.add(self._joined)
        return True

    cdef object resolve_default(self, Relation attr):
        if isinstance(attr._default_, str):
            try:
                attr._default_ = self._eval(attr, attr._default_)
            except NameError:
                return False
        elif callable(attr._default_) and not isinstance(attr._default_, Expression):
            attr._default_ = attr._default_(attr._entity_)

        if isinstance(attr._default_, Expression):
            return True
        else:
            raise ValueError("Invalid value for join expression: %r" % attr._default_)


cdef class OneToMany(RelationImpl):
    def __repr__(self):
        return "OneToMany %r" % self._joined

    cdef object determine_join_expr(self, EntityType entity, Relation attr):
        if self._joined is not entity and not can_determine_join_cond(self._joined):
            return False

        if self.joined is None:
            self.joined = self._joined.alias()

        if attr._default_:
            self.join_expr = replace_entity(attr._default_, self._joined, self.joined)
        else:
            self.join_expr = determine_join_expr(self.joined, entity)

        entity_aliased = get_alias_target(entity)
        if entity_aliased is not entity:
            self.join_expr = replace_entity(self.join_expr, entity_aliased, entity)

        return True

    cdef object resolve_default(self, Relation attr):
        if isinstance(attr._default_, str):
            try:
                attr._default_ = self._eval(attr, attr._default_)
            except NameError:
                return False
        elif callable(attr._default_) and not isinstance(attr._default_, Expression):
            attr._default_ = attr._default_(attr._entity_)

        if isinstance(attr._default_, Expression):
            return True
        else:
            raise ValueError("Invalid value for join expression: %r" % attr._default_)


cdef class ManyToMany(RelationImpl):
    def __cinit__(self, joined, state_impl, across):
        self._across = across

    def __repr__(self):
        return "ManyToMany %r => %r" % (self._across, self._joined)

    cdef object determine_join_expr(self, EntityType entity, Relation attr):
        if self._joined is not entity and (not can_determine_join_cond(self._joined) or not can_determine_join_cond(self._across)):
            return False

        if self.joined is None:
            self.joined = self._joined.alias()

        if self.across is None:
            self.across = self._across.alias()

        if attr._default_:
            self.across_join_expr = attr._default_[self._across]
            self.across_join_expr = replace_entity(self.across_join_expr, self._across, self.across)
            self.across_join_expr = replace_entity(self.across_join_expr, self._joined, self.joined)
            self.join_expr = attr._default_[self._joined]
            self.join_expr = replace_entity(self.join_expr, self._across, self.across)
            self.join_expr = replace_entity(self.join_expr, self._joined, self.joined)
        else:
            self.across_join_expr = determine_join_expr(self.across, entity)
            self.join_expr = determine_join_expr(self.across, self.joined)

        entity_aliased = get_alias_target(entity)
        if entity_aliased is not entity:
            self.join_expr = replace_entity(self.join_expr, entity_aliased, entity)
            self.across_join_expr = replace_entity(self.across_join_expr, entity_aliased, entity)

        return True

    cdef object resolve_default(self, Relation attr):
        if isinstance(attr._default_, dict):
            resolved = {}
            for entity, join in attr._default_.items():
                if isinstance(entity, str):
                    try:
                        entity = self._eval(attr, entity)
                    except NameError:
                        return False

                if not isinstance(entity, EntityType):
                    raise ValueError("Invalid value for join expression: %r" % attr._default_)

                if isinstance(join, str):
                    try:
                        join = self._eval(attr, join)
                    except NameError:
                        return False

                if not isinstance(join, Expression):
                    raise ValueError("Invalid value for join expression: %r" % attr._default_)

                resolved[entity] = join
            attr._default_ = resolved
            return True
        else:
            raise ValueError("Invalid value for join expression: %r" % attr._default_)

    cpdef object clone(self):
        cdef ManyToMany c = type(self)(self._joined, self.state_impl, self._across)
        c.joined = self.joined
        c.across = self.across
        return c


cdef determine_join_expr(EntityType entity, EntityType joined):
    if not entity.__extgroups__:
        raise JoinError("Can't determine join condition between %s <-> %s" % (entity, joined))

    cdef tuple keys
    cdef Field field
    cdef ForeignKey fk
    cdef object found = None
    cdef EntityAttributeExtGroup group

    for group in filter(lambda v: v.type is ForeignKey, entity.__extgroups__):
        fk = <ForeignKey>group.items[0]

        if fk.ref._entity_ is joined:
            if found is not None:
                raise JoinError("Multiple join conditions between %s <-> %s" % (entity, joined))

            found = fk.attr == fk.ref
            for i in range(1, len(group.items)):
                fk = <ForeignKey>group.items[i]
                found &= fk.attr == fk.ref

    if found is None:
        aliased_ent = get_alias_target(entity)
        aliased_join = get_alias_target(joined)

        if aliased_ent is not entity or aliased_join is not joined:
            found = determine_join_expr(aliased_ent, aliased_join)
            if aliased_ent is not entity:
                found = replace_entity(found, aliased_ent, entity)
            if aliased_join is not joined:
                found = replace_entity(found, aliased_join, joined)
        else:
            raise JoinError("Can't determine join condition between %s <-> %s" % (entity, joined))

    return found


# ****************************************************************************
# ** VALUE STORES **
# ****************************************************************************


cdef class ValueStore:
    cpdef object state_init(self, object initial):
        raise NotImplementedError()

    cpdef object state_set(self, object initial, object current, object value):
        raise NotImplementedError()

    cpdef object state_get_dirty(self, object initial, object current):
        raise NotImplementedError()


cdef class RelatedItem(ValueStore):
    cpdef object state_init(self, object initial):
        return NOTSET

    cpdef object state_set(self, object initial, object current, object value):
        if value is not None and not isinstance(value, EntityBase):
            raise TypeError("Can't set attribute with this value: %r" % value)
        return value

    cpdef object state_get_dirty(self, object initial, object current):
        if initial is NOTSET or initial is None:
            if current is NOTSET or current is None:
                return NOTSET
            else:
                return ([current], [], [])
        else:
            if current is None:
                return ([], [current], [])
            elif initial == current:
                if current.__state__.is_dirty:
                    return ([], [], [current])
                else:
                    return NOTSET
            elif current is NOTSET:
                if initial.__state__.is_dirty:
                    return ([], [initial], [])
                else:
                    return NOTSET
            else:
                return ([current], [initial], [])


cdef class RelatedList(ValueStore):
    cpdef object state_init(self, object initial):
        if initial is NOTSET:
            return []
        else:
            return list(initial)

    cpdef object state_set(self, object initial, object current, object value):
        if value is None or isinstance(value, list):
            return value
        else:
            raise ValueError("Related list value must be list or None")

    cpdef object state_get_dirty(self, object initial, object current):
        if current is NOTSET:
            return NOTSET

        cdef list add = []
        cdef list rem = []
        cdef list chg = []
        cdef EntityBase ent

        if initial is NOTSET:
            add.extend(current)
        else:
            for ent in current:
                if ent in initial:
                    if ent.__state__.is_dirty:
                        chg.append(ent)
                else:
                    add.append(ent)

            for ent in initial:
                if ent not in current:
                    rem.append(ent)

        if add or rem or chg:
            return (add, rem, chg)
        else:
            return NOTSET


cdef class RelatedDict(ValueStore):
    pass


cdef class Loading(EntityAttributeExt):
    def __cinit__(self, *, bint always=False, list fields=None):
        self.always = always
        self.fields = fields

    cpdef clone(self):
        return Loading(always=self.always, fields=self.fields)

    def __repr__(self):
        return "@Loading(always=%s, fields=%s)" % (self.always, self.fields)


cdef bint can_determine_join_cond(EntityType entity):
    cdef EntityAttribute attr

    for attr in entity.__deferred__:
        if attr.get_ext(ForeignKey):
            return False

    return True
