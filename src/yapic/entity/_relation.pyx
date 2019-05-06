import sys
import cython
from cpython.ref cimport Py_XDECREF, Py_XINCREF, Py_DECREF, Py_INCREF, Py_CLEAR
from cpython.object cimport PyObject, PyObject_RichCompareBool, Py_EQ
from cpython.tuple cimport PyTuple_SetItem, PyTuple_GetItem, PyTuple_New, PyTuple_GET_SIZE, PyTuple_SET_ITEM, PyTuple_GET_ITEM, PyTuple_Pack
from cpython.module cimport PyImport_Import, PyModule_GetDict

from ._entity cimport EntityType, EntityBase, EntityAttribute, EntityAttributeImpl, get_alias_target, NOTSET
from ._expression cimport Expression, Visitor, PathExpression
from ._field cimport Field, ForeignKey, collect_foreign_keys
from ._factory cimport Factory, ForwardDecl, new_instance_from_forward, is_forward_decl
from ._visitors cimport replace_entity
from ._error cimport JoinError


cdef class Relation(EntityAttribute):
    cdef object bind(self, EntityType entity):
        cdef RelationImpl impl
        if EntityAttribute.bind(self, entity):
            impl = self._impl_
            if self._default_:
                impl = self._impl_
                if not impl.resolve_default(self):
                    return False

            return impl.determine_join_expr(entity, self)
        else:
            return False

    def __getattr__(self, name):
        cdef EntityType joined = self._impl_.joined
        cdef EntityAttribute attr = getattr(joined, name)
        return PathExpression(self, [attr])

    cpdef visit(self, Visitor visitor):
        return visitor.visit_relation(self)

    def __repr__(self):
        return "<Relation %s :: %s>" % (self._entity_, self._impl_)

    cpdef object clone(self):
        cdef EntityAttribute res = type(self)(self._impl_.clone())
        res._exts_ = self.clone_exts(res)
        res._deps_ = set(self._deps_)
        return res


# cdef class RelationAttribute(Expression):
#     def __cinit__(self, Relation relation, EntityAttribute attr):
#         self.relation = relation
#         self.attr = attr

#     cpdef visit(self, Visitor visitor):
#         return visitor.visit_relation_attribute(self)

#     def __getattr__(self, name):
#         return getattr(self.attr, name)

#     def __getitem__(self, index):
#         return self.attr[index]

#     def __repr__(self):
#         return "<RelationAttribute %s :: %s>" % (self.relation, self.attr._name_)


cdef class RelationImpl(EntityAttributeImpl):
    def __cinit__(self, joined, state_impl, *args):
        self.joined = joined
        self.state_impl = state_impl

    cdef object determine_join_expr(self, EntityType entity, Relation attr):
        raise NotImplementedError()

    cdef object resolve_default(self, Relation attr):
        raise NotImplementedError()

    cpdef object clone(self):
        return type(self)(self.joined, self.state_impl)

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
        return eval(expr, <object>mdict, <object>ldict)


cdef class ManyToOne(RelationImpl):
    def __repr__(self):
        return "ManyToOne %r" % self.joined

    cdef object determine_join_expr(self, EntityType entity, Relation attr):
        if self.joined is not entity and self.joined.__deferred__:
            return False

        if attr._default_:
            self.join_expr = attr._default_
        else:
            self.join_expr = determine_join_expr(entity, self.joined)
        attr._deps_.add(self.joined)
        return True

    cdef object resolve_default(self, Relation attr):
        if isinstance(attr._default_, str):
            try:
                attr._default_ = self._eval(attr, attr._default_)
            except NameError:
                return False


        if isinstance(attr._default_, Expression):
            return True
        else:
            raise ValueError("Invalid value for join expression: %r" % attr._default_)


cdef class OneToMany(RelationImpl):
    def __repr__(self):
        return "OneToMany %r" % self.joined

    cdef object determine_join_expr(self, EntityType entity, Relation attr):
        if self.joined is not entity and self.joined.__deferred__:
            return False

        if attr._default_:
            self.join_expr = attr._default_
        else:
            self.join_expr = determine_join_expr(self.joined, entity)
        return True

    cdef object resolve_default(self, Relation attr):
        if isinstance(attr._default_, str):
            try:
                attr._default_ = self._eval(attr, attr._default_)
            except NameError:
                return False

        if isinstance(attr._default_, Expression):
            return True
        else:
            raise ValueError("Invalid value for join expression: %r" % attr._default_)


cdef class ManyToMany(RelationImpl):
    def __cinit__(self, joined, state_impl, across):
        self.across = across

    def __repr__(self):
        return "ManyToMany %r => %r" % (self.across, self.joined)

    cdef object determine_join_expr(self, EntityType entity, Relation attr):
        if self.joined is not entity and (self.joined.__deferred__ or self.across.__deferred__):
            return False

        if attr._default_:
            self.across_join_expr = attr._default_[self.across]
            self.join_expr = attr._default_[self.joined]
        else:
            self.across_join_expr = determine_join_expr(self.across, entity)
            self.join_expr = determine_join_expr(self.across, self.joined)
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
        return type(self)(self.joined, self.state_impl, self.across)


cdef determine_join_expr(EntityType entity, EntityType joined):
    cdef dict fks = collect_foreign_keys(entity)

    cdef list keys
    cdef Field field
    cdef ForeignKey fk
    cdef object found = None

    for fk_name, keys in fks.items():
        fk = <ForeignKey>keys[0]

        if fk.ref._entity_ is joined:
            if found is not None:
                raise JoinError("Multiple join conditions between %s <-> %s" % (entity, joined))

            found = fk.attr == fk.ref
            for i in range(1, len(keys)):
                fk = <ForeignKey>keys[i]
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


# cdef class RelatedItem(ValueStore):
#     def __cinit__(self):
#         self.original = NULL
#         self.current = NULL

#     cdef object get_value(self):
#         if self.current is NULL:
#             if self.original is NULL:
#                 return None
#             else:
#                 return <object>self.original
#         else:
#             return <object>self.current

#     cdef bint set_value(self, object value):
#         if self.current is NULL:
#             self.current = <PyObject*>value
#             Py_XINCREF(self.current)
#         elif self.original is NULL or not PyObject_RichCompareBool(<object>self.original, value, Py_EQ):
#             Py_XDECREF(self.current)
#             self.current = <PyObject*>value
#             Py_XINCREF(self.current)
#         return 1

#     cdef bint del_value(self):
#         Py_XDECREF(self.current)
#         self.current = <PyObject*>None
#         Py_XINCREF(self.current)
#         return 1

#     cpdef reset(self):
#         if self.current is not NULL:
#             Py_XDECREF(self.original)
#             self.original = self.current
#             self.current = NULL

#     def __dealloc__(self):
#         Py_CLEAR(self.original)
#         Py_CLEAR(self.current)


# cdef class RelatedContainer(ValueStore):
#     def __cinit__(self):
#         # todo: maybe use set instead of list
#         self.__removed__ = []
#         self.__added__ = []

#     cpdef _op_add(self, object value):
#         try:
#             key = self.__removed__.index(value)
#         except:
#             pass
#         else:
#             del self.__removed__[key]

#         if value not in self.__added__:
#             self.__added__.append(value)

#     cpdef _op_del(self, object value):
#         try:
#             key = self.__added__.index(value)
#         except:
#             pass
#         else:
#             del self.__added__[key]

#         if value not in self.__removed__:
#             self.__removed__.append(value)

#     cpdef reset(self):
#         self.__added__ = []
#         self.__removed__ = []


# cdef class RelatedList(RelatedContainer):
#     def __cinit__(self):
#         self.value = []

#     cdef object get_value(self):
#         return self

#     cdef bint set_value(self, object value):
#         for item in self.value:
#             self._op_del(item)

#         if isinstance(value, tuple):
#             self.value = list(<tuple>value)
#         else:
#             self.value = value

#         for item in self.value:
#             self._op_add(item)
#         return 1

#     cdef bint del_value(self):
#         for item in self.value:
#             self._op_del(item)
#         self.value = []
#         return 1

#     cpdef append(self, object o):
#         self.value.append(o)
#         self._op_add(o)

#     cpdef extend(self, list o):
#         self.value.extend(o)
#         for item in o:
#             self._op_add(item)

#     cpdef insert(self, object index, object o):
#         self.value.insert(index, o)
#         self._op_add(o)

#     cpdef remove(self, object o):
#         self.value.remove(o)
#         self._op_del(o)

#     cpdef pop(self, object index = None):
#         item = self.value.pop(index)
#         self._op_del(item)
#         return item

#     cpdef clear(self):
#         for item in self.value:
#             self._op_del(item)
#         del self.value[:]

#     cpdef reset(self):
#         del self.__removed__[:]
#         del self.__added__[:]

#     def __getitem__(self, key):
#         return self.value[key]

#     def __setitem__(self, key, value):
#         self._op_delitems_by_key(key)

#         self.value[key] = value

#         if isinstance(key, int):
#             self._op_add(value)
#         elif isinstance(key, slice):
#             for x in value:
#                 self._op_add(x)

#     def __delitem__(self, key):
#         self._op_delitems_by_key(key)
#         del self.value[key]

#     def __len__(self):
#         return len(self.value)

#     def __contains__(self, value):
#         return value in self.value

#     def __iter__(self):
#         return iter(self.value)

#     def __str__(self):
#         return str(self.value)

#     def __repr__(self):
#         return repr(self.value)

#     cdef void _op_delitems_by_key(self, key):
#         if isinstance(key, int):
#             if key >= 0 and key < len(self.value):
#                 self._op_del(self.value[key])
#         elif isinstance(key, slice):
#             for x in xrange(*key.indices(len(self.value))):
#                 self._op_del(self.value[x])


# cdef class RelatedDict(RelatedContainer):
#     cpdef get(self, object key, object dv = None):
#         raise NotImplementedError()

#     cpdef items(self):
#         raise NotImplementedError()

#     cpdef keys(self):
#         raise NotImplementedError()

#     cpdef pop(self, object key, object dv = None):
#         raise NotImplementedError()

#     cpdef popitem(self):
#         raise NotImplementedError()

#     cpdef setdefault(self, object key, object dv = None):
#         raise NotImplementedError()

#     cpdef update(self, object other):
#         raise NotImplementedError()

#     cpdef values(self):
#         raise NotImplementedError()

#     cpdef clear(self):
#         raise NotImplementedError()

#     cpdef reset(self):
#         raise NotImplementedError()

#     # __getitem__
#     # __setitem__
#     # __delitem__
#     # __len__
#     # __contains__
#     # __iter__
