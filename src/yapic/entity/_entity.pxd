import cython
from cpython.object cimport PyObject

from ._expression cimport AliasExpression, Expression
from ._registry cimport Registry


cdef class NOTSET:
    pass


cdef class EntityType(type):
    cdef readonly tuple __attrs__
    cdef readonly tuple __fields__
    cdef readonly tuple __pk__
    cdef readonly list __deferred__
    cdef public list __fix_entries__
    cdef PyObject* registry
    cdef PyObject* meta
    cdef set deps
    cdef object __weakref__

    cdef object resolve_deferred(self)


cpdef bint is_entity_alias(object o)
cpdef EntityType get_alias_target(EntityType o)


cdef class EntityBase:
    cdef readonly EntityState __state__
    cdef int iter_index


cdef class EntityAttribute(Expression):
    cdef object _impl
    cdef readonly str _key_
    cdef readonly int _index_
    cdef readonly str _name_
    cdef readonly object _default_
    cdef readonly EntityAttributeImpl _impl_
    cdef readonly EntityType _entity_
    cdef readonly list _exts_
    cdef readonly set _deps_

    # returns true when successfully bind, otherwise the system can try bind in the later time
    cdef object bind(self, EntityType entity)
    cpdef clone(self)
    cpdef get_ext(self, ext_type)
    cpdef clone_exts(self, EntityAttribute attr)


cdef class EntityAttributeExt:
    cdef readonly EntityAttribute attr
    cdef list _tmp
    cdef bint bound

    # returns true when successfully bind, otherwise the system can try bind in the later time
    cpdef object bind(self, EntityAttribute attr)
    cpdef object clone(self)


cdef class EntityAttributeImpl:
    cdef bint inited

    # returns true when successfully bind, otherwise the system can try bind in the later time
    cpdef object init(self, EntityAttribute attr)
    cpdef object clone(self)

    cdef object state_init(self, object initial)
    cdef object state_set(self, object initial, object current, object value)
    cdef object state_get_dirty(self, object initial, object current)


@cython.final
@cython.freelist(1000)
cdef class EntityState:
    cdef EntityType entity
    cdef tuple initial
    cdef tuple current
    cdef int field_count

    @staticmethod
    cdef EntityState create_from_dict(EntityType entity, dict data)

    cdef object update(self, dict data, bint is_initial)

    cdef bint set_value(self, EntityAttribute attr, object value)
    cdef object get_value(self, EntityAttribute attr)
    cdef void del_value(self, EntityAttribute attr)

    cdef list data_for_insert(self)
    cdef list data_for_update(self)

    cdef object attr_changes(self, EntityAttribute attr)
    cdef object init_current(self)

    cdef reset_all(self)
    cdef reset_attr(self, EntityAttribute attr)



@cython.final
cdef class DependencyList(list):
    cpdef add(self, EntityType item)
