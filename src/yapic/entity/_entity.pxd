import cython
from cpython.object cimport PyObject

from ._expression cimport AliasExpression, Expression


cdef class EntityType(type):
    cdef readonly tuple __attrs__
    cdef readonly tuple __fields__
    # cdef readonly tuple __relations__
    cdef PyObject* meta
    cdef object __weakref__

    # cpdef __clone__(self, dict meta)

cpdef bint is_entity_alias(object o)
cpdef EntityType get_alias_target(EntityType o)


cdef class EntityBase:
    # cdef readonly FieldState __fstate__
    # cdef readonly object __rstate__
    cdef readonly EntityState __state__


cdef class EntityAttribute(Expression):
    cdef object _impl
    cdef str _attr_name_in_class
    cdef readonly int _index_
    cdef readonly str _name_
    cdef readonly object _default_
    cdef readonly EntityType _entity_
    cdef readonly list _exts_

    cdef bind(self, EntityType entity)
    cpdef clone(self)
    cpdef get_ext(self, ext_type)
    cpdef clone_exts(self, EntityAttribute attr)


cdef class EntityAttributeExt:
    cdef readonly EntityAttribute attr
    cdef list _tmp

    cpdef object bind(self, EntityAttribute attr)
    cpdef object clone(self)


cdef class EntityAttributeImpl:
    cdef bint inited
    cpdef init(self, EntityType entity)
    cpdef object clone(self)

    cdef PyObject* state_set(self, PyObject* current, PyObject* value)
    cdef PyObject* state_get(self, PyObject* current)
    # cdef object state_del(self, PyObject** current)



# cdef class EntityAliasExpression(Expression):
#     cdef readonly EntityType entity
#     cdef readonly str value

#     cdef readonly tuple __fields__
#     cdef readonly tuple __relations__


@cython.final
@cython.freelist(1000)
cdef class FieldState:
    cdef tuple fields
    cdef tuple data
    cdef tuple dirty
    cdef bint is_dirty

    cdef bint set_value(self, int index, object value)
    cdef object get_value(self, int index)
    cdef void del_value(self, int index)

    cpdef reset(self)


@cython.final
@cython.freelist(1000)
cdef class EntityState:
    cdef tuple data
    cdef int field_count

    @staticmethod
    cdef EntityState create_from_dict(EntityType entity, dict data)

    cdef object update(self, EntityType entity, dict data, bint is_initial)

    cdef bint set_value(self, EntityAttribute attr, object value)
    cdef object get_value(self, EntityAttribute attr)
    cdef void del_value(self, EntityAttribute attr)

    cdef list data_for_insert(self, EntityType entity)
    cdef list data_for_update(self, EntityType entity)

    cpdef reset(self)


cdef class EntityStateValue:
    pass
