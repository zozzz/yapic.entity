import cython
from cpython.object cimport PyObject

from ._entity cimport EntityType, EntityAttribute, EntityAttributeImpl, EntityAttributeExt
from ._expression cimport Expression, PathExpression
from ._factory cimport Factory
from ._field cimport Field


cdef class Relation(EntityAttribute):
    cdef object update_join_expr(self)


@cython.final
cdef class RelatedAttribute(EntityAttribute):
    cdef readonly Relation __relation__
    cdef readonly EntityAttribute __rattr__
    cdef readonly PathExpression __rpath__


cdef class RelatedAttributeImpl(EntityAttributeImpl):
    pass


cdef class RelationImpl(EntityAttributeImpl):
    cdef readonly ValueStore state_impl
    # orignal joined entity
    cdef readonly EntityType _joined
    # entity alias for relation
    cdef readonly EntityType joined
    cdef readonly object join_expr

    # cdef object new_value_store(self)
    # cdef void set_value_store_type(self, object t)
    cdef object determine_join_expr(self, EntityType entity, Relation attr)
    cdef object resolve_default(self, Relation attr)
    cdef object _eval(self, Relation attr, str expr)


cdef class ManyToOne(RelationImpl):
    pass


cdef class OneToMany(RelationImpl):
    pass

cdef class ManyToMany(RelationImpl):
    cdef readonly EntityType _across
    cdef readonly EntityType across
    cdef readonly object across_join_expr


# @cython.final
# @cython.freelist(1000)
# cdef class RelationState:
#     cdef tuple data

#     cdef object get_value(self, int index)
#     cdef bint set_value(self, int index, object value)
#     cdef bint del_value(self, int index)
#     cpdef reset(self)


cdef class ValueStore:
    # cdef object get_value(self)
    # cdef bint set_value(self, object value)
    # cdef bint del_value(self)
    # cpdef reset(self)

    cpdef object state_init(self, object initial)
    cpdef object state_set(self, object initial, object current, object value)
    cpdef object state_get_dirty(self, object initial, object current)


cdef class RelatedItem(ValueStore):
    pass


cdef class RelatedList(ValueStore):
    pass


cdef class RelatedDict(ValueStore):
    pass


cdef class Loading(EntityAttributeExt):
    cdef readonly bint always
    cdef readonly list fields


# cdef class RelatedItem(ValueStore):
#     cdef PyObject* original
#     cdef PyObject* current


# # cpdef RCOP_ADD = 1
# # cpdef RCOP_MOD = 2
# # cpdef RCOP_DEL = 4


# cdef class RelatedContainer(ValueStore):
#     # dict of tuple (RCOP_ADD / RCOP_MOD / RCOP_DEL, original_value)
#     # cdef readonly dict __operations__
#     cdef readonly list __removed__
#     cdef readonly list __added__

#     cpdef _op_add(self, object value)
#     cpdef _op_del(self, object value)


# cdef class RelatedList(RelatedContainer):
#     cdef list value


#     cpdef append(self, object o)
#     cpdef extend(self, list o)
#     cpdef insert(self, object index, object o)
#     cpdef remove(self, object o)
#     cpdef pop(self, object index=*)
#     cpdef clear(self)
#     cdef void _op_delitems_by_key(self, key)

#     # __getitem__
#     # __setitem__
#     # __delitem__
#     # __len__
#     # __contains__
#     # __iter__


# cdef class RelatedDict(RelatedContainer):
#     cdef dict value

#     cpdef get(self, object key, object dv=*)
#     cpdef items(self)
#     cpdef keys(self)
#     cpdef pop(self, object key, object dv=*)
#     cpdef popitem(self)
#     cpdef setdefault(self, object key, object dv=*)
#     cpdef update(self, object other)
#     cpdef values(self)
#     cpdef clear(self)

#     # __getitem__
#     # __setitem__
#     # __delitem__
#     # __len__
#     # __contains__
#     # __iter__


cdef determine_join_expr(EntityType entity, EntityType joined)
