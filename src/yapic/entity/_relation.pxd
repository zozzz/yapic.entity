import cython
from cpython.object cimport PyObject

from ._entity cimport EntityType, EntityAttribute, EntityAttributeImpl, EntityAttributeExt
from ._expression cimport Expression, PathExpression
from ._factory cimport Factory
from ._field cimport Field


cdef class Relation(EntityAttribute):
    pass


@cython.final
cdef class RelatedAttribute(EntityAttribute):
    cdef readonly Relation __relation__
    cdef readonly EntityAttribute __rattr__
    cdef readonly PathExpression __rpath__


cdef class RelatedAttributeImpl(EntityAttributeImpl):
    pass


cdef class RelationImpl(EntityAttributeImpl):
    cdef object joined_entity_ref
    cdef EntityType joined_alias_ref
    cdef readonly Expression join_expr

    cdef readonly ValueStore state_impl

    cdef EntityType get_joined_entity(self)
    cdef EntityType get_joined_alias(self)

    cdef object resolve_default(self, EntityAttribute relation)
    cdef object _eval(self, Relation attr, str expr)
    cdef bint _can_resolve(self, EntityAttribute relation)


cdef class ManyToOne(RelationImpl):
    cdef Expression _determine_join_expr(self, EntityAttribute relation)


cdef class OneToMany(RelationImpl):
    cdef Expression _determine_join_expr(self, EntityAttribute relation)


cdef class ManyToMany(RelationImpl):
    cdef object across_entity_ref
    cdef EntityType across_alias_ref
    cdef readonly Expression across_join_expr

    cdef EntityType get_across_entity(self)
    cdef EntityType get_across_alias(self)

    cdef tuple _determine_join_expr(self, EntityAttribute relation)


cdef class ValueStore:
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
