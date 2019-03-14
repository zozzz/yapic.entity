import cython

from ._entity cimport EntityType
from ._expression cimport Expression
from ._field cimport Field


cdef class Relation(Expression):
    cdef object _impl
    cdef readonly EntityType __entity__

    cdef void bind(self, EntityType entity)


cdef class RelationField(Expression):
    cdef Relation relation
    cdef Field field


cdef class RelationImpl:
    cpdef object get_value(self)
    cpdef void set_value(self, object value)
    cpdef void del_value(self)


cdef class ManyToOne(RelationImpl):
    cdef readonly EntityType joined
    cdef readonly RelatedItem value


cdef class OneToMany(RelationImpl):
    cdef readonly EntityType joined
    cdef readonly RelatedContainer value


cdef class ManyToMany(RelationImpl):
    cdef readonly EntityType joined
    cdef readonly EntityType across
    cdef readonly RelatedContainer value


cdef class RelatedItem:
    cdef object original
    cdef object current

    cdef object get_value(self)
    cdef void set_value(self, object value)
    cdef void del_value(self)
    cpdef reset(self)


cdef class RelatedContainer:
    # dict of tuple (OP_REMOVE / OP_ADD / OP_REPLACE, original_value)
    cdef readonly dict __operations__

    cpdef _set_item(self, object key, object value)
    cpdef _get_item(self, object key)
    cpdef _del_item(self, object key)


cdef class RelatedList(RelatedContainer):
    cdef list value

    cpdef append(self, object o)
    cpdef extend(self, object o)
    cpdef insert(self, object index, object o)
    cpdef remove(self, object o)
    cpdef pop(self, object index=*)
    cpdef clear(self)
    cpdef reset(self)

    # __getitem__
    # __setitem__
    # __delitem__
    # __len__
    # __contains__
    # __iter__


cdef class RelatedDict(RelatedContainer):
    cdef dict value

    cpdef get(self, object key, object dv=*)
    cpdef items(self)
    cpdef keys(self)
    cpdef pop(self, object key, object dv=*)
    cpdef popitem(self)
    cpdef setdefault(self, object key, object dv=*)
    cpdef update(self, object other)
    cpdef values(self)
    cpdef clear(self)
    cpdef reset(self)

    # __getitem__
    # __setitem__
    # __delitem__
    # __len__
    # __contains__
    # __iter__
