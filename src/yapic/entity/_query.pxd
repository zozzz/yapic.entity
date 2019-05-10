import cython
from ._expression cimport Expression, Visitor
from ._entity cimport EntityType


@cython.final
cdef class Query(Expression):
    cdef readonly list _select_from
    cdef readonly list _columns
    cdef readonly list _where
    cdef readonly list _order
    cdef readonly list _group
    cdef readonly list _having
    cdef readonly list _distinct
    cdef readonly list _prefix
    cdef readonly list _suffix
    cdef readonly dict _joins
    cdef readonly dict _aliases
    cdef readonly slice _range
    cdef readonly list _entities

    cpdef Query clone(self)
    cdef Query finalize(self)
    # cdef _add_entity(self, EntityType ent)


cdef class QueryFinalizer(Visitor):
    cdef readonly Query q
    cdef readonly list rpks


cdef class RowProcessor:
    pass
