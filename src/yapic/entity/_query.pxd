import cython
from ._expression cimport Expression, Visitor
from ._entity cimport EntityType


@cython.final
cdef class Query(Expression):
    cdef readonly list from_clause
    cdef readonly list columns
    cdef readonly list entity_columns
    cdef readonly list where_clause
    cdef readonly list orders
    cdef readonly list groups
    cdef readonly list havings
    cdef readonly list distincts
    cdef readonly list prefixes
    cdef readonly list suffixes
    cdef readonly dict joins
    cdef readonly dict entities
    cdef readonly slice range

    cpdef Query clone(self)
    cdef Query finalize(self)
    cdef _add_entity(self, EntityType ent)


cdef class RawExpression(Expression):
    cdef readonly str sql


cpdef raw(self, str sql)


cdef class QueryFinalizer(Visitor):
    cdef Query q
