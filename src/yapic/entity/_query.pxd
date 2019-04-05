import cython
from ._expression cimport Expression, Visitor


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
    cdef readonly slice range

    cpdef Query clone(self)
    cdef Query finalize(self)


cdef class RawExpression(Expression):
    cdef readonly str sql


cpdef raw(self, str sql)


cdef class QueryFinalizer(Visitor):
    cdef Query q
