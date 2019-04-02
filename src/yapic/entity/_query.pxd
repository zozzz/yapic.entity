import cython
from ._expression cimport Expression



@cython.final
cdef class Query:
    # cdef readonly QueryExecContext ctx

    cdef readonly list from_clause
    cdef readonly list columns
    cdef readonly list where_clause
    cdef readonly list orders
    cdef readonly list groups
    cdef readonly list havings
    cdef readonly list distincts
    cdef readonly list prefixes
    cdef readonly list suffixes
    cdef readonly list joins
    cdef readonly slice range

    cpdef Query clone(self)


# cdef class QueryExecContext:
#     cpdef QueryExecContext clone(self)


cdef class RawExpression(Expression):
    cdef readonly str sql


cpdef raw(self, str sql)
