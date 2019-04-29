from yapic.entity._expression cimport Visitor
from yapic.entity._query cimport Query

from ._dialect cimport Dialect


cdef class QueryCompiler(Visitor):
    cdef readonly Dialect dialect
    cdef readonly list select

    cpdef compile_select(self, Query query)
