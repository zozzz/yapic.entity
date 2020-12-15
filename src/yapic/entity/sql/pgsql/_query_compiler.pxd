from .._query cimport Query, QueryCompiler
from .._dialect cimport Dialect


cdef class PostgreQueryCompiler(QueryCompiler):
    cdef list parts
    cdef dict table_alias
    cdef list params
    cdef PostgreQueryCompiler parent
    cdef bint inline_values

    cpdef init_subquery(self, PostgreQueryCompiler parent)
