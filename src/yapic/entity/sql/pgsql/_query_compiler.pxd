from yapic.entity._query cimport Query

from .._query_compiler cimport QueryCompiler
from .._dialect cimport Dialect


cdef class PostgreQueryCompiler(QueryCompiler):
    cdef list parts
    cdef dict table_alias
    cdef list params
    cdef PostgreQueryCompiler parent
    cdef readonly list rcos_list
    cdef bint inline_values

    cpdef init_subquery(self, PostgreQueryCompiler parent)
