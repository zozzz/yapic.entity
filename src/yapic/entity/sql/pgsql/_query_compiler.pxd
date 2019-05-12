from yapic.entity._query cimport Query

from .._query_compiler cimport QueryCompiler
from .._dialect cimport Dialect


cdef class PostgreQueryCompiler(QueryCompiler):
    cdef list parts
    cdef dict table_alias
    cdef list params
    cdef PostgreQueryCompiler parent
    cdef readonly list rcos_list

    cpdef init_subquery(self, PostgreQueryCompiler parent)
