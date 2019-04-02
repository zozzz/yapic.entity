from .._query_compiler cimport QueryCompiler
from .._dialect cimport Dialect


cdef class PostgreQueryCompiler(QueryCompiler):
    cdef list parts
    cdef dict table_alias
    cdef list params
