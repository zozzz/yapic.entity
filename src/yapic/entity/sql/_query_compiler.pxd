from yapic.entity._expression cimport Visitor
from yapic.entity._query cimport Query
from yapic.entity._entity cimport EntityType

from ._dialect cimport Dialect


cdef class QueryCompiler(Visitor):
    cdef readonly Dialect dialect
    cdef readonly list select

    cpdef compile_select(self, Query query)
    cpdef compile_insert(self, EntityType entity, list attrs, list names, list values, bint inline_values=*)
    cpdef compile_insert_or_update(self, EntityType entity, list attrs, list names, list values, bint inline_values=*)
    cpdef compile_update(self, EntityType entity, list attrs, list names, list values, bint inline_values=*)
    cpdef compile_delete(self, EntityType entity, list attrs, list names, list values, bint inline_values=*)
