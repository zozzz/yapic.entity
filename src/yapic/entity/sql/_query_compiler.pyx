from yapic.entity._query cimport Query
from yapic.entity._entity cimport EntityType

from ._dialect cimport Dialect


cdef class QueryCompiler:
    def __cinit__(self, Dialect dialect):
        self.dialect = dialect
        self.select = []

    cpdef compile_select(self, Query query):
        raise NotImplementedError()

    cpdef compile_insert(self, EntityType entity, list attrs, list names, list values, bint inline_values=False):
        raise NotImplementedError()

    cpdef compile_insert_or_update(self, EntityType entity, list attrs, list names, list values, bint inline_values=False):
        raise NotImplementedError()

    cpdef compile_update(self, EntityType entity, list attrs, list names, list values, bint inline_values=False):
        raise NotImplementedError()

    cpdef compile_delete(self, EntityType entity, list attrs, list names, list values, bint inline_values=False):
        raise NotImplementedError()
