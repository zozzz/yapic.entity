from yapic.entity._query cimport Query

from ._dialect cimport Dialect


cdef class QueryCompiler:
    def __cinit__(self, Dialect dialect):
        self.dialect = dialect
        self.select = []

    cpdef compile_select(self, Query query):
        raise NotImplementedError()
