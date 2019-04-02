from yapic.entity._entity cimport EntityType
from ._ddl cimport DDLCompiler
from ._query_compiler cimport QueryCompiler


cdef class Dialect:
    cpdef DDLCompiler create_ddl_compiler(self):
        raise NotImplementedError()

    cpdef QueryCompiler create_query_compiler(self):
        raise NotImplementedError()

    cpdef str quote_ident(self, str ident):
        raise NotImplementedError()

    cpdef str quote_value(self, object value):
        raise NotImplementedError()

    cpdef str table_qname(self, EntityType entity):
        raise NotImplementedError()
