from yapic.entity._entity cimport EntityType
from yapic.entity._field cimport Field, StorageType

from ._ddl cimport DDLCompiler
from ._query_compiler cimport QueryCompiler


cdef class Dialect:
    cpdef DDLCompiler create_ddl_compiler(self):
        raise NotImplementedError()

    cpdef QueryCompiler create_query_compiler(self):
        raise NotImplementedError()

    cpdef str quote_ident(self, str ident):
        raise NotImplementedError()

    cpdef object quote_value(self, object value):
        raise NotImplementedError()

    cpdef str table_qname(self, EntityType entity):
        raise NotImplementedError()

    cpdef StorageType get_field_type(self, Field field):
        return field.get_type(self.type_factory)

