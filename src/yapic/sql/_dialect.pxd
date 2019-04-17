from yapic.entity._entity cimport EntityType
from yapic.entity._field cimport Field, StorageTypeFactory, StorageType

from ._ddl cimport DDLCompiler, DDLReflect
from ._query_compiler cimport QueryCompiler

cdef class Dialect:
    cdef StorageTypeFactory type_factory

    cpdef DDLCompiler create_ddl_compiler(self)
    cpdef DDLReflect create_ddl_reflect(self, EntityType base)
    cpdef QueryCompiler create_query_compiler(self)

    cpdef str quote_ident(self, str ident)
    cpdef object quote_value(self, object value)
    cpdef str table_qname(self, EntityType entity)
    cpdef StorageType get_field_type(self, Field field)
