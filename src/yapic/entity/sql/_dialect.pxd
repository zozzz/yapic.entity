from yapic.entity._entity cimport EntityType, EntityBase
from yapic.entity._field cimport Field, StorageTypeFactory, StorageType
from yapic.entity._expression cimport Expression
from yapic.entity._entity_diff cimport EntityDiff

from ._ddl cimport DDLCompiler, DDLReflect
from ._query_compiler cimport QueryCompiler

cdef class Dialect:
    cpdef DDLCompiler create_ddl_compiler(self)
    cpdef DDLReflect create_ddl_reflect(self, EntityType base)
    cpdef QueryCompiler create_query_compiler(self)
    cpdef StorageTypeFactory create_type_factory(self)

    cpdef str quote_ident(self, str ident)
    cpdef object quote_value(self, object value)
    cpdef str table_qname(self, EntityType entity)
    cpdef StorageType get_field_type(self, Field field)
    cpdef bint expression_eq(self, Expression a, Expression b)
    cpdef EntityDiff entity_diff(self, EntityType a, EntityType b)
