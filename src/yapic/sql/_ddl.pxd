from yapic.entity._entity cimport EntityType
from yapic.entity._field cimport Field
from yapic.entity._field_impl cimport StorageType

from ._str_builder cimport UnicodeBuilder
from ._dialect cimport Dialect

cdef class DDLCompiler:
    cdef readonly Dialect dialect

    cpdef StorageType guess_type(self, Field field)
    # def compile_entity(self, EntityType entity)
    # async def reflect_entity(self, connection, str table_name)


cdef class DDLReflect:
    pass
