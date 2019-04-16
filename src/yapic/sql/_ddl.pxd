from yapic.entity._entity cimport EntityType
from yapic.entity._field cimport Field

from ._dialect cimport Dialect

cdef class DDLCompiler:
    cdef readonly Dialect dialect

    # def compile_entity(self, EntityType entity)
    # async def reflect_entity(self, connection, str table_name)


cdef class DDLReflect:
    pass
