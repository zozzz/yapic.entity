from .._ddl cimport DDLCompiler, DDLReflect

cdef class PostgreDDLCompiler(DDLCompiler):
    pass


cdef class PostgreDDLReflect(DDLReflect):
    pass
