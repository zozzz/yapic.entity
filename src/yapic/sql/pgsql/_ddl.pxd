from yapic.entity._field cimport Field
from yapic.entity._field_impl cimport StorageType, IntImpl, StringImpl, ChoiceImpl
from .._ddl cimport DDLCompiler

cdef class PostgreDLLCompiler(DDLCompiler):
    cdef _int_type(self, Field field, IntImpl impl)
    cdef _string_type(self, Field field, StringImpl impl)
    cdef _choice_type(self, Field field, ChoiceImpl impl)


cdef class PostgreType(StorageType):
    cdef readonly str pre_sql
