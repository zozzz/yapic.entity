from ._entity cimport EntityAttributeImpl
from ._field cimport Field


cdef class FieldImpl(EntityAttributeImpl):
    cpdef object read(self, value)
    cpdef object write(self, value)
    cpdef bint eq(self, a, b)


cdef class StorageType:
    cdef readonly name
    cpdef requirements(self)


cdef class StringImpl(FieldImpl):
    pass


cdef class IntImpl(FieldImpl):
    pass


cdef class ChoiceImpl(FieldImpl):
    cdef object _enum
    cdef readonly bint is_multi
