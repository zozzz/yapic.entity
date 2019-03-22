from ._field cimport Field


cdef class FieldImpl:
    cpdef object read(self, value)
    cpdef object write(self, value)
    cpdef bint eq(self, a, b)


cdef class StringImpl(FieldImpl):
    pass


cdef class IntImpl(FieldImpl):
    pass


cdef class ChoiceImpl(FieldImpl):
    cdef object _enum
    cdef readonly bint is_multi
