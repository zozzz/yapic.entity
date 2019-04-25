from ._entity cimport EntityAttributeImpl
from ._field cimport Field, FieldImpl


cdef class StringImpl(FieldImpl):
    pass


cdef class BytesImpl(FieldImpl):
    pass


cdef class IntImpl(FieldImpl):
    pass


cdef class BoolImpl(FieldImpl):
    pass


cdef class DateImpl(FieldImpl):
    pass


cdef class DateTimeImpl(FieldImpl):
    pass


cdef class DateTimeTzImpl(FieldImpl):
    pass


cdef class ChoiceImpl(FieldImpl):
    cdef object _enum
    cdef readonly bint is_multi
