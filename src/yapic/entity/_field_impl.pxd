from ._entity cimport EntityType, EntityAttributeImpl
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


cdef class JsonImpl(FieldImpl):
    cdef EntityType _entity_


cdef class CompositeImpl(FieldImpl):
    cdef EntityType _entity_


cdef class AutoImpl(FieldImpl):
    pass
