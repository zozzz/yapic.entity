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


cdef class TimeImpl(FieldImpl):
    pass


cdef class TimeTzImpl(FieldImpl):
    pass


cdef class NumericImpl(FieldImpl):
    pass


cdef class FloatImpl(FieldImpl):
    pass


cdef class UUIDImpl(FieldImpl):
    pass


cdef class ChoiceImpl(FieldImpl):
    cdef object _enum
    cdef readonly bint is_multi


cdef class EntityTypeImpl(FieldImpl):
    cdef readonly EntityType _entity_


cdef class JsonImpl(EntityTypeImpl):
    pass


cdef class CompositeImpl(EntityTypeImpl):
    pass


cdef class NamedTupleImpl(CompositeImpl):
    pass


cdef class AutoImpl(FieldImpl):
    pass
