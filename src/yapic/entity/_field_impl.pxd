from ._entity cimport EntityType, EntityBase, EntityAttributeImpl
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


cdef class EntityTypeImpl(FieldImpl):
    cdef readonly EntityType _entity_


cdef class JsonImpl(FieldImpl):
    cdef EntityType _object_
    cdef EntityType _list_
    cdef bint _any_

    cdef bint __check_dirty(self, object value)
    cdef object __list_item(self, object value)


# cdef class JsonArrayImpl(JsonImpl):
#     cdef bint __check_dirty(self, list value)


cdef class CompositeImpl(EntityTypeImpl):
    cpdef object data_for_write(self, EntityBase value, bint for_insert)


cdef class NamedTupleImpl(CompositeImpl):
    pass


cdef class AutoImpl(FieldImpl):
    cdef object _ref_impl


cdef class ChoiceImpl(AutoImpl):
    cdef object _enum


cdef class ArrayImpl(FieldImpl):
    cdef object _item_impl_
