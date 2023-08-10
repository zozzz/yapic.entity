from ._entity cimport EntityType, EntityBase, EntityAttributeImpl, EntityAttribute
from ._field cimport Field


cdef class FieldImpl(EntityAttributeImpl):
    pass


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
    # TODO: rename
    cdef readonly EntityType _entity_


cdef class JsonImpl(FieldImpl):
    cdef readonly EntityType _object_
    cdef readonly EntityType _list_
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
    cdef readonly object _ref_impl


cdef class ChoiceImpl(AutoImpl):
    cdef readonly object _enum
    cdef readonly EntityAttribute _relation

    cdef object _coerce(self, object value)


cdef class ArrayImpl(FieldImpl):
    cdef readonly object _item_impl_
