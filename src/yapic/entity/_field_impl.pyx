from enum import Enum, Flag
from datetime import date, datetime

from ._entity cimport EntityType, EntityAttributeImpl


cdef class StringImpl(FieldImpl):
    def __repr__(self):
        return "String"


cdef class BytesImpl(FieldImpl):
    def __repr__(self):
        return "Bytes"


cdef class ChoiceImpl(FieldImpl):
    def __cinit__(self, enum):
        if not issubclass(enum, Enum):
            raise TypeError("Choice type argument must be a subclass of Enum type")

        self._enum = enum
        self.is_multi = issubclass(enum, Flag)

    @property
    def enum(self):
        return self._enum

    def __repr__(self):
        return "Choice(%r)" % [item.value for item in self._enum]


cdef class IntImpl(FieldImpl):
    def __repr__(self):
        return "Int"


cdef class BoolImpl(FieldImpl):
    def __repr__(self):
        return "Bool"


cdef class DateImpl(FieldImpl):
    def __repr__(self):
        return "Date"


cdef class DateTimeImpl(FieldImpl):
    def __repr__(self):
        return "DateTime"


cdef class DateTimeTzImpl(FieldImpl):
    def __repr__(self):
        return "DateTimeTz"
