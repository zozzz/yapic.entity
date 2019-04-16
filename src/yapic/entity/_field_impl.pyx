from enum import Enum, Flag
from datetime import date, datetime

from ._entity cimport EntityType, EntityAttributeImpl


cdef class FieldImpl(EntityAttributeImpl):
    cpdef init(self, EntityType entity):
        pass

    cpdef object read(self, value):
        pass

    cpdef object write(self, value):
        pass

    cpdef bint eq(self, a, b):
        return a == b


cdef class StorageType:
    cpdef requirements(self):
        raise NotImplementedError()


cdef class StringImpl(FieldImpl):
    cpdef read(self, value):
        if isinstance(value, str):
            return value
        elif isinstance(value, bytes):
            return value.decode("utf-8")
        else:
            return str(value)

    cpdef write(self, value):
        if isinstance(value, bytes):
            return value
        elif not isinstance(value, str):
            value = str(value)
        return value.encode("utf-8")

    def __repr__(self):
        return "String"


cdef class ChoiceImpl(FieldImpl):
    def __cinit__(self, enum):
        if not issubclass(enum, Enum):
            raise TypeError("Choice type argument must be a subclass of Enum type")

        self._enum = enum
        self.is_multi = issubclass(enum, Flag)

    @property
    def enum(self):
        return self._enum

    cpdef read(self, value):
        return str(value)

    cpdef write(self, value):
        return str(value)

    def __repr__(self):
        return "Choice(%r)" % [item.value for item in self._enum]


cdef class IntImpl(FieldImpl):
    cpdef read(self, value):
        return int(value)

    cpdef write(self, value):
        return str(int(value)).encode("utf-8")

    def __repr__(self):
        return "Int"


cdef class BoolImpl(FieldImpl):
    cpdef read(self, value):
        return bool(value)

    cpdef write(self, value):
        return 1 if value else 0

    def __repr__(self):
        return "Bool"


cdef class DateImpl(FieldImpl):
    cpdef read(self, value):
        return datetime.strptime(value, "%Y-%m-%d").date()

    cpdef write(self, value):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")

    def __repr__(self):
        return "Date"


cdef class DateTimeImpl(FieldImpl):
    cpdef read(self, value):
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")

    cpdef write(self, value):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")

    def __repr__(self):
        return "DateTime"


cdef class DateTimeTzImpl(FieldImpl):
    cpdef read(self, value):
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f%z")

    cpdef write(self, value):
        if value.utcoffset() is None:
            raise ValueError("datetime value must have timezone information")
        return value.strftime("%Y-%m-%d %H:%M:%S.%f%z")

    def __repr__(self):
        return "DateTimeTz"
