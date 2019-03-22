from enum import Enum, Flag


cdef class FieldImpl:
    cpdef object read(self, value):
        pass

    cpdef object write(self, value):
        pass

    cpdef bint eq(self, a, b):
        return self.write(a) == self.write(b)


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

    cpdef bint eq(self, a, b):
        return a == b

    def __repr__(self):
        return "Int"
