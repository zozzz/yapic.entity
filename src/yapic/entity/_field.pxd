import cython

from ._expression cimport Expression
from ._entity cimport EntityType



cdef class Field(Expression):
    cdef FieldImpl _impl
    cdef object _default

    cdef readonly str name
    cdef readonly int min_size
    cdef readonly int max_size
    cdef readonly int index
    cdef readonly object extensions
    cdef readonly EntityType entity

    cdef void bind(self, EntityType entity)
    cdef bint values_is_eq(self, object a, object b)


cdef class FieldImpl:
    cpdef object read(self, value)
    cpdef object write(self, value)
    cpdef bint eq(self, a, b)


cdef class FieldExtension:
    cdef readonly Field field
