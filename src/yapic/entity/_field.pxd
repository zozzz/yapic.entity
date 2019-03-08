import cython

from ._expression cimport Expression
from ._entity cimport EntityType


@cython.auto_pickle(False)
cdef class Field(Expression):
    cdef readonly object default_
    cdef readonly str name
    cdef readonly int min_size
    cdef readonly int max_size
    cdef readonly int index
    cdef readonly object extensions
    cdef readonly FieldImpl __impl__
    cdef readonly EntityType entity

    cdef void bind(self, EntityType entity, str name)
    cdef bint values_is_eq(self, object a, object b)


@cython.auto_pickle(False)
cdef class FieldImpl:
    cpdef object read(self, value)
    cpdef object write(self, value)
    cpdef bint eq(self, a, b)


@cython.auto_pickle(False)
cdef class FieldExtension:
    cdef readonly Field field
