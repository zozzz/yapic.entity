import cython

from ._entity cimport EntityType
from ._field cimport Field


@cython.final
cdef class EntityDiff:
    cdef readonly EntityType a
    cdef readonly EntityType b
    cdef readonly list changes


# cpdef dict field_eq(Field a, Field b)


# cpdef list compare_exts(list a, list b, list include)
