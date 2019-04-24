import cython
from ._entity cimport EntityType



@cython.final
cdef class Registry:
    cdef readonly object entities

    cpdef object register(self, str name, EntityType entity)
    cpdef keys(self)
    cpdef values(self)
    cpdef items(self)


@cython.final
cdef class RegistryDiff:
    cdef readonly Registry a
    cdef readonly Registry b

    cdef readonly list changes
