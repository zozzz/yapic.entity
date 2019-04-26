import cython
from ._entity cimport EntityType



@cython.final
cdef class Registry:
    cdef readonly object entities
    cdef list deferred

    cpdef object register(self, str name, EntityType entity)
    cpdef keys(self)
    cpdef values(self)
    cpdef items(self)

    cdef resolve_deferred(self)


@cython.final
cdef class RegistryDiff:
    cdef readonly Registry a
    cdef readonly Registry b

    cdef readonly list changes
