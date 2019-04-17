import cython
from ._entity cimport EntityType



@cython.final
cdef class Registry:
    cdef readonly object entities

    cpdef object register(self, str name, EntityType entity)
