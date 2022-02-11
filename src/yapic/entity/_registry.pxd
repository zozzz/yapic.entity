import cython
from ._entity cimport EntityType, EntityAttribute



@cython.final
cdef class Registry:
    cdef object __weakref__
    cdef readonly object entities
    cdef readonly object locals
    cdef list deferred
    cdef list resolved
    cdef set resolving
    cdef int in_resolving
    cdef bint is_draft

    cdef object register(self, str name, EntityType entity)
    cdef _finalize_entities(self)

    cpdef keys(self)
    cpdef values(self)
    cpdef items(self)
    # cpdef filter(self, fn)
    cpdef list get_foreign_key_refs(self, EntityAttribute column)
    cpdef list get_referenced_foreign_keys(self, EntityAttribute column)
    cdef __get_for_resolving(self)


@cython.final
cdef class RegistryDiff:
    cdef readonly Registry a
    cdef readonly Registry b

    cdef readonly list changes

    cpdef list compare_data(self, list a_ents, list b_ents)
