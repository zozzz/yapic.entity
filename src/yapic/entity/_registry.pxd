import cython
from ._entity cimport EntityType, EntityAttribute


@cython.final
cdef class Registry:
    cdef object __weakref__
    cdef readonly object entities
    cdef readonly ScopeDict locals
    cdef readonly object deferred

    cdef list resolved
    # cdef set resolving
    cdef bint in_resolving
    cdef bint is_draft

    cdef object register(self, EntityType entity)
    cdef _finalize_entities(self)

    cpdef keys(self)
    cpdef values(self)
    cpdef items(self)
    # cpdef filter(self, fn)
    cpdef list get_foreign_key_refs(self, EntityAttribute column)
    cpdef list get_referenced_foreign_keys(self, EntityAttribute column)


@cython.final
cdef class RegistryDiff:
    cdef readonly Registry a
    cdef readonly Registry b

    cdef readonly list changes

    cpdef list compare_data(self, list a_ents, list b_ents)


@cython.final
cdef class ScopeDict(dict):
    cdef set_path(self, str path, object value)
