from ._entity cimport EntityType


cdef class Trigger:
    cdef readonly str name
    cdef readonly str before
    cdef readonly str after
    cdef readonly str for_each
    cdef readonly str when
    cdef readonly list params
    cdef readonly list args
    cdef readonly str body
    cdef str unique_name
    # cdef readonly EntityType entity

    cdef str get_unique_name(self, EntityType entity)
    # cdef bind(self, EntityType entity)
    # cdef clone(self)


cdef class OnUpdateTrigger(Trigger):
    pass


cdef class PolymorphParentDeleteTrigger(Trigger):
    cdef readonly EntityType parent_entity
