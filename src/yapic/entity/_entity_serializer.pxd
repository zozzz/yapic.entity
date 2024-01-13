import cython

from ._entity cimport EntityBase, EntityType, EntityAttribute, EntityAttributeExt


@cython.final
@cython.freelist(200)
cdef class SerializerCtx:
    cdef bint skip_attribute(self, EntityAttribute attr, object value)
    cdef SerializerCtx enter(self, str path)


@cython.final
@cython.freelist(200)
cdef class EntitySerializer:
    cdef readonly EntityBase instance
    cdef readonly EntityType entity
    cdef readonly SerializerCtx ctx
    cdef int idx
    cdef int length

    # cdef object _next(self)


cdef class DontSerialize(EntityAttributeExt):
    pass
