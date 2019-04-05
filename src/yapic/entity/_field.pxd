import cython

from ._entity cimport EntityType, EntityAttribute, EntityAttributeExt
from ._field_impl cimport FieldImpl


cdef class Field(EntityAttribute):
    cdef readonly int min_size
    cdef readonly int max_size
    cdef readonly object nullable

    cdef bint values_is_eq(self, object a, object b)


cdef class FieldExtension(EntityAttributeExt):
    pass


cdef class PrimaryKey(FieldExtension):
    cdef readonly bint auto_increment


cdef class Index(FieldExtension):
    pass


cdef class ForeignKey(FieldExtension):
    cdef object _ref
    cdef readonly str name
    cdef readonly str on_update
    cdef readonly str on_delete


cdef dict collect_foreign_keys(EntityType entity)
