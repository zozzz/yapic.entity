import cython

from ._entity cimport EntityType, EntityAttribute, EntityAttributeExt, EntityAttributeImpl


cdef class Field(EntityAttribute):
    cdef dict type_cache
    cdef readonly int min_size
    cdef readonly int max_size
    cdef readonly object nullable

    # cdef bint values_is_eq(self, object a, object b)
    cpdef StorageType get_type(self, StorageTypeFactory factory)


cdef class FieldExtension(EntityAttributeExt):
    pass


cdef class FieldImpl(EntityAttributeImpl):
    pass


cdef class StorageType:
    cdef readonly name
    cpdef object encode(self, object value)
    cpdef object decode(self, object value)


cdef class StorageTypeFactory:
    cpdef StorageType create(self, Field field)


cdef class PrimaryKey(FieldExtension):
    pass


cdef class AutoIncrement(FieldExtension):
    cdef readonly EntityType sequence


cdef class Index(FieldExtension):
    pass


cdef class ForeignKey(FieldExtension):
    cdef object _ref
    cdef readonly Field ref
    cdef readonly str name
    cdef readonly str on_update
    cdef readonly str on_delete


cdef dict collect_foreign_keys(EntityType entity)
