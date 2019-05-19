import cython

from ._entity cimport EntityType, EntityAttribute, EntityAttributeExt, EntityAttributeImpl, get_alias_target


cdef class Field(EntityAttribute):
    cdef dict type_cache
    cdef readonly int min_size
    cdef readonly int max_size
    cdef readonly object nullable

    # cdef bint values_is_eq(self, object a, object b)
    cpdef StorageType get_type(self, StorageTypeFactory factory)


cdef inline bint field_eq(Field a, Field b):
    return a._uid_ is b._uid_
    # return get_alias_target(a._entity_) is get_alias_target(b._entity_) \
    #     and a._name_ == b._name_


# cdef class FieldProxy(Field):
#     cdef readonly Field __proxied__


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
