import cython

from ._entity cimport EntityType, EntityAttribute, EntityAttributeExt, EntityAttributeImpl, get_alias_target
from ._expression cimport Expression


cdef class Field(EntityAttribute):
    cdef dict type_cache
    cdef readonly int min_size
    cdef readonly int max_size
    cdef readonly object nullable
    cdef readonly object on_update

    # cdef bint values_is_eq(self, object a, object b)
    cpdef StorageType get_type(self, StorageTypeFactory factory)


cdef inline bint field_eq(Field a, Field b):
    return a._uid_ is b._uid_


cdef class FieldExtension(EntityAttributeExt):
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
    cdef object _seq_arg
    cdef readonly EntityType sequence


cdef class Index(FieldExtension):
    cdef readonly str name
    cdef readonly str method
    cdef readonly bint unique
    cdef readonly str collate
    cdef readonly str expr


cdef class Unique(FieldExtension):
    cdef readonly str name


cdef class ForeignKey(FieldExtension):
    cdef object _ref
    cdef readonly Field ref
    cdef readonly str name
    cdef readonly str on_update
    cdef readonly str on_delete


# name: chk_table__field
# comment: {expr: hash, table: tablename, field: fieldname}
cdef class Check(FieldExtension):
    cdef readonly str name
    cdef readonly str expr
    cdef readonly dict props

    cdef str expr_hash(self)

