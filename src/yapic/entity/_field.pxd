import cython

from ._expression cimport Expression
from ._entity cimport EntityType
from ._field_impl cimport FieldImpl


cdef class Field(Expression):
    cdef FieldImpl _impl
    cdef object _default

    cdef readonly str name
    cdef readonly int min_size
    cdef readonly int max_size
    cdef readonly int index
    cdef readonly object nullable
    cdef readonly object extensions
    cdef readonly EntityType entity

    cdef void bind(self, EntityType entity)
    cdef bint values_is_eq(self, object a, object b)
    cdef object get_ext(self, ext_type)


cdef class FieldExtension:
    cdef readonly Field field


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
