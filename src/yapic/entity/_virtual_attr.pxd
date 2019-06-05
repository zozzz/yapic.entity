from ._entity cimport EntityAttribute, EntityAttributeImpl
from ._expression cimport BinaryExpression


cdef class VirtualAttribute(EntityAttribute):
    cdef readonly object _get
    cdef readonly object _set
    cdef readonly object _del
    cdef readonly object _cmp
    cdef readonly object _val


cdef class VirtualAttributeImpl(EntityAttributeImpl):
    pass
