from ._entity cimport EntityAttribute, EntityAttributeImpl
from ._expression cimport Expression, BinaryExpression, OrderExpression, PathExpression


cdef class VirtualAttribute(EntityAttribute):
    cdef readonly object _get
    cdef readonly object _set
    cdef readonly object _del
    cdef readonly object _cmp
    cdef readonly object _val
    cdef readonly object _order
    cdef object _source

    cdef Expression get_value_expr(self, object query)
    cdef Expression get_order_expr(self, object query, object op)
    cdef Expression get_compare_expr(self, object query, object op, object value)
    cdef object get_source(self)
    cdef VirtualAttribute with_path(self, PathExpression path)


cdef class VirtualAttributeImpl(EntityAttributeImpl):
    pass


cdef class VirtualBinaryExpression(BinaryExpression):
    cpdef Expression _create_expr_(self, object query)


cdef class VirtualOrderExpression(OrderExpression):
    cpdef Expression _create_expr_(self, object query)
