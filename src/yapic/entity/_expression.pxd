import cython


cdef class Visitor:
    cpdef visit(self, Expression expr)


cdef class Expression:
    cpdef visit(self, Visitor visitor)


cdef class BinaryExpression(Expression):
    cpdef readonly Expression left
    cpdef readonly Expression right
    cpdef readonly object op


cdef class UnaryExpression(Expression):
    cpdef readonly Expression expr
    cpdef readonly object op


cdef class ConstExpression(Expression):
    cpdef readonly object value
    cpdef readonly type type


cdef Expression coerce_expression(object expr)
