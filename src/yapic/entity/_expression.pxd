import cython


cdef class Visitor:
    cpdef visit(self, Expression expr)


cdef class Expression:
    cpdef visit(self, Visitor visitor)
    cpdef asc(self)
    cpdef desc(self)
    cpdef cast(self, str to)
    cpdef alias(self, str alias)


cdef class BinaryExpression(Expression):
    cpdef readonly Expression left
    cpdef readonly Expression right
    cpdef readonly object op


cdef class UnaryExpression(Expression):
    cpdef readonly Expression expr
    cpdef readonly object op


cdef class CastExpression(Expression):
    cpdef readonly Expression expr
    cpdef readonly object to


cdef class ConstExpression(Expression):
    cpdef readonly object value
    cpdef readonly type type


cdef class AliasExpression(Expression):
    cpdef readonly Expression expr
    cpdef readonly str value


cdef Expression coerce_expression(object expr)


cdef class DirectionExpression(Expression):
    cdef readonly Expression expr
    cdef readonly bint is_asc


cpdef direction(Expression expr, str dir)
# cpdef alias(object obj, str alias)

