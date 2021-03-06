import cython


cdef class Visitor:
    cpdef visit(self, Expression expr)


cdef class Expression:
    cpdef visit(self, Visitor visitor)
    cpdef asc(self)
    cpdef desc(self)
    cpdef cast(self, str to)
    cpdef alias(self, str alias)

    cdef BinaryExpression _new_binary_expr(self, object other, object op)


cdef class BinaryExpression(Expression):
    cpdef readonly Expression left
    cpdef readonly Expression right
    cpdef readonly object op
    cpdef readonly bint negated


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
    cpdef readonly Expression expr
    cpdef readonly bint is_asc


cdef class CallExpression(Expression):
    cpdef readonly Expression callable
    cpdef readonly tuple args


cdef class RawExpression(Expression):
    cpdef readonly str expr


cdef class PathExpression(Expression):
    cpdef readonly list _path_


cdef class VirtualExpressionVal(Expression):
    cpdef readonly object _virtual_
    cpdef readonly object _source_

    cpdef Expression _create_expr_(self, object q)


cdef class VirtualExpressionBinary(BinaryExpression):
    cpdef Expression _create_expr_(self, object q)


cdef class VirtualExpressionDir(Expression):
    cpdef readonly VirtualExpressionVal expr
    cpdef readonly object op

    cpdef Expression _create_expr_(self, object q)


# cdef class GetAttrExprisson(Expression):
#     cdef readonly Expression obj
#     cdef readonly object path


# cdef class GetItemExprisson(Expression):
#     cdef readonly Expression obj
#     cdef object index


cpdef direction(Expression expr, str dir)
cpdef raw(str expr)

