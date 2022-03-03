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
    cdef readonly Expression left
    cdef readonly Expression right
    cdef readonly object op
    cdef readonly bint negated


cdef class UnaryExpression(Expression):
    cdef readonly Expression expr
    cdef readonly object op


cdef class CastExpression(Expression):
    cdef readonly Expression expr
    cdef readonly object to


cdef class ConstExpression(Expression):
    cdef readonly object value
    cdef readonly type type


cdef class AliasExpression(Expression):
    cdef readonly Expression expr
    cdef readonly str value

cdef class ColumnRefExpression(Expression):
    cdef readonly Expression expr
    cdef readonly int index


cdef Expression coerce_expression(object expr)


cdef class OrderExpression(Expression):
    cdef readonly Expression expr
    cdef readonly bint is_asc


cdef class CallExpression(Expression):
    cdef readonly Expression callable
    cdef readonly tuple args


cdef class RawExpression(Expression):
    cdef readonly str expr


cdef class PathExpression(Expression):
    cdef readonly list _path_


# cdef class VirtualExpressionVal(Expression):
#     cdef readonly object _virtual_
#     cdef readonly object _source_

#     cpdef Expression _create_expr_(self, object q)





ctypedef enum EPA:
    GETATTR = 1
    GETITEM = 2
    CALL = 3


@cython.final
cdef class ExpressionPlaceholder(Expression):
    cdef object origin
    cdef list path
    cdef str name

    cdef Expression eval(self, object origin)


@cython.final
cdef class MultiExpression(Expression):
    cdef readonly tuple expressions
    cdef readonly object combinator

# cdef class GetAttrExprisson(Expression):
#     cdef readonly Expression obj
#     cdef readonly object path


# cdef class GetItemExprisson(Expression):
#     cdef readonly Expression obj
#     cdef object index


cpdef direction(Expression expr, str dir)
cpdef raw(str expr)

