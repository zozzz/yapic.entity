import cython


@cython.auto_pickle(False)
cdef class Visitor:
    cpdef visit(self, Expression expr)


@cython.auto_pickle(False)
cdef class Expression:
    cpdef visit(self, Visitor visitor)


@cython.auto_pickle(False)
cdef class BinaryExpression(Expression):
    cpdef readonly Expression left
    cpdef readonly Expression right
    cpdef readonly object op


@cython.auto_pickle(False)
cdef class UnaryExpression(Expression):
    cpdef readonly Expression expr
    cpdef readonly object op


@cython.auto_pickle(False)
cdef class ConstExpression(Expression):
    cpdef readonly object value
    cpdef readonly type type


cdef Expression coerce_expression(object expr)
