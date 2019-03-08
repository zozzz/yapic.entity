import operator
import cython


@cython.auto_pickle(False)
cdef class Expression:
    cpdef visit(self, Visitor visitor):
        raise NotImplementedError("%s::visit", type(self))

    def __lt__(self, other): return BinaryExpression(self, other, operator.__lt__)
    def __le__(self, other): return BinaryExpression(self, other, operator.__le__)
    def __eq__(self, other): return BinaryExpression(self, other, operator.__eq__)
    def __ne__(self, other): return BinaryExpression(self, other, operator.__ne__)
    def __ge__(self, other): return BinaryExpression(self, other, operator.__ge__)
    def __gt__(self, other): return BinaryExpression(self, other, operator.__gt__)

    def __add__(self, other): return BinaryExpression(self, other, operator.__add__)
    def __sub__(self, other): return BinaryExpression(self, other, operator.__sub__)
    def __and__(self, other): return BinaryExpression(self, other, operator.__and__)
    def __or__(self, other): return BinaryExpression(self, other, operator.__or__)
    def __xor__(self, other): return BinaryExpression(self, other, operator.__xor__)
    def __invert__(self): return UnaryExpression(self, operator.__invert__)
    def __lshift__(self, other): return BinaryExpression(self, other, operator.__lshift__)
    def __rshift__(self, other): return BinaryExpression(self, other, operator.__rshift__)
    def __mod__(self, other): return BinaryExpression(self, other, operator.__mod__)
    def __mul__(self, other): return BinaryExpression(self, other, operator.__mul__)
    def __truediv__(self, other): return BinaryExpression(self, other, operator.__truediv__)
    def __neg__(self): return UnaryExpression(self, operator.__neg__)
    def __pos__(self): return UnaryExpression(self, operator.__pos__)
    def __pow__(self, other, modulo): return BinaryExpression(self, other, operator.__pow__)
    def in_(self, other): return BinaryExpression(self, other, operator.__contains__)

    def __repr__(self):
        return "<Expr EMPTY>"


@cython.auto_pickle(False)
cdef class BinaryExpression(Expression):
    def __cinit__(self, left, right, op):
        self.left = coerce_expression(left)
        self.right = coerce_expression(right)
        self.op = op

    def __repr__(self):
        return "<Expr %r %r %r>" % (self.left, self.op, self.right)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_binary(self)


@cython.auto_pickle(False)
cdef class UnaryExpression(Expression):
    def __cinit__(self, expr, op):
        self.expr = coerce_expression(expr)
        self.op = op

    def __repr__(self):
        return "<Expr %r%r>" % (self.op, self.expr)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_unary(self)


@cython.auto_pickle(False)
cdef class ConstExpression(Expression):
    def __cinit__(self, object value, type type):
        self.value = value
        self.type = type

    def __repr__(self):
        return "<Expr %s typeof %s>" % (self.value, self.type.__name__)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_const(self)


@cython.auto_pickle(False)
cdef class Visitor:
    cpdef visit(self, Expression expr):
        return expr.visit(self)

    def visit_binary(self, binary):
        if binary.op is operator.__or__:
            fn_name = "visit_binary_or"
        elif binary.op is operator.__and__:
            fn_name = "visit_binary_and"
        else:
            fn_name = "visit_binary_%s" % binary.op.__name__
        return getattr(self, fn_name)(binary)


cdef Expression coerce_expression(object expr):
    if isinstance(expr, Expression):
        return <Expression>expr
    else:
        return ConstExpression(expr, type(expr))
