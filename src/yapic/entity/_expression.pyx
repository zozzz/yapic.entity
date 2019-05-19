import operator
import cython
from cpython.object cimport PyObject
from cpython.tuple cimport PyTuple_New, PyTuple_GET_ITEM, PyTuple_SET_ITEM, PyTuple_GET_SIZE


cdef extern from "Python.h":
    int _Py_HashPointer(void* ptr)


cdef class Expression:
    cpdef visit(self, Visitor visitor):
        raise NotImplementedError("%s::visit", type(self))

    def __hash__(self): return _Py_HashPointer(<PyObject*>(<object>self))
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
    def __invert__(self):
        if isinstance(self, BinaryExpression):
            op = (<BinaryExpression>self).op
            if op is operator.__lt__:
                op = operator.__ge__
            elif op is operator.__le__:
                op = operator.__gt__
            elif op is operator.__eq__:
                op = operator.__ne__
            elif op is operator.__ne__:
                op = operator.__eq__
            elif op is operator.__ge__:
                op = operator.__lt__
            elif op is operator.__gt__:
                op = operator.__le__
            else:
                return UnaryExpression(self, operator.__invert__)
            return BinaryExpression((<BinaryExpression>self).left, (<BinaryExpression>self).right, op)
        else:
            return UnaryExpression(self, operator.__invert__)

    def __lshift__(self, other): return BinaryExpression(self, other, operator.__lshift__)
    def __rshift__(self, other): return BinaryExpression(self, other, operator.__rshift__)
    def __mod__(self, other): return BinaryExpression(self, other, operator.__mod__)
    def __mul__(self, other): return BinaryExpression(self, other, operator.__mul__)
    def __truediv__(self, other): return BinaryExpression(self, other, operator.__truediv__)
    def __neg__(self): return UnaryExpression(self, operator.__neg__)
    def __pos__(self): return UnaryExpression(self, operator.__pos__)
    def __pow__(self, other, modulo): return BinaryExpression(self, other, operator.__pow__)
    def __abs__(self): return UnaryExpression(self, operator.__abs__)
    def __call__(self, *args): return CallExpression(self, args)
    def in_(self, *values):
        if len(values) == 1 and (isinstance(values[0], list) or isinstance(values[0], tuple)):
            values = values[0]
        return BinaryExpression(self, values, in_)
    def is_true(self): return self == True
    def is_false(self): return self == False
    def is_null(self): return self == None
    def is_none(self): return self == None

    cpdef asc(self): return DirectionExpression(self, True)
    cpdef desc(self): return DirectionExpression(self, False)
    cpdef cast(self, str to): return CastExpression(self, to)
    cpdef alias(self, str alias): return AliasExpression(self, alias)

    def __repr__(self):
        return "<Expr EMPTY>"


cdef class BinaryExpression(Expression):
    def __cinit__(self, left, right, op):
        self.left = coerce_expression(left)
        self.right = coerce_expression(right)
        self.op = op

    def __repr__(self):
        return "<Expr %r %r %r>" % (self.left, self.op, self.right)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_binary(self)


cdef class UnaryExpression(Expression):
    def __cinit__(self, expr, op):
        self.expr = coerce_expression(expr)
        self.op = op

    def __repr__(self):
        return "<Expr %r%r>" % (self.op, self.expr)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_unary(self)


cdef class CastExpression(Expression):
    def __cinit__(self, expr, to):
        self.expr = coerce_expression(expr)
        self.to = to

    def __repr__(self):
        return "<Cast %r to %r>" % (self.expr, self.to)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_cast(self)

    cpdef cast(self, str to):
        return CastExpression(self.expr, to)


cdef class ConstExpression(Expression):
    def __cinit__(self, object value, type type):
        self.value = value
        self.type = type

    def __repr__(self):
        return "<Expr %s typeof %s>" % (self.value, self.type.__name__)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_const(self)

    cpdef alias(self, str alias):
        return self


cdef class DirectionExpression(Expression):
    def __cinit__(self, Expression expr, bint is_asc):
        self.expr = expr
        self.is_asc = is_asc

    def __repr__(self):
        return "<Direction %s %s>" % (self.expr, "ASC" if self.is_asc else "DESC")

    cpdef visit(self, Visitor visitor):
        return visitor.visit_direction(self)

    cpdef asc(self):
        return DirectionExpression(self.expr, True)

    cpdef desc(self):
        return DirectionExpression(self.expr, False)


cdef class CallExpression(Expression):
    def __cinit__(self, Expression callable, args):
        self.callable = callable
        self.args = tuple(map(coerce_expression, args))

    def __repr__(self):
        if self.args:
            return "<Call %s( %s )>" % (self.callable, self.args)
        else:
            return "<Call %s()>" % (self.callable)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_call(self)


cdef class RawExpression(Expression):
    def __cinit__(self, str expr):
        self.expr = expr

    def __repr__(self):
        return "<RAW %r>" % (self.expr)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_raw(self)


cdef class AliasExpression(Expression):
    def __cinit__(self, Expression expr, str alias):
        self.expr = expr
        self.value = alias

    def __repr__(self):
        return "<Alias %s AS %s>" % (self.expr, self.value)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_alias(self)

    cpdef alias(self, str alias):
        return AliasExpression(self.expr, alias)


cdef class PathExpression(Expression):
    def __cinit__(self, Expression primary, list path):
        self._primary_ = primary
        self._path_ = path

    # @cython.wraparound(True)
    def __getattr__(self, object key):
        last_item = self._path_[len(self._path_) - 1]
        new_path = list(self._path_)
        if isinstance(last_item, Expression):
            obj = getattr(last_item, key)
            if isinstance(obj, PathExpression) and last_item is (<PathExpression>obj)._primary_:
                new_path.extend((<PathExpression>obj)._path_)
            else:
                new_path.append(obj)
        else:
            new_path.append(key)
        return PathExpression(self._primary_, new_path)

    # @cython.wraparound(True)
    def __getitem__(self, object key):
        last_item = self._path_[len(self._path_) - 1]
        new_path = list(self._path_)
        if isinstance(last_item, Expression):
            obj = last_item[key]
            if isinstance(obj, PathExpression) and last_item is (<PathExpression>obj)._primary_:
                new_path.extend((<PathExpression>obj)._path_)
            else:
                new_path.append(obj)
        else:
            new_path.append(key)
        return PathExpression(self._primary_, new_path)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_path(self)

    def __repr__(self):
        return "<Path %s %r>" % (self._primary_, self._path_)


# cdef class GetAttrExprisson(Expression):
#     def __cinit__(self, Expression obj, object name):
#         self.obj = obj
#         self.name = name

#     def __getattr__(self, name):
#         if isinstance(self.name, str):
#             return GetAttrExprisson(self, name)
#         else:
#             return getattr(self.name, name)

#     cpdef visit(self, Visitor visitor):
#         return visitor.visit_getattr(self)


# cdef class GetItemExprisson(Expression):
#     def __cinit__(self, Expression obj, object index):
#         self.obj = obj
#         self.index = index

#     cpdef visit(self, Visitor visitor):
#         return visitor.visit_getitem(self)


cdef class Visitor:
    cpdef visit(self, Expression expr):
        return expr.visit(self)

    def visit_binary(self, binary):
        if binary.op is operator.__or__:
            fn_name = "visit_binary_or"
        elif binary.op is operator.__and__:
            fn_name = "visit_binary_and"
        elif binary.op is in_:
            fn_name = "visit_binary_in"
        else:
            fn_name = f"visit_binary_{binary.op.__name__}"
        return getattr(self, fn_name)(binary)

    def visit_unary(self, unary):
        fn_name = f"visit_unary_{unary.op.__name__}"
        return getattr(self, fn_name)(unary)


cdef Expression coerce_expression(object expr):
    if isinstance(expr, Expression):
        return <Expression>expr
    else:
        if isinstance(expr, list) or isinstance(expr, tuple):
            expr = tuple(coerce_expression(x) for x in expr)
        return ConstExpression(expr, type(expr))


def and_(*expr):
    if not expr:
        raise ValueError("Expression must not be empty")

    res = expr[0]

    for i in range(1, len(expr)):
        res &= expr[i]

    return res


def or_(*expr):
    if not expr:
        raise ValueError("Expression must not be empty")

    res = expr[0]

    for i in range(1, len(expr)):
        res |= expr[i]

    return res


def in_(expr, value):
    return expr.in_(value)

cpdef direction(Expression expr, str dir):
    return DirectionExpression(expr, str(dir).lower() == "asc")

cpdef raw(str expr):
    return RawExpression(expr)


cdef class RawIdFactory:
    def __getattribute__(self, name):
        return RawExpression(name)


func = RawIdFactory()
const = RawIdFactory()
